#include "macros.hh"
#include "array.writer.hh"
#include "zarr.common.hh"
#include "zarr.stream.hh"
#include "sink.creator.hh"

#include <cmath>
#include <functional>
#include <latch>
#include <stdexcept>

#ifdef min
#undef min
#endif

bool
zarr::downsample(const ArrayWriterConfig& config,
                 ArrayWriterConfig& downsampled_config)
{
    // downsample dimensions
    std::vector<ZarrDimension> downsampled_dims(config.dimensions->ndims());
    for (auto i = 0; i < config.dimensions->ndims(); ++i) {
        const auto& dim = config.dimensions->at(i);
        // don't downsample channels
        if (dim.type == ZarrDimensionType_Channel) {
            downsampled_dims[i] = dim;
        } else {
            const uint32_t array_size_px =
              (dim.array_size_px + (dim.array_size_px % 2)) / 2;

            const uint32_t chunk_size_px =
              dim.array_size_px == 0
                ? dim.chunk_size_px
                : std::min(dim.chunk_size_px, array_size_px);

            CHECK(chunk_size_px);
            const uint32_t n_chunks =
              (array_size_px + chunk_size_px - 1) / chunk_size_px;

            const uint32_t shard_size_chunks =
              dim.array_size_px == 0
                ? 1
                : std::min(n_chunks, dim.shard_size_chunks);

            downsampled_dims[i] = { dim.name,
                                    dim.type,
                                    array_size_px,
                                    chunk_size_px,
                                    shard_size_chunks };
        }
    }
    downsampled_config.dimensions = std::make_shared<ArrayDimensions>(
      std::move(downsampled_dims), config.dtype);

    downsampled_config.level_of_detail = config.level_of_detail + 1;
    downsampled_config.bucket_name = config.bucket_name;
    downsampled_config.store_path = config.store_path;

    downsampled_config.dtype = config.dtype;

    // copy the Blosc compression parameters
    downsampled_config.compression_params = config.compression_params;

    // can we downsample downsampled_config?
    for (auto i = 0; i < config.dimensions->ndims(); ++i) {
        // downsampling made the chunk size strictly smaller
        const auto& dim = config.dimensions->at(i);
        const auto& downsampled_dim = downsampled_config.dimensions->at(i);

        if (dim.chunk_size_px > downsampled_dim.chunk_size_px) {
            return false;
        }
    }

    return true;
}

/// Writer
zarr::ArrayWriter::ArrayWriter(const ArrayWriterConfig& config,
                               std::shared_ptr<ThreadPool> thread_pool)
  : ArrayWriter(std::move(config), thread_pool, nullptr)
{
}

zarr::ArrayWriter::ArrayWriter(
  const ArrayWriterConfig& config,
  std::shared_ptr<ThreadPool> thread_pool,
  std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : config_{ config }
  , thread_pool_{ thread_pool }
  , s3_connection_pool_{ s3_connection_pool }
  , bytes_to_flush_{ 0 }
  , frames_written_{ 0 }
  , append_chunk_index_{ 0 }
  , is_finalizing_{ false }
{
}

size_t
zarr::ArrayWriter::write_frame(std::span<const std::byte> data)
{
    const auto nbytes_data = data.size();
    const auto nbytes_frame =
      bytes_of_frame(*config_.dimensions, config_.dtype);

    if (nbytes_frame != nbytes_data) {
        LOG_ERROR("Frame size mismatch: expected ",
                  nbytes_frame,
                  ", got ",
                  nbytes_data,
                  ". Skipping");
        return 0;
    }

    if (chunk_buffers_.empty()) {
        make_buffers_();
    }

    // split the incoming frame into tiles and write them to the chunk
    // buffers
    const auto bytes_written = write_frame_to_chunks_(data);
    EXPECT(bytes_written == nbytes_data, "Failed to write frame to chunks");

    LOG_DEBUG("Wrote ", bytes_written, " bytes of frame ", frames_written_);
    bytes_to_flush_ += bytes_written;
    ++frames_written_;

    if (should_flush_()) {
        flush_();
    }

    return bytes_written;
}

bool
zarr::ArrayWriter::is_s3_array_() const
{
    return config_.bucket_name.has_value();
}

bool
zarr::ArrayWriter::make_data_sinks_()
{
    std::string data_root;
    std::function<size_t(const ZarrDimension&)> parts_along_dimension;
    switch (version_()) {
        case ZarrVersion_2:
            parts_along_dimension = chunks_along_dimension;
            data_root = config_.store_path + "/" +
                        std::to_string(config_.level_of_detail) + "/" +
                        std::to_string(append_chunk_index_);
            break;
        case ZarrVersion_3:
            parts_along_dimension = shards_along_dimension;
            data_root = config_.store_path + "/data/root/" +
                        std::to_string(config_.level_of_detail) + "/c" +
                        std::to_string(append_chunk_index_);
            break;
        default:
            LOG_ERROR("Unsupported Zarr version");
            return false;
    }

    SinkCreator creator(thread_pool_, s3_connection_pool_);

    if (is_s3_array_()) {
        if (!creator.make_data_sinks(*config_.bucket_name,
                                     data_root,
                                     config_.dimensions.get(),
                                     parts_along_dimension,
                                     data_sinks_)) {
            LOG_ERROR("Failed to create data sinks in ",
                      data_root,
                      " for bucket ",
                      *config_.bucket_name);
            return false;
        }
    } else if (!creator.make_data_sinks(data_root,
                                        config_.dimensions.get(),
                                        parts_along_dimension,
                                        data_sinks_)) {
        LOG_ERROR("Failed to create data sinks in ", data_root);
        return false;
    }

    return true;
}

bool
zarr::ArrayWriter::make_metadata_sink_()
{
    if (metadata_sink_) {
        LOG_INFO("Metadata sink already exists");
        return true;
    }

    std::string metadata_path;
    switch (version_()) {
        case ZarrVersion_2:
            metadata_path = config_.store_path + "/" +
                            std::to_string(config_.level_of_detail) +
                            "/.zarray";
            break;
        case ZarrVersion_3:
            metadata_path = config_.store_path + "/meta/root/" +
                            std::to_string(config_.level_of_detail) +
                            ".array.json";
            break;
        default:
            LOG_ERROR("Unsupported Zarr version");
            return false;
    }

    if (is_s3_array_()) {
        SinkCreator creator(thread_pool_, s3_connection_pool_);
        metadata_sink_ =
          creator.make_sink(*config_.bucket_name, metadata_path);
    } else {
        metadata_sink_ = zarr::SinkCreator::make_sink(metadata_path);
    }

    if (!metadata_sink_) {
        LOG_ERROR("Failed to create metadata sink: ", metadata_path);
        return false;
    }

    return true;
}

void
zarr::ArrayWriter::make_buffers_() noexcept
{
    LOG_DEBUG("Creating chunk buffers");

    const size_t n_chunks = config_.dimensions->number_of_chunks_in_memory();
    chunk_buffers_.resize(n_chunks); // no-op if already the correct size

    const auto nbytes = config_.dimensions->bytes_per_chunk();

    for (auto& buf : chunk_buffers_) {
        buf.resize(nbytes);
        std::fill(buf.begin(), buf.end(), std::byte(0));
    }
}

size_t
zarr::ArrayWriter::write_frame_to_chunks_(std::span<const std::byte> data)
{
    // break the frame into tiles and write them to the chunk buffers
    const auto bytes_per_px = bytes_of_type(config_.dtype);

    const auto& dimensions = config_.dimensions;

    const auto& x_dim = dimensions->width_dim();
    const auto frame_cols = x_dim.array_size_px;
    const auto tile_cols = x_dim.chunk_size_px;

    const auto& y_dim = dimensions->height_dim();
    const auto frame_rows = y_dim.array_size_px;
    const auto tile_rows = y_dim.chunk_size_px;

    if (tile_cols == 0 || tile_rows == 0) {
        return 0;
    }

    const auto bytes_per_row = tile_cols * bytes_per_px;

    size_t bytes_written = 0;

    const auto n_tiles_x = (frame_cols + tile_cols - 1) / tile_cols;
    const auto n_tiles_y = (frame_rows + tile_rows - 1) / tile_rows;

    // don't take the frame id from the incoming frame, as the camera may have
    // dropped frames
    const auto frame_id = frames_written_;

    // offset among the chunks in the lattice
    const auto group_offset = dimensions->tile_group_offset(frame_id);
    // offset within the chunk
    const auto chunk_offset =
      static_cast<long long>(dimensions->chunk_internal_offset(frame_id));

    for (auto i = 0; i < n_tiles_y; ++i) {
        // TODO (aliddell): we can optimize this when tiles_per_frame_x_ is 1
        for (auto j = 0; j < n_tiles_x; ++j) {
            const auto c = group_offset + i * n_tiles_x + j;
            auto& chunk = chunk_buffers_[c];
            auto chunk_it = chunk.begin() + chunk_offset;

            for (auto k = 0; k < tile_rows; ++k) {
                const auto frame_row = i * tile_rows + k;
                if (frame_row < frame_rows) {
                    const auto frame_col = j * tile_cols;

                    const auto region_width =
                      std::min(frame_col + tile_cols, frame_cols) - frame_col;

                    const auto region_start = static_cast<long long>(
                      bytes_per_px * (frame_row * frame_cols + frame_col));
                    const auto nbytes =
                      static_cast<long long>(region_width * bytes_per_px);
                    const auto region_stop = region_start + nbytes;
                    if (region_stop > data.size()) {
                        LOG_ERROR("Buffer overflow");
                        return bytes_written;
                    }

                    // copy region
                    if (nbytes > std::distance(chunk_it, chunk.end())) {
                        LOG_ERROR("Buffer overflow");
                        return bytes_written;
                    }
                    std::copy(data.begin() + region_start,
                              data.begin() + region_stop,
                              chunk_it);

                    bytes_written += (region_stop - region_start);
                }
                chunk_it += static_cast<long long>(bytes_per_row);
            }
        }
    }

    return bytes_written;
}

bool
zarr::ArrayWriter::should_flush_() const
{
    const auto& dims = config_.dimensions;
    size_t frames_before_flush = dims->final_dim().chunk_size_px;
    for (auto i = 1; i < dims->ndims() - 2; ++i) {
        frames_before_flush *= dims->at(i).array_size_px;
    }

    CHECK(frames_before_flush > 0);
    return frames_written_ % frames_before_flush == 0;
}

void
zarr::ArrayWriter::compress_buffers_()
{
    if (!config_.compression_params.has_value()) {
        return;
    }

    LOG_DEBUG("Compressing");

    BloscCompressionParams params = config_.compression_params.value();
    const auto bytes_per_px = bytes_of_type(config_.dtype);

    std::scoped_lock lock(buffers_mutex_);
    std::latch latch(chunk_buffers_.size());
    for (auto& chunk : chunk_buffers_) {
        EXPECT(thread_pool_->push_job(
                 [&params, buf = &chunk, bytes_per_px, &latch](
                   std::string& err) -> bool {
                     bool success = false;
                     const size_t bytes_of_chunk = buf->size();

                     try {
                         const auto tmp_size =
                           bytes_of_chunk + BLOSC_MAX_OVERHEAD;
                         std::vector<std::byte> tmp(tmp_size);
                         const auto nb =
                           blosc_compress_ctx(params.clevel,
                                              params.shuffle,
                                              bytes_per_px,
                                              bytes_of_chunk,
                                              buf->data(),
                                              tmp.data(),
                                              tmp_size,
                                              params.codec_id.c_str(),
                                              0 /* blocksize - 0:automatic */,
                                              1);

                         tmp.resize(nb);
                         buf->swap(tmp);

                         success = true;
                     } catch (const std::exception& exc) {
                         err = "Failed to compress chunk: " +
                               std::string(exc.what());
                     }
                     latch.count_down();

                     return success;
                 }),
               "Failed to push to job queue");
    }

    // wait for all threads to finish
    latch.wait();
}

void
zarr::ArrayWriter::flush_()
{
    if (bytes_to_flush_ == 0) {
        return;
    }

    // compress buffers and write out
    compress_buffers_();
    CHECK(flush_impl_());

    const auto should_rollover = should_rollover_();
    if (should_rollover) {
        rollover_();
    }

    if (should_rollover || is_finalizing_) {
        CHECK(write_array_metadata_());
    }

    // reset buffers
    make_buffers_();

    // reset state
    bytes_to_flush_ = 0;
}

void
zarr::ArrayWriter::close_sinks_()
{
    for (auto i = 0; i < data_sinks_.size(); ++i) {
        EXPECT(finalize_sink(std::move(data_sinks_[i])),
               "Failed to finalize sink ",
               i);
    }
    data_sinks_.clear();
}

void
zarr::ArrayWriter::rollover_()
{
    LOG_DEBUG("Rolling over");

    close_sinks_();
    ++append_chunk_index_;
}

bool
zarr::finalize_array(std::unique_ptr<ArrayWriter>&& writer)
{
    if (writer == nullptr) {
        LOG_INFO("Array writer is null. Nothing to finalize.");
        return true;
    }

    writer->is_finalizing_ = true;
    try {
        writer->flush_(); // data sinks finalized here
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to finalize array writer: ", exc.what());
        return false;
    }

    if (!finalize_sink(std::move(writer->metadata_sink_))) {
        LOG_ERROR("Failed to finalize metadata sink");
        return false;
    }

    writer.reset();
    return true;
}

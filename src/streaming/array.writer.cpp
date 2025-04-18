#include "macros.hh"
#include "array.writer.hh"
#include "zarr.common.hh"
#include "zarr.stream.hh"
#include "sink.hh"

#include <cmath>
#include <functional>
#include <stdexcept>

#if defined(min) || defined(max)
#undef min
#undef max
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

    if (data_buffers_.empty()) {
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
        CHECK(compress_and_flush_data_());

        if (should_rollover_()) {
            rollover_();
            CHECK(write_array_metadata_());
        }

        make_buffers_();
        bytes_to_flush_ = 0;
    }

    return bytes_written;
}

size_t
zarr::ArrayWriter::bytes_to_allocate_per_chunk_() const
{
    size_t bytes_per_chunk = config_.dimensions->bytes_per_chunk();
    if (config_.compression_params) {
        bytes_per_chunk += BLOSC_MAX_OVERHEAD;
    }

    return bytes_per_chunk;
}

bool
zarr::ArrayWriter::is_s3_array_() const
{
    return config_.bucket_name.has_value();
}

void
zarr::ArrayWriter::make_data_paths_()
{
    if (data_paths_.empty()) {
        data_paths_ = construct_data_paths(
          data_root_(), *config_.dimensions, parts_along_dimension_());
    }
}

bool
zarr::ArrayWriter::make_metadata_sink_()
{
    if (metadata_sink_) {
        return true;
    }

    const auto metadata_path = metadata_path_();
    metadata_sink_ =
      is_s3_array_()
        ? make_s3_sink(*config_.bucket_name, metadata_path, s3_connection_pool_)
        : make_file_sink(metadata_path);

    if (!metadata_sink_) {
        LOG_ERROR("Failed to create metadata sink: ", metadata_path);
        return false;
    }

    return true;
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

    const auto bytes_per_chunk = dimensions->bytes_per_chunk();
    const auto bytes_per_row = tile_cols * bytes_per_px;

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

    const auto* data_ptr = data.data();
    const auto data_size = data.size();

    size_t bytes_written = 0;
    const auto n_tiles = n_tiles_x * n_tiles_y;

    // Using the entire thread pool breaks in CI due to a likely resource
    // contention. Using 75% of the thread pool should be enough to avoid, but
    // we should still find a fix if we can.
#pragma omp parallel for reduction(+ : bytes_written)                          \
  num_threads(std::max(3 * thread_pool_->n_threads() / 4, 1u))
    for (auto tile = 0; tile < n_tiles; ++tile) {
        const auto tile_idx_y = tile / n_tiles_x;
        const auto tile_idx_x = tile % n_tiles_x;

        const auto chunk_idx =
          group_offset + tile_idx_y * n_tiles_x + tile_idx_x;
        const auto chunk_start = get_chunk_data_(chunk_idx);
        auto chunk_pos = chunk_offset;

        for (auto k = 0; k < tile_rows; ++k) {
            const auto frame_row = tile_idx_y * tile_rows + k;
            if (frame_row < frame_rows) {
                const auto frame_col = tile_idx_x * tile_cols;

                const auto region_width =
                  std::min(frame_col + tile_cols, frame_cols) - frame_col;

                const auto region_start =
                  bytes_per_px * (frame_row * frame_cols + frame_col);
                const auto nbytes = region_width * bytes_per_px;

                // copy region
                EXPECT(region_start + nbytes <= data_size,
                       "Buffer overflow in framme. Region start: ",
                       region_start,
                       " nbytes: ",
                       nbytes,
                       " data size: ",
                       data_size);
                EXPECT(chunk_pos + nbytes <= bytes_per_chunk,
                       "Buffer overflow in chunk. Chunk pos: ",
                       chunk_pos,
                       " nbytes: ",
                       nbytes,
                       " bytes per chunk: ",
                       bytes_per_chunk);
                memcpy(
                  chunk_start + chunk_pos, data_ptr + region_start, nbytes);
                bytes_written += nbytes;
            }
            chunk_pos += bytes_per_row;
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
        if (writer->bytes_to_flush_ > 0) {
            CHECK(writer->compress_and_flush_data_());
        }
        if (writer->frames_written_ > 0) {
            CHECK(writer->write_array_metadata_());
        }
        writer->close_sinks_();
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

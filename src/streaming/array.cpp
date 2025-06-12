#include "array.hh"
#include "macros.hh"
#include "sink.hh"
#include "zarr.common.hh"
#include "zarr.stream.hh"

#include <omp.h>

#include <cmath>
#include <functional>
#include <stdexcept>

namespace {
#ifdef __APPLE__
const int omp_threads = omp_get_max_threads(); // Full parallelization on macOS
#else
const int omp_threads = omp_get_max_threads() <= 4 ? 1 : omp_get_max_threads();
#endif
} // namespace

zarr::Array::Array(std::shared_ptr<ArrayConfig> config,
                   std::shared_ptr<ThreadPool> thread_pool,
                   std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : ZarrNode(config, thread_pool, s3_connection_pool)
  , bytes_to_flush_{ 0 }
  , frames_written_{ 0 }
  , append_chunk_index_{ 0 }
  , is_closing_{ false }
{
    // check that the config is actually an ArrayConfig
    CHECK(std::dynamic_pointer_cast<ArrayConfig>(config_));
}

size_t
zarr::Array::write_frame(ConstByteSpan data)
{
    const auto nbytes_data = data.size();
    const auto nbytes_frame =
      bytes_of_frame(*config_->dimensions, config_->dtype);

    if (nbytes_frame != nbytes_data) {
        LOG_ERROR("Frame size mismatch: expected ",
                  nbytes_frame,
                  ", got ",
                  nbytes_data,
                  ". Skipping");
        return 0;
    }

    if (data_buffers_.empty()) {
        std::unique_lock lock(buffers_mutex_);
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
        std::unique_lock lock(buffers_mutex_);
        CHECK(compress_and_flush_data_());

        if (should_rollover_()) {
            rollover_();
            CHECK(write_metadata_());
        }

        make_buffers_();
        bytes_to_flush_ = 0;
    }

    return bytes_written;
}

bool
zarr::Array::close_()
{
    bool retval = false;
    is_closing_ = true;
    try {
        if (bytes_to_flush_ > 0) {
            std::unique_lock lock(buffers_mutex_);
            CHECK(compress_and_flush_data_());
        }
        close_sinks_();

        if (frames_written_ > 0) {
            CHECK(write_metadata_());
            for (auto& [key, sink] : metadata_sinks_) {
                EXPECT(zarr::finalize_sink(std::move(sink)),
                       "Failed to finalize metadata sink ",
                       key);
            }
        }
        metadata_sinks_.clear();
        retval = true;
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to finalize array writer: ", exc.what());
    }

    is_closing_ = false;
    return retval;
}

std::shared_ptr<zarr::ArrayConfig>
zarr::Array::array_config_() const
{
    return std::dynamic_pointer_cast<ArrayConfig>(config_);
}

size_t
zarr::Array::bytes_to_allocate_per_chunk_() const
{
    size_t bytes_per_chunk = config_->dimensions->bytes_per_chunk();
    if (config_->compression_params) {
        bytes_per_chunk += BLOSC_MAX_OVERHEAD;
    }

    return bytes_per_chunk;
}

bool
zarr::Array::is_s3_array_() const
{
    return config_->bucket_name.has_value();
}

void
zarr::Array::make_data_paths_()
{
    if (data_paths_.empty()) {
        data_paths_ = construct_data_paths(
          data_root_(), *config_->dimensions, parts_along_dimension_());
    }
}

size_t
zarr::Array::write_frame_to_chunks_(std::span<const std::byte> data)
{
    std::unique_lock lock(buffers_mutex_);

    // break the frame into tiles and write them to the chunk buffers
    const auto bytes_per_px = bytes_of_type(config_->dtype);

    const auto& dimensions = config_->dimensions;

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

    std::vector<BytePtr> chunk_data(n_tiles);
    for (auto i = 0; i < n_tiles; ++i) {
        chunk_data[i] = get_chunk_data_(group_offset + i);
    }

#pragma omp parallel for num_threads(omp_threads) reduction(+ : bytes_written)
    for (auto tile = 0; tile < n_tiles; ++tile) {
        const auto tile_idx_y = tile / n_tiles_x;
        const auto tile_idx_x = tile % n_tiles_x;

        const auto chunk_start = chunk_data[tile];

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
zarr::Array::should_flush_() const
{
    const auto& dims = config_->dimensions;
    size_t frames_before_flush = dims->final_dim().chunk_size_px;
    for (auto i = 1; i < dims->ndims() - 2; ++i) {
        frames_before_flush *= dims->at(i).array_size_px;
    }

    CHECK(frames_before_flush > 0);
    return frames_written_ % frames_before_flush == 0;
}

void
zarr::Array::rollover_()
{
    LOG_DEBUG("Rolling over");

    close_sinks_();
    ++append_chunk_index_;
}

bool
zarr::finalize_array(std::unique_ptr<Array>&& array)
{
    if (array == nullptr) {
        LOG_INFO("Array writer is null. Nothing to finalize.");
        return true;
    }

    try {
        if (!array->close_()) {
            return false;
        }
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to close_ array: ", exc.what());
        return false;
    }

    array.reset();
    return true;
}

#include "array.hh"
#include "macros.hh"
#include "sink.hh"
#include "zarr.common.hh"

#include <cstring>
#include <functional>
#include <stdexcept>

zarr::Array::Array(std::shared_ptr<ArrayConfig> config,
                   std::shared_ptr<ThreadPool> thread_pool,
                   std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : ArrayBase(config, thread_pool, s3_connection_pool)
  , bytes_to_flush_{ 0 }
  , frames_written_{ 0 }
  , append_chunk_index_{ 0 }
  , is_closing_{ false }
{
    const size_t n_chunks = config_->dimensions->number_of_chunks_in_memory();
    EXPECT(n_chunks > 0, "Array has zero chunks in memory");
    chunk_buffers_ = std::vector<LockedBuffer>(n_chunks);
}

size_t
zarr::Array::write_frame(LockedBuffer& data)
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

    if (bytes_to_flush_ == 0) { // first frame, we need to init the buffers
        fill_buffers_();
    }

    // split the incoming frame into tiles and write them to the chunk
    // buffers
    const auto bytes_written = write_frame_to_chunks_(data);
    EXPECT(bytes_written == nbytes_data, "Failed to write frame to chunks");

    LOG_DEBUG("Wrote ",
              bytes_written,
              " bytes of frame ",
              frames_written_,
              " to LOD ",
              config_->level_of_detail);
    bytes_to_flush_ += bytes_written;
    ++frames_written_;

    if (should_flush_()) {
        CHECK(compress_and_flush_data_());

        if (should_rollover_()) {
            rollover_();
            CHECK(write_metadata_());
        }
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

void
zarr::Array::fill_buffers_()
{
    LOG_DEBUG("Filling chunk buffers");

    const auto n_bytes = config_->dimensions->bytes_per_chunk();

    for (auto& buf : chunk_buffers_) {
        buf.resize_and_fill(n_bytes, 0);
    }
}

size_t
zarr::Array::write_frame_to_chunks_(LockedBuffer& data)
{
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

    size_t bytes_written = 0;
    const auto n_tiles = n_tiles_x * n_tiles_y;

    auto frame = data.take();

#pragma omp parallel for reduction(+ : bytes_written)
    for (auto tile = 0; tile < n_tiles; ++tile) {
        auto& chunk_buffer = chunk_buffers_[tile + group_offset];
        bytes_written += chunk_buffer.with_lock([chunk_offset,
                                                 frame_rows,
                                                 frame_cols,
                                                 tile_rows,
                                                 tile_cols,
                                                 tile,
                                                 n_tiles_x,
                                                 bytes_per_px,
                                                 bytes_per_row,
                                                 bytes_per_chunk,
                                                 &frame](auto& chunk_data) {
            const auto* data_ptr = frame.data();
            const auto data_size = frame.size();

            const auto chunk_start = chunk_data.data();

            const auto tile_idx_y = tile / n_tiles_x;
            const auto tile_idx_x = tile % n_tiles_x;

            auto chunk_pos = chunk_offset;
            size_t bytes_written = 0;

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

            return bytes_written;
        });
    }

    data.assign(std::move(frame));

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

#include "array.hh"
#include "macros.hh"
#include "sink.hh"
#include "zarr.common.hh"

#include <nlohmann/json.hpp>
#include <crc32c/crc32c.h>

#include <algorithm> // std::fill
#include <cstring>
#include <functional>
#include <future>
#include <stdexcept>

using json = nlohmann::json;

namespace {
std::string
sample_type_to_dtype(ZarrDataType t)
{
    switch (t) {
        case ZarrDataType_uint8:
            return "uint8";
        case ZarrDataType_uint16:
            return "uint16";
        case ZarrDataType_uint32:
            return "uint32";
        case ZarrDataType_uint64:
            return "uint64";
        case ZarrDataType_int8:
            return "int8";
        case ZarrDataType_int16:
            return "int16";
        case ZarrDataType_int32:
            return "int32";
        case ZarrDataType_int64:
            return "int64";
        case ZarrDataType_float32:
            return "float32";
        case ZarrDataType_float64:
            return "float64";
        default:
            throw std::runtime_error("Invalid ZarrDataType: " +
                                     std::to_string(static_cast<int>(t)));
    }
}

std::string
shuffle_to_string(uint8_t shuffle)
{
    switch (shuffle) {
        case 0:
            return "noshuffle";
        case 1:
            return "shuffle";
        case 2:
            return "bitshuffle";
        default:
            throw std::runtime_error("Invalid shuffle value: " +
                                     std::to_string(shuffle));
    }
}
} // namespace

zarr::Array::Array(std::shared_ptr<ArrayConfig> config,
                   std::shared_ptr<ThreadPool> thread_pool,
                   std::shared_ptr<FileHandlePool> file_handle_pool,
                   std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : ArrayBase(config, thread_pool, file_handle_pool, s3_connection_pool)
  , bytes_to_flush_{ 0 }
  , frames_written_{ 0 }
  , append_chunk_index_{ 0 }
  , current_layer_{ 0 }
  , is_closing_{ false }
{
    const size_t n_chunks = config_->dimensions->number_of_chunks_in_memory();
    EXPECT(n_chunks > 0, "Array has zero chunks in memory");
    chunk_buffers_ = std::vector<LockedBuffer>(n_chunks);

    const auto& dims = config_->dimensions;
    const auto number_of_shards = dims->number_of_shards();
    const auto chunks_per_shard = dims->chunks_per_shard();

    shard_file_offsets_.resize(number_of_shards, 0);
    shard_tables_.resize(number_of_shards);

    for (auto& table : shard_tables_) {
        table.resize(2 * chunks_per_shard);
        std::ranges::fill(table, std::numeric_limits<uint64_t>::max());
    }

    data_root_ = node_path_() + "/c/" + std::to_string(append_chunk_index_);
}

size_t
zarr::Array::memory_usage() const noexcept
{
    size_t total = 0;
    for (const auto& buf : chunk_buffers_) {
        total += buf.size();
    }

    return total;
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

std::vector<std::string>
zarr::Array::metadata_keys_() const
{
    return { "zarr.json" };
}

bool
zarr::Array::make_metadata_()
{
    metadata_strings_.clear();

    std::vector<size_t> array_shape, chunk_shape, shard_shape;
    const auto& dims = config_->dimensions;

    size_t append_size = frames_written_;
    for (auto i = dims->ndims() - 3; i > 0; --i) {
        const auto& dim = dims->at(i);
        const auto& array_size_px = dim.array_size_px;
        CHECK(array_size_px);
        append_size = (append_size + array_size_px - 1) / array_size_px;
    }
    array_shape.push_back(append_size);

    const auto& final_dim = dims->final_dim();
    chunk_shape.push_back(final_dim.chunk_size_px);
    shard_shape.push_back(final_dim.shard_size_chunks * chunk_shape.back());
    for (auto i = 1; i < dims->ndims(); ++i) {
        const auto& dim = dims->at(i);
        array_shape.push_back(dim.array_size_px);
        chunk_shape.push_back(dim.chunk_size_px);
        shard_shape.push_back(dim.shard_size_chunks * chunk_shape.back());
    }

    json metadata;
    metadata["shape"] = array_shape;
    metadata["chunk_grid"] = json::object({
      { "name", "regular" },
      {
        "configuration",
        json::object({ { "chunk_shape", shard_shape } }),
      },
    });
    metadata["chunk_key_encoding"] = json::object({
      { "name", "default" },
      {
        "configuration",
        json::object({ { "separator", "/" } }),
      },
    });
    metadata["fill_value"] = 0;
    metadata["attributes"] = json::object();
    metadata["zarr_format"] = 3;
    metadata["node_type"] = "array";
    metadata["storage_transformers"] = json::array();
    metadata["data_type"] = sample_type_to_dtype(config_->dtype);
    metadata["storage_transformers"] = json::array();

    std::vector<std::string> dimension_names(dims->ndims());
    for (auto i = 0; i < dimension_names.size(); ++i) {
        dimension_names[i] = dims->at(i).name;
    }
    metadata["dimension_names"] = dimension_names;

    auto codecs = json::array();

    auto sharding_indexed = json::object();
    sharding_indexed["name"] = "sharding_indexed";

    auto configuration = json::object();
    configuration["chunk_shape"] = chunk_shape;

    auto codec = json::object();
    codec["configuration"] = json::object({ { "endian", "little" } });
    codec["name"] = "bytes";

    auto index_codec = json::object();
    index_codec["configuration"] = json::object({ { "endian", "little" } });
    index_codec["name"] = "bytes";

    auto crc32_codec = json::object({ { "name", "crc32c" } });
    configuration["index_codecs"] = json::array({
      index_codec,
      crc32_codec,
    });

    configuration["index_location"] = "end";
    configuration["codecs"] = json::array({ codec });

    if (config_->compression_params) {
        const auto params = *config_->compression_params;

        auto compression_config = json::object();
        compression_config["blocksize"] = 0;
        compression_config["clevel"] = params.clevel;
        compression_config["cname"] = params.codec_id;
        compression_config["shuffle"] = shuffle_to_string(params.shuffle);
        compression_config["typesize"] = bytes_of_type(config_->dtype);

        auto compression_codec = json::object();
        compression_codec["configuration"] = compression_config;
        compression_codec["name"] = "blosc";
        configuration["codecs"].push_back(compression_codec);
    }

    sharding_indexed["configuration"] = configuration;

    codecs.push_back(sharding_indexed);

    metadata["codecs"] = codecs;

    metadata_strings_.emplace("zarr.json", metadata.dump(4));

    return true;
}

bool
zarr::Array::close_()
{
    bool retval = false;
    is_closing_ = true;
    try {
        if (bytes_to_flush_ > 0) {
            CHECK(compress_and_flush_data_());
        } else {
            CHECK(close_impl_());
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
zarr::Array::close_impl_()
{
    if (current_layer_ == 0) {
        return true;
    }

    // write the table
    const auto& dims = config_->dimensions;
    const auto n_shards = dims->number_of_shards();
    std::vector<std::future<void>> futures;

    std::atomic<char> all_successful = 1;

    for (auto shard_idx = 0; shard_idx < n_shards; ++shard_idx) {
        const std::string data_path = data_paths_[shard_idx];
        auto* file_offset = shard_file_offsets_.data() + shard_idx;
        auto* shard_table = shard_tables_.data() + shard_idx;

        auto promise = std::make_shared<std::promise<void>>();
        futures.emplace_back(promise->get_future());

        auto job = [shard_idx,
                    data_path,
                    shard_table,
                    file_offset,
                    promise,
                    &all_successful,
                    this](std::string& err) {
            bool success = true;

            try {
                std::unique_ptr<Sink> sink;

                if (data_sinks_.contains(
                      data_path)) { // sink already constructed
                    sink = std::move(data_sinks_[data_path]);
                    data_sinks_.erase(data_path);
                } else {
                    sink = make_data_sink_(data_path);
                }

                if (sink == nullptr) {
                    err = "Failed to create sink for " + data_path;
                    success = false;
                } else {
                    const auto table_size =
                      shard_table->size() * sizeof(uint64_t);
                    std::vector<uint8_t> table(table_size + sizeof(uint32_t));

                    // copy the table data
                    memcpy(table.data(), shard_table->data(), table_size);
                    const auto* table_ptr = table.data();

                    // compute crc32 checksum of the table
                    const uint32_t checksum =
                      crc32c::Crc32c(table_ptr, table_size);
                    memcpy(
                      table.data() + table_size, &checksum, sizeof(uint32_t));

                    if (!sink->write(*file_offset, table)) {
                        err = "Failed to write table and checksum to shard " +
                              std::to_string(shard_idx);
                        success = false;
                    }
                }
            } catch (const std::exception& exc) {
                err = "Failed to flush data: " + std::string(exc.what());
                success = false;
            }

            all_successful.fetch_and(success);
            promise->set_value();

            return success;
        };

        // one thread is reserved for processing the frame queue and runs the
        // entire lifetime of the stream
        if (thread_pool_->n_threads() == 1 || !thread_pool_->push_job(job)) {
            if (std::string err; !job(err)) {
                LOG_ERROR(err);
            }
        }
    }

    return all_successful;
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
          data_root_, *config_->dimensions, shards_along_dimension);
    }
}

std::unique_ptr<zarr::Sink>
zarr::Array::make_data_sink_(std::string_view path)
{
    const auto is_s3 = is_s3_array_();

    std::unique_ptr<Sink> sink;

    // create parent directories if needed
    if (is_s3) {
        const auto bucket_name = *config_->bucket_name;
        sink = make_s3_sink(bucket_name, path, s3_connection_pool_);
    } else {
        const auto parent_paths = get_parent_paths(data_paths_);
        CHECK(make_dirs(parent_paths, thread_pool_));

        sink = make_file_sink(path, file_handle_pool_);
    }

    return sink;
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

ByteVector
zarr::Array::consolidate_chunks_(uint32_t shard_index)
{
    const auto& dims = config_->dimensions;
    CHECK(shard_index < dims->number_of_shards());

    const auto chunks_per_shard = dims->chunks_per_shard();
    const auto chunks_in_mem = dims->number_of_chunks_in_memory();
    const auto n_layers = dims->chunk_layers_per_shard();

    const auto chunks_per_layer = chunks_per_shard / n_layers;
    const auto layer_offset = current_layer_ * chunks_per_layer;
    const auto chunk_offset = current_layer_ * chunks_in_mem;

    auto& shard_table = shard_tables_[shard_index];
    const auto file_offset = shard_file_offsets_[shard_index];
    shard_table[2 * layer_offset] = file_offset;

    uint64_t last_chunk_offset = shard_table[2 * layer_offset];
    uint64_t last_chunk_size = shard_table[2 * layer_offset + 1];
    size_t shard_size = last_chunk_size;

    for (auto i = 1; i < chunks_per_layer; ++i) {
        const auto offset_idx = 2 * (layer_offset + i);
        const auto size_idx = offset_idx + 1;
        if (shard_table[size_idx] == std::numeric_limits<uint64_t>::max()) {
            continue;
        }

        shard_table[offset_idx] = last_chunk_offset + last_chunk_size;
        last_chunk_offset = shard_table[offset_idx];
        last_chunk_size = shard_table[size_idx];
        shard_size += last_chunk_size;
    }

    std::vector<uint8_t> shard_layer(shard_size);

    const auto chunk_indices_this_layer =
      dims->chunk_indices_for_shard_layer(shard_index, current_layer_);

    size_t offset = 0;
    for (const auto& idx : chunk_indices_this_layer) {
        // this clears the chunk data out of the LockedBuffer
        const auto chunk = chunk_buffers_[idx - chunk_offset].take();
        std::copy(chunk.begin(), chunk.end(), shard_layer.begin() + offset);

        offset += chunk.size();
    }

    EXPECT(offset == shard_size,
           "Consolidated shard size does not match expected: ",
           offset,
           " != ",
           shard_size);

    return std::move(shard_layer);
}

bool
zarr::Array::compress_and_flush_data_()
{
    // construct paths to shard sinks if they don't already exist
    if (data_paths_.empty()) {
        make_data_paths_();
    }

    // create parent directories if needed
    const auto is_s3 = is_s3_array_();
    if (!is_s3) {
        const auto parent_paths = get_parent_paths(data_paths_);
        CHECK(make_dirs(parent_paths, thread_pool_)); // no-op if they exist
    }

    const auto& dims = config_->dimensions;

    const auto n_shards = dims->number_of_shards();
    CHECK(data_paths_.size() == n_shards);

    const auto chunks_in_memory = chunk_buffers_.size();
    const auto n_layers = dims->chunk_layers_per_shard();
    CHECK(n_layers > 0);

    const auto chunk_group_offset = current_layer_ * chunks_in_memory;

    std::atomic<char> all_successful = 1;

    auto write_table = is_closing_ || should_rollover_();

    std::vector<std::future<void>> futures;

    // queue jobs to compress all chunks
    const auto bytes_of_raw_chunk = config_->dimensions->bytes_per_chunk();
    const auto bytes_per_px = bytes_of_type(config_->dtype);

    for (auto i = 0; i < chunks_in_memory; ++i) {
        auto promise = std::make_shared<std::promise<void>>();
        futures.emplace_back(promise->get_future());

        const auto chunk_idx = i + chunk_group_offset;
        const auto shard_idx = dims->shard_index_for_chunk(chunk_idx);
        const auto internal_idx = dims->shard_internal_index(chunk_idx);
        auto* shard_table = shard_tables_.data() + shard_idx;

        if (config_->compression_params) {
            const auto compression_params = config_->compression_params.value();

            auto job = [&chunk_buffer = chunk_buffers_[i],
                        bytes_per_px,
                        compression_params,
                        shard_table,
                        shard_idx,
                        chunk_idx,
                        internal_idx,
                        promise,
                        &all_successful](std::string& err) {
                bool success = false;

                try {
                    if (!chunk_buffer.compress(compression_params,
                                               bytes_per_px)) {
                        err = "Failed to compress chunk " +
                              std::to_string(chunk_idx) + " (internal index " +
                              std::to_string(internal_idx) + " of shard " +
                              std::to_string(shard_idx) + ")";
                    }

                    // update shard table with size
                    shard_table->at(2 * internal_idx + 1) = chunk_buffer.size();
                    success = true;
                } catch (const std::exception& exc) {
                    err = exc.what();
                }

                promise->set_value();

                all_successful.fetch_and(static_cast<char>(success));
                return success;
            };

            // one thread is reserved for processing the frame queue and runs
            // the entire lifetime of the stream
            if (thread_pool_->n_threads() == 1 ||
                !thread_pool_->push_job(job)) {
                std::string err;
                if (!job(err)) {
                    LOG_ERROR(err);
                }
            }
        } else {
            // no compression, just update shard table with size
            shard_table->at(2 * internal_idx + 1) = bytes_of_raw_chunk;
        }
    }

    // if we're not compressing, there aren't any futures to wait for
    for (auto& future : futures) {
        future.wait();
    }
    futures.clear();

    const auto bucket_name = config_->bucket_name;
    auto connection_pool = s3_connection_pool_;

    // wait for the chunks in each shard to finish compressing, then defragment
    // and write the shard
    for (auto shard_idx = 0; shard_idx < n_shards; ++shard_idx) {
        const std::string data_path = data_paths_[shard_idx];
        auto* file_offset = shard_file_offsets_.data() + shard_idx;
        auto* shard_table = shard_tables_.data() + shard_idx;

        auto promise = std::make_shared<std::promise<void>>();
        futures.emplace_back(promise->get_future());

        auto job = [shard_idx,
                    is_s3,
                    data_path,
                    shard_table,
                    file_offset,
                    write_table,
                    bucket_name,
                    connection_pool,
                    promise,
                    &all_successful,
                    this](std::string& err) {
            bool success = true;
            std::unique_ptr<Sink> sink;

            try {
                // consolidate chunks in shard
                const auto shard_data = consolidate_chunks_(shard_idx);

                if (data_sinks_.contains(data_path)) { // S3 sink, constructed
                    sink = std::move(data_sinks_[data_path]);
                    data_sinks_.erase(data_path);
                } else {
                    sink = make_data_sink_(data_path);
                }

                if (sink == nullptr) {
                    err = "Failed to create sink for " + data_path;
                    success = false;
                } else {
                    success = sink->write(*file_offset, shard_data);
                    if (!success) {
                        err = "Failed to write shard at path " + data_path;
                    } else {
                        *file_offset += shard_data.size();

                        if (write_table) {
                            const size_t table_size =
                              shard_table->size() * sizeof(uint64_t);
                            std::vector<uint8_t> table(
                              table_size + sizeof(uint32_t), 0);

                            memcpy(
                              table.data(), shard_table->data(), table_size);

                            // compute crc32 checksum of the table
                            const uint32_t checksum =
                              crc32c::Crc32c(table.data(), table_size);
                            memcpy(table.data() + table_size,
                                   &checksum,
                                   sizeof(uint32_t));

                            if (!sink->write(*file_offset, table)) {
                                err = "Failed to write table and checksum to "
                                      "shard " +
                                      std::to_string(shard_idx);
                                success = false;
                            }
                        }
                    }
                }
            } catch (const std::exception& exc) {
                err = "Failed to flush data: " + std::string(exc.what());
                success = false;
            }

            if (sink != nullptr) {
                data_sinks_.emplace(data_path, std::move(sink));
            }

            all_successful.fetch_and(success);
            promise->set_value();

            return success;
        };

        // one thread is reserved for processing the frame queue and runs the
        // entire lifetime of the stream
        if (thread_pool_->n_threads() == 1 || !thread_pool_->push_job(job)) {
            std::string err;
            if (!job(err)) {
                LOG_ERROR(err);
            }
        }
    }

    // wait for all threads to finish
    for (auto& future : futures) {
        future.wait();
    }

    // reset shard tables and file offsets
    if (write_table) {
        for (auto& table : shard_tables_) {
            std::fill(
              table.begin(), table.end(), std::numeric_limits<uint64_t>::max());
        }

        std::fill(shard_file_offsets_.begin(), shard_file_offsets_.end(), 0);
        current_layer_ = 0;
    } else {
        ++current_layer_;
    }

    return static_cast<bool>(all_successful);
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

bool
zarr::Array::should_rollover_() const
{
    const auto& dims = config_->dimensions;
    const auto& append_dim = dims->final_dim();
    size_t frames_before_flush =
      append_dim.chunk_size_px * append_dim.shard_size_chunks;
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
    data_root_ = node_path_() + "/c/" + std::to_string(append_chunk_index_);
}

void
zarr::Array::close_sinks_()
{
    data_paths_.clear();

    for (auto& [path, sink] : data_sinks_) {
        EXPECT(
          finalize_sink(std::move(sink)), "Failed to finalize sink at ", path);
    }
    data_sinks_.clear();
}

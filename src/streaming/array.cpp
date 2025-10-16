#include "array.hh"
#include "macros.hh"
#include "zarr.common.hh"

#include <blosc.h>
#include <nlohmann/json.hpp>

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
                   std::shared_ptr<ThreadPool> thread_pool)
  : ArrayBase(config, thread_pool)
  , bytes_to_flush_{ 0 }
  , frames_written_{ 0 }
  , append_chunk_index_{ 0 }
  , current_layer_{ 0 }
  , is_closing_{ false }
{
    const size_t n_chunks = config_->dimensions->number_of_chunks_in_memory();
    EXPECT(n_chunks > 0, "Array has zero chunks in memory");
    chunk_buffers_ = std::vector<std::vector<uint8_t>>(n_chunks);

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

bool
zarr::Array::make_metadata_(std::string& metadata_str)
{
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

    metadata_str = metadata.dump(4);

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
        } else if (current_layer_ > 0) {
            CHECK(flush_tables_());
        }
        close_io_streams_();

        if (frames_written_ > 0) {
            CHECK(write_metadata_());
        }
        retval = true;
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to finalize array writer: ", exc.what());
    }

    is_closing_ = false;
    return retval;
}

void
zarr::Array::make_data_paths_()
{
    if (data_paths_.empty()) {
        const auto& dimensions = config_->dimensions;

        std::queue<std::string> paths_queue;
        paths_queue.emplace(data_root_);

        // create intermediate paths
        for (auto i = 1;                  // skip the last dimension
             i < dimensions->ndims() - 1; // skip the x dimension
             ++i) {
            const auto& dim = dimensions->at(i);
            const auto n_parts = shards_along_dimension(dim);
            CHECK(n_parts);

            auto n_paths = paths_queue.size();
            for (auto j = 0; j < n_paths; ++j) {
                const auto path = paths_queue.front();
                paths_queue.pop();

                for (auto k = 0; k < n_parts; ++k) {
                    const auto kstr = std::to_string(k);
                    paths_queue.push(path + (path.empty() ? kstr : "/" + kstr));
                }
            }
        }

        // create final paths
        data_paths_.reserve(paths_queue.size() *
                            shards_along_dimension(dimensions->width_dim()));
        {
            const auto& dim = dimensions->width_dim();
            const auto n_parts = shards_along_dimension(dim);
            CHECK(n_parts);

            auto n_paths = paths_queue.size();
            for (auto i = 0; i < n_paths; ++i) {
                const auto path = paths_queue.front();
                paths_queue.pop();
                for (auto j = 0; j < n_parts; ++j)
                    data_paths_.push_back(path + "/" + std::to_string(j));
            }
        }
    }
}

void
zarr::Array::fill_buffers_()
{
    LOG_DEBUG("Filling chunk buffers");

    const auto n_bytes = config_->dimensions->bytes_per_chunk();

    for (auto& buf : chunk_buffers_) {
        buf.resize(n_bytes); // no-op if already that size
        std::ranges::fill(buf, 0);
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
        const auto* data_ptr = frame.data();
        const auto data_size = frame.size();

        const auto chunk_start = chunk_buffer.data();

        const auto tile_idx_y = tile / n_tiles_x;
        const auto tile_idx_x = tile % n_tiles_x;

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

    data.assign(std::move(frame));

    return bytes_written;
}

zarr::Array::ShardLayer
zarr::Array::collect_chunks_(uint32_t shard_index)
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

    for (auto i = 1; i < chunks_per_layer; ++i) {
        const auto offset_idx = 2 * (layer_offset + i);
        const auto size_idx = offset_idx + 1;
        if (shard_table[size_idx] == std::numeric_limits<uint64_t>::max()) {
            continue;
        }

        shard_table[offset_idx] = last_chunk_offset + last_chunk_size;
        last_chunk_offset = shard_table[offset_idx];
        last_chunk_size = shard_table[size_idx];
    }

    const auto chunk_indices_this_layer =
      dims->chunk_indices_for_shard_layer(shard_index, current_layer_);

    ShardLayer layer{ file_offset, {} };
    layer.chunks.reserve(chunk_indices_this_layer.size());

    for (const auto& idx : chunk_indices_this_layer) {
        layer.chunks.emplace_back(chunk_buffers_[idx - chunk_offset]);
    }

    return std::move(layer);
}

bool
zarr::Array::compress_and_flush_data_()
{
    if (!compress_chunks_()) {
        LOG_ERROR("Failed to compress chunk data");
        return false;
    }

    update_table_entries_();

    if (!flush_data_()) {
        LOG_ERROR("Failed to flush chunk data");
        return false;
    }

    if (is_closing_ || should_rollover_()) { // flush table
        if (!flush_tables_()) {
            LOG_ERROR("Failed to flush shard tables");
            return false;
        }
        current_layer_ = 0;
    } else {
        ++current_layer_;
        CHECK(current_layer_ < config_->dimensions->chunk_layers_per_shard());
    }

    return true;
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
    return frames_written_ % dims->frames_before_flush() == 0;
}

bool
zarr::Array::compress_chunks_()
{
    if (!config_->compression_params) {
        return true; // nothing to do
    }

    std::atomic<char> all_successful = 1;

    const auto& params = *config_->compression_params;
    const size_t bytes_per_px = bytes_of_type(config_->dtype);

    const auto& dims = config_->dimensions;

    const uint32_t chunks_in_memory = chunk_buffers_.size();
    const uint32_t chunk_group_offset = current_layer_ * chunks_in_memory;

    std::vector<std::future<void>> futures;
    futures.reserve(chunks_in_memory);

    for (size_t i = 0; i < chunks_in_memory; ++i) {
        auto promise = std::make_shared<std::promise<void>>();
        futures.emplace_back(promise->get_future());

        const uint32_t chunk_idx = i + chunk_group_offset;
        const uint32_t shard_idx = dims->shard_index_for_chunk(chunk_idx);
        const uint32_t internal_idx = dims->shard_internal_index(chunk_idx);
        auto* shard_table = shard_tables_.data() + shard_idx;

        auto job = [&chunk_buffer = chunk_buffers_[i],
                    bytes_per_px,
                    &params,
                    shard_table,
                    shard_idx,
                    chunk_idx,
                    internal_idx,
                    promise,
                    &all_successful](std::string& err) {
            bool success = false;

            try {
                std::vector<uint8_t> compressed_data(chunk_buffer.size() +
                                                     BLOSC_MAX_OVERHEAD);
                const auto n_bytes_compressed =
                  blosc_compress_ctx(params.clevel,
                                     params.shuffle,
                                     bytes_per_px,
                                     chunk_buffer.size(),
                                     chunk_buffer.data(),
                                     compressed_data.data(),
                                     compressed_data.size(),
                                     params.codec_id.c_str(),
                                     0,
                                     1);

                if (n_bytes_compressed <= 0) {
                    err = "blosc_compress_ctx failed with code " +
                          std::to_string(n_bytes_compressed) + " for chunk " +
                          std::to_string(chunk_idx) + " (internal index " +
                          std::to_string(internal_idx) + " of shard " +
                          std::to_string(shard_idx) + ")";
                    success = false;
                } else {
                    compressed_data.resize(n_bytes_compressed);
                    chunk_buffer.swap(compressed_data);

                    // update shard table with size
                    shard_table->at(2 * internal_idx + 1) = chunk_buffer.size();
                    success = true;
                }
            } catch (const std::exception& exc) {
                err = exc.what();
            }

            promise->set_value();

            all_successful.fetch_and(static_cast<char>(success));
            return success;
        };

        // one thread is reserved for processing the frame queue and runs
        // the entire lifetime of the stream
        if (thread_pool_->n_threads() == 1 || !thread_pool_->push_job(job)) {
            if (std::string err; !job(err)) {
                LOG_ERROR(err);
            }
        }
    }

    for (auto& future : futures) {
        future.wait();
    }

    return static_cast<bool>(all_successful);
}

void
zarr::Array::update_table_entries_()
{
    const uint32_t chunks_in_memory = chunk_buffers_.size();
    const uint32_t chunk_group_offset = current_layer_ * chunks_in_memory;
    const auto& dims = config_->dimensions;

    for (auto i = 0; i < chunks_in_memory; ++i) {
        const auto& chunk_buffer = chunk_buffers_[i];
        const uint32_t chunk_idx = i + chunk_group_offset;
        const uint32_t shard_idx = dims->shard_index_for_chunk(chunk_idx);
        const uint32_t internal_idx = dims->shard_internal_index(chunk_idx);
        auto& shard_table = shard_tables_[shard_idx];

        shard_table[2 * internal_idx + 1] = chunk_buffer.size();
    }
}

void
zarr::Array::rollover_()
{
    LOG_DEBUG("Rolling over");

    close_io_streams_();
    ++append_chunk_index_;
    data_root_ = node_path_() + "/c/" + std::to_string(append_chunk_index_);
}

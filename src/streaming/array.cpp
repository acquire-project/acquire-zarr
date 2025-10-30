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
  , append_shard_index_{ 0 }
  , is_closing_{ false }
  , current_layer_{ 0 }
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

    data_root_ = node_path_() + "/c/" + std::to_string(append_shard_index_);
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
zarr::Array::write_frame(std::vector<uint8_t>& data)
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

    if (should_flush_layer_()) {
        EXPECT(compress_and_flush_data_(), "Failed to flush chunk layer data");
        bytes_to_flush_ = 0;

        const auto& dims = config_->dimensions;
        const auto lps = dims->chunk_layers_per_shard();
        current_layer_ = (current_layer_ + 1) % lps;

        if (should_rollover_()) {
            close_shards_();
            CHECK(write_metadata_());
        }
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

    configuration["index_location"] = index_location_();
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
            if (!compress_and_flush_data_()) {
                LOG_ERROR("Failed to flush remaining data on close");
                return false;
            }
            bytes_to_flush_ = 0;
        }
        finalize_append_shard_();

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
zarr::Array::write_frame_to_chunks_(std::vector<uint8_t>& data)
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

    std::vector<uint8_t> frame = std::move(data);

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

    data = std::move(frame);

    return bytes_written;
}

bool
zarr::Array::should_flush_layer_() const
{
    const auto& dims = config_->dimensions;
    const size_t frames_per_layer = dims->frames_per_layer();
    return frames_written_ % frames_per_layer == 0;
}

bool
zarr::Array::should_rollover_() const
{
    const auto& dims = config_->dimensions;
    const size_t frames_per_shard = dims->frames_per_shard();
    return frames_written_ % frames_per_shard == 0;
}

void
zarr::Array::close_shards_()
{
    LOG_DEBUG("Rolling over");

    finalize_append_shard_();

    // advance to the next shard index
    if (!is_closing_) {
        data_root_ =
          node_path_() + "/c/" + std::to_string(++append_shard_index_);
    }
}

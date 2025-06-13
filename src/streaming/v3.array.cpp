#include "v3.array.hh"

#include "definitions.hh"
#include "macros.hh"
#include "sink.hh"
#include "zarr.common.hh"

#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <crc32c/crc32c.h>

#include <algorithm> // std::fill
#include <semaphore>
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
            throw std::runtime_error(
              fmt::format("Invalid ZarrDataType: {}", static_cast<int>(t)));
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
            throw std::runtime_error(
              fmt::format("Invalid shuffle value: {}", shuffle));
    }
}
} // namespace

zarr::V3Array::V3Array(std::shared_ptr<ArrayConfig> config,
                       std::shared_ptr<ThreadPool> thread_pool,
                       std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : Array(config, thread_pool, s3_connection_pool)
  , current_layer_{ 0 }
{
    const auto& dims = config_->dimensions;
    const auto number_of_shards = dims->number_of_shards();
    const auto chunks_per_shard = dims->chunks_per_shard();

    shard_file_offsets_.resize(number_of_shards, 0);
    shard_tables_.resize(number_of_shards);

    for (auto& table : shard_tables_) {
        table.resize(2 * chunks_per_shard);
        std::fill(
          table.begin(), table.end(), std::numeric_limits<uint64_t>::max());
    }
}

std::vector<std::string>
zarr::V3Array::metadata_keys_() const
{
    return { "zarr.json" };
}

bool
zarr::V3Array::make_metadata_()
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

size_t
zarr::V3Array::compute_chunk_offsets_and_defrag_(uint32_t shard_index)
{
    const auto& dims = config_->dimensions;
    CHECK(shard_index < dims->number_of_shards());

    const auto chunks_per_shard = dims->chunks_per_shard();
    const auto n_layers = dims->chunk_layers_per_shard();

    const auto chunks_per_layer = chunks_per_shard / n_layers;
    const auto layer_offset = current_layer_ * chunks_per_layer;

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

    // no need to defragment if no compression
    if (!config_->compression_params) {
        return shard_size;
    }

    auto& buffer = data_buffers_[shard_index];
    const auto nbytes_chunk = bytes_to_allocate_per_chunk_();

    auto k = 1;
    size_t offset_to_copy_to = shard_table[2 * layer_offset + 1];
    for (auto i = 1; i < chunks_per_layer; ++i) {
        const auto chunk_size = shard_table[2 * (layer_offset + i) + 1];
        if (chunk_size == std::numeric_limits<uint64_t>::max()) {
            continue;
        }

        const auto offset_to_copy_from = k * nbytes_chunk;
        EXPECT(offset_to_copy_from + chunk_size < buffer.size(),
               "Chunk size exceeds buffer size: ",
               offset_to_copy_from + chunk_size,
               " > ",
               buffer.size());
        EXPECT(offset_to_copy_to + chunk_size <= buffer.size(),
               "Buffer overflow in defragmentation: ",
               offset_to_copy_to + chunk_size,
               " > ",
               buffer.size());

        std::copy(buffer.begin() + offset_to_copy_from,
                  buffer.begin() + offset_to_copy_from + chunk_size,
                  buffer.begin() + offset_to_copy_to);
        offset_to_copy_to += chunk_size;
        ++k;
    }

    EXPECT(offset_to_copy_to == shard_size,
           "Defragmentation did not produce expected size: ",
           offset_to_copy_to,
           " != ",
           shard_size);
    return shard_size;
}

std::string
zarr::V3Array::data_root_() const
{
    return fmt::format("{}/c/{}", node_path_(), append_chunk_index_);
}

const DimensionPartsFun
zarr::V3Array::parts_along_dimension_() const
{
    return shards_along_dimension;
}

void
zarr::V3Array::make_buffers_()
{
    LOG_DEBUG("Creating shard buffers");

    const auto& dims = config_->dimensions;
    const size_t n_shards = dims->number_of_shards();

    // no-op if already the correct size
    data_buffers_.resize(n_shards);

    const auto n_bytes = bytes_to_allocate_per_chunk_();

    const auto n_layers = dims->chunk_layers_per_shard();
    EXPECT(n_layers > 0, "Shard size of 0 in append dimension");

    const auto n_chunks = dims->chunks_per_shard() / n_layers;

    for (auto& buf : data_buffers_) {
        buf.resize(n_chunks * n_bytes);
        std::fill(buf.begin(), buf.end(), std::byte(0));
    }
}

BytePtr
zarr::V3Array::get_chunk_data_(uint32_t index)
{
    const auto& dims = config_->dimensions;
    const auto shard_idx = dims->shard_index_for_chunk(index);
    auto& shard = data_buffers_[shard_idx];

    auto internal_idx = dims->shard_internal_index(index);
    const auto& chunk_indices = dims->chunk_indices_for_shard(shard_idx);

    // ragged shard
    if (internal_idx >= chunk_indices.size() ||
        chunk_indices[internal_idx] != index) {
        const auto it =
          std::find(chunk_indices.begin(), chunk_indices.end(), index);
        EXPECT(it != chunk_indices.end(),
               "Chunk index ",
               index,
               " not found in shard ",
               shard_idx);
        internal_idx = std::distance(chunk_indices.begin(), it);
    }

    const auto n_bytes = bytes_to_allocate_per_chunk_();
    const auto offset = internal_idx * n_bytes;

    const auto shard_size = shard.size();
    EXPECT(offset + n_bytes <= shard_size,
           "Attempted to access chunk data at index ",
           index,
           " with offset ",
           offset,
           " and n_bytes ",
           n_bytes,
           " in shard of size ",
           shard_size);

    return shard.data() + offset;
}

bool
zarr::V3Array::compress_and_flush_data_()
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
    CHECK(data_buffers_.size() == n_shards);

    const auto chunks_in_memory = dims->number_of_chunks_in_memory();
    const auto n_layers = dims->chunk_layers_per_shard();
    CHECK(n_layers > 0);

    const auto chunk_group_offset = current_layer_ * chunks_in_memory;

    std::atomic<char> all_successful = 1;

    auto write_table = is_closing_ || should_rollover_();
    shard_latch_ = std::make_shared<std::latch>(n_shards);

    // get count of chunks per shard to construct chunk latches
    std::vector<uint32_t> chunks_per_shard(n_shards);
    for (auto i = 0; i < chunks_in_memory; ++i) {
        const auto chunk_idx = i + chunk_group_offset;
        const auto shard_idx = dims->shard_index_for_chunk(chunk_idx);
        ++chunks_per_shard[shard_idx];
    }

    chunk_latches_.resize(n_shards);
    for (auto i = 0; i < n_shards; ++i) {
        chunk_latches_[i] = std::make_unique<std::latch>(chunks_per_shard[i]);
    }

    // queue jobs to compress all chunks
    const auto bytes_of_raw_chunk = config_->dimensions->bytes_per_chunk();
    const auto bytes_per_px = bytes_of_type(config_->dtype);

    for (auto i = 0; i < chunks_in_memory; ++i) {
        const auto chunk_idx = i + chunk_group_offset;
        const auto shard_idx = dims->shard_index_for_chunk(chunk_idx);
        const auto internal_idx = dims->shard_internal_index(chunk_idx);
        auto* shard_table = shard_tables_.data() + shard_idx;
        auto* latch = chunk_latches_.at(shard_idx).get();

        if (config_->compression_params) {
            const auto compression_params = config_->compression_params.value();

            auto* chunk_ptr = get_chunk_data_(i);
            if (!chunk_ptr) {
                LOG_ERROR("Failed to get chunk data for chunk index ",
                          chunk_idx);
                latch->count_down();
                return false;
            }
            EXPECT(thread_pool_->push_job(
                     std::move([bytes_per_px,
                                bytes_of_raw_chunk,
                                compression_params,
                                chunk_ptr,
                                shard_table,
                                internal_idx,
                                latch,
                                &all_successful](std::string& err) {
                         bool success = true;

                         try {
                             const int nb = compress_buffer_in_place(
                               chunk_ptr,
                               bytes_of_raw_chunk + BLOSC_MAX_OVERHEAD,
                               bytes_of_raw_chunk,
                               compression_params,
                               bytes_per_px);
                             EXPECT(nb > 0, "Failed to compress chunk");

                             // update shard table with size
                             shard_table->at(2 * internal_idx + 1) = nb;
                         } catch (const std::exception& exc) {
                             err = exc.what();
                             success = false;
                         }

                         latch->count_down();

                         all_successful.fetch_and(static_cast<char>(success));
                         return success;
                     })),
                   "Failed to push job to thread pool");
        } else {
            // no compression, just update shard table with size
            shard_table->at(2 * internal_idx + 1) = bytes_of_raw_chunk;
            chunk_latches_[shard_idx]->count_down();
        }
    }

    const auto bucket_name = config_->bucket_name;
    auto connection_pool = s3_connection_pool_;

    // wait for the chunks in each shard to finish compressing, then defragment
    // and write the shard
    std::counting_semaphore<MAX_CONCURRENT_FILES> semaphore(
      MAX_CONCURRENT_FILES);
    for (auto shard_idx = 0; shard_idx < n_shards; ++shard_idx) {
        std::string err;
        const std::string data_path = data_paths_[shard_idx];
        auto* chunk_latch = chunk_latches_[shard_idx].get();
        auto* shard_ptr = data_buffers_[shard_idx].data();
        auto* file_offset = shard_file_offsets_.data() + shard_idx;
        auto* shard_table = shard_tables_.data() + shard_idx;
        EXPECT(
          thread_pool_->push_job(
            std::move([shard_idx,
                       is_s3,
                       data_path,
                       shard_table,
                       file_offset,
                       write_table,
                       shard_ptr,
                       bucket_name,
                       connection_pool,
                       &semaphore,
                       shard_latch = shard_latch_,
                       chunk_latch,
                       &all_successful,
                       this](std::string& err) {
                chunk_latch->wait();

                bool success = true;
                std::unique_ptr<Sink> sink;

                try {
                    // defragment chunks in shard
                    const auto shard_size =
                      compute_chunk_offsets_and_defrag_(shard_idx);

                    semaphore.acquire();
                    if (data_sinks_.contains(data_path)) {
                        sink = std::move(data_sinks_[data_path]);
                    } else if (is_s3) {
                        sink = make_s3_sink(
                          *bucket_name, data_path, connection_pool);
                    } else {
                        sink = make_file_sink(data_path);
                    }

                    std::span shard_data(shard_ptr, shard_size);
                    success = sink->write(*file_offset, shard_data);
                    if (!success) {
                        semaphore.release();

                        err = fmt::format("Failed to write shard at path '{}'",
                                          data_path);
                        shard_latch_->count_down();
                        all_successful = 0;
                        return false;
                    }

                    *file_offset += shard_size;

                    if (write_table) {
                        const auto* table_ptr =
                          reinterpret_cast<std::byte*>(shard_table->data());
                        const auto table_size =
                          shard_table->size() * sizeof(uint64_t);
                        EXPECT(
                          sink->write(*file_offset, { table_ptr, table_size }),
                          "Failed to write table");

                        // compute crc32 checksum of the table
                        uint32_t checksum = crc32c::Crc32c(
                          reinterpret_cast<const uint8_t*>(table_ptr),
                          table_size);
                        EXPECT(
                          sink->write(*file_offset + table_size,
                                      { reinterpret_cast<std::byte*>(&checksum),
                                        sizeof(checksum) }),
                          "Failed to write checksum");
                    }
                    if (!is_s3) {
                        EXPECT(finalize_sink(std::move(sink)),
                               "Failed to finalize sink at path ",
                               data_path);
                    }
                } catch (const std::exception& exc) {
                    err =
                      fmt::format("Exception writing shard at path '{}': {}",
                                  data_path,
                                  exc.what());
                    success = false;
                }
                semaphore.release();

                // save the S3 sink for later
                if (is_s3 && !data_sinks_.contains(data_path)) {
                    data_sinks_.emplace(data_path, std::move(sink));
                } else if (is_s3) {
                    data_sinks_[data_path] = std::move(sink);
                }

                shard_latch_->count_down();

                all_successful.fetch_and(static_cast<char>(success));
                return success;
            })),
          "Failed to push job to thread pool");
    }

    // wait for all threads to finish
    shard_latch_->wait();
    shard_latch_.reset();
    chunk_latches_.clear();

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

void
zarr::V3Array::close_sinks_()
{
    data_paths_.clear();

    for (auto& [path, sink] : data_sinks_) {
        EXPECT(
          finalize_sink(std::move(sink)), "Failed to finalize sink at ", path);
    }
    data_sinks_.clear();
}

bool
zarr::V3Array::should_rollover_() const
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

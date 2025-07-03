#include "v3.array.hh"

#include "definitions.hh"
#include "macros.hh"
#include "sink.hh"
#include "zarr.common.hh"

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

ByteVector
zarr::V3Array::consolidate_chunks_(uint32_t shard_index)
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

std::string
zarr::V3Array::data_root_() const
{
    return node_path_() + "/c/" + std::to_string(append_chunk_index_);
}

const DimensionPartsFun
zarr::V3Array::parts_along_dimension_() const
{
    return shards_along_dimension;
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

    const auto chunks_in_memory = chunk_buffers_.size();
    const auto n_layers = dims->chunk_layers_per_shard();
    CHECK(n_layers > 0);

    const auto chunk_group_offset = current_layer_ * chunks_in_memory;

    std::atomic<char> all_successful = 1;

    auto write_table = is_closing_ || should_rollover_();

    // get count of chunks per shard to construct chunk latches
    std::vector<uint32_t> chunks_per_shard(n_shards);
    for (auto i = 0; i < chunks_in_memory; ++i) {
        const auto chunk_idx = i + chunk_group_offset;
        const auto shard_idx = dims->shard_index_for_chunk(chunk_idx);
        ++chunks_per_shard[shard_idx];
    }

    auto chunk_latch = std::make_shared<std::latch>(chunks_in_memory);

    // queue jobs to compress all chunks
    const auto bytes_of_raw_chunk = config_->dimensions->bytes_per_chunk();
    const auto bytes_per_px = bytes_of_type(config_->dtype);

    for (auto i = 0; i < chunks_in_memory; ++i) {
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
                        latch = chunk_latch,
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

                latch->count_down();

                all_successful.fetch_and(static_cast<char>(success));
                return success;
            };

            // one thread is reserved for processing the frame queue and runs
            // the entire lifetime of the stream
            if (thread_pool_->n_threads() == 1 ||
                !thread_pool_->push_job(std::move(job))) {
                std::string err;
                if (!job(err)) {
                    LOG_ERROR(err);
                }
            }
        } else {
            // no compression, just update shard table with size
            shard_table->at(2 * internal_idx + 1) = bytes_of_raw_chunk;
            chunk_latch->count_down();
        }
    }

    chunk_latch->wait();

    const auto bucket_name = config_->bucket_name;
    auto connection_pool = s3_connection_pool_;
    auto shard_latch = std::make_shared<std::latch>(n_shards);

    // wait for the chunks in each shard to finish compressing, then defragment
    // and write the shard
    std::counting_semaphore<MAX_CONCURRENT_FILES> semaphore(
      MAX_CONCURRENT_FILES);
    for (auto shard_idx = 0; shard_idx < n_shards; ++shard_idx) {
        const std::string data_path = data_paths_[shard_idx];
        auto* file_offset = shard_file_offsets_.data() + shard_idx;
        auto* shard_table = shard_tables_.data() + shard_idx;

        auto job = [shard_idx,
                    is_s3,
                    data_path,
                    shard_table,
                    file_offset,
                    write_table,
                    bucket_name,
                    connection_pool,
                    latch = shard_latch,
                    &semaphore,
                    &all_successful,
                    this](std::string& err) {
            bool success = true;
            std::unique_ptr<Sink> sink;
            bool semaphore_acquired = false;

            try {
                semaphore.acquire();
                semaphore_acquired = true;

                // defragment chunks in shard
                const auto shard_data = consolidate_chunks_(shard_idx);

                if (data_sinks_.contains(data_path)) { // S3 sink, constructed
                    sink = std::move(data_sinks_[data_path]);
                    data_sinks_.erase(data_path);
                }

                if (!sink && is_s3) { // S3 sink, not yet constructed
                    sink = make_s3_sink(*bucket_name, data_path, connection_pool);
                } else if (!is_s3) { // file sink
                    sink = make_file_sink(data_path);
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
                            const auto* table_ptr =
                              reinterpret_cast<uint8_t*>(shard_table->data());
                            const auto table_size =
                              shard_table->size() * sizeof(uint64_t);

                            // compute crc32 checksum of the table
                            const uint32_t checksum = crc32c::Crc32c(
                              reinterpret_cast<const uint8_t*>(table_ptr), table_size);
                            const auto* checksum_bytes =
                              reinterpret_cast<const uint8_t*>(&checksum);

                            if (!sink->write(*file_offset, { table_ptr, table_size })) {
                                err = "Failed to write table to shard " +
                                      std::to_string(shard_idx);
                                success = false;
                            } else if (!sink->write(*file_offset + table_size,
                                                    { checksum_bytes, sizeof(checksum) })) {
                                err = "Failed to write table checksum to shard " +
                                      std::to_string(shard_idx);
                                success = false;
                            }
                        }

                        if (!is_s3 && !finalize_sink(std::move(sink))) {
                            err = "Failed to finalize sink at path " + data_path;
                            success = false;
                        }
                    }
                }
            } catch (const std::exception& exc) {
                err = "Failed to flush data: " + std::string(exc.what());
                success = false;
            }

            // Cleanup and single point of latch countdown
            if (semaphore_acquired) {
                semaphore.release();
            }

            if (is_s3 && sink) {
                data_sinks_.emplace(data_path, std::move(sink));
            }

            all_successful.fetch_and(static_cast<char>(success));
            latch->count_down();

            return success;
        };

        // one thread is reserved for processing the frame queue and runs the
        // entire lifetime of the stream
        if (thread_pool_->n_threads() == 1 ||
            !thread_pool_->push_job(std::move(job))) {
            std::string err;
            if (!job(err)) {
                LOG_ERROR(err);
            }
        }
    }

    // wait for all threads to finish
    shard_latch->wait();

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

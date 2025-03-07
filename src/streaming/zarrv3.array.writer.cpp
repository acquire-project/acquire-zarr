#include "zarrv3.array.writer.hh"

#include "definitions.hh"
#include "macros.hh"
#include "sink.hh"
#include "zarr.common.hh"

#include <nlohmann/json.hpp>
#include <crc32c/crc32c.h>

#include <algorithm> // std::fill
#include <latch>
#include <semaphore>
#include <stdexcept>

#ifdef max
#undef max
#endif

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

zarr::ZarrV3ArrayWriter::ZarrV3ArrayWriter(
  const ArrayWriterConfig& config,
  std::shared_ptr<ThreadPool> thread_pool)
  : ZarrV3ArrayWriter(config, thread_pool, nullptr)
{
}

zarr::ZarrV3ArrayWriter::ZarrV3ArrayWriter(
  const ArrayWriterConfig& config,
  std::shared_ptr<ThreadPool> thread_pool,
  std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : ArrayWriter(config, thread_pool, s3_connection_pool)
  , current_layer_{ 0 }
{
    const auto& dims = config_.dimensions;
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

size_t
zarr::ZarrV3ArrayWriter::compute_chunk_offsets_and_defrag_(uint32_t shard_index)
{
    const auto& dims = config_.dimensions;
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
    if (!config_.compression_params) {
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
        std::copy(buffer.begin() + offset_to_copy_from,
                  buffer.begin() + offset_to_copy_from + chunk_size,
                  buffer.begin() + offset_to_copy_to);
        offset_to_copy_to += chunk_size;
        ++k;
    }

    CHECK(offset_to_copy_to == shard_size);
    return shard_size;
}

std::string
zarr::ZarrV3ArrayWriter::data_root_() const
{
    return config_.store_path + "/" + std::to_string(config_.level_of_detail) +
           "/c/" + std::to_string(append_chunk_index_);
}

std::string
zarr::ZarrV3ArrayWriter::metadata_path_() const
{
    return config_.store_path + "/" + std::to_string(config_.level_of_detail) +
           "/zarr.json";
}

const DimensionPartsFun
zarr::ZarrV3ArrayWriter::parts_along_dimension_() const
{
    return shards_along_dimension;
}

void
zarr::ZarrV3ArrayWriter::make_buffers_()
{
    LOG_DEBUG("Creating shard buffers");

    const auto& dims = config_.dimensions;
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
zarr::ZarrV3ArrayWriter::get_chunk_data_(uint32_t index)
{
    const auto& dims = config_.dimensions;
    const auto shard_idx = dims->shard_index_for_chunk(index);
    auto& shard = data_buffers_[shard_idx];

    const auto internal_idx = dims->shard_internal_index(index);
    const auto n_bytes = bytes_to_allocate_per_chunk_();
    const auto offset = internal_idx * n_bytes;

    const auto shard_size = shard.size();
    CHECK(offset + n_bytes <= shard_size);
    return shard.data() + offset;
}

bool
zarr::ZarrV3ArrayWriter::compress_and_flush_data_()
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

    const auto& dims = config_.dimensions;

    const auto n_shards = dims->number_of_shards();
    CHECK(data_paths_.size() == n_shards);

    const auto chunks_in_memory = dims->number_of_chunks_in_memory();
    const auto n_layers = dims->chunk_layers_per_shard();
    CHECK(n_layers > 0);

    const auto chunks_per_layer = dims->chunks_per_shard() / n_layers;
    const auto chunk_group_offset = current_layer_ * chunks_in_memory;

    std::atomic<char> all_successful = 1;

    auto write_table = is_finalizing_ || should_rollover_();
    std::latch shard_latch(n_shards);

    // get count of chunks per shard to construct chunk latches
    std::vector<uint32_t> chunks_per_shard(n_shards);
    for (auto i = 0; i < chunks_in_memory; ++i) {
        const auto chunk_idx = i + chunk_group_offset;
        const auto shard_idx = dims->shard_index_for_chunk(chunk_idx);
        ++chunks_per_shard[shard_idx];
    }

    std::unordered_map<uint32_t, std::latch> chunk_latches;
    for (uint32_t i = 0; i < n_shards; ++i) {
        chunk_latches.emplace(i, chunks_per_shard[i]);
    }

    // queue jobs to compress all chunks
    const auto compression_params = config_.compression_params;
    const auto bytes_of_raw_chunk = config_.dimensions->bytes_per_chunk();
    const auto bytes_per_px = bytes_of_type(config_.dtype);

    for (auto i = 0; i < chunks_in_memory; ++i) {
        const auto chunk_idx = i + chunk_group_offset;
        const auto shard_idx = dims->shard_index_for_chunk(chunk_idx);
        const auto internal_idx = dims->shard_internal_index(chunk_idx);
        auto& shard_table = shard_tables_[shard_idx];
        auto& latch = chunk_latches.at(shard_idx);

        if (compression_params) {
            EXPECT(thread_pool_->push_job(std::move([bytes_per_px,
                                                     bytes_of_raw_chunk,
                                                     &compression_params,
                                                     chunk_ptr =
                                                       get_chunk_data_(i),
                                                     &shard_table,
                                                     internal_idx,
                                                     &latch,
                                                     &all_successful](
                                                      std::string& err) {
                bool success = true;

                try {
                    const int nb = compress_buffer_in_place(
                      chunk_ptr,
                      bytes_of_raw_chunk + BLOSC_MAX_OVERHEAD,
                      bytes_of_raw_chunk,
                      *compression_params,
                      bytes_per_px);
                    EXPECT(nb > 0, "Failed to compress chunk");

                    // update shard table with size
                    shard_table[2 * internal_idx + 1] = nb;
                } catch (const std::exception& exc) {
                    err = exc.what();
                    success = false;
                }

                latch.count_down();

                all_successful.fetch_and(static_cast<char>(success));
                return success;
            })),
                   "Failed to push job to thread pool");
        } else {
            // no compression, just update shard table with size
            shard_table[2 * internal_idx + 1] = bytes_of_raw_chunk;
            chunk_latches.at(shard_idx).count_down();
        }
    }

    const auto bucket_name = config_.bucket_name;
    auto connection_pool = s3_connection_pool_;

    // wait for the chunks in each shard to finish compressing, then defragment
    // and write the shard
    std::counting_semaphore<MAX_CONCURRENT_FILES> semaphore(
      MAX_CONCURRENT_FILES);
    for (auto shard_idx = 0; shard_idx < n_shards; ++shard_idx) {
        const auto& data_path = data_paths_[shard_idx];
        EXPECT(thread_pool_->push_job(
                 std::move([shard_idx,
                            is_s3,
                            &data_path,
                            &chunk_table = shard_tables_[shard_idx],
                            file_offset = &shard_file_offsets_[shard_idx],
                            write_table,
                            shard_ptr = data_buffers_[shard_idx].data(),
                            bucket_name,
                            connection_pool,
                            &semaphore,
                            &shard_latch,
                            &chunk_latch = chunk_latches.at(shard_idx),
                            &all_successful,
                            this](std::string& err) {
                     chunk_latch.wait();

                     bool success = true;
                     std::unique_ptr<Sink> sink;

                     try {
                         // defragment chunks in shard
                         const auto shard_size =
                           compute_chunk_offsets_and_defrag_(shard_idx);

                         semaphore.acquire();
                         if (s3_data_sinks_.contains(data_path)) {
                             sink = std::move(s3_data_sinks_[data_path]);
                         } else if (is_s3) {
                             sink = make_s3_sink(
                               *bucket_name, data_path, connection_pool);
                         } else {
                             sink =
                               make_file_sink(data_path, *file_offset == 0);
                         }

                         std::span shard_data(shard_ptr, shard_size);
                         success = sink->write(*file_offset, shard_data);
                         if (!success) {
                             semaphore.release();

                             err = "Failed to write shard at path " + data_path;
                             shard_latch.count_down();
                             all_successful = 0;
                             return false;
                         }

                         *file_offset += shard_size;

                         if (write_table) {
                             const auto* table_ptr =
                               reinterpret_cast<std::byte*>(chunk_table.data());
                             const auto table_size =
                               chunk_table.size() * sizeof(uint64_t);
                             EXPECT(sink->write(*file_offset,
                                                { table_ptr, table_size }),
                                    "Failed to write table");

                             // compute crc32 checksum of the table
                             uint32_t checksum = crc32c::Crc32c(
                               reinterpret_cast<const uint8_t*>(table_ptr),
                               table_size);
                             EXPECT(sink->write(
                                      *file_offset + table_size,
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
                           "Failed to flush data: " + std::string(exc.what());
                         success = false;
                     }
                     semaphore.release();

                     // save the S3 sink for later
                     if (is_s3 && !s3_data_sinks_.contains(data_path)) {
                         s3_data_sinks_.emplace(data_path, std::move(sink));
                     } else if (is_s3) {
                         s3_data_sinks_[data_path] = std::move(sink);
                     }

                     shard_latch.count_down();

                     all_successful.fetch_and(static_cast<char>(success));
                     return success;
                 })),
               "Failed to push job to thread pool");
    }

    // wait for all threads to finish
    shard_latch.wait();

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
zarr::ZarrV3ArrayWriter::write_array_metadata_()
{
    if (!make_metadata_sink_()) {
        return false;
    }

    using json = nlohmann::json;

    std::vector<size_t> array_shape, chunk_shape, shard_shape;
    const auto& dims = config_.dimensions;

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
    metadata["data_type"] = sample_type_to_dtype(config_.dtype);
    metadata["storage_transformers"] = json::array();

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

    if (config_.compression_params) {
        const auto params = *config_.compression_params;

        auto compression_config = json::object();
        compression_config["blocksize"] = 0;
        compression_config["clevel"] = params.clevel;
        compression_config["cname"] = params.codec_id;
        compression_config["shuffle"] = shuffle_to_string(params.shuffle);
        compression_config["typesize"] = bytes_of_type(config_.dtype);

        auto compression_codec = json::object();
        compression_codec["configuration"] = compression_config;
        compression_codec["name"] = "blosc";
        configuration["codecs"].push_back(compression_codec);
    }

    sharding_indexed["configuration"] = configuration;

    codecs.push_back(sharding_indexed);

    metadata["codecs"] = codecs;

    std::string metadata_str = metadata.dump(4);
    std::span data = { reinterpret_cast<std::byte*>(metadata_str.data()),
                       metadata_str.size() };

    return metadata_sink_->write(0, data);
}

void
zarr::ZarrV3ArrayWriter::close_sinks_()
{
    data_paths_.clear();

    for (auto& [path, sink] : s3_data_sinks_) {
        EXPECT(
          finalize_sink(std::move(sink)), "Failed to finalize sink at ", path);
    }
    s3_data_sinks_.clear();
}

bool
zarr::ZarrV3ArrayWriter::should_rollover_() const
{
    const auto& dims = config_.dimensions;
    const auto& append_dim = dims->final_dim();
    size_t frames_before_flush =
      append_dim.chunk_size_px * append_dim.shard_size_chunks;
    for (auto i = 1; i < dims->ndims() - 2; ++i) {
        frames_before_flush *= dims->at(i).array_size_px;
    }

    CHECK(frames_before_flush > 0);
    return frames_written_ % frames_before_flush == 0;
}

#include "macros.hh"
#include "zarrv3.array.writer.hh"
#include "sink.hh"
#include "zarr.common.hh"

#include <nlohmann/json.hpp>
#include <crc32c/crc32c.h>

#include <algorithm> // std::fill
#include <latch>
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

template<typename T>
std::vector<size_t>
argsort(const std::vector<T>& arr)
{
    std::vector<size_t> indices(arr.size());
    std::iota(indices.begin(), indices.end(), 0);

    std::stable_sort(
      indices.begin(), indices.end(), [&arr](size_t left, size_t right) {
          return arr[left] < arr[right];
      });

    return indices;
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
  , flushed_count_{ 0 }
{
    const auto number_of_shards = config_.dimensions->number_of_shards();
    const auto chunks_per_shard = config_.dimensions->chunks_per_shard();

    shard_file_offsets_.resize(number_of_shards, 0);
    shard_tables_.resize(number_of_shards);

    for (auto& table : shard_tables_) {
        table.resize(2 * chunks_per_shard);
        std::fill(
          table.begin(), table.end(), std::numeric_limits<uint64_t>::max());
    }
}

size_t
zarr::ZarrV3ArrayWriter::defragment_chunks_in_shard_(uint32_t shard_index)
{
    const auto& dims = config_.dimensions;

    const auto n_layers = dims->final_dim().shard_size_chunks;
    EXPECT(n_layers > 0, "Shard size of 0 in append dimension");

    const auto n_chunks_in_memory = dims->chunks_per_shard() / n_layers;
    const auto nbytes_chunk = bytes_to_allocate_per_chunk_();

    if (!config_.compression_params) {
        return n_chunks_in_memory * nbytes_chunk;
    }

    const auto n_shards = dims->number_of_shards();
    CHECK(shard_index < n_shards);
    const auto n_chunks_total = dims->number_of_chunks_in_memory();

    std::vector<uint32_t> chunks_in_shard;
    std::vector<uint32_t> internal_indices;

    for (auto i = 0; i < n_chunks_total; ++i) {
        if (dims->shard_index_for_chunk(i) == shard_index) {
            chunks_in_shard.push_back(i);
            internal_indices.push_back(dims->shard_internal_index(i));
        }
    }

    const auto argsorted_indices = argsort(internal_indices);
    if (!std::is_sorted(argsorted_indices.begin(), argsorted_indices.end())) {
        std::vector tmp_copy(chunks_in_shard);
        for (auto i = 0; i < chunks_in_shard.size(); ++i) {
            chunks_in_shard[i] = tmp_copy[argsorted_indices[i]];
        }
    }

    // size of first chunk in shard
    size_t shard_size = chunk_sizes_compressed_[chunks_in_shard[0]];

    auto& buffer = data_buffers_[shard_index];
    for (auto i = 1; i < n_chunks_in_memory; ++i) {
        const auto this_chunk = chunks_in_shard[i];
        const auto chunk_size = chunk_sizes_compressed_[this_chunk];
        const auto offset_to_copy_from = i * nbytes_chunk;

        std::copy(buffer.begin() + offset_to_copy_from,
                  buffer.begin() + offset_to_copy_from + chunk_size,
                  buffer.begin() + shard_size);

        shard_size += chunk_size;
    }

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
    data_buffers_.resize(n_shards); // no-op if already the correct size

    const auto n_bytes = bytes_to_allocate_per_chunk_();

    const auto n_layers = dims->final_dim().shard_size_chunks;
    EXPECT(n_layers > 0, "Shard size of 0 in append dimension");

    const auto n_chunks = dims->chunks_per_shard() / n_layers;

    for (auto& buf : data_buffers_) {
        buf.resize(n_chunks * n_bytes);
        std::fill(buf.begin(), buf.end(), std::byte(0));
    }

    std::fill(
      chunk_sizes_compressed_.begin(), chunk_sizes_compressed_.end(), n_bytes);
}

BytePtr
zarr::ZarrV3ArrayWriter::get_chunk_data_(uint32_t index)
{
    const auto shard_idx = config_.dimensions->shard_index_for_chunk(index);
    auto& shard = data_buffers_[shard_idx];

    const auto internal_idx = config_.dimensions->shard_internal_index(index);
    const auto n_bytes = bytes_to_allocate_per_chunk_();
    const auto offset = internal_idx * n_bytes;

    const auto shard_size = shard.size();
    CHECK(offset + n_bytes <= shard_size);
    return shard.data() + offset;
}

bool
zarr::ZarrV3ArrayWriter::compress_and_flush_data_()
{
    // create shard files if they don't exist
    if (data_sinks_.empty() && !make_data_sinks_()) {
        return false;
    }

    const auto n_shards = config_.dimensions->number_of_shards();
    CHECK(data_sinks_.size() == n_shards);

    // construct shard indices for each chunk
    std::vector<std::vector<size_t>> chunk_in_shards(n_shards);
    const auto chunks_in_memory =
      config_.dimensions->number_of_chunks_in_memory();
    auto chunk_group_offset = flushed_count_ * chunks_in_memory;
    for (auto i = 0; i < chunks_in_memory; ++i) {
        const auto index =
          config_.dimensions->shard_index_for_chunk(i + chunk_group_offset);
        chunk_in_shards[index].push_back(i);
    }

    std::atomic<char> all_successful = 1;

    auto write_table = is_finalizing_ || should_rollover_();
    std::latch shard_latch(n_shards);
    std::unordered_map<int, std::latch> chunk_latches;

    // queue jobs to compress all chunks
    for (auto i = 0; i < n_shards; ++i) {
        const auto& chunks = chunk_in_shards.at(i);

        chunk_latches.emplace(i, chunks.size());

        for (const auto& chunk : chunks) {
            EXPECT(thread_pool_->push_job(std::move(
                     [&chunk_latch = chunk_latches.at(i), chunk, this](
                       std::string& err) {
                         bool success = true;
                         try {
                             EXPECT(compress_chunk_(chunk),
                                    "Failed to compress chunk ",
                                    chunk);
                         } catch (const std::exception& exc) {
                             err = "Failed to compress chunk: " +
                                   std::string(exc.what());
                             success = false;
                         }

                         chunk_latch.count_down();
                         return success;
                     })),
                   "Failed to push job to thread pool");
        }
    }

    // wait for the chunks in each shard to finish compressing, then defragment
    // and write the shard
    for (auto i = 0; i < n_shards; ++i) {
        const auto& chunks = chunk_in_shards.at(i);
        auto& chunk_table = shard_tables_.at(i);
        auto* file_offset = &shard_file_offsets_.at(i);
        auto& chunk_latch = chunk_latches.at(i);

        EXPECT(thread_pool_->push_job(std::move([&sink = data_sinks_.at(i),
                                                 &chunks,
                                                 &chunk_table,
                                                 &chunk_latch,
                                                 &all_successful,
                                                 &shard_latch,
                                                 i,
                                                 write_table,
                                                 file_offset,
                                                 chunk_group_offset,
                                                 this](std::string& err) {
            bool success = true;
            chunk_latch.wait();

            try {
                // defragment chunks in shard
                const auto shard_size = defragment_chunks_in_shard_(i);

                std::span shard_data(data_buffers_[i].data(), shard_size);
                success = sink->write(*file_offset, shard_data);
                if (!success) {
                    err = "Failed to write shard";
                    return false;
                }

                // update the chunk table with the correct offsets and sizes
                for (const auto& chunk_idx : chunks) {
                    const auto chunk_size = chunk_sizes_compressed_[chunk_idx];
                    const auto internal_idx =
                      config_.dimensions->shard_internal_index(
                        chunk_idx + chunk_group_offset);
                    chunk_table[2 * internal_idx] = *file_offset;
                    chunk_table[2 * internal_idx + 1] = chunk_size;

                    *file_offset += chunk_size;
                }

                if (write_table) {
                    const auto* table_ptr =
                      reinterpret_cast<std::byte*>(chunk_table.data());
                    const auto table_size =
                      chunk_table.size() * sizeof(uint64_t);
                    EXPECT(sink->write(*file_offset, { table_ptr, table_size }),
                           "Failed to write table");

                    // compute crc32 checksum of the table
                    uint32_t checksum = crc32c::Crc32c(
                      reinterpret_cast<const uint8_t*>(table_ptr), table_size);
                    EXPECT(
                      sink->write(*file_offset + table_size,
                                  { reinterpret_cast<std::byte*>(&checksum),
                                    sizeof(checksum) }),
                      "Failed to write checksum");
                }
            } catch (const std::exception& exc) {
                err = "Failed to flush data: " + std::string(exc.what());
                success = false;
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
        flushed_count_ = 0;
    } else {
        ++flushed_count_;
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

    size_t append_size = frames_written_;
    for (auto i = config_.dimensions->ndims() - 3; i > 0; --i) {
        const auto& dim = config_.dimensions->at(i);
        const auto& array_size_px = dim.array_size_px;
        CHECK(array_size_px);
        append_size = (append_size + array_size_px - 1) / array_size_px;
    }
    array_shape.push_back(append_size);

    const auto& final_dim = config_.dimensions->final_dim();
    chunk_shape.push_back(final_dim.chunk_size_px);
    shard_shape.push_back(final_dim.shard_size_chunks * chunk_shape.back());
    for (auto i = 1; i < config_.dimensions->ndims(); ++i) {
        const auto& dim = config_.dimensions->at(i);
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

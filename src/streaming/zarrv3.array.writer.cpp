#include "macros.hh"
#include "zarrv3.array.writer.hh"
#include "sink.creator.hh"
#include "zarr.common.hh"

#include <nlohmann/json.hpp>
#include <zlib.h> // crc32

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

bool
zarr::ZarrV3ArrayWriter::flush_impl_()
{
    // create shard files if they don't exist
    if (data_sinks_.empty() && !make_data_sinks_()) {
        return false;
    }

    const auto n_shards = config_.dimensions->number_of_shards();
    CHECK(data_sinks_.size() == n_shards);

    // get shard indices for each chunk
    std::vector<std::vector<size_t>> chunk_in_shards(n_shards);
    const auto chunks_in_memory =
      config_.dimensions->number_of_chunks_in_memory();
    auto chunk_group_offset = flushed_count_ * chunks_in_memory;
    for (auto i = 0; i < chunk_buffers_.size(); ++i) {
        const auto index =
          config_.dimensions->shard_index_for_chunk(i + chunk_group_offset);
        chunk_in_shards[index].push_back(i);
    }

    // write out chunks to shards
    auto write_table = is_finalizing_ || should_rollover_();
    std::latch latch(n_shards);
    for (auto i = 0; i < n_shards; ++i) {
        const auto& chunks = chunk_in_shards.at(i);
        auto& chunk_table = shard_tables_.at(i);
        auto* file_offset = &shard_file_offsets_.at(i);

        EXPECT(thread_pool_->push_job(std::move([&sink = data_sinks_.at(i),
                                                 &chunks,
                                                 &chunk_table,
                                                 file_offset,
                                                 write_table,
                                                 &latch,
                                                 chunk_group_offset,
                                                 this](std::string& err) {
            bool success = false;

            try {
                for (const auto& chunk_idx : chunks) {
                    auto& chunk = chunk_buffers_.at(chunk_idx);
                    std::span data(chunk);
                    success = sink->write(*file_offset, data);
                    if (!success) {
                        break;
                    }

                    const auto internal_idx =
                      config_.dimensions->shard_internal_index(
                        chunk_idx + chunk_group_offset);
                    chunk_table.at(2 * internal_idx) = *file_offset;
                    chunk_table.at(2 * internal_idx + 1) = chunk.size();

                    *file_offset += chunk.size();
                }

                if (success && write_table) {
                    const auto* table =
                      reinterpret_cast<std::byte*>(chunk_table.data());
                    std::span data{ table,
                                    chunk_table.size() * sizeof(uint64_t) };
                    success = sink->write(*file_offset, data);
                    EXPECT(success, "Failed to write table");

                    *file_offset += data.size();

                    // compute crc32 checksum of the table
                    const auto* table_c =
                      reinterpret_cast<const unsigned char*>(table);

                    uint32_t crc = crc32(0L, Z_NULL, 0);
                    crc = crc32(crc, table_c, data.size());
                    std::span crc_data{ reinterpret_cast<std::byte*>(&crc),
                                        sizeof(crc) };
                    success = sink->write(*file_offset, crc_data);
                }
            } catch (const std::exception& exc) {
                err = "Failed to write chunk: " + std::string(exc.what());
            }

            latch.count_down();
            return success;
        })),
               "Failed to push job to thread pool");
    }

    // wait for all threads to finish
    latch.wait();

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

    return true;
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

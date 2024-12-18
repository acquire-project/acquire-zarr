#include "macros.hh"
#include "zarrv3.array.writer.hh"
#include "sink.creator.hh"
#include "zarr.common.hh"

#include <nlohmann/json.hpp>

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
{
    const auto n_shards = config_.dimensions->number_of_shards();
    const auto chunks_in_memory =
      config_.dimensions->number_of_chunks_in_memory();
    const auto chunks_per_shard = config_.dimensions->chunks_per_shard();

    shard_file_offsets_.resize(n_shards, 0);
    shard_tables_.resize(n_shards);

    for (auto& table : shard_tables_) {
        table.resize(2 * chunks_per_shard);
        std::fill(
          table.begin(), table.end(), std::numeric_limits<uint64_t>::max());
    }

    // get shard indices for each chunk
    chunk_in_shards_.resize(n_shards);
    for (auto i = 0; i < chunks_in_memory; ++i) {
        const auto index = config_.dimensions->shard_index_for_chunk(i);
        chunk_in_shards_[index].push_back(i);
    }
}

std::string
zarr::ZarrV3ArrayWriter::data_root_() const
{
    return config_.store_path + "/data/root/" +
           std::to_string(config_.level_of_detail) + "/c" +
           std::to_string(append_chunk_index_);
}

std::string
zarr::ZarrV3ArrayWriter::metadata_path_() const
{
    return config_.store_path + "/meta/root/" +
           std::to_string(config_.level_of_detail) + ".array.json";
}

bool
zarr::ZarrV3ArrayWriter::make_data_sinks_()
{
    const auto data_root = data_root_();

    if (is_s3_array_()) {
        SinkCreator creator(thread_pool_, s3_connection_pool_);
        if (!creator.make_data_s3_sinks(*config_.bucket_name,
                                        data_root,
                                        config_.dimensions.get(),
                                        shards_along_dimension,
                                        data_sinks_)) {
            LOG_ERROR("Failed to create data sinks in ",
                      data_root,
                      " for bucket ",
                      *config_.bucket_name);
            return false;
        }
    } else {
        const auto n_shards = config_.dimensions->number_of_shards();
        const auto chunks_in_memory =
          config_.dimensions->number_of_chunks_in_memory();
        const auto chunks_before_flush = chunks_in_memory / n_shards;
        const auto chunks_per_shard = config_.dimensions->chunks_per_shard();

        if (!make_shard_writers(data_root,
                                chunks_before_flush,
                                chunks_per_shard,
                                *config_.dimensions,
                                thread_pool_,
                                shard_writers_)) {
            LOG_ERROR("Failed to create shard writers in ", data_root);
            return false;
        }
    }

    return true;
}

void
zarr::ZarrV3ArrayWriter::compress_and_flush_()
{
    if (bytes_to_flush_ == 0) {
        LOG_DEBUG("No data to flush");
        return;
    }

    // create shard files if they don't exist
    const auto n_shards = chunk_in_shards_.size();

    if (is_s3_array_()) {
        if (data_sinks_.empty()) {
            CHECK(make_data_sinks_());
        }
        CHECK(data_sinks_.size() == n_shards);
        CHECK(compress_and_flush_to_s3_());
    } else {
        if (shard_writers_.empty()) {
            CHECK(make_data_sinks_());
        }
        CHECK(shard_writers_.size() == n_shards);
        CHECK(compress_and_flush_to_filesystem_());
    }
}

bool
zarr::ZarrV3ArrayWriter::compress_and_flush_to_filesystem_()
{
    const auto n_chunks = chunk_buffers_.size();
    std::latch latch(n_chunks);

    for (auto i = 0; i < n_chunks; ++i) {
        const auto shard_index = config_.dimensions->shard_index_for_chunk(i);
        auto& shard = shard_writers_[shard_index];

        const auto internal_index = config_.dimensions->shard_internal_index(i);

        auto job =
          [this, i, internal_index, &shard, &latch](std::string& err) -> bool {
            bool success = true;
            if (compress_chunk_buffer_(i)) { // no-op if compression is disabled
                shard->add_chunk(&chunk_buffers_[i], internal_index);
            } else {
                err = "Failed to compress chunk " + std::to_string(i);
                success = false;
            }
            latch.count_down();

            return success;
        };

        EXPECT(thread_pool_->push_job(std::move(job)),
               "Failed to push job to thread pool");
    }

    latch.wait();

    return true;
}

bool
zarr::ZarrV3ArrayWriter::compress_and_flush_to_s3_()
{
    const auto n_shards = chunk_in_shards_.size();

    const auto write_table = is_finalizing_ || should_rollover_();

    // write out chunks to shards
    std::latch latch(n_shards);

    for (auto i = 0; i < n_shards; ++i) {
        const auto& chunks = chunk_in_shards_.at(i);
        auto& chunk_table = shard_tables_.at(i);
        auto* file_offset = &shard_file_offsets_.at(i);

        EXPECT(thread_pool_->push_job([&sink = data_sinks_.at(i),
                                       &chunks,
                                       &chunk_table,
                                       file_offset,
                                       write_table,
                                       &latch,
                                       this](std::string& err) {
            bool success = false;

            try {
                for (const auto& chunk_idx : chunks) {
                    // no-op if compression is disabled
                    compress_chunk_buffer_(chunk_idx);

                    auto& chunk = chunk_buffers_.at(chunk_idx);
                    std::span data{ reinterpret_cast<std::byte*>(chunk.data()),
                                    chunk.size() };
                    success = sink->write(*file_offset, data);
                    if (!success) {
                        break;
                    }

                    const auto internal_idx =
                      config_.dimensions->shard_internal_index(chunk_idx);
                    chunk_table.at(2 * internal_idx) = *file_offset;
                    chunk_table.at(2 * internal_idx + 1) = chunk.size();

                    *file_offset += chunk.size();
                }

                if (success && write_table) {
                    auto* table =
                      reinterpret_cast<std::byte*>(chunk_table.data());
                    std::span data{ table,
                                    chunk_table.size() * sizeof(uint64_t) };
                    success = sink->write(*file_offset, data);
                }
            } catch (const std::exception& exc) {
                err = "Failed to write chunk: " + std::string(exc.what());
            }

            latch.count_down();
            return success;
        }),
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
    }

    return true;
}

void
zarr::ZarrV3ArrayWriter::close_sinks_()
{
    for (auto i = 0; i < data_sinks_.size(); ++i) {
        EXPECT(finalize_sink(std::move(data_sinks_[i])),
               "Failed to finalize sink ",
               i);
    }
    data_sinks_.clear();

    for (auto i = 0; i < shard_writers_.size(); ++i) {
        EXPECT(finalize_shard_writer(std::move(shard_writers_[i])),
               "Failed to finalize shard writer ",
               i);
    }
    shard_writers_.clear();
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
    shard_shape.push_back(final_dim.shard_size_chunks);
    for (auto i = 1; i < config_.dimensions->ndims(); ++i) {
        const auto& dim = config_.dimensions->at(i);
        array_shape.push_back(dim.array_size_px);
        chunk_shape.push_back(dim.chunk_size_px);
        shard_shape.push_back(dim.shard_size_chunks);
    }

    json metadata;
    metadata["attributes"] = json::object();
    metadata["chunk_grid"] = json::object({
      { "chunk_shape", chunk_shape },
      { "separator", "/" },
      { "type", "regular" },
    });

    metadata["chunk_memory_layout"] = "C";
    metadata["data_type"] = sample_type_to_dtype(config_.dtype);
    metadata["extensions"] = json::array();
    metadata["fill_value"] = 0;
    metadata["shape"] = array_shape;

    if (config_.compression_params) {
        const auto params = *config_.compression_params;
        metadata["compressor"] = json::object({
          { "codec", "https://purl.org/zarr/spec/codec/blosc/1.0" },
          { "configuration",
            json::object({
              { "blocksize", 0 },
              { "clevel", params.clevel },
              { "cname", params.codec_id },
              { "shuffle", params.shuffle },
            }) },
        });
    } else {
        metadata["compressor"] = nullptr;
    }

    // sharding storage transformer
    // TODO (aliddell):
    // https://github.com/zarr-developers/zarr-python/issues/877
    metadata["storage_transformers"] = json::array();
    metadata["storage_transformers"][0] = json::object({
      { "type", "indexed" },
      { "extension",
        "https://purl.org/zarr/spec/storage_transformers/sharding/1.0" },
      { "configuration",
        json::object({
          { "chunks_per_shard", shard_shape },
        }) },
    });

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

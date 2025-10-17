#include "macros.hh"
#include "s3.array.hh"
#include "zarr.common.hh"

#include <blosc.h>
#include <crc32c/crc32c.h>

#include <cstring> // memcpy
#include <future>

zarr::S3Array::S3Array(std::shared_ptr<ArrayConfig> config,
                       std::shared_ptr<ThreadPool> thread_pool,
                       std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : Array(config, thread_pool)
  , S3Storage(*config->bucket_name, s3_connection_pool)
{
    CHECK(config_->dimensions);
}

bool
zarr::S3Array::write_metadata_()
{
    std::string metadata;
    if (!make_metadata_(metadata)) {
        LOG_ERROR("Failed to make metadata.");
        return false;
    }

    if (last_written_metadata_ == metadata) {
        return true; // no changes
    }
    const std::string key = node_path_() + "/zarr.json";

    bool success;
    if ((success = write_string(key, metadata, 0) && finalize_object(key))) {
        last_written_metadata_ = metadata;
    }
    return success;
}

std::string
zarr::S3Array::index_location_() const
{
    return "end";
}

bool
zarr::S3Array::compress_and_flush_data_()
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

void
zarr::S3Array::close_io_streams_()
{
    for (const auto& key : data_paths_) {
        EXPECT(finalize_object(key), "Failed to finalize S3 object at ", key);
    }

    data_paths_.clear();
}

bool
zarr::S3Array::compress_chunks_()
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
zarr::S3Array::update_table_entries_()
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

bool
zarr::S3Array::flush_data_()
{
    // construct paths to shard sinks if they don't already exist
    if (data_paths_.empty()) {
        make_data_paths_();
    }

    const auto& dims = config_->dimensions;

    const auto n_shards = dims->number_of_shards();
    CHECK(data_paths_.size() == n_shards);

    std::atomic<char> all_successful = 1;

    std::vector<std::future<void>> futures;

    // wait for the chunks in each shard to finish compressing, then defragment
    // and write the shard
    for (auto shard_idx = 0; shard_idx < n_shards; ++shard_idx) {
        const std::string data_path = data_paths_[shard_idx];
        auto* file_offset = shard_file_offsets_.data() + shard_idx;

        auto promise = std::make_shared<std::promise<void>>();
        futures.emplace_back(promise->get_future());

        auto job =
          [shard_idx, data_path, file_offset, promise, &all_successful, this](
            std::string& err) {
              bool success = true;

              try {
                  const auto shard_data = collect_chunks_(shard_idx);
                  if (shard_data.chunks.empty()) {
                      LOG_ERROR("Failed to collect chunks for shard ",
                                shard_idx);
                      return false;
                  }
                  if (shard_data.offset != *file_offset) {
                      LOG_ERROR("Inconsistent file offset for shard ",
                                shard_idx,
                                ": expected ",
                                *file_offset,
                                ", got ",
                                shard_data.offset);
                      return false;
                  }

                  size_t layer_offset = shard_data.offset;
                  for (auto& chunk : shard_data.chunks) {
                      if (!write_binary(data_path, chunk, layer_offset)) {
                          err = "Failed to write chunk " +
                                std::to_string(shard_idx) + " at offset " +
                                std::to_string(layer_offset) + " to path " +
                                data_path;
                          success = false;
                          break;
                      }
                      layer_offset += chunk.size();
                  }
                  *file_offset = layer_offset;
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

    return static_cast<bool>(all_successful);
}

bool
zarr::S3Array::flush_tables_()
{
    // construct paths to shard sinks if they don't already exist
    if (data_paths_.empty()) {
        make_data_paths_();
    }

    const auto& dims = config_->dimensions;
    const auto n_shards = dims->number_of_shards();

    for (auto shard_idx = 0; shard_idx < n_shards; ++shard_idx) {
        const auto* shard_table = shard_tables_.data() + shard_idx;
        auto* file_offset = shard_file_offsets_.data() + shard_idx;

        const size_t table_size = shard_table->size() * sizeof(uint64_t);
        std::vector<uint8_t> table(table_size + sizeof(uint32_t), 0);

        memcpy(table.data(), shard_table->data(), table_size);

        // compute crc32 checksum of the table
        const uint32_t checksum = crc32c::Crc32c(table.data(), table_size);
        memcpy(table.data() + table_size, &checksum, sizeof(uint32_t));

        std::string data_path = data_paths_[shard_idx];

        if (!write_binary(data_path, table, *file_offset)) {
            LOG_ERROR("Failed to write table and checksum to shard ",
                      shard_idx,
                      " at path ",
                      data_path);
            return false;
        }
    }

    // don't reset state if we're closing
    if (!is_closing_) {
        for (auto& table : shard_tables_) {
            std::ranges::fill(table, std::numeric_limits<uint64_t>::max());
        }
        std::ranges::fill(shard_file_offsets_, 0);
        current_layer_ = 0;
    }

    return true;
}

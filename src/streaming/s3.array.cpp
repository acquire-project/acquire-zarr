#include "macros.hh"
#include "s3.array.hh"

#include <crc32c/crc32c.h>

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
    const std::string path = node_path_() + "/zarr.json";

    return write_string_(path, metadata, 0);
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
                  // consolidate chunks in shard
                  const auto shard_data = consolidate_chunks_(shard_idx);
                  if (!write_binary_(data_path, shard_data, *file_offset)) {
                      err = "Failed to write shard at path " + data_path;
                      success = false;
                  } else {
                      *file_offset = shard_data.size();
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

        if (!write_binary_(data_path, table, *file_offset)) {
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

void
zarr::S3Array::close_io_streams_()
{
    for (const auto& key : data_paths_) {
        s3_objects_.erase(key);
    }

    data_paths_.clear();
}

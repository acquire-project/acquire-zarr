#include "fs.array.hh"
#include "macros.hh"

#include <crc32c/crc32c.h>

#include <cstring> // memcp
#include <filesystem>
#include <future>
#include <unordered_set>

void*
make_flags();

void
destroy_flags(void* flags);

bool
seek_and_write(void* handle, size_t offset, ConstByteSpan data);

namespace fs = std::filesystem;

namespace {
std::vector<std::string>
get_parent_paths(const std::vector<std::string>& file_paths)
{
    std::unordered_set<std::string> unique_paths;
    for (const auto& file_path : file_paths) {
        unique_paths.emplace(fs::path(file_path).parent_path().string());
    }

    return { unique_paths.begin(), unique_paths.end() };
}

bool
make_dirs(const std::vector<std::string>& dir_paths,
          std::shared_ptr<zarr::ThreadPool> thread_pool)
{
    if (dir_paths.empty()) {
        return true;
    }
    EXPECT(thread_pool, "Thread pool not provided.");

    std::atomic<char> all_successful = 1;
    const std::unordered_set unique_paths(dir_paths.begin(), dir_paths.end());

    std::vector<std::future<void>> futures;

    for (const auto& path : unique_paths) {
        auto promise = std::make_shared<std::promise<void>>();
        futures.emplace_back(promise->get_future());

        auto job = [path, promise, &all_successful](std::string& err) {
            bool success = true;
            try {
                if (fs::is_directory(path) || path.empty()) {
                    promise->set_value();
                    return success;
                }

                std::error_code ec;
                if (!fs::create_directories(path, ec) &&
                    !fs::is_directory(path)) {
                    err = "Failed to create directory '" + path +
                          "': " + ec.message();
                    success = false;
                }
            } catch (const std::exception& exc) {
                err =
                  "Failed to create directory '" + path + "': " + exc.what();
                success = false;
            }

            promise->set_value();
            all_successful.fetch_and(success);
            return success;
        };

        if (thread_pool->n_threads() == 1 || !thread_pool->push_job(job)) {
            if (std::string err; !job(err)) {
                LOG_ERROR(err);
            }
        }
    }

    // wait for all jobs to finish
    for (auto& future : futures) {
        future.wait();
    }

    return all_successful;
}
} // namespace

zarr::FSArray::FSArray(std::shared_ptr<ArrayConfig> config,
                       std::shared_ptr<ThreadPool> thread_pool,
                       std::shared_ptr<FileHandlePool> file_handle_pool)
  : Array(config, thread_pool)
  , FSStorage(file_handle_pool)
{
}

bool
zarr::FSArray::write_metadata_()
{
    std::string metadata;
    if (!make_metadata_(metadata)) {
        LOG_ERROR("Failed to make metadata.");
        return false;
    }

    if (last_written_metadata_ == metadata) {
        return true; // no changes
    }
    const std::string path = node_path_() + "/zarr.json";

    bool success;
    if ((success = write_string(path, metadata, 0))) {
        last_written_metadata_ = metadata;
    }

    return success;
}

bool
zarr::FSArray::flush_data_()
{
    // construct paths to shard sinks if they don't already exist
    if (data_paths_.empty()) {
        make_data_paths_();
    }

    // create parent directories if needed
    const auto parent_paths = get_parent_paths(data_paths_);
    CHECK(make_dirs(parent_paths, thread_pool_)); // no-op if they exist

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

        const auto shard_data = collect_chunks_(shard_idx);
        if (shard_data.chunks.empty()) {
            LOG_ERROR("Failed to collect chunks for shard ", shard_idx);
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
            auto promise = std::make_shared<std::promise<void>>();
            futures.emplace_back(promise->get_future());

            const auto handle = get_handle_(data_path);
            if (handle == nullptr) {
                LOG_ERROR("Failed to get file handle for ", data_path);
                return false;
            }

            const auto chunk_size = chunk.size(); // we move it below
            auto job = [data_path,
                        handle,
                        layer_offset,
                        chunk = std::move(chunk),
                        promise](std::string& err) {
                bool success;
                try {
                    success = seek_and_write(handle.get(), layer_offset, chunk);
                } catch (const std::exception& exc) {
                    err = "Failed to write chunk at offset " +
                          std::to_string(layer_offset) + " to path " +
                          data_path + ": " + exc.what();
                    success = false;
                }

                promise->set_value();
                return success;
            };

            // one thread is reserved for processing the frame queue and runs
            // the entire lifetime of the stream
            if (thread_pool_->n_threads() == 1 ||
                !thread_pool_->push_job(job)) {
                std::string err;
                if (!job(err)) {
                    LOG_ERROR(err);
                }
            }

            layer_offset += chunk_size;
        }

        *file_offset = layer_offset;
    }

    // wait for all threads to finish
    // for (auto& future : futures) {
    //     future.wait();
    // }
    //
    // return static_cast<bool>(all_successful);
    return true;
}

bool
zarr::FSArray::flush_tables_()
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

        const auto handle = get_handle_(data_path);
        if (handle == nullptr) {
            LOG_ERROR("Failed to get file handle for ", data_path);
            return false;
        }

        if (!seek_and_write(handle.get(), *file_offset, table)) {
            LOG_ERROR("Failed to write table and checksum to shard ",
                      shard_idx,
                      " at path ",
                      data_path);
            return false;
        }
        *file_offset += table.size();

        handles_.erase(data_path); // close the handle
    }

    // don't reset state if we're closing
    if (!is_closing_) {
        for (auto& table : shard_tables_) {
            std::ranges::fill(table, std::numeric_limits<uint64_t>::max());
        }
        std::ranges::fill(shard_file_offsets_, 0);
    }

    return true;
}

void
zarr::FSArray::close_io_streams_()
{
    for (const auto& path : data_paths_) {
        file_handle_pool_->close_handle(path);
    }

    data_paths_.clear();
}

std::shared_ptr<void>
zarr::FSArray::get_handle_(const std::string& path)
{
    std::unique_lock lock(mutex_);
    if (handles_.contains(path)) {
        return handles_[path];
    }

    void* flags = make_flags();
    const auto handle = file_handle_pool_->get_handle(path, flags);
    destroy_flags(flags);

    handles_.emplace(path, handle);
    return handle;
}

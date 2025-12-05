#include "fs.array.hh"
#include "macros.hh"
#include "zarr.common.hh"

#include <blosc.h>
#include <crc32c/crc32c.h>

#include <cstring> // memcp
#include <filesystem>
#include <ranges>
#include <span>
#include <unordered_set>

void*
make_flags();

void
destroy_flags(void* flags);

bool
seek_and_write(void* handle, size_t offset, std::span<const uint8_t> data);

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

bool
zarr::FSArray::ShardFile::close()
{
    // finish writing chunks
    for (auto& future : chunk_futures) {
        future.wait();
    }
    chunk_futures.clear();

    // compute table checksum and write it out
    const size_t table_size = table.size() * sizeof(uint64_t);
    const auto* table_data = reinterpret_cast<const uint8_t*>(table.data());
    const uint32_t checksum = crc32c::Crc32c(table_data, table_size);

    const size_t table_buffer_size = table.size() * sizeof(uint64_t);
    constexpr size_t checksum_size = sizeof(uint32_t);

    std::vector<uint8_t> table_buffer(table_buffer_size + checksum_size);
    memcpy(table_buffer.data(), table_data, table_size);
    memcpy(table_buffer.data() + table_buffer_size, &checksum, checksum_size);

    if (!seek_and_write(handle.get(), 0, table_buffer)) {
        LOG_ERROR("Failed to write table and checksum for shard at ", path);
        return false;
    }

    return true;
}

zarr::FSArray::FSArray(std::shared_ptr<ArrayConfig> config,
                       std::shared_ptr<ThreadPool> thread_pool,
                       std::shared_ptr<FileHandlePool> file_handle_pool)
  : Array(config, thread_pool)
  , FSStorage(file_handle_pool)
  , table_size_bytes_(config->dimensions->chunks_per_shard() * 2 *
                        sizeof(uint64_t) +
                      sizeof(uint32_t))
{
    std::ranges::fill(shard_file_offsets_, table_size_bytes_);
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

std::string
zarr::FSArray::index_location_() const
{
    return "start";
}

bool
zarr::FSArray::compress_and_flush_data_()
{
    const auto& dims = config_->dimensions;
    const uint32_t chunks_per_shard = dims->chunks_per_shard();
    const auto n_shards = dims->number_of_shards();

    // construct paths to shard sinks if they don't already exist
    if (data_paths_.empty()) {
        make_data_paths_();
        CHECK(data_paths_.size() == n_shards);

        // create parent directories if needed
        const auto parent_paths = get_parent_paths(data_paths_);
        CHECK(make_dirs(parent_paths, thread_pool_)); // no-op if they exist

        // create shard files
        std::unique_lock lock(shard_files_mutex_);
        for (const auto& path : data_paths_) {
            auto shard_file = std::make_shared<ShardFile>();
            shard_file->path = path;
            shard_file->handle = get_handle_(path);
            shard_file->table = std::vector(
              2 * chunks_per_shard, std::numeric_limits<uint64_t>::max());
            shard_file->file_offset = table_size_bytes_;

            shard_files_[path] = std::move(shard_file);
        }
    }

    const uint32_t chunks_in_mem = dims->number_of_chunks_in_memory();
    const uint32_t n_layers = dims->chunk_layers_per_shard();
    const uint32_t chunks_per_layer = chunks_per_shard / n_layers;

    const size_t bytes_per_px = bytes_of_type(config_->dtype);

    // this layer's entries in the shard table begin here
    const uint32_t layer_offset = current_layer_ * chunks_per_layer;

    // this layer's entries in the (global) chunk grid begin here
    const uint32_t chunk_offset = current_layer_ * chunks_in_mem;

    for (auto shard_idx = 0; shard_idx < n_shards; ++shard_idx) {
        const std::string data_path = data_paths_[shard_idx];
        auto shard_file = shard_files_[data_path];

        // chunk storage is at chunk_index - chunk_offset
        const auto chunk_indices_this_layer =
          dims->chunk_indices_for_shard_layer(shard_idx, current_layer_);

        const auto& params = config_->compression_params;

        const size_t future_offset = shard_file->chunk_futures.size();
        shard_file->chunk_futures.resize(shard_file->chunk_futures.size() +
                                         chunk_indices_this_layer.size());

#pragma omp parallel for
        for (auto i = 0; i < chunk_indices_this_layer.size(); ++i) {
            const uint32_t chunk_idx = chunk_indices_this_layer[i];
            CHECK(chunk_idx >= chunk_offset);
            uint32_t internal_index = dims->shard_internal_index(chunk_idx);
            auto promise =
              std::make_shared<std::promise<void>>(); // TODO (not a shared
                                                      // pointer and std::move?)

            auto& chunk_data = chunk_buffers_[chunk_idx - chunk_offset];
            const size_t bytes_of_chunk = chunk_data.size();

            shard_file->chunk_futures[i + future_offset] = promise->get_future();
            // shard_file->chunk_futures.push_back(promise->get_future());

            auto job = [chunk_data = std::move(chunk_data),
                        &params,
                        shard_file,
                        bytes_per_px,
                        internal_index,
                        promise](std::string& err) {
                bool success = true;
                std::vector<uint8_t> compressed;
                const uint8_t* data_out = nullptr;
                uint64_t chunk_size_out = 0;

                try {
                    // compress here
                    if (params) {
                        compressed.resize(chunk_data.size() +
                                          BLOSC_MAX_OVERHEAD);
                        const auto n_bytes_compressed =
                          blosc_compress_ctx(params->clevel,
                                             params->shuffle,
                                             bytes_per_px,
                                             chunk_data.size(),
                                             chunk_data.data(),
                                             compressed.data(),
                                             compressed.size(),
                                             params->codec_id.c_str(),
                                             0,
                                             1);
                        if (n_bytes_compressed <= 0) {
                            err = "blosc_compress_ctx failed with code " +
                                  std::to_string(n_bytes_compressed) +
                                  " for chunk " +
                                  std::to_string(internal_index) + " of shard ";
                            success = false;
                        }
                        data_out = compressed.data();
                        chunk_size_out = n_bytes_compressed;
                    } else {
                        data_out = chunk_data.data();
                        chunk_size_out = chunk_data.size();
                    }
                    EXPECT(success, err);
                    EXPECT(data_out != nullptr, err);
                    EXPECT(chunk_size_out != 0, err);

                    uint64_t file_offset_local;
                    {
                        std::lock_guard lock(shard_file->offset_mutex);
                        file_offset_local = shard_file->file_offset;
                        shard_file->file_offset += chunk_size_out;
                    }

                    // write data
                    success =
                      seek_and_write(shard_file->handle.get(),
                                     file_offset_local,
                                     std::span(data_out, chunk_size_out));
                    EXPECT(success,
                           "Failed to write chunk data to ",
                           shard_file->path,
                           " internal index ",
                           internal_index);

                    // write table entry
                    shard_file->table[2 * internal_index] = file_offset_local;
                    shard_file->table[2 * internal_index + 1] = chunk_size_out;
                } catch (const std::exception& exc) {
                    err = "Failed to write chunk " +
                          std::to_string(internal_index) + " of shard at " +
                          shard_file->path + ": " + exc.what();
                    success = false;
                }

                promise->set_value();
                return success;
            };

            // one thread is reserved for processing the frame queue and runs
            // the entire lifetime of the stream
            if (thread_pool_->n_threads() == 1 ||
                !thread_pool_->push_job(job)) {
                if (std::string err; !job(err)) {
                    LOG_ERROR(err);
                }
            }

            if (!is_closing_) {
                auto& chunk = chunk_buffers_[chunk_idx - chunk_offset];
                chunk.resize(bytes_of_chunk);
                std::ranges::fill(chunk, 0);
            }
        }

        // if we're about to roll over to a new append shard, signal that we're
        // not going to add any more chunks and that we can wait to close
        // if (current_layer_ == n_layers - 1) {
        //     auto job = [shard_file, this](std::string& err) -> bool {
        //         bool success;
        //
        //         try {
        //             success = shard_file->close();
        //             std::unique_lock lock(shard_files_mutex_);
        //             shard_files_.erase(shard_file->path);
        //             file_handle_pool_->close_handle(shard_file->path);
        //             shard_files_cv_.notify_all();
        //         } catch (const std::exception& exc) {
        //             err = exc.what();
        //             success = false;
        //         }
        //
        //         return success;
        //     };
        //
        //     // one thread is reserved for processing the frame queue and runs
        //     // the entire lifetime of the stream
        //     if (thread_pool_->n_threads() == 1 ||
        //         !thread_pool_->push_job(job)) {
        //         if (std::string err; !job(err)) {
        //             LOG_ERROR(err);
        //         }
        //     }
        // }
    }

    return true;
}

void
zarr::FSArray::finalize_append_shard_()
{
    data_paths_.clear();

    if (is_closing_) {
        // close all shards
        for (auto& shard_file : shard_files_ | std::views::values) {
            EXPECT(shard_file->close(),
                   "Failed to close shard file at path ",
                   shard_file->path);
        }
        shard_files_.clear();
        // wait on all the shards to be written out
        // std::unique_lock lock(shard_files_mutex_);
        // shard_files_cv_.wait(lock, [this] { return shard_files_.empty(); });
    }
}

std::shared_ptr<void>
zarr::FSArray::get_handle_(const std::string& path)
{
    std::unique_lock lock(handles_mutex_);
    if (const auto it = handles_.find(path); it != handles_.end()) {
        return it->second;
    }

    void* flags = make_flags();
    const auto handle = file_handle_pool_->get_handle(path, flags);
    destroy_flags(flags);

    handles_.emplace(path, handle);
    return handle;
}

void
zarr::FSArray::write_table_entries_(uint32_t shard_idx)
{
    CHECK(shard_idx < shard_tables_.size());
    const auto& path = data_paths_[shard_idx];
    const auto handle = get_handle_(path);

    EXPECT(
      handle != nullptr, "Failed to get file handle for finalizing ", path);

    // compute table checksum and write it out
    auto& shard_table = shard_tables_[shard_idx];
    const size_t table_size = shard_table.size() * sizeof(uint64_t);
    const auto* table_data =
      reinterpret_cast<const uint8_t*>(shard_table.data());
    const uint32_t checksum = crc32c::Crc32c(table_data, table_size);

    const size_t table_buffer_size = shard_table.size() * sizeof(uint64_t);
    constexpr size_t checksum_size = sizeof(uint32_t);

    std::vector<uint8_t> table_buffer(table_buffer_size + checksum_size);
    memcpy(table_buffer.data(), table_data, table_size);
    memcpy(table_buffer.data() + table_buffer_size, &checksum, checksum_size);

    EXPECT(seek_and_write(handle.get(), 0, table_buffer),
           "Failed to write final checksum for shard at ",
           path);

    std::ranges::fill(shard_table, std::numeric_limits<uint64_t>::max());
    shard_file_offsets_[shard_idx] = table_size_bytes_;
}

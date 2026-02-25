#include "macros.hh"
#include "resource.pool.hh"

#include <stack>
#include <nlohmann/detail/input/parser.hpp>

uint64_t
get_max_active_handles(); // defined in platform.cpp

namespace {
std::string
join(const zarr::S3Settings& s3_settings, const std::string& object_key)
{
    std::stringstream ss;
    ss << s3_settings.endpoint << "/" << s3_settings.bucket_name << "/"
       << object_key;

    return ss.str();
}
} // namespace

zarr::ResourcePool::ResourcePool(size_t max_threads)
  : max_active_files_(get_max_active_handles())
  , max_memory_bytes_(8ULL << 30) // 8 GiB (TODO (aliddell): make configurable)
  , active_memory_bytes_(0)
{
    thread_pool_ = std::make_shared<ThreadPool>(
      max_threads, [this](const std::string& err) { set_error_(err); });
    EXPECT(thread_pool_, "Null pointer: thread pool");
}

const std::optional<std::string>&
zarr::ResourcePool::ResourcePool::error() const
{
    return error_;
}

uint64_t
zarr::ResourcePool::active_file_handles()
{
    cull_unused_files_();

    std::unique_lock lock(files_mutex_);
    return files_.size();
}

uint64_t
zarr::ResourcePool::active_s3_connections()
{
    cull_unused_s3_connections_();

    std::unique_lock lock(s3_connections_mutex_);
    return s3_connections_.size();
}

uint64_t
zarr::ResourcePool::memory_usage()
{
    cull_unused_buffers_();

    std::unique_lock lock(buffers_mutex_);
    return active_memory_bytes_;
}

std::shared_ptr<zarr::ThreadPool>
zarr::ResourcePool::thread_pool() const
{
    return thread_pool_;
}

std::shared_ptr<zarr::FileHandle>
zarr::ResourcePool::get_file_handle(const std::string& path, void* flags)
{
    std::unique_lock lock(files_mutex_);
    if (files_.size() >= max_active_files_) {
        files_cv_.wait(lock, [this]() {
            cull_unused_files_();
            return files_.size() < max_active_files_;
        });
    }

    std::shared_ptr<FileHandle> conn;
    if (const auto file_it = files_.find(path);
        file_it == files_.end() || file_it->second.expired()) {
        // flush and close on destruct
        conn = std::shared_ptr<FileHandle>(new FileHandle(path, flags),
                                           FileHandle::FlushDeleter());
        files_.emplace(path, conn);
    } else {
        conn = file_it->second.lock();
    }

    return conn;
}

std::shared_ptr<zarr::S3Connection>
zarr::ResourcePool::get_s3_connection(const std::string& object_key,
                                      const S3Settings& s3_settings)
{
    const std::string full_path = join(s3_settings, object_key);
    std::shared_ptr<S3Connection> conn;
    if (const auto conn_it = s3_connections_.find(full_path);
        conn_it == s3_connections_.end() || conn_it->second.expired()) {
        conn = std::make_shared<S3Connection>(s3_settings);
        s3_connections_.emplace(full_path, conn);
    } else {
        conn = conn_it->second.lock();
    }

    return conn;
}

std::shared_ptr<zarr::ResourcePool::DataBuffer>
zarr::ResourcePool::get_data_buffer(uint64_t bytes)
{
    std::unique_lock lock(buffers_mutex_);
    if (active_memory_bytes_ + bytes >= max_memory_bytes_) {
        buffers_cv_.wait(lock, [this, bytes]() {
            cull_unused_buffers_();
            return active_memory_bytes_ + bytes < max_memory_bytes_;
        });
    }
    active_memory_bytes_ += bytes;

    auto buffer = std::make_shared<DataBuffer>(bytes, 0);
    buffers_.emplace_back(buffer, bytes);
    return buffer;
}

void
zarr::ResourcePool::ResourcePool::set_error_(const std::string& error)
{
    error_ = error;
}

void
zarr::ResourcePool::cull_unused_files_()
{
    std::unique_lock lock(files_mutex_);

    std::vector<std::string> expired_keys;
    for (const auto& [key, file] : files_) {
        if (file.expired()) {
            expired_keys.push_back(key);
        }
    }

    for (const auto& key : expired_keys) {
        files_.erase(key);
    }

    files_cv_.notify_all();
}

void
zarr::ResourcePool::cull_unused_buffers_()
{
    std::unique_lock lock(buffers_mutex_);

    std::stack<int> expired_buffers;
    for (auto i = 0; i < buffers_.size(); ++i) {
        if (const auto& [data, bytes] = buffers_[i]; data.expired()) {
            expired_buffers.push(i);
            active_memory_bytes_ -= bytes;
        }
    }

    while (!expired_buffers.empty()) {
        buffers_.erase(buffers_.begin() + expired_buffers.top());
        expired_buffers.pop();
    }

    buffers_cv_.notify_all();
}

void
zarr::ResourcePool::cull_unused_s3_connections_()
{
    std::unique_lock lock(s3_connections_mutex_);

    std::vector<std::string> expired_keys;
    for (const auto& [key, conn] : s3_connections_) {
        if (conn.expired()) {
            expired_keys.push_back(key);
        }
    }

    for (const auto& key : expired_keys) {
        s3_connections_.erase(key);
    }

    s3_connections_cv_.notify_all();
}

ZarrResourcePool_s::ZarrResourcePool_s(unsigned int max_threads)
{
    inner_ = std::make_shared<zarr::ResourcePool>(max_threads);
}

std::shared_ptr<zarr::ResourcePool>
ZarrResourcePool_s::inner() const
{
    return inner_;
}
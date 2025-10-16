#include "definitions.hh"
#include "file.handle.hh"
#include "macros.hh"

void*
init_handle(const std::string& filename, void* flags);

void
destroy_handle(void* handle);

bool
flush_file(void* handle);

uint64_t
get_max_active_handles();

zarr::FileHandle::FileHandle(const std::string& filename, void* flags)
  : handle_(init_handle(filename, flags))
{
}

zarr::FileHandle::~FileHandle()
{
    destroy_handle(handle_);
}

void*
zarr::FileHandle::get() const
{
    return handle_;
}

zarr::FileHandlePool::FileHandlePool()
  : max_active_handles_(get_max_active_handles())
{
}

zarr::FileHandlePool::~FileHandlePool()
{
    // wait until the pool has been drained
    std::unique_lock lock(mutex_);
    while (!handle_map_.empty()) {
        if (!evict_idle_handle_()) {
            cv_.wait(lock, [&] { return true; });
        }
    }
}

std::shared_ptr<void>
zarr::FileHandlePool::get_handle(const std::string& filename, void* flags)
{
    std::unique_lock lock(mutex_);
    if (const auto it = handle_map_.find(filename); it != handle_map_.end()) {
        return it->second->second.lock();
    }

    cv_.wait(lock, [&] { return handles_.size() < max_active_handles_; });
    std::shared_ptr<void> handle(init_handle(filename, flags), [](void* h) {
        flush_file(h);
        destroy_handle(h);
    });

    EXPECT(handle != nullptr, "Failed to create file handle for " + filename);

    handles_.emplace_front(filename, handle);
    handle_map_[filename] = handles_.begin();

    return handle;
}

void
zarr::FileHandlePool::close_handle(const std::string& filename)
{
    std::unique_lock lock(mutex_);
    if (const auto it = handle_map_.find(filename); it != handle_map_.end()) {
        handles_.erase(it->second);
        handle_map_.erase(it);
        cv_.notify_all();
    }
}

bool
zarr::FileHandlePool::evict_idle_handle_()
{
    bool evicted = false;
    for (auto it = handles_.begin(); it != handles_.end(); ++it) {
        if (it->second.expired()) {
            handle_map_.erase(it->first);
            handles_.erase(it);
            evicted = true;
        }
    }

    if (evicted) {
        cv_.notify_all();
    }

    return evicted;
}

#include "file.handle.hh"
#include "macros.hh"

#include <chrono>

void*
init_handle(const std::string& filename, void* flags);

void
destroy_handle(void* handle);

bool
flush_file(void* handle);

uint64_t
get_max_active_handles();

zarr::FileHandle::FileHandle(const std::string& filename, void* flags)
  : filename_(filename)
  , handle_(init_handle(filename, flags))
{
}

zarr::FileHandle::~FileHandle()
{
    flush_file(handle_);
    destroy_handle(handle_);
}

void*
zarr::FileHandle::get() const
{
    return handle_;
}

zarr::FileHandleGuard::FileHandleGuard(const std::string& filename,
                                       void* flags,
                                       FileHandlePool* pool)
  : pool_(pool)
{
    EXPECT(pool_ != nullptr, "FileHandlePool pointer cannot be null");

    handle_ = pool_->get_handle_(filename, flags);
    EXPECT(handle_ != nullptr, "Failed to get file handle for " + filename);
}

zarr::FileHandleGuard::~FileHandleGuard()
{
    pool_->return_handle_(std::move(handle_));
}

void*
zarr::FileHandleGuard::get() const
{
    return handle_->get();
}

zarr::FileHandlePool::FileHandlePool()
  : max_active_handles_(get_max_active_handles())
  , n_active_handles_(0)
{
}

zarr::FileHandlePool::~FileHandlePool()
{
    std::cerr << "Max ever active file handles: "
              << max_ever_active_handles_.load() << std::endl;
}

zarr::FileHandleGuard
zarr::FileHandlePool::get(const std::string& filename, void* flags)
{
    return FileHandleGuard(filename, flags, this);
}

std::unique_ptr<zarr::FileHandle>
zarr::FileHandlePool::get_handle_(const std::string& filename, void* flags)
{
    std::unique_lock lock(mutex_);
    if (n_active_handles_ >= max_active_handles_) {
        cv_.wait(lock,
                 [this]() { return n_active_handles_ < max_active_handles_; });
    }

    std::unique_ptr<FileHandle> handle;
    {
        lock.unlock();
        handle = get_from_cache_(filename, flags);
        lock.lock();
    }

    EXPECT(handle != nullptr, "Failed to get or create file handle");
    ++n_active_handles_;

    if (n_active_handles_ > max_ever_active_handles_) {
        max_ever_active_handles_.store(n_active_handles_);
    }

    return handle;
}

void
zarr::FileHandlePool::return_handle_(std::unique_ptr<FileHandle>&& handle)
{
    if (handle == nullptr) {
        return;
    }

    std::unique_lock lock(mutex_);

    if (handle != nullptr && n_active_handles_ > 0) {
        --n_active_handles_;
    }

    handles_.insert(handles_.begin(), std::move(handle));
    handle_map_.insert({ handles_.front()->filename(), handles_.begin() });
    cv_.notify_one();
}

std::unique_ptr<zarr::FileHandle>
zarr::FileHandlePool::get_from_cache_(const std::string& filename, void* flags)
{
    std::unique_lock lock(mutex_);
    if (!handle_map_.contains(filename)) {
        lock.unlock();
        create_and_add_to_cache_(filename, flags);
        lock.lock();
    }

    const auto& map_it = handle_map_.find(filename);
    EXPECT(map_it != handle_map_.end(), "Handle not found in cache");

    const auto& list_it = map_it->second;
    EXPECT(list_it != handles_.end(), "Handle iterator invalid");

    std::unique_ptr<FileHandle> handle = std::move(*list_it);
    handle_map_.erase(map_it);
    // no need to erase from handles_ since we are moving the unique_ptr

    return handle;
}

void
zarr::FileHandlePool::return_to_cache_(std::unique_ptr<FileHandle>&& handle)
{
    if (handle == nullptr) {
        return;
    }

    std::unique_lock lock(mutex_);
    if (n_active_handles_ == 0) {
        return;
    }

    handles_.insert(handles_.begin(), std::move(handle));
    handle_map_.insert({ handles_.front()->filename(), handles_.begin() });
    --n_active_handles_;
    cv_.notify_one();
}

void
zarr::FileHandlePool::create_and_add_to_cache_(const std::string& filename,
                                               void* flags)
{
    std::unique_lock lock(mutex_);

    // evict least recently used handles if necessary
    if (n_active_handles_ + handles_.size() >= max_active_handles_) {
        lock.unlock();
        evict_lru_();
        lock.lock();
    }

    auto handle = std::make_unique<FileHandle>(filename, flags);
    handles_.insert(handles_.begin(), std::move(handle));
    handle_map_[filename] = handles_.begin();
}

void
zarr::FileHandlePool::evict_lru_()
{
    std::unique_lock lock(mutex_);
    while (n_active_handles_ + handles_.size() >= max_active_handles_) {
        if (handles_.empty()) {
            cv_.wait(lock, [this]() {
                return n_active_handles_ + handles_.size() <
                       max_active_handles_;
            });
        }

        auto lru_it = std::prev(handles_.end());
        const std::string& filename = (*lru_it)->filename();
        handle_map_.erase(filename);
        handles_.erase(lru_it);
    }
}

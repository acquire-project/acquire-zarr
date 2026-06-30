#include "definitions.hh"
#include "file.handle.hh"
#include "macros.hh"

#include <chrono>
#include <thread>

void*
init_handle(const std::string& filename, const void* flags);

void
destroy_handle(void* handle);

bool
flush_file(void* handle);

uint64_t
get_max_active_handles();

void*
make_flags();

void
destroy_flags(const void*);

zarr::FileHandle::FileHandle(const std::string& filename)
{
    const void* flags = make_flags();
    handle_ = init_handle(filename, flags);
    destroy_flags(flags);
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
  , cache_space_available_(true)
{
}

zarr::BorrowedHandle::~BorrowedHandle()
{
    if (pool_ && handle_) {
        pool_->return_handle(filename_);
    }
}

zarr::BorrowedHandle
zarr::FileHandlePool::get_handle(const std::string& filename)
{
    std::unique_lock lock(mutex_);

    // block until we can serve from cache or a slot frees up. eviction is
    // handled by return_handle, which notifies after dropping a refcount.
    cv_.wait(lock, [&] {
        return cache_.contains(filename) || cache_space_available_;
    });

    auto it = cache_.find(filename);
    if (it == cache_.end()) {
        // Not cached: open a new handle. Retry briefly on failure so a
        // transient open error (e.g. a momentary ENOENT during a concurrent
        // rename, or a filesystem hiccup) is recovered instead of failing the
        // write. Open BEFORE mutating cache_/lru_order_ so a failure leaves
        // pool state untouched; on persistent failure return a null handle,
        // which FileSink::write()/flush_() report as a recoverable false rather
        // than letting an exception escape and terminate the writer.
        constexpr int max_open_attempts = 3;
        constexpr auto open_retry_backoff = std::chrono::milliseconds(10);
        std::shared_ptr<FileHandle> handle;
        bool relocked = false;
        for (int attempt = 1;; ++attempt) {
            try {
                handle = std::make_shared<FileHandle>(filename);
                break;
            } catch (const std::exception& e) {
                if (attempt >= max_open_attempts) {
                    LOG_ERROR("Failed to open file handle for '",
                              filename,
                              "' after ",
                              max_open_attempts,
                              " attempts: ",
                              e.what());
                    return BorrowedHandle();
                }
                LOG_WARNING("Open failed for '",
                            filename,
                            "' (attempt ",
                            attempt,
                            "/",
                            max_open_attempts,
                            "); retrying: ",
                            e.what());
                // Drop the pool mutex around the backoff so other handle
                // requests are not blocked while this one waits.
                lock.unlock();
                std::this_thread::sleep_for(open_retry_backoff);
                lock.lock();
                relocked = true;
            }
        }

        // If the lock was dropped to retry, another thread may have opened and
        // cached this same file meanwhile; reuse that entry and drop ours.
        if (relocked) {
            it = cache_.find(filename);
        }
        if (it == cache_.end()) {
            lru_order_.push_front(filename);
            auto [new_it, _] =
              cache_.emplace(filename,
                             CacheEntry{
                               .handle = std::move(handle),
                               .lru_it = lru_order_.begin(),
                               .refcount = 0,
                             });
            it = new_it;
        }
    } else {
        // move to front of LRU
        lru_order_.splice(lru_order_.begin(), lru_order_, it->second.lru_it);
        it->second.lru_it = lru_order_.begin();
    }

    cache_space_available_ = cache_.size() < max_active_handles_;
    cv_.notify_one();

    ++it->second.refcount;
    return BorrowedHandle(it->second.handle.get(), filename, this);
}

void
zarr::FileHandlePool::return_handle(const std::string& filename)
{
    std::unique_lock lock(mutex_);

    const auto it = cache_.find(filename);
    if (it == cache_.end()) {
        return;
    }

    if (it->second.refcount > 0) {
        --it->second.refcount;
    }

    if (cache_.size() >= max_active_handles_) {
        evict_lru_();
    }
    cache_space_available_ = cache_.size() < max_active_handles_;

    cv_.notify_all();
}

void
zarr::FileHandlePool::close(const std::string& filename)
{
    std::unique_lock lock(mutex_);

    const auto it = cache_.find(filename);
    if (it == cache_.end() || it->second.refcount > 0) {
        // Not cached, or still borrowed by an in-flight write: leave it. A
        // borrowed handle is reclaimed by the next return_handle eviction once
        // the borrow ends.
        return;
    }

    lru_order_.erase(it->second.lru_it);
    cache_.erase(it); // destroys FileHandle -> close
    cache_space_available_ = cache_.size() < max_active_handles_;

    cv_.notify_all();
}

void
zarr::FileHandlePool::evict_lru_()
{
    // iterate from back (least recent) looking for idle handle
    for (auto it = lru_order_.rbegin(); it != lru_order_.rend(); ++it) {
        if (auto cache_it = cache_.find(*it);
            cache_it != cache_.end() && cache_it->second.refcount == 0) {
            cache_.erase(cache_it); // destroys FileHandle -> close
            lru_order_.erase(std::next(it).base());
            return;
        }
    }
}

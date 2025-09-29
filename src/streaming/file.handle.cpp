#include "definitions.hh"
#include "file.handle.hh"

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
  , n_active_handles_(0)
{
}

std::unique_ptr<zarr::FileHandle>
zarr::FileHandlePool::get_handle(const std::string& filename, void* flags)
{
    std::unique_lock lock(mutex_);
    if (n_active_handles_ >= max_active_handles_) {
        cv_.wait(lock,
                 [this]() { return n_active_handles_ < max_active_handles_; });
    }
    ++n_active_handles_;

    return std::make_unique<FileHandle>(filename, flags);
}

void
zarr::FileHandlePool::return_handle(std::unique_ptr<FileHandle>&& handle)
{
    std::unique_lock lock(mutex_);

    if (handle != nullptr && n_active_handles_ > 0) {
        --n_active_handles_;
    }

    // handle will be destroyed when going out of scope
    flush_file(handle->get());
}

#include "fs.storage.hh"
#include "macros.hh"

#include <span>

void*
make_flags();

void
destroy_flags(void* flags);

bool
seek_and_write(void* handle, size_t offset, ConstByteSpan data);

zarr::FSStorage::FSStorage(std::shared_ptr<FileHandlePool> file_handle_pool)
  : file_handle_pool_(file_handle_pool)
{
}

bool
zarr::FSStorage::write_binary_(const std::string& path,
                               const std::vector<uint8_t>& data,
                               size_t offset) const
{
    void* flags = make_flags();
    auto handle = file_handle_pool_->get_handle(path, flags);
    destroy_flags(flags);

    if (handle == nullptr) {
        LOG_ERROR("Failed to get file handle for ", path);
        return false;
    }

    if (!seek_and_write(handle.get(), offset, data)) {
        LOG_ERROR("Failed to write binary data to ", path);
        return false;
    }

    return true;
}

bool
zarr::FSStorage::write_string_(const std::string& path,
                               const std::string& data,
                               size_t offset) const
{
    void* flags = make_flags();
    auto handle = file_handle_pool_->get_handle(path, flags);
    destroy_flags(flags);

    if (handle == nullptr) {
        LOG_ERROR("Failed to get file handle for ", path);
        return false;
    }

    std::span span{ reinterpret_cast<const uint8_t*>(data.data()),
                    data.size() };
    if (!seek_and_write(handle.get(), offset, span)) {
        LOG_ERROR("Failed to write string to ", path);
        return false;
    }

    return true;
}

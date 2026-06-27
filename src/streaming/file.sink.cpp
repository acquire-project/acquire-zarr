#include "file.sink.hh"
#include "macros.hh"

#include <string_view>

bool
seek_and_write(void* handle, size_t offset, ConstByteSpan data);

bool
flush_file(void* handle);

zarr::FileSink::FileSink(std::string_view filename,
                         std::shared_ptr<FileHandlePool> file_handle_pool)
  : file_handle_pool_(file_handle_pool)
  , filename_(filename)
{
    EXPECT(file_handle_pool_ != nullptr, "File handle pool not provided.");
}

zarr::FileSink::~FileSink()
{
    // The sink is being finalized/destroyed, so this file is sealed: the
    // writer will never touch it again. Proactively close the pooled handle so
    // the OS frees the fd and a later unlink can reclaim the disk blocks. Left
    // open, the pool keeps the handle for the store's lifetime, and deleting a
    // sealed chunk frees no space on the local filesystem (issue #226).
    if (file_handle_pool_ != nullptr) {
        file_handle_pool_->close(filename_);
    }
}

bool
zarr::FileSink::write(size_t offset, ConstByteSpan data)
{
    if (data.data() == nullptr || data.size() == 0) {
        return true;
    }

    const auto borrowed = file_handle_pool_->get_handle(filename_);
    if (borrowed.handle_ == nullptr) {
        LOG_ERROR("Failed to get file handle for ", filename_);
        return false;
    }

    bool retval = false;
    try {
        retval = seek_and_write(borrowed.handle_->get(), offset, data);
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to write to file ", filename_, ": ", exc.what());
    }

    return retval;
    // borrowed goes out of scope here, return_handle called automatically
}

bool
zarr::FileSink::flush_()
{
    const auto borrowed = file_handle_pool_->get_handle(filename_);
    if (borrowed.handle_ == nullptr) {
        LOG_ERROR("Failed to get file handle for ", filename_);
        return false;
    }

    bool retval = false;
    try {
        retval = flush_file(borrowed.handle_->get());
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to flush file ", filename_, ": ", exc.what());
    }

    return retval;
}
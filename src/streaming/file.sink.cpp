#include "file.sink.hh"
#include "macros.hh"

#include <string_view>

bool
seek_and_write(void* handle, size_t offset, ConstByteSpan data);

bool
flush_file(void* handle);

bool
truncate_file(void* handle, size_t size);

zarr::FileSink::FileSink(std::string_view filename,
                         std::shared_ptr<FileHandlePool> file_handle_pool,
                         bool truncate_to_fit)
  : file_handle_pool_(file_handle_pool)
  , filename_(filename)
  , truncate_to_fit_(truncate_to_fit)
{
    EXPECT(file_handle_pool_ != nullptr, "File handle pool not provided.");
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
        if (retval && truncate_to_fit_) {
            retval =
              truncate_file(borrowed.handle_->get(), offset + data.size());
        }
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
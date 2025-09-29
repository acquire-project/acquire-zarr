#include "file.sink.hh"
#include "macros.hh"

#include <string_view>

void*
make_flags();

void
destroy_flags(void*);

bool
seek_and_write(void* handle, size_t offset, ConstByteSpan data);

bool
flush_file(void* handle);

zarr::FileSink::FileSink(std::string_view filename,
                         std::shared_ptr<FileHandlePool> file_handle_pool)
  : file_handle_pool_(file_handle_pool)
  , filename_(filename)
  , flags_(make_flags())
{
    EXPECT(file_handle_pool_ != nullptr, "File handle pool not provided.");
}

zarr::FileSink::~FileSink()
{
    destroy_flags(flags_);
    flags_ = nullptr;
}

bool
zarr::FileSink::write(size_t offset, ConstByteSpan data)
{
    if (data.data() == nullptr || data.size() == 0) {
        return true;
    }

    auto handle = file_handle_pool_->get_handle(filename_, flags_);
    if (handle == nullptr) {
        LOG_ERROR("Failed to get file handle for ", filename_);
        return false;
    }

    bool retval = false;
    try {
        retval = seek_and_write(handle->get(), offset, data);
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to write to file ", filename_, ": ", exc.what());
    }

    file_handle_pool_->return_handle(std::move(handle));

    return retval;
}

bool
zarr::FileSink::flush_()
{
    auto handle = file_handle_pool_->get_handle(filename_, flags_);
    if (handle == nullptr) {
        LOG_ERROR("Failed to get file handle for ", filename_);
        return false;
    }

    bool retval = false;
    try {
        retval = flush_file(handle->get());
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to flush file ", filename_, ": ", exc.what());
    }
    file_handle_pool_->return_handle(std::move(handle));

    return retval;
}
#include "file.sink.hh"
#include "macros.hh"
#include "zarr.common.hh"

#include <string_view>

size_t
get_page_size();

size_t
get_sector_size(const std::string&);

size_t
align_to_system_size(size_t, size_t, size_t);

void*
make_flags(bool);

void
destroy_flags(void* flags);

bool
seek_and_write(void* handle, size_t offset, ConstByteSpan data);

bool
write_vectors(void*,
              size_t,
              size_t,
              size_t,
              const std::vector<std::vector<uint8_t>>&);

bool
flush_file(void* handle);

namespace {
// only use vectorized writes if >= 8 threads
constexpr size_t VECTORIZE_THRESHOLD = 8;
const bool CAN_WRITE_VECTORIZED =
  std::thread::hardware_concurrency() > VECTORIZE_THRESHOLD;
} // namespace

zarr::FileSink::FileSink(std::string_view filename,
                         std::shared_ptr<FileHandlePool> file_handle_pool)
  : file_handle_pool_(file_handle_pool)
  , filename_(filename)
  , flags_(make_flags(CAN_WRITE_VECTORIZED))
  , vectorized_(CAN_WRITE_VECTORIZED)
  , page_size_(0)
  , sector_size_(0)
{
    EXPECT(file_handle_pool_ != nullptr, "File handle pool not provided.");

    page_size_ = get_page_size();
    sector_size_ = get_sector_size(filename_);
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

    std::unique_ptr<FileHandle> handle;

    bool retval = false;
    try {
        if (vectorized_) {
            destroy_flags(flags_);
            flags_ = nullptr;
            flags_ = make_flags(false);
            vectorized_ = false;
        }

        handle = file_handle_pool_->get_handle(filename_, flags_);
        if (handle == nullptr) {
            LOG_ERROR("Failed to get file handle for ", filename_);
            return false;
        }

        retval = seek_and_write(handle->get(), offset, data);
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to write to file ", filename_, ": ", exc.what());
    }

    file_handle_pool_->return_handle(std::move(handle));

    return retval;
}

bool
zarr::FileSink::write(size_t offset,
                      const std::vector<std::vector<uint8_t>>& buffers)
{
    if (buffers.empty()) {
        return true;
    }

    // fallback to non-vectorized (consolidated) write if not supported
    if (!CAN_WRITE_VECTORIZED) {
        size_t consolidated_size = 0;
        for (const auto& buffer : buffers) {
            consolidated_size += buffer.size();
        }
        std::vector<uint8_t> consolidated(consolidated_size, 0);

        consolidated_size = 0;
        for (const auto& buffer : buffers) {
            std::ranges::copy(buffer, consolidated.data() + consolidated_size);
            consolidated_size += buffer.size();
        }

        return write(offset, consolidated);
    }

    bool retval = false;
    std::unique_ptr<FileHandle> handle;

    try {
        if (!vectorized_) {
            destroy_flags(flags_);
            flags_ = nullptr;
            flags_ = make_flags(true);
            vectorized_ = true;
        }

        handle = file_handle_pool_->get_handle(filename_, flags_);
        if (handle == nullptr) {
            LOG_ERROR("Failed to get file handle for ", filename_);
            return false;
        }

        offset = align_to_system_size(offset);
        retval = write_vectors(
          handle->get(), offset, page_size_, sector_size_, buffers);
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to write to file ", filename_, ": ", exc.what());
    }

    file_handle_pool_->return_handle(std::move(handle));

    return retval;
}

size_t
zarr::FileSink::align_to_system_size(size_t size)
{
    return ::align_to_system_size(size, page_size_, sector_size_);
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
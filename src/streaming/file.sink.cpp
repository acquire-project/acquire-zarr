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

void
init_handle(void**, const std::string&, bool);

void
destroy_handle(void**);

void
reopen_handle(void**, const std::string&, bool);

bool
seek_and_write(void**, size_t, ConstByteSpan);

bool
write_vectors(void**,
              size_t,
              size_t,
              size_t,
              const std::vector<std::vector<uint8_t>>&);

bool
flush_file(void**);

namespace {
// only use vectorized writes if >= 8 threads
constexpr size_t VECTORIZE_THRESHOLD = 8;
const size_t CAN_WRITE_VECTORIZED =
  std::thread::hardware_concurrency() > VECTORIZE_THRESHOLD;
} // namespace

zarr::FileSink::FileSink(std::string_view filename)
  : filename_(filename)
  , vectorized_(CAN_WRITE_VECTORIZED)
  , page_size_(0)
  , sector_size_(0)
{
    init_handle(&handle_, filename_, vectorized_);

    page_size_ = get_page_size();
    sector_size_ = get_sector_size(filename_);
}

zarr::FileSink::~FileSink()
{
    destroy_handle(&handle_);
}

bool
zarr::FileSink::write(size_t offset, ConstByteSpan data)
{
    if (data.data() == nullptr || data.size() == 0) {
        return true;
    }

    std::lock_guard lock(mutex_);
    if (vectorized_) {
        reopen_handle(&handle_, filename_, vectorized_ = false);
    }

    return seek_and_write(&handle_, offset, data);
}

bool
zarr::FileSink::write(size_t& offset,
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

    std::lock_guard lock(mutex_);
    if (!vectorized_) {
        reopen_handle(&handle_, filename_, vectorized_ = true);
    }

    offset = align_to_system_size(offset);
    return write_vectors(&handle_, offset, page_size_, sector_size_, buffers);
}

size_t
zarr::FileSink::align_to_system_size(size_t size)
{
    return ::align_to_system_size(size, page_size_, sector_size_);
}

bool
zarr::FileSink::flush_()
{
    return flush_file(&handle_);
}
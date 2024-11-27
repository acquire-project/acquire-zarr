#include "vectorized.file.writer.hh"
#include "macros.hh"

namespace {
#ifdef _WIN32
std::string
get_last_error_as_string()
{
    DWORD errorMessageID = ::GetLastError();
    if (errorMessageID == 0) {
        return std::string(); // No error message has been recorded
    }

    LPSTR messageBuffer = nullptr;

    size_t size = FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                                   FORMAT_MESSAGE_FROM_SYSTEM |
                                   FORMAT_MESSAGE_IGNORE_INSERTS,
                                 NULL,
                                 errorMessageID,
                                 MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                                 (LPSTR)&messageBuffer,
                                 0,
                                 NULL);

    std::string message(messageBuffer, size);

    LocalFree(messageBuffer);

    return message;
}

size_t
get_sector_size(const std::string& path)
{
    // Get volume root path
    char volume_path[MAX_PATH];
    if (!GetVolumePathNameA(path.c_str(), volume_path, MAX_PATH)) {
        return 0;
    }

    DWORD sectors_per_cluster;
    DWORD bytes_per_sector;
    DWORD number_of_free_clusters;
    DWORD total_number_of_clusters;

    if (!GetDiskFreeSpaceA(volume_path,
                           &sectors_per_cluster,
                           &bytes_per_sector,
                           &number_of_free_clusters,
                           &total_number_of_clusters)) {
        return 0;
    }

    return bytes_per_sector;
}
#endif

bool
is_aligned(const void* ptr, size_t alignment)
{
    return reinterpret_cast<uintptr_t>(ptr) % alignment == 0;
}
} // namespace

zarr::VectorizedFileWriter::VectorizedFileWriter(const std::string& path)
{
#ifdef _WIN32
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    page_size_ = si.dwPageSize;

    sector_size_ = get_sector_size(path);
    if (sector_size_ == 0) {
        throw std::runtime_error("Failed to get sector size");
    }

    handle_ = CreateFileA(path.c_str(),
                          GENERIC_WRITE,
                          0, // No sharing
                          nullptr,
                          OPEN_ALWAYS,
                          FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING |
                            FILE_FLAG_SEQUENTIAL_SCAN,
                          nullptr);
    if (handle_ == INVALID_HANDLE_VALUE) {
        auto err = get_last_error_as_string();
        throw std::runtime_error("Failed to open file '" + path + "': " + err);
    }
#else
    page_size_ = sysconf(_SC_PAGESIZE);
    fd_ = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("Failed to open file: " + path);
    }
#endif
}

zarr::VectorizedFileWriter::~VectorizedFileWriter()
{
#ifdef _WIN32
    if (handle_ != INVALID_HANDLE_VALUE) {
        CloseHandle(handle_);
    }
#else
    if (fd_ >= 0) {
        close(fd_);
    }
#endif
}

bool
zarr::VectorizedFileWriter::write_vectors(
  const std::vector<std::vector<std::byte>>& buffers,
  const std::vector<size_t>& offsets)
{
    std::lock_guard<std::mutex> lock(mutex_);

#ifdef _WIN32
    size_t total_bytes_to_write = 0;
    for (const auto& buffer : buffers) {
        total_bytes_to_write += buffer.size();
    }

    const size_t nbytes_aligned = align_size_(total_bytes_to_write);
    CHECK(nbytes_aligned >= total_bytes_to_write);

    auto* aligned_ptr = (std::byte*)_aligned_malloc(nbytes_aligned, page_size_);
    if (!aligned_ptr) {
        return false;
    }

    auto* cur = aligned_ptr;
    for (const auto& buffer : buffers) {
        std::copy(buffer.begin(), buffer.end(), cur);
        cur += buffer.size();
    }

    std::vector<FILE_SEGMENT_ELEMENT> segments(nbytes_aligned / page_size_);

    cur = aligned_ptr;
    for (auto& segment : segments) {
        memset(&segment, 0, sizeof(segment));
        segment.Buffer = PtrToPtr64(cur);
        cur += page_size_;
    }

    OVERLAPPED overlapped = { 0 };
    overlapped.Offset = static_cast<DWORD>(offsets[0] & 0xFFFFFFFF);
    overlapped.OffsetHigh = static_cast<DWORD>(offsets[0] >> 32);
    overlapped.hEvent = CreateEvent(nullptr, TRUE, FALSE, nullptr);

    DWORD bytes_written;

    bool retval{ true };
    if (!WriteFileGather(
          handle_, segments.data(), nbytes_aligned, nullptr, &overlapped)) {
        if (GetLastError() != ERROR_IO_PENDING) {
            LOG_ERROR("Failed to write file: ", get_last_error_as_string());
            retval = false;
        }

        // Wait for the operation to complete
        if (!GetOverlappedResult(handle_, &overlapped, &bytes_written, TRUE)) {
            LOG_ERROR("Failed to get overlapped result: ",
                      get_last_error_as_string());
            retval = false;
        }
    }

    _aligned_free(aligned_ptr);
    return retval;
#else
    std::vector<struct iovec> iovecs;
    iovecs.reserve(buffers.size());

    for (const auto& buffer : buffers) {
        if (!is_aligned(buffer.data(), page_size_)) {
            return false;
        }
        struct iovec iov;
        iov.iov_base =
          const_cast<void*>(static_cast<const void*>(buffer.data()));
        iov.iov_len = buffer.size();
        iovecs.push_back(iov);
    }

    if (lseek(fd_, offsets[0], SEEK_SET) == -1) {
        return false;
    }

    ssize_t total_bytes = 0;
    for (const auto& buffer : buffers) {
        total_bytes += buffer.size();
    }

    ssize_t bytes_written = writev(fd_, iovecs.data(), iovecs.size());
    if (bytes_written != total_bytes) {
        return false;
    }
#endif
    return true;
}

size_t
zarr::VectorizedFileWriter::align_size_(size_t size) const
{
    return align_to_sector_(align_to_page_(size));
}

size_t
zarr::VectorizedFileWriter::align_to_page_(size_t size) const
{
    return (size + page_size_ - 1) & ~(page_size_ - 1);
}

size_t
zarr::VectorizedFileWriter::align_to_sector_(size_t size) const
{
    return (size + sector_size_ - 1) & ~(sector_size_ - 1);
}
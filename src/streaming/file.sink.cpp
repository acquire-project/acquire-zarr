#include "file.sink.hh"
#include "macros.hh"

#include <filesystem>

#ifdef _WIN32
#include <windows.h>
#else
#include <cstring>
#include <fcntl.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

namespace fs = std::filesystem;

namespace {
#ifdef _WIN32
static size_t page_size_ = 0;
static size_t sector_size_ = 0;

size_t
get_page_size()
{
    if (page_size_ == 0) {
        SYSTEM_INFO system_info;
        GetSystemInfo(&system_info);
        page_size_ = system_info.dwPageSize;
    }
    return page_size_;
}

size_t
get_sector_size()
{
    if (sector_size_ == 0) {
        char volume_path[MAX_PATH];
        if (!GetVolumePathNameA("C:\\", volume_path, MAX_PATH)) {
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

        sector_size_ = bytes_per_sector;
    }
    return sector_size_;
}

size_t
align_to_page(size_t size)
{
    const auto page_size = get_page_size();
    return (size + page_size - 1) & ~(page_size - 1);
}

size_t
align_size(size_t size)
{
    size = align_to_page(size);
    const auto sector_size = get_sector_size();
    return (size + sector_size - 1) & ~(sector_size - 1);
}

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

void
init_handle(void** handle, const std::string& filename, bool vectorized)
{
    EXPECT(handle, "Expected nonnull pointer to file handle.");
    HANDLE* fd = new HANDLE;

    if (vectorized) {
        *fd = CreateFileA(filename.c_str(),
                          GENERIC_WRITE,
                          0, // No sharing
                          nullptr,
                          OPEN_ALWAYS,
                          FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING |
                            FILE_FLAG_SEQUENTIAL_SCAN,
                          nullptr);
    } else {
        *fd = CreateFileA(filename.c_str(),
                          GENERIC_WRITE,
                          0, // No sharing
                          nullptr,
                          OPEN_ALWAYS,
                          FILE_FLAG_OVERLAPPED,
                          nullptr);
    }

    if (*fd == INVALID_HANDLE_VALUE) {
        const auto err = get_last_error_as_string();
        delete handle;
        throw std::runtime_error("Failed to open file '" + filename +
                                 "': " + err);
    }
    *handle = (void*)fd;
}

bool
seek_and_write(void** handle, size_t offset, ConstByteSpan data)
{
    CHECK(handle);
    auto* fd = reinterpret_cast<HANDLE*>(*handle);

    auto* cur = reinterpret_cast<const char*>(data.data());
    auto* end = cur + data.size();

    int retries = 0;
    OVERLAPPED overlapped = { 0 };
    overlapped.hEvent = CreateEventA(nullptr, TRUE, FALSE, nullptr);

    while (cur < end && retries < 3) {
        DWORD written = 0;
        DWORD remaining = (DWORD)(end - cur); // may truncate
        overlapped.Pointer = (void*)offset;
        if (!WriteFile(*fd, cur, (DWORD)remaining, nullptr, &overlapped) &&
            GetLastError() != ERROR_IO_PENDING) {
            const auto err = get_last_error_as_string();
            LOG_ERROR("Failed to write to file: ", err);
            CloseHandle(overlapped.hEvent);
            return false;
        }

        if (!GetOverlappedResult(*fd, &overlapped, &written, TRUE)) {
            LOG_ERROR("Failed to get overlapped result: ",
                      get_last_error_as_string());
            CloseHandle(overlapped.hEvent);
            return false;
        }
        retries += (written == 0);
        offset += written;
        cur += written;
    }

    CloseHandle(overlapped.hEvent);
    return (retries < 3);
}

bool
seek_and_write_vectors(void** handle,
                       size_t offset,
                       const std::vector<ByteSpan>& buffers)
{
    bool retval = true;
    auto* fd = reinterpret_cast<HANDLE*>(*handle);

    const auto page_size = get_page_size();

    size_t total_bytes_to_write = 0;
    for (const auto& buffer : buffers) {
        total_bytes_to_write += buffer.size();
    }

    const size_t nbytes_aligned = align_size(total_bytes_to_write);
    CHECK(nbytes_aligned >= total_bytes_to_write);

    auto* aligned_ptr =
      static_cast<std::byte*>(_aligned_malloc(nbytes_aligned, page_size));
    if (!aligned_ptr) {
        return false;
    }

    auto* cur = aligned_ptr;
    for (const auto& buffer : buffers) {
        std::copy(buffer.begin(), buffer.end(), cur);
        cur += buffer.size();
    }

    std::vector<FILE_SEGMENT_ELEMENT> segments(nbytes_aligned / page_size);

    cur = aligned_ptr;
    for (auto& segment : segments) {
        memset(&segment, 0, sizeof(segment));
        segment.Buffer = PtrToPtr64(cur);
        cur += page_size;
    }

    OVERLAPPED overlapped = { 0 };
    overlapped.Offset = static_cast<DWORD>(offset & 0xFFFFFFFF);
    overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);
    overlapped.hEvent = CreateEvent(nullptr, TRUE, FALSE, nullptr);

    DWORD bytes_written;

    if (!WriteFileGather(
          *fd, segments.data(), nbytes_aligned, nullptr, &overlapped)) {
        if (GetLastError() != ERROR_IO_PENDING) {
            LOG_ERROR("Failed to write file: ", get_last_error_as_string());
            retval = false;
        }

        // Wait for the operation to complete
        if (!GetOverlappedResult(*fd, &overlapped, &bytes_written, TRUE)) {
            LOG_ERROR("Failed to get overlapped result: ",
                      get_last_error_as_string());
            retval = false;
        }
    }

    _aligned_free(aligned_ptr);

    return retval;
}

bool
flush_file(void** handle)
{
    CHECK(handle);
    return true;
}

void
destroy_handle(void** handle)
{
    auto* fd = reinterpret_cast<HANDLE*>(*handle);
    if (fd) {
        if (*fd != INVALID_HANDLE_VALUE) {
            CloseHandle(*fd);
        }
        delete fd;
    }
}
#else
std::string
get_last_error_as_string()
{
    return strerror(errno);
}

void
init_handle(void** handle, const std::string& filename, bool)
{
    EXPECT(handle, "Expected nonnull pointer file handle.");
    auto* fd = new int;

    *fd = open(filename.data(), O_WRONLY | O_CREAT, 0644);
    if (*fd < 0) {
        const auto err = get_last_error_as_string();
        throw std::runtime_error("Failed to open file: '" + filename +
                                 "': " + err);
    }
    *handle = (void*)fd;
}

bool
seek_and_write(void** handle, size_t offset, ConstByteSpan data)
{
    CHECK(handle);
    auto* fd = reinterpret_cast<int*>(*handle);

    auto* cur = reinterpret_cast<const char*>(data.data());
    auto* end = cur + data.size();

    int retries = 0;
    while (cur < end && retries < 3) {
        size_t remaining = end - cur;
        ssize_t written = pwrite(*fd, cur, remaining, offset);
        if (written < 0) {
            const auto err = get_last_error_as_string();
            throw std::runtime_error("Failed to write to file: " + err);
        }
        retries += (written == 0);
        offset += written;
        cur += written;
    }

    return (retries < 3);
}

bool
seek_and_write_vectors(void** handle,
                       size_t offset,
                       const std::vector<ByteSpan>& buffers)
{
    bool retval = true;

    CHECK(handle);
    auto* fd = reinterpret_cast<int*>(*handle);

    std::vector<struct iovec> iovecs(buffers.size());

    for (auto i = 0; i < buffers.size(); ++i) {
        auto* iov = &iovecs[i];
        memset(iov, 0, sizeof(struct iovec));
        iov->iov_base =
          const_cast<void*>(static_cast<const void*>(buffers[i].data()));
        iov->iov_len = buffers[i].size();
    }

    ssize_t total_bytes = 0;
    for (const auto& buffer : buffers) {
        total_bytes += static_cast<ssize_t>(buffer.size());
    }

    ssize_t bytes_written = pwritev(*fd,
                                    iovecs.data(),
                                    static_cast<int>(iovecs.size()),
                                    static_cast<int>(offset));

    if (bytes_written != total_bytes) {
        auto error = get_last_error_as_string();
        LOG_ERROR("Failed to write file: ", error);
        retval = false;
    }

    return retval;
}

bool
flush_file(void** handle)
{
    CHECK(handle);
    auto* fd = reinterpret_cast<int*>(*handle);

    const auto res = fsync(*fd);
    if (res < 0) {
        LOG_ERROR("Failed to flush file: ", get_last_error_as_string());
    }

    return res == 0;
}

void
destroy_handle(void** handle)
{
    auto* fd = reinterpret_cast<int*>(*handle);
    if (fd) {
        if (*fd >= 0) {
            close(*fd);
        }
        delete fd;
    }
}
#endif
}

zarr::FileSink::FileSink(std::string_view filename, bool vectorized)
{
    std::string file_path{ filename };
    init_handle(&handle_, file_path, vectorized);
}

zarr::FileSink::~FileSink()
{
    destroy_handle(&handle_);
}

bool
zarr::FileSink::write(size_t offset, ConstByteSpan data)
{
    const auto bytes_of_buf = data.size();
    if (data.data() == nullptr || bytes_of_buf == 0) {
        return true;
    }

    return seek_and_write(&handle_, offset, data);
}

bool
zarr::FileSink::write_vectors(size_t offset, const std::vector<ByteSpan>& data)
{
    return seek_and_write_vectors(&handle_, offset, data);
}

bool
zarr::FileSink::flush_()
{
    return flush_file(&handle_);
}

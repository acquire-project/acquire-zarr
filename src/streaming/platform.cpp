#include "platform.hh"
#include "macros.hh"

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>

#include <cstring>
#endif

namespace {
size_t
align_to(size_t size, size_t alignment)
{
    if (alignment == 0) {
        return size;
    }

    return (size + alignment - 1) & ~(alignment - 1);
}
} // namespace

#ifdef _WIN32
namespace {
size_t _PAGE_SIZE = 0;
size_t _SECTOR_SIZE = 0;

[[nodiscard]]
size_t
get_sector_size()
{
    if (_SECTOR_SIZE == 0) {
        DWORD bytes_per_sector;
        GetDiskFreeSpace(NULL, &bytes_per_sector, NULL, NULL, NULL);
        _SECTOR_SIZE = bytes_per_sector;
    }
    return _SECTOR_SIZE;
}

[[nodiscard]]
size_t
get_page_size()
{
    if (_PAGE_SIZE == 0) {
        SYSTEM_INFO sys_info;
        GetSystemInfo(&sys_info);
        _PAGE_SIZE = sys_info.dwPageSize;
    }
    return _PAGE_SIZE;
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
} // namespace

size_t
zarr::align_to_system_boundary(size_t size)
{
    size = align_to(size, get_page_size());
    return align_to(size, get_sector_size());
}

zarr::VectorizedFile::VectorizedFile(std::string_view path)
  : inner_(INVALID_HANDLE_VALUE)
{
    inner_ = (void*)CreateFileA(path.data(),
                                GENERIC_WRITE,
                                0, // no sharing
                                nullptr,
                                OPEN_ALWAYS,
                                FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING |
                                  FILE_FLAG_SEQUENTIAL_SCAN,
                                nullptr);

    EXPECT(inner_ != INVALID_HANDLE_VALUE, "Failed to open file: ", path);
}

zarr::VectorizedFile::~VectorizedFile()
{
    if (inner_ != INVALID_HANDLE_VALUE) {
        CloseHandle((HANDLE)inner_);
    }
}

bool
zarr::file_write_vectorized(VectorizedFile& file,
                            const std::vector<std::span<std::byte>>& buffers,
                            size_t offset)
{
    auto fh = (HANDLE)file.inner_;
    EXPECT(fh != INVALID_HANDLE_VALUE, "Invalid file handle");

    const auto page_size = get_page_size();

    bool success = true;

    size_t total_bytes_to_write = 0;
    for (const auto& buffer : buffers) {
        total_bytes_to_write += buffer.size();
    }

    const size_t nbytes_aligned =
      align_to_system_boundary(total_bytes_to_write);
    CHECK(nbytes_aligned >= total_bytes_to_write);

    auto* aligned =
      static_cast<std::byte*>(_aligned_malloc(nbytes_aligned, page_size));
    if (!aligned) {
        return false;
    }

    auto* cur = aligned;
    for (const auto& buffer : buffers) {
        std::copy(buffer.begin(), buffer.end(), cur);
        cur += buffer.size();
    }

    std::vector<FILE_SEGMENT_ELEMENT> segments(nbytes_aligned / page_size);

    cur = aligned;
    for (auto& segment : segments) {
        memset(&segment, 0, sizeof(segment));
        segment.Buffer = PtrToPtr64(cur);
        cur += page_size;
    }

    OVERLAPPED overlapped = { 0 };
    overlapped.Offset = static_cast<DWORD>(offset & 0xFFFFFFFF);
    overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);
    overlapped.hEvent = CreateEvent(nullptr, TRUE, FALSE, nullptr);

    if (!WriteFileGather(
          fh, segments.data(), nbytes_aligned, nullptr, &overlapped)) {
        if (GetLastError() != ERROR_IO_PENDING) {
            LOG_ERROR("Failed to write file : ", get_last_error_as_string());
            success = false;
        }

        // Wait for the operation to complete
        DWORD bytes_written = 0;
        if (success &&
            !GetOverlappedResult(fh, &overlapped, &bytes_written, TRUE)) {
            LOG_ERROR("Failed to get overlapped result: ",
                      get_last_error_as_string());
            success = false;
        }
        EXPECT(bytes_written == nbytes_aligned,
               "Expected to write ",
               nbytes_aligned,
               " bytes, wrote ",
               bytes_written);
    }

    _aligned_free(aligned);

    return success;
}
#else // _WIN32
namespace {
size_t _PAGE_SIZE = 0;

size_t
get_page_size()
{
    if (_PAGE_SIZE == 0) {
        _PAGE_SIZE = sysconf(_SC_PAGE_SIZE);
    }
    return _PAGE_SIZE;
}

std::string
get_last_error_as_string()
{
    return strerror(errno);
}
} // namespace

size_t
zarr::align_to_system_boundary(size_t size)
{
    return align_to(size, get_page_size());
}

zarr::VectorizedFile::VectorizedFile(std::string_view path)
  : inner_(nullptr)
{
    inner_ = new int(open(path.data(), O_WRONLY | O_CREAT, 0644));
    EXPECT(inner_ != nullptr, "Failed to open file: ", path);
    EXPECT(*(int*)inner_ != -1, "Failed to open file: ", path);
}

zarr::VectorizedFile::~VectorizedFile()
{
    if (inner_ != nullptr) {
        if (*(int*)inner_ != -1) {
            close(*(int*)inner_);
        }
        delete (int*)inner_;
    }
}

bool
zarr::file_write_vectorized(zarr::VectorizedFile& file,
                            const std::vector<std::span<std::byte>>& buffers,
                            size_t offset)
{
    auto fd = *(int*)file.inner_;
    EXPECT(fd != -1, "Invalid file descriptor");

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

    ssize_t bytes_written = pwritev(fd,
                                    iovecs.data(),
                                    static_cast<int>(iovecs.size()),
                                    static_cast<int>(offset));

    if (bytes_written != total_bytes) {
        LOG_ERROR("Failed to write file: ", get_last_error_as_string());
        return false;
    }

    return true;
}
#endif
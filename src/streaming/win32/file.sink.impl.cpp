#include "definitions.hh"
#include "macros.hh"

#include <string_view>

#include <windows.h>
#include <zarr.common.hh>

std::string
get_last_error_as_string()
{
    const DWORD error_message_id = ::GetLastError();
    if (error_message_id == 0) {
        return ""; // No error message has been recorded
    }

    LPSTR message_buffer = nullptr;

    constexpr auto format = FORMAT_MESSAGE_ALLOCATE_BUFFER |
                            FORMAT_MESSAGE_FROM_SYSTEM |
                            FORMAT_MESSAGE_IGNORE_INSERTS;
    constexpr auto lang_id = MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT);
    const size_t size = FormatMessageA(format,
                                       nullptr,
                                       error_message_id,
                                       lang_id,
                                       reinterpret_cast<LPSTR>(&message_buffer),
                                       0,
                                       nullptr);

    std::string message(message_buffer, size);

    LocalFree(message_buffer);

    return message;
}

size_t
get_page_size()
{
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    EXPECT(si.dwPageSize > 0, "Could not get system page size");

    return si.dwPageSize;
}

size_t
get_sector_size(const std::string& path)
{
    // get volume root path
    char volume_path[MAX_PATH];
    EXPECT(GetVolumePathNameA(path.c_str(), volume_path, MAX_PATH),
           "Failed to get volume name for path '",
           path,
           "'");

    DWORD sectors_per_cluster;
    DWORD bytes_per_sector;
    DWORD number_of_free_clusters;
    DWORD total_number_of_clusters;

    EXPECT(GetDiskFreeSpaceA(volume_path,
                             &sectors_per_cluster,
                             &bytes_per_sector,
                             &number_of_free_clusters,
                             &total_number_of_clusters),
           "Failed to get disk free space for volume: " +
             std::string(volume_path));

    EXPECT(bytes_per_sector > 0, "Could not get sector size");

    return bytes_per_sector;
}

size_t
align_to_system_size(const size_t size,
                     const size_t page_size,
                     const size_t sector_size)
{
    return zarr::align_to(zarr::align_to(size, page_size), sector_size);
}

void
init_handle(void** handle, const std::string& filename, bool vectorized)
{
    EXPECT(handle, "Expected nonnull pointer to file handle.");
    auto* fd = new HANDLE;

    const DWORD flags = vectorized
                          ? FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING |
                              FILE_FLAG_SEQUENTIAL_SCAN
                          : FILE_FLAG_OVERLAPPED;

    *fd = CreateFileA(filename.c_str(),
                      GENERIC_WRITE,
                      0, // No sharing
                      nullptr,
                      OPEN_ALWAYS,
                      flags,
                      nullptr);

    if (*fd == INVALID_HANDLE_VALUE) {
        const std::string err = get_last_error_as_string();
        delete fd;
        throw std::runtime_error("Failed to open file: '" + filename +
                                 "': " + err);
    }
    *handle = reinterpret_cast<void*>(fd);
}

bool
seek_and_write(void** handle, size_t offset, ConstByteSpan data)
{
    CHECK(handle);
    const auto* fd = static_cast<HANDLE*>(*handle);

    auto* cur = reinterpret_cast<const char*>(data.data());
    auto* end = cur + data.size();

    int retries = 0;
    OVERLAPPED overlapped = { 0 };
    overlapped.hEvent = CreateEventA(nullptr, TRUE, FALSE, nullptr);

    constexpr size_t max_retries = 3;
    while (cur < end && retries < max_retries) {
        DWORD written = 0;
        const auto remaining = static_cast<DWORD>(end - cur); // may truncate
        overlapped.Pointer = reinterpret_cast<void*>(offset);
        if (!WriteFile(*fd, cur, remaining, nullptr, &overlapped) &&
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
        retries += (written == 0) ? 1 : 0;
        offset += written;
        cur += written;
    }

    CloseHandle(overlapped.hEvent);
    return (retries < max_retries);
}

bool
flush_file(void** handle)
{
    CHECK(handle);
    if (const auto* fd = static_cast<HANDLE*>(*handle);
        fd && *fd != INVALID_HANDLE_VALUE) {
        return FlushFileBuffers(*fd);
    }
    return true;
}

void
destroy_handle(void** handle)
{
    if (const auto* fd = static_cast<HANDLE*>(*handle)) {
        if (*fd != INVALID_HANDLE_VALUE) {
            FlushFileBuffers(*fd); // Ensure all buffers are flushed
            CloseHandle(*fd);
        }
        delete fd;
    }
}

void
reopen_handle(void** handle, const std::string& filename, bool vectorized)
{
    destroy_handle(handle);
    init_handle(handle, filename, vectorized);
}

bool
write_vectors(void** handle,
              size_t offset,
              size_t page_size,
              size_t sector_size,
              const std::vector<std::vector<uint8_t>>& buffers)
{
    EXPECT(handle, "Expected nonnull pointer to file handle.");
    const auto* fd = static_cast<HANDLE*>(*handle);
    if (fd == nullptr || *fd == INVALID_HANDLE_VALUE) {
        throw std::runtime_error("Expected valid file handle");
    }

    size_t total_bytes_to_write = 0;
    for (const auto& buffer : buffers) {
        total_bytes_to_write += buffer.size();
    }

    const size_t offset_aligned = zarr::align_to(offset, get_page_size());
    if (offset_aligned != offset) {
        LOG_ERROR("Aligned offset is not equalt to offset: ",
                  offset_aligned,
                  " != ",
                  offset);
        return false;
    }

    const size_t nbytes_aligned =
      align_to_system_size(total_bytes_to_write, page_size, sector_size);
    if (nbytes_aligned < total_bytes_to_write) {
        LOG_ERROR("Aligned size is less than total bytes to write: ",
                  nbytes_aligned,
                  " < ",
                  total_bytes_to_write);
        return false;
    }

    auto* aligned_ptr =
      static_cast<uint8_t*>(_aligned_malloc(nbytes_aligned, get_page_size()));
    if (!aligned_ptr) {
        return false;
    }

    auto* cur = aligned_ptr;
    for (const auto& buffer : buffers) {
        std::ranges::copy(buffer, cur);
        cur += buffer.size();
    }

    std::vector<FILE_SEGMENT_ELEMENT> segments(nbytes_aligned /
                                               get_page_size());

    cur = aligned_ptr;
    for (auto& segment : segments) {
        memset(&segment, 0, sizeof(segment));
        segment.Buffer = PtrToPtr64(cur);
        cur += get_page_size();
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
            return false;
        }

        // Wait for the operation to complete
        if (!GetOverlappedResult(*fd, &overlapped, &bytes_written, TRUE)) {
            LOG_ERROR("Failed to get overlapped result: ",
                      get_last_error_as_string());
            return false;
        }
    }

    _aligned_free(aligned_ptr);

    return true;
}
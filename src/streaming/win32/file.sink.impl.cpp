#include "definitions.hh"
#include "macros.hh"
#include "platform.hh"

#include <string_view>

#include <windows.h>
#include <zarr.common.hh>

void
init_handle(void** handle,
            const std::string& filename,
            size_t& sector_size,
            bool vectorized)
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

    sector_size = get_system_alignment_size(filename);
    EXPECT(sector_size > 0, "Could not get sector size for file: ", filename);
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
reopen_handle(void** handle,
              const std::string& filename,
              size_t& sector_size,
              bool vectorized)
{
    destroy_handle(handle);
    init_handle(handle, filename, sector_size, vectorized);
}

bool
write_vectors(void** handle,
              size_t& offset,
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
    if (offset_aligned < offset) {
        LOG_ERROR("Aligned offset is less than offset: ",
                  offset_aligned,
                  " < ",
                  offset);
        return false;
    }
    offset = offset_aligned;

    const size_t nbytes_aligned =
      align_to_system_size(total_bytes_to_write, sector_size);
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
#include "definitions.hh"
#include "macros.hh"

#include <string_view>

#include <windows.h>

std::string
get_last_error_as_string()
{
    auto error_message_id = ::GetLastError();
    if (error_message_id == 0) {
        return std::string(); // No error message has been recorded
    }

    LPSTR message_buffer = nullptr;

    const auto format = FORMAT_MESSAGE_ALLOCATE_BUFFER |
                        FORMAT_MESSAGE_FROM_SYSTEM |
                        FORMAT_MESSAGE_IGNORE_INSERTS;
    const auto lang_id = MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT);
    size_t size = FormatMessageA(format,
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

void*
make_flags()
{
    auto* flags = new DWORD;
    *flags = FILE_FLAG_OVERLAPPED;
    return flags;
}

void
destroy_flags(void* flags)
{
    const auto* fd = static_cast<DWORD*>(flags);
    delete fd;
}

uint64_t
get_max_active_handles()
{
    return _getmaxstdio();
}

void*
init_handle(const std::string& filename, void* flags)
{
    auto* fd = new HANDLE;
    *fd = CreateFileA(filename.c_str(),
                      GENERIC_WRITE,
                      0, // No sharing
                      nullptr,
                      OPEN_ALWAYS,
                      *static_cast<DWORD*>(flags),
                      nullptr);

    if (*fd == INVALID_HANDLE_VALUE) {
        const auto err = get_last_error_as_string();
        delete fd;
        throw std::runtime_error("Failed to open file: '" +
                                 std::string(filename) + "': " + err);
    }
    return fd;
}

bool
seek_and_write(void* handle, size_t offset, ConstByteSpan data)
{
    CHECK(handle);
    const auto* fd = static_cast<HANDLE*>(handle);

    auto* cur = reinterpret_cast<const char*>(data.data());
    auto* end = cur + data.size();

    int retries = 0;
    OVERLAPPED overlapped = { 0 };
    overlapped.hEvent = CreateEventA(nullptr, TRUE, FALSE, nullptr);

    constexpr auto max_retries = 3;
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
        retries += written == 0 ? 1 : 0;
        offset += written;
        cur += written;
    }

    CloseHandle(overlapped.hEvent);
    return retries < max_retries;
}

bool
flush_file(void* handle)
{
    CHECK(handle);
    if (const auto* fd = static_cast<HANDLE*>(handle); *fd != INVALID_HANDLE_VALUE) {
        return FlushFileBuffers(*fd);
    }
    return true;
}

void
destroy_handle(void* handle)
{
    if (const auto* fd = static_cast<HANDLE*>(handle)) {
        if (*fd != INVALID_HANDLE_VALUE) {
            FlushFileBuffers(*fd); // Ensure all buffers are flushed
            CloseHandle(*fd);
        }
        delete fd;
    }
}
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
destroy_flags(const void* flags)
{
    const auto* fd = static_cast<const DWORD*>(flags);
    delete fd;
}

uint64_t
get_max_active_handles()
{
    // We open files via CreateFileA, which returns a Win32 HANDLE backed by
    // the per-process kernel-object quota -- not by the CRT file-descriptor
    // table that _getmaxstdio() reports. The kernel ceiling is documented at
    // 16,777,216 per process; the practical ceiling (limited by kernel pool
    // memory) is in the 10,000s on typical systems. There is no setrlimit
    // analogue on Win32, so this is a self-imposed throttle rather than a
    // system fact; pick a number that's well under the kernel ceiling but
    // high enough that the pool isn't the bottleneck for concurrent shards.
    // CreateFile will return INVALID_HANDLE_VALUE with
    // ERROR_TOO_MANY_OPEN_FILES if we ever do hit the kernel limit.
    return 8192;
}

void*
init_handle(const std::string& filename, const void* flags)
{
    auto* fd = new HANDLE;
    *fd = CreateFileA(filename.c_str(),
                      GENERIC_WRITE,
                      FILE_SHARE_READ | FILE_SHARE_WRITE,
                      nullptr,
                      OPEN_ALWAYS,
                      *static_cast<const DWORD*>(flags),
                      nullptr);

    if (*fd == INVALID_HANDLE_VALUE) {
        const auto err = get_last_error_as_string();
        delete fd;
        throw std::runtime_error("Failed to open file: '" +
                                 std::string(filename) + "': " + err);
    }
    return fd;
}

namespace {
// One manual-reset event per worker thread, reused across writes. The files
// are opened FILE_FLAG_OVERLAPPED and we wait synchronously on each write, so
// a fresh kernel event was being created and destroyed on every single chunk
// write. Reusing a thread-local event removes that per-write syscall pair.
struct ThreadEvent
{
    HANDLE handle{ CreateEventA(nullptr, TRUE, FALSE, nullptr) };
    ~ThreadEvent()
    {
        if (handle) {
            CloseHandle(handle);
        }
    }
};
} // namespace

bool
seek_and_write(void* handle, size_t offset, ConstByteSpan data)
{
    CHECK(handle);
    const auto* fd = static_cast<HANDLE*>(handle);

    auto* cur = reinterpret_cast<const char*>(data.data());
    auto* end = cur + data.size();

    thread_local ThreadEvent thread_event;
    const HANDLE event = thread_event.handle;
    if (event == nullptr) {
        LOG_ERROR("Failed to create overlapped event: ",
                  get_last_error_as_string());
        return false;
    }

    int retries = 0;
    OVERLAPPED overlapped = { 0 };
    overlapped.hEvent = event;

    constexpr auto max_retries = 3;
    while (cur < end && retries < max_retries) {
        ResetEvent(event); // clear state from the previous write
        DWORD written = 0;
        const auto remaining = static_cast<DWORD>(end - cur); // may truncate
        overlapped.Pointer = reinterpret_cast<void*>(offset);
        if (!WriteFile(*fd, cur, remaining, nullptr, &overlapped) &&
            GetLastError() != ERROR_IO_PENDING) {
            const auto err = get_last_error_as_string();
            LOG_ERROR("Failed to write to file: ", err);
            return false;
        }

        if (!GetOverlappedResult(*fd, &overlapped, &written, TRUE)) {
            LOG_ERROR("Failed to get overlapped result: ",
                      get_last_error_as_string());
            return false;
        }
        retries += written == 0 ? 1 : 0;
        offset += written;
        cur += written;
    }

    return retries < max_retries;
}

bool
flush_file(void* handle)
{
    CHECK(handle);
    if (const auto* fd = static_cast<HANDLE*>(handle);
        *fd != INVALID_HANDLE_VALUE) {
        return FlushFileBuffers(*fd);
    }
    return true;
}

bool
truncate_file(void* handle, size_t size)
{
    CHECK(handle);
    const auto* fd = static_cast<HANDLE*>(handle);
    if (*fd == INVALID_HANDLE_VALUE) {
        return false;
    }

    // FileEndOfFileInfo takes an explicit size and ignores the file pointer,
    // which FILE_FLAG_OVERLAPPED writes never move (unlike SetEndOfFile).
    FILE_END_OF_FILE_INFO eof_info = { 0 };
    eof_info.EndOfFile.QuadPart = static_cast<LONGLONG>(size);
    if (!SetFileInformationByHandle(
          *fd, FileEndOfFileInfo, &eof_info, sizeof(eof_info))) {
        LOG_ERROR("Failed to truncate file: ", get_last_error_as_string());
        return false;
    }
    return true;
}

void
destroy_handle(void* handle)
{
    if (const auto* fd = static_cast<HANDLE*>(handle)) {
        if (*fd != INVALID_HANDLE_VALUE) {
            // No FlushFileBuffers here: sinks are explicitly flushed at
            // finalize (flush_file -> FlushFileBuffers), so forcing a physical
            // disk sync on every handle eviction/close is redundant and very
            // expensive. CloseHandle still flushes written data to the OS
            // cache, so the store is correct/readable after close.
            CloseHandle(*fd);
        }
        delete fd;
    }
}
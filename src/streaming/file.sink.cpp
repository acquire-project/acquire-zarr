#include "file.sink.hh"
#include "macros.hh"

#include <filesystem>

#ifdef WIN32_
#include <windows.h>
#else
#include <cstring>
#include <fcntl.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

namespace fs = std::filesystem;

namespace {
#ifdef WIN32_
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

void
init_handle(void** handle, const std::string& filename)
{
    EXPECT(handle, "Expected nonnull pointer to file handle.");
    HANDLE* fd = new HANDLE;

    *fd = CreateFileA(filename.c_str(),
                      GENERIC_WRITE,
                      0, // No sharing
                      nullptr,
                      OPEN_ALWAYS,
                      FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING |
                        FILE_FLAG_SEQUENTIAL_SCAN,
                      nullptr);
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
    OVERLAPPED ovl = file->overlapped;
    while (cur < end && retries < 3) {
        DWORD written = 0;
        DWORD remaining = (DWORD)(end - cur); // may truncate
        ovl.Pointer = (void*)offset;
        WriteFile(*fd, cur, (DWORD)remaining, 0, &ovl);
        CHECK(GetOverlappedResult(*fd, &ovl, &written, TRUE));
        retries += (written == 0);
        offset += written;
        cur += written;
    }
    return (retries < 3);
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
init_handle(void** handle, const std::string& filename)
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

zarr::FileSink::FileSink(std::string_view filename)
//  : file_(filename.data(), std::ios::binary | std::ios::trunc)
{
    std::string file_path{ filename };
    init_handle(&handle_, file_path);
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
zarr::FileSink::write_vectors(size_t offset,
                              const std::vector<ConstByteSpan>& data)
{
    LOG_ERROR("Not yet implemented.");
    return false;
}

bool
zarr::FileSink::flush_()
{
    return flush_file(&handle_);
}

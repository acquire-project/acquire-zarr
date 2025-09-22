#include "definitions.hh"
#include "macros.hh"

#include <cstring>
#include <fcntl.h>
#include <sys/uio.h>
#include <unistd.h>

std::string
get_last_error_as_string()
{
    if (auto* err = strerror(errno); err != nullptr) {
        return std::string(err);
    }
    return "";
}

size_t
get_page_size()
{
    return sysconf(_SC_PAGESIZE);
}

size_t
get_sector_size(const std::string& /*path*/)
{
    return 0; // no additional alignment needed on POSIX
}

size_t
align_to_system_size(const size_t size, const size_t, const size_t)
{
    return size; // no additional alignment needed on POSIX
}

void
init_handle(void** handle, const std::string& filename, bool)
{
    EXPECT(handle, "Expected nonnull pointer file handle.");
    auto* fd = new int;

    *fd = open(filename.c_str(), O_WRONLY | O_CREAT, 0644);
    if (*fd < 0) {
        const auto err = get_last_error_as_string();
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
    const auto* fd = static_cast<int*>(*handle);

    auto* cur = reinterpret_cast<const char*>(data.data());
    auto* end = cur + data.size();

    int retries = 0;
    const auto max_retries = 3;
    while (cur < end && retries < max_retries) {
        size_t remaining = end - cur;
        ssize_t written = pwrite(*fd, cur, remaining, offset);
        if (written < 0) {
            const auto err = get_last_error_as_string();
            throw std::runtime_error("Failed to write to file: " + err);
        }
        retries += (written == 0) ? 1 : 0;
        offset += written;
        cur += written;
    }

    return retries < max_retries;
}

bool
flush_file(void** handle)
{
    CHECK(handle);
    const auto* fd = static_cast<int*>(*handle);

    const auto res = fsync(*fd);
    if (res < 0) {
        LOG_ERROR("Failed to flush file: ", get_last_error_as_string());
    }

    return res == 0;
}

void
destroy_handle(void** handle)
{
    if (const auto* fd = static_cast<int*>(*handle)) {
        if (*fd >= 0) {
            close(*fd);
        }
        delete fd;
    }
}

void
reopen_handle(void**, const std::string&, bool)
{
    // no-op for POSIX implementation, as the same flags are used for sequential
    // or vectorized writes
}

bool
write_vectors(void** handle,
              size_t offset,
              size_t /* page_size */,
              size_t /* sector_size */,
              const std::vector<std::vector<uint8_t>>& buffers)
{
    CHECK(handle);
    const auto* fd = static_cast<int*>(*handle);

    std::vector<iovec> iovecs(buffers.size());

    for (auto i = 0; i < buffers.size(); ++i) {
        auto* iov = &iovecs[i];
        memset(iov, 0, sizeof(iovec));
        iov->iov_base =
          const_cast<void*>(static_cast<const void*>(buffers[i].data()));
        iov->iov_len = buffers[i].size();
    }

    ssize_t total_bytes = 0;
    for (const auto& buffer : buffers) {
        total_bytes += static_cast<ssize_t>(buffer.size());
    }

    const ssize_t bytes_written = pwritev(*fd,
                                          iovecs.data(),
                                          static_cast<int>(iovecs.size()),
                                          static_cast<off_t>(offset));

    if (bytes_written != total_bytes) {
        LOG_ERROR("Failed to write file: ",
                  get_last_error_as_string(),
                  ". Expected bytes written: ",
                  total_bytes,
                  ", actual: ",
                  bytes_written);
        return false;
    }

    return true;
}
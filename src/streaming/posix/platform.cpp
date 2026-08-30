#include "definitions.hh"
#include "macros.hh"

#include <string_view>

#include <cstring>
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <unistd.h>

std::string
get_last_error_as_string()
{
    return strerror(errno);
}

void*
make_flags(bool direct_io)
{
    auto* flags = new int;
    *flags = O_WRONLY | O_CREAT;

    if (direct_io) {
        // A streaming writer never reads back what it wrote, so page-cache
        // residency is pure cost; on a large sustained write it exhausts the
        // host's high-order free lists and starves unrelated kernel-context
        // contiguous allocations.
        //
        // Not the default: shards pack variable-length compressed chunks at
        // unaligned offsets and append an index footer, and a block-backed
        // filesystem rejects unaligned direct writes with EINVAL. NFS accepts
        // them, since the client turns direct writes into WRITE RPCs without
        // imposing the alignment check.
#ifdef O_DIRECT
        *flags |= O_DIRECT;
#else
        // macOS and other POSIX platforms without O_DIRECT: warn once rather
        // than on every open.
        [[maybe_unused]] static const bool warned = [] {
            LOG_WARNING("Direct I/O was requested, but O_DIRECT is not "
                        "available on this platform; writes will go through "
                        "the OS page cache.");
            return true;
        }();
#endif
    }

    return flags;
}

void
destroy_flags(const void* flags)
{
    const auto* fd = static_cast<const int*>(flags);
    delete fd;
}

uint64_t
get_max_active_handles()
{
    rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        return rl.rlim_cur; // current soft limit
        // rl.rlim_max gives hard limit
    }
    return 0; // error
}

void*
init_handle(const std::string& filename, const void* flags)
{
    auto* fd = new int;

    *fd = open(filename.data(), *static_cast<const int*>(flags), 0644);
    if (*fd < 0) {
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
    const auto* fd = static_cast<int*>(handle);

    auto* cur = reinterpret_cast<const char*>(data.data());
    auto* end = cur + data.size();

    int retries = 0;
    constexpr auto max_retries = 3;
    while (cur < end && retries < max_retries) {
        const size_t remaining = end - cur;
        const ssize_t written = pwrite(*fd, cur, remaining, offset);
        if (written < 0) {
            const auto err = get_last_error_as_string();
            throw std::runtime_error("Failed to write to file: " + err);
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
    const auto* fd = static_cast<int*>(handle);

    const auto res = fsync(*fd);
    if (res < 0) {
        LOG_ERROR("Failed to flush file: ", get_last_error_as_string());
    }

    return res == 0;
}

bool
truncate_file(void* handle, size_t size)
{
    CHECK(handle);
    const auto* fd = static_cast<int*>(handle);
    if (*fd < 0) {
        return false;
    }

    if (ftruncate(*fd, static_cast<off_t>(size)) < 0) {
        LOG_ERROR("Failed to truncate file: ", get_last_error_as_string());
        return false;
    }
    return true;
}

void
destroy_handle(void* handle)
{
    if (const auto* fd = static_cast<int*>(handle)) {
        if (*fd >= 0) {
            close(*fd);
        }
        delete fd;
    }
}
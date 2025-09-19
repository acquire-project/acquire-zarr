#include <sys/uio.h>
#include <unistd.h>

#include <cstddef> // size_t
#include <cstring> // strerror
#include <mutex>
#include <stdexcept>

#include "macros.hh"

namespace {
size_t PAGE_SIZE = 0;
std::mutex page_mutex;
} // namespace

size_t
get_page_size()
{
    std::lock_guard lock(page_mutex);
    if (PAGE_SIZE == 0) {
        PAGE_SIZE = sysconf(_SC_PAGESIZE);
        EXPECT(PAGE_SIZE > 0, "Failed to get page size");
    }

    return PAGE_SIZE;
}

size_t
get_system_alignment_size(const std::string& /*path*/)
{
    return 0; // no additional alignment needed on POSIX
}

size_t
align_to_system_size(const size_t size, const size_t /*align*/)
{
    return size; // no additional alignment needed on POSIX
}

std::string
get_last_error_as_string()
{
    if (auto* err = strerror(errno); err != nullptr) {
        return std::string(err);
    }
    return "";
}
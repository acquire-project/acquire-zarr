#include <windows.h>

#include <cstddef>
#include <mutex>
#include <string>

#include "macros.hh"

#include <zarr.common.hh>

namespace {
size_t PAGE_SIZE = 0;
std::mutex page_mutex;
} // namespace

size_t
get_page_size()
{
    std::lock_guard lock(page_mutex);
    if (PAGE_SIZE == 0) {
        SYSTEM_INFO si;
        GetSystemInfo(&si);
        PAGE_SIZE = si.dwPageSize;
        EXPECT(PAGE_SIZE > 0, "Failed to get page size");
    }

    return PAGE_SIZE;
}

size_t
get_system_alignment_size(const std::string& path)
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
align_to_system_size(const size_t size, const size_t align)
{
    // on Windows, we align first to page size then to `align` (sector size)
    const size_t page_aligned = zarr::align_to(size, get_page_size());
    return zarr::align_to(page_aligned, align);
}

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
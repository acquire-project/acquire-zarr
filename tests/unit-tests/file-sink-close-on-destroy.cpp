// Verify that destroying a FileSink closes the pooled file handle so the OS
// can reclaim disk blocks when the file is subsequently deleted (issue #226).
//
// Observable:
//   Windows -- CreateFileA uses FILE_SHARE_READ|FILE_SHARE_WRITE but not
//              FILE_SHARE_DELETE, so DeleteFile fails (ERROR_SHARING_VIOLATION)
//              while the HANDLE is still cached in the pool.  With the fix the
//              destructor calls pool->close(), evicting the handle before the
//              remove.
//   Linux   -- unlink always succeeds, but open fds keep the inode alive and
//              disk blocks are not freed until all fds are closed.  We count
//              /proc/self/fd entries before and after to confirm the fd was
//              actually released.

#include "file.sink.hh"
#include "unit.test.macros.hh"

#include <filesystem>

namespace fs = std::filesystem;

#if defined(__linux__)
static int
count_open_fds()
{
    int n = 0;
    for ([[maybe_unused]] auto& _ : fs::directory_iterator("/proc/self/fd")) {
        ++n;
    }
    return n;
}
#endif

int
main()
{
    int retval = 0;
    const fs::path tmp_path = fs::temp_directory_path() / TEST;

    try {
        auto pool = std::make_shared<zarr::FileHandlePool>();

#if defined(__linux__)
        const int fds_before = count_open_fds();
#endif

        {
            char data[] = "close-on-destroy";
            std::span<uint8_t> span = { reinterpret_cast<uint8_t*>(data),
                                        sizeof(data) - 1 };
            auto sink = std::make_unique<zarr::FileSink>(
              tmp_path.string(), pool);
            CHECK(sink->write(0, span));
            CHECK(zarr::finalize_sink(std::move(sink)));
            // ~FileSink() called here -- pool->close(filename_) must evict the
            // cached handle before we attempt the remove below.
        }

#if defined(__linux__)
        const int fds_after = count_open_fds();
        EXPECT(fds_after <= fds_before,
               "Open fd count did not decrease after FileSink destruction "
               "(before=",
               fds_before,
               " after=",
               fds_after,
               "): handle may still be cached in the pool");
#endif

        // On Windows this fails with ERROR_SHARING_VIOLATION if the HANDLE is
        // still open; on all platforms it must succeed with the fix in place.
        std::error_code ec;
        EXPECT(fs::remove(tmp_path, ec),
               "fs::remove failed after FileSink destruction "
               "(handle may still be open): ",
               ec.message());

    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
        retval = 1;
    }

    std::error_code ec;
    fs::remove(tmp_path, ec);
    return retval;
}

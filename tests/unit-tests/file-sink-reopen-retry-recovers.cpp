// Companion to file-sink-reopen-after-close: that test covers the give-up path
// (a persistent open failure is reported as a recoverable false); this one
// covers the RECOVERY path added by the open retry in get_handle.
//
// After #226 a finalized file's handle is closed, so writing it again must
// reopen via get_handle() -> open(). get_handle retries a failed open with a
// short backoff. Here the first open fails (the directory is gone) and another
// thread recreates the directory during the backoff window, so a later attempt
// succeeds: write() must return true. This proves the retry recovers a
// transient failure rather than only failing gracefully.
//
// Timing: the writer's first open runs microseconds after the worker is
// signalled and reliably fails (dir absent); the directory is recreated ~3 ms
// later, well inside the retry window, so a subsequent attempt succeeds. Even
// under scheduling skew the test never fails spuriously -- worst case the first
// attempt happens to succeed and the retry simply isn't exercised.

#include "file.sink.hh"
#include "unit.test.macros.hh"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <span>
#include <thread>

namespace fs = std::filesystem;

int
main()
{
    int retval = 0;
    const fs::path dir = fs::temp_directory_path() / (std::string(TEST) + "-d");
    const fs::path file = dir / "chunk";

    try {
        auto pool = std::make_shared<zarr::FileHandlePool>();

        char data[] = "reopen-retry-recovers";
        std::span<uint8_t> span = { reinterpret_cast<uint8_t*>(data),
                                    sizeof(data) - 1 };

        fs::create_directories(dir);

        // Write + finalize so the pooled handle is closed (#226); the next
        // write must reopen.
        {
            auto sink =
              std::make_unique<zarr::FileSink>(file.string(), pool);
            CHECK(sink->write(0, span));
            CHECK(zarr::finalize_sink(std::move(sink)));
        }

        // Remove the directory: the next open fails (ENOENT) until recreated.
        std::error_code ec;
        fs::remove_all(dir, ec);

        // Recreate the directory shortly after the write starts -- inside
        // get_handle's retry/backoff window -- so a retry attempt succeeds.
        std::atomic<bool> started{ false };
        std::thread recreate([&] {
            while (!started.load()) {
                std::this_thread::yield();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(3));
            std::error_code mk;
            fs::create_directories(dir, mk);
        });

        auto sink2 = std::make_unique<zarr::FileSink>(file.string(), pool);
        started.store(true);
        const bool ok = sink2->write(0, span); // attempt 1 fails, retry recovers
        recreate.join();

        EXPECT(ok,
               "FileSink::write did not recover after the directory was "
               "recreated during get_handle's retry backoff -- the open retry "
               "is not working.");
        EXPECT(fs::exists(file),
               "expected the chunk file to exist after a recovered write");

    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
        retval = 1;
    }

    std::error_code ec;
    fs::remove_all(dir, ec);
    return retval;
}

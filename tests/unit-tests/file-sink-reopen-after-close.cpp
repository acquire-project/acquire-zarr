// Regression test for the #226 close-on-finalize change.
//
// #226 made ~FileSink call pool->close() so a finalized file's fd is released
// and the OS can reclaim its disk blocks. The side effect: a file that was
// written, finalized, and is then written AGAIN must REOPEN via
// FileHandlePool::get_handle() -> open(). If that reopen fails -- e.g. a
// concurrent process removed the containing directory, which is exactly what
// `livescreen sync` reclaim does to sealed chunk directories -- get_handle()
// throws std::runtime_error (from init_handle()).
//
// In FileSink::write() that get_handle() call sits OUTSIDE the try/catch, so
// the throw escapes write(), propagates up through the acquire-zarr worker,
// and is reported as a fatal "Failed to append data to Zarr stream: Internal
// error" that kills the writer thread. Before #226 the handle stayed open for
// the store's lifetime, so this reopen never happened and the failure mode did
// not exist.
//
// Desired contract: a failed (re)open is a recoverable I/O error -- write()
// should return false, never throw out of itself. This test encodes that
// contract, so it is EXPECTED TO FAIL on current main (write() throws) and to
// pass once write()/get_handle handle open failures gracefully.

#include "file.sink.hh"
#include "unit.test.macros.hh"

#include <filesystem>
#include <span>

namespace fs = std::filesystem;

int
main()
{
    int retval = 0;
    const fs::path dir = fs::temp_directory_path() / (std::string(TEST) + "-d");
    const fs::path file = dir / "chunk";

    try {
        auto pool = std::make_shared<zarr::FileHandlePool>();

        char data[] = "reopen-after-close";
        std::span<uint8_t> span = { reinterpret_cast<uint8_t*>(data),
                                    sizeof(data) - 1 };

        fs::create_directories(dir);

        // 1. Write + finalize. ~FileSink closes the pooled handle (#226), so the
        //    file is no longer held open by the pool.
        {
            auto sink =
              std::make_unique<zarr::FileSink>(file.string(), pool);
            CHECK(sink->write(0, span));
            CHECK(zarr::finalize_sink(std::move(sink)));
        }

        // 2. The directory disappears underneath the writer (models sync reclaim
        //    deleting a sealed chunk dir, or any concurrent removal).
        std::error_code ec;
        fs::remove_all(dir, ec);

        // 3. A further write must REOPEN the file (its handle was closed in 1).
        //    open() now fails with ENOENT (parent gone). The writer must see a
        //    graceful failure -- not an exception escaping write().
        auto sink2 = std::make_unique<zarr::FileSink>(file.string(), pool);
        bool threw = false;
        bool ok = true;
        try {
            ok = sink2->write(0, span);
        } catch (const std::exception& e) {
            threw = true;
            LOG_ERROR("write() threw on a failed reopen: ", e.what());
        }

        EXPECT(!threw,
               "FileSink::write threw on a failed reopen instead of returning "
               "false. An uncaught throw here escapes write(), propagates "
               "through the acquire-zarr worker, and surfaces as a fatal "
               "'Failed to append data to Zarr stream: Internal error' that "
               "kills the writer thread (#226 close-on-finalize regression).");
        EXPECT(!ok,
               "FileSink::write should return false when the file cannot be "
               "(re)opened.");

    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
        retval = 1;
    }

    std::error_code ec;
    fs::remove_all(dir, ec);
    return retval;
}

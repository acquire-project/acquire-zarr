#include "unit.test.macros.hh"
#include "sink.hh"

#include <filesystem>

namespace fs = std::filesystem;

int
main()
{
    int retval = 1;
    auto temp_dir = fs::temp_directory_path() / TEST;

    auto thread_pool = std::make_shared<zarr::ThreadPool>(
      std::thread::hardware_concurrency(),
      [](const std::string& err) { LOG_ERROR("Error: ", err); });

    std::vector<std::string> dir_paths = { (temp_dir / "a").string(),
                                           (temp_dir / "b/c").string(),
                                           (temp_dir / "d/e/f").string() };

    try {
        for (const auto& dir_path : dir_paths) {
            EXPECT(
              !fs::exists(dir_path), "Directory ", dir_path, " already exists");
        }

        EXPECT(zarr::make_dirs(dir_paths, thread_pool),
               "Failed to create dirs.");
        for (const auto& dir_path : dir_paths) {
            EXPECT(fs::is_directory(temp_dir / dir_path),
                   "Failed to create directory ",
                   dir_path);
        }
        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }

    // cleanup
    if (fs::exists(temp_dir)) {
        fs::remove_all(temp_dir);
    }

    return retval;
}

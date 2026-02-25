#include "thread.pool.hh"
#include "unit-test-utils.hh"

#include <filesystem>
#include <fstream>
#include <iostream>

namespace fs = std::filesystem;

int
main()
{
    int retval = 0;

    fs::path tmp_path = fs::temp_directory_path() / TEST;

    try {
        CHECK(!fs::exists(tmp_path));

        zarr::ThreadPool pool{ 1, [](const std::string&) {} };

        CHECK(pool.push_job([&tmp_path](std::string&) {
            std::ofstream ofs(tmp_path);
            ofs << "Hello, Acquire!";
            ofs.close();
            return true;
        }));
        pool.await_stop();

        CHECK(fs::exists(tmp_path));

        std::ifstream ifs(tmp_path);
        CHECK(ifs.is_open());

        std::string contents;
        while (!ifs.eof()) {
            std::getline(ifs, contents);
        }
        ifs.close();

        EXPECT_STR_EQ(contents.c_str(), "Hello, Acquire!");
    } catch (const std::exception& e) {
        LOG_ERROR("Caught exception: ", e.what());
        retval = 1;
    }

    std::error_code ec;
    if (!fs::remove(tmp_path, ec)) {
        LOG_ERROR("Failed to remove file: ", ec.message());
        retval = 1;
    }

    return retval;
}
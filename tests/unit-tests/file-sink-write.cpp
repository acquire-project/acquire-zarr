#include "file.sink.hh"
#include "unit.test.macros.hh"

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
        {
            char str[] = "Hello, Acquire!";
            auto sink = std::make_unique<zarr::FileSink>(
              tmp_path.string(), std::make_shared<zarr::FileHandlePool>());

            std::span data = { reinterpret_cast<uint8_t*>(str),
                               sizeof(str) - 1 };
            CHECK(sink->write(0, data));
            CHECK(zarr::finalize_sink(std::move(sink)));
        }

        // The file tmp_path should now contain the string "Hello, world!\n".
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
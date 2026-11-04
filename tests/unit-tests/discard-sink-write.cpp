#include "array.dimensions.hh"
#include "discard.sink.hh"
#include "unit.test.macros.hh"

#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

int
main()
{
    int retval = 0;
    const fs::path scratch = fs::temp_directory_path() / TEST;
    const fs::path original_cwd = fs::current_path();

    try {
        fs::remove_all(scratch);
        fs::create_directories(scratch);
        fs::current_path(scratch); // a stray relative write lands here

        CHECK(zarr::is_discard_path("discard://"));
        CHECK(zarr::is_discard_path("discard://a/b/zarr.json"));
        CHECK(!zarr::is_discard_path("discard:/a"));
        CHECK(!zarr::is_discard_path("/tmp/discard://a"));
        CHECK(!zarr::is_discard_path("file://a"));

        auto sink = zarr::make_file_sink(
          "discard://store.zarr/0/0", std::make_shared<zarr::FileHandlePool>());
        CHECK(sink);
        CHECK(dynamic_cast<zarr::DiscardSink*>(sink.get()) != nullptr);
        CHECK(sink->memory_usage() == 0);

        std::vector<uint8_t> data(1 << 20, 0xab);
        CHECK(sink->write(0, data));
        CHECK(sink->write(1 << 30, data)); // offset beyond any real file
        CHECK(zarr::finalize_sink(std::move(sink)));

        // metadata sinks take whole-file writes only, as FileSink does
        auto metadata_sink =
          zarr::make_file_sink("discard://store.zarr/zarr.json",
                               std::make_shared<zarr::FileHandlePool>(),
                               /*truncate_to_fit=*/true);
        CHECK(metadata_sink->write(0, data));
        CHECK(!metadata_sink->write(1, data));
        CHECK(zarr::finalize_sink(std::move(metadata_sink)));

        std::vector<ZarrDimension> dim_list{
            { "t", ZarrDimensionType_Time, 0, 1, 1 },
            { "y", ZarrDimensionType_Space, 4, 2, 1 },
            { "x", ZarrDimensionType_Space, 4, 2, 1 },
        };
        ArrayDimensions dims(std::move(dim_list), ZarrDataType_uint8);
        const auto paths = zarr::construct_data_paths(
          "discard://store.zarr",
          dims,
          [](const ZarrDimension&) { return 2; },
          /*make_directories=*/true);
        EXPECT_EQ(size_t, paths.size(), 4);
        for (const auto& path : paths) {
            CHECK(zarr::is_discard_path(path));
        }

        CHECK(fs::is_empty(scratch));
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
        retval = 1;
    }

    fs::current_path(original_cwd);
    fs::remove_all(scratch);
    return retval;
}

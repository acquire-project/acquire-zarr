#include "unit-test-utils.hh"
#include "sink.hh"
#include "array.dimensions.hh"

#include <string>
#include <unordered_set>
#include <vector>

namespace {
auto
create_parts_fun(size_t parts)
{
    return [parts](const ZarrDimension&) { return parts; };
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        std::vector<ZarrDimension> dims{
            { "time", ZarrDimensionType_Time, 50, 16, 2 },
            { "height", ZarrDimensionType_Space, 100, 32, 2 },
            { "width", ZarrDimensionType_Space, 100, 32, 2 }
        };
        ArrayDimensions dimensions(std::move(dims), ZarrDataType_uint8);
        {
            const auto parts_fun = create_parts_fun(2);
            const auto paths =
              zarr::construct_data_paths("", dimensions, parts_fun);

            EXPECT_EQ(int, paths.size(), 4);
            EXPECT_STR_EQ(paths[0].c_str(), "0/0");
            EXPECT_STR_EQ(paths[1].c_str(), "0/1");
            EXPECT_STR_EQ(paths[2].c_str(), "1/0");
            EXPECT_STR_EQ(paths[3].c_str(), "1/1");
        }

        {
            const auto parts_fun = create_parts_fun(3);
            const auto paths =
              zarr::construct_data_paths("", dimensions, parts_fun);

            EXPECT_EQ(int, paths.size(), 9);
            EXPECT_STR_EQ(paths[0].c_str(), "0/0");
            EXPECT_STR_EQ(paths[1].c_str(), "0/1");
            EXPECT_STR_EQ(paths[2].c_str(), "0/2");
            EXPECT_STR_EQ(paths[3].c_str(), "1/0");
            EXPECT_STR_EQ(paths[4].c_str(), "1/1");
            EXPECT_STR_EQ(paths[5].c_str(), "1/2");
            EXPECT_STR_EQ(paths[6].c_str(), "2/0");
            EXPECT_STR_EQ(paths[7].c_str(), "2/1");
            EXPECT_STR_EQ(paths[8].c_str(), "2/2");
        }

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Test failed: ", e.what());
        throw;
    }

    return retval;
}

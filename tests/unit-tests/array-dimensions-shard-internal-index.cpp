#include "array.dimensions.hh"
#include "unit-test-utils.hh"

#include <stdexcept>

int
main()
{
    int retval = 1;

    std::vector<ZarrDimension> dims;
    dims.emplace_back("t",
                      ZarrDimensionType_Time,
                      0,
                      32, // 32 timepoints / chunk
                      1); // 1 shard
    dims.emplace_back("y",
                      ZarrDimensionType_Space,
                      960,
                      320, // 3 chunks
                      2);  // 2 ragged shards
    dims.emplace_back("x",
                      ZarrDimensionType_Space,
                      1080,
                      270, // 4 chunks
                      3);  // 2 ragged shards
    ArrayDimensions dimensions(std::move(dims), ZarrDataType_uint64);

    try {
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(0), 0);
        EXPECT_EQ(int, dimensions.shard_internal_index(0), 0);

        EXPECT_EQ(int, dimensions.shard_index_for_chunk(1), 0);
        EXPECT_EQ(int, dimensions.shard_internal_index(1), 1);

        EXPECT_EQ(int, dimensions.shard_index_for_chunk(2), 0);
        EXPECT_EQ(int, dimensions.shard_internal_index(2), 2);

        EXPECT_EQ(int, dimensions.shard_index_for_chunk(3), 1);
        EXPECT_EQ(int, dimensions.shard_internal_index(3), 0);

        EXPECT_EQ(int, dimensions.shard_index_for_chunk(4), 0);
        EXPECT_EQ(int, dimensions.shard_internal_index(4), 3);

        EXPECT_EQ(int, dimensions.shard_index_for_chunk(5), 0);
        EXPECT_EQ(int, dimensions.shard_internal_index(5), 4);

        EXPECT_EQ(int, dimensions.shard_index_for_chunk(6), 0);
        EXPECT_EQ(int, dimensions.shard_internal_index(6), 5);

        EXPECT_EQ(int, dimensions.shard_index_for_chunk(7), 1);
        EXPECT_EQ(int, dimensions.shard_internal_index(7), 3);

        EXPECT_EQ(int, dimensions.shard_index_for_chunk(8), 2);
        EXPECT_EQ(int, dimensions.shard_internal_index(8), 0);

        EXPECT_EQ(int, dimensions.shard_index_for_chunk(9), 2);
        EXPECT_EQ(int, dimensions.shard_internal_index(9), 1);

        EXPECT_EQ(int, dimensions.shard_index_for_chunk(10), 2);
        EXPECT_EQ(int, dimensions.shard_internal_index(10), 2);

        EXPECT_EQ(int, dimensions.shard_index_for_chunk(11), 3);
        EXPECT_EQ(int, dimensions.shard_internal_index(11), 0);
        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }

    return retval;
}
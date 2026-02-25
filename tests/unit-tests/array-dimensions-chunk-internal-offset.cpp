#include "array.dimensions.hh"
#include "unit-test-utils.hh"

#include <stdexcept>

int
main()
{
    int retval = 1;

    std::vector<ZarrDimension> dims;
    dims.emplace_back(
      "t", ZarrDimensionType_Time, 0, 5, 0); // 5 timepoints / chunk
    dims.emplace_back("c", ZarrDimensionType_Channel, 3, 2, 0); // 2 chunks
    dims.emplace_back("z", ZarrDimensionType_Space, 5, 2, 0);   // 3 chunks
    dims.emplace_back("y", ZarrDimensionType_Space, 48, 16, 0); // 3 chunks
    dims.emplace_back("x", ZarrDimensionType_Space, 64, 16, 0); // 4 chunks
    ArrayDimensions dimensions(std::move(dims), ZarrDataType_uint16);

    try {
        EXPECT_EQ(int, dimensions.chunk_internal_offset(0), 0);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(1), 512);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(2), 0);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(3), 512);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(4), 0);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(5), 1024);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(6), 1536);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(7), 1024);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(8), 1536);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(9), 1024);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(10), 0);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(11), 512);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(12), 0);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(13), 512);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(14), 0);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(15), 2048);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(16), 2560);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(17), 2048);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(18), 2560);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(19), 2048);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(20), 3072);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(21), 3584);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(22), 3072);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(23), 3584);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(24), 3072);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(25), 2048);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(26), 2560);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(27), 2048);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(28), 2560);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(29), 2048);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(30), 4096);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(31), 4608);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(32), 4096);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(33), 4608);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(34), 4096);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(35), 5120);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(36), 5632);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(37), 5120);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(38), 5632);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(39), 5120);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(40), 4096);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(41), 4608);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(42), 4096);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(43), 4608);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(44), 4096);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(45), 6144);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(46), 6656);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(47), 6144);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(48), 6656);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(49), 6144);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(50), 7168);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(51), 7680);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(52), 7168);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(53), 7680);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(54), 7168);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(55), 6144);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(56), 6656);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(57), 6144);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(58), 6656);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(59), 6144);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(60), 8192);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(61), 8704);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(62), 8192);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(63), 8704);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(64), 8192);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(65), 9216);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(66), 9728);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(67), 9216);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(68), 9728);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(69), 9216);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(70), 8192);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(71), 8704);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(72), 8192);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(73), 8704);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(74), 8192);
        EXPECT_EQ(int, dimensions.chunk_internal_offset(75), 0);

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }

    return retval;
}
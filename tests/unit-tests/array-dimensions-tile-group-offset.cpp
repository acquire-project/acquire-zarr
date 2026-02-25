#include "zarr.common.hh"
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
    ArrayDimensions dimensions(std::move(dims), ZarrDataType_float32);

    try {
        EXPECT_EQ(int, dimensions.tile_group_offset(0), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(1), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(2), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(3), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(4), 24);
        EXPECT_EQ(int, dimensions.tile_group_offset(5), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(6), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(7), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(8), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(9), 24);
        EXPECT_EQ(int, dimensions.tile_group_offset(10), 36);
        EXPECT_EQ(int, dimensions.tile_group_offset(11), 36);
        EXPECT_EQ(int, dimensions.tile_group_offset(12), 48);
        EXPECT_EQ(int, dimensions.tile_group_offset(13), 48);
        EXPECT_EQ(int, dimensions.tile_group_offset(14), 60);
        EXPECT_EQ(int, dimensions.tile_group_offset(15), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(16), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(17), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(18), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(19), 24);
        EXPECT_EQ(int, dimensions.tile_group_offset(20), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(21), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(22), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(23), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(24), 24);
        EXPECT_EQ(int, dimensions.tile_group_offset(25), 36);
        EXPECT_EQ(int, dimensions.tile_group_offset(26), 36);
        EXPECT_EQ(int, dimensions.tile_group_offset(27), 48);
        EXPECT_EQ(int, dimensions.tile_group_offset(28), 48);
        EXPECT_EQ(int, dimensions.tile_group_offset(29), 60);
        EXPECT_EQ(int, dimensions.tile_group_offset(30), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(31), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(32), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(33), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(34), 24);
        EXPECT_EQ(int, dimensions.tile_group_offset(35), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(36), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(37), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(38), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(39), 24);
        EXPECT_EQ(int, dimensions.tile_group_offset(40), 36);
        EXPECT_EQ(int, dimensions.tile_group_offset(41), 36);
        EXPECT_EQ(int, dimensions.tile_group_offset(42), 48);
        EXPECT_EQ(int, dimensions.tile_group_offset(43), 48);
        EXPECT_EQ(int, dimensions.tile_group_offset(44), 60);
        EXPECT_EQ(int, dimensions.tile_group_offset(45), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(46), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(47), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(48), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(49), 24);
        EXPECT_EQ(int, dimensions.tile_group_offset(50), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(51), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(52), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(53), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(54), 24);
        EXPECT_EQ(int, dimensions.tile_group_offset(55), 36);
        EXPECT_EQ(int, dimensions.tile_group_offset(56), 36);
        EXPECT_EQ(int, dimensions.tile_group_offset(57), 48);
        EXPECT_EQ(int, dimensions.tile_group_offset(58), 48);
        EXPECT_EQ(int, dimensions.tile_group_offset(59), 60);
        EXPECT_EQ(int, dimensions.tile_group_offset(60), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(61), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(62), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(63), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(64), 24);
        EXPECT_EQ(int, dimensions.tile_group_offset(65), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(66), 0);
        EXPECT_EQ(int, dimensions.tile_group_offset(67), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(68), 12);
        EXPECT_EQ(int, dimensions.tile_group_offset(69), 24);
        EXPECT_EQ(int, dimensions.tile_group_offset(70), 36);
        EXPECT_EQ(int, dimensions.tile_group_offset(71), 36);
        EXPECT_EQ(int, dimensions.tile_group_offset(72), 48);
        EXPECT_EQ(int, dimensions.tile_group_offset(73), 48);
        EXPECT_EQ(int, dimensions.tile_group_offset(74), 60);
        EXPECT_EQ(int, dimensions.tile_group_offset(75), 0);

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }

    return retval;
}
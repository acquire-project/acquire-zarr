#include "array.dimensions.hh"
#include "unit-test-utils.hh"

#include <stdexcept>

int
main()
{
    int retval = 1;

    try {
        std::vector<ZarrDimension> dims;
        dims.emplace_back("t",
                          ZarrDimensionType_Time,
                          0,
                          5,  // 5 timepoints / chunk
                          2); // 2 chunks / shard
        dims.emplace_back("c",
                          ZarrDimensionType_Channel,
                          8,
                          4,  // 8 / 4 = 2 chunks
                          2); // 4 / 2 = 2 shards
        dims.emplace_back("z",
                          ZarrDimensionType_Space,
                          6,
                          2,  // 6 / 2 = 3 chunks
                          1); // 3 / 1 = 3 shards
        dims.emplace_back("y",
                          ZarrDimensionType_Space,
                          48,
                          16, // 48 / 16 = 3 chunks
                          1); // 3 / 1 = 3 shards
        dims.emplace_back("x",
                          ZarrDimensionType_Space,
                          64,
                          16, // 64 / 16 = 4 chunks
                          2); // 4 / 2 = 2 shards
        ArrayDimensions dimensions(std::move(dims), ZarrDataType_uint32);

        EXPECT_EQ(int, dimensions.shard_index_for_chunk(0), 0);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(1), 0);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(2), 1);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(3), 1);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(4), 2);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(5), 2);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(6), 3);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(7), 3);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(8), 4);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(9), 4);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(10), 5);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(11), 5);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(12), 6);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(13), 6);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(14), 7);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(15), 7);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(16), 8);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(17), 8);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(18), 9);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(19), 9);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(20), 10);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(21), 10);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(22), 11);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(23), 11);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(24), 12);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(25), 12);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(26), 13);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(27), 13);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(28), 14);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(29), 14);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(30), 15);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(31), 15);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(32), 16);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(33), 16);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(34), 17);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(35), 17);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(36), 0);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(37), 0);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(38), 1);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(39), 1);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(40), 2);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(41), 2);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(42), 3);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(43), 3);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(44), 4);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(45), 4);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(46), 5);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(47), 5);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(48), 6);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(49), 6);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(50), 7);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(51), 7);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(52), 8);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(53), 8);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(54), 9);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(55), 9);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(56), 10);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(57), 10);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(58), 11);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(59), 11);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(60), 12);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(61), 12);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(62), 13);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(63), 13);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(64), 14);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(65), 14);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(66), 15);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(67), 15);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(68), 16);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(69), 16);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(70), 17);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(71), 17);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(72), 0);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(73), 0);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(74), 1);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(75), 1);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(76), 2);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(77), 2);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(78), 3);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(79), 3);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(80), 4);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(81), 4);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(82), 5);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(83), 5);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(84), 6);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(85), 6);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(86), 7);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(87), 7);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(88), 8);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(89), 8);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(90), 9);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(91), 9);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(92), 10);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(93), 10);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(94), 11);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(95), 11);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(96), 12);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(97), 12);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(98), 13);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(99), 13);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(100), 14);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(101), 14);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(102), 15);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(103), 15);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(104), 16);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(105), 16);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(106), 17);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(107), 17);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(108), 0);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(109), 0);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(110), 1);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(111), 1);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(112), 2);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(113), 2);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(114), 3);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(115), 3);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(116), 4);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(117), 4);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(118), 5);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(119), 5);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(120), 6);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(121), 6);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(122), 7);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(123), 7);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(124), 8);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(125), 8);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(126), 9);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(127), 9);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(128), 10);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(129), 10);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(130), 11);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(131), 11);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(132), 12);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(133), 12);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(134), 13);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(135), 13);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(136), 14);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(137), 14);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(138), 15);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(139), 15);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(140), 16);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(141), 16);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(142), 17);
        EXPECT_EQ(int, dimensions.shard_index_for_chunk(143), 17);

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }

    return retval;
}
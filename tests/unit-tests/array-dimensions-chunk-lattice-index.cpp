#include "array.dimensions.hh"
#include "unit-test-utils.hh"

#include <stdexcept>

int
main()
{
    int retval = 1;

    try {
        std::vector<ZarrDimension> dims;
        dims.emplace_back(
          "t", ZarrDimensionType_Time, 0, 5, 0); // 5 timepoints / chunk
        dims.emplace_back("c", ZarrDimensionType_Channel, 3, 2, 0); // 2 chunks
        dims.emplace_back("z", ZarrDimensionType_Space, 5, 2, 0);   // 3 chunks
        dims.emplace_back("y", ZarrDimensionType_Space, 48, 16, 0); // 3 chunks
        dims.emplace_back("x", ZarrDimensionType_Space, 64, 16, 0); // 4 chunks
        ArrayDimensions dimensions(std::move(dims), ZarrDataType_uint8);

        EXPECT_EQ(int, dimensions.chunk_lattice_index(0, 2), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(0, 1), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(0, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(1, 2), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(1, 1), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(1, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(2, 2), 1);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(2, 1), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(2, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(3, 2), 1);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(3, 1), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(3, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(4, 2), 2);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(4, 1), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(4, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(5, 2), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(5, 1), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(5, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(12, 2), 1);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(12, 1), 1);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(12, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(19, 2), 2);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(19, 1), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(19, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(26, 2), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(26, 1), 1);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(26, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(33, 2), 1);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(33, 1), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(33, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(40, 2), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(40, 1), 1);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(40, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(47, 2), 1);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(47, 1), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(47, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(54, 2), 2);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(54, 1), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(54, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(61, 2), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(61, 1), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(61, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(68, 2), 1);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(68, 1), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(68, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(74, 2), 2);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(74, 1), 1);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(74, 0), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(75, 2), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(75, 1), 0);
        EXPECT_EQ(int, dimensions.chunk_lattice_index(75, 0), 1);

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }

    return retval;
}
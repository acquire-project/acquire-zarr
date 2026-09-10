// Band math for dim-1 "courtesy flush".
// @see czbiohub-sf/livescreen-acquisition#210
#include "array.dimensions.hh"
#include "unit.test.macros.hh"

#include <stdexcept>

namespace {
ArrayDimensions
make_dims(std::vector<ZarrDimension> dims,
          const std::vector<size_t>& order = {})
{
    return ArrayDimensions(std::move(dims), ZarrDataType_uint16, order);
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        // [t, z, y, x], append chunk 1, chunked z: the reported layout
        {
            std::vector<ZarrDimension> dims;
            dims.emplace_back("t", ZarrDimensionType_Time, 0, 1, 1);
            dims.emplace_back("z", ZarrDimensionType_Space, 1000, 64, 3);
            dims.emplace_back("y", ZarrDimensionType_Space, 64, 64, 1);
            dims.emplace_back("x", ZarrDimensionType_Space, 64, 64, 1);
            auto d = make_dims(std::move(dims));

            EXPECT(d.supports_dim1_banding(), "Expected banding to be enabled");
            // ceil(1000 / 64) == 16 bands along z
            EXPECT_EQ(int, d.dim1_band_count(), 16);
            // one z chunk's worth of frames per band (no dims inside z)
            EXPECT_EQ(int, d.frames_per_dim1_band(), 64);
            // a whole z volume per append-chunk layer
            EXPECT_EQ(int, d.frames_per_chunk_layer(), 1000);
            EXPECT_EQ(int, d.frames_per_shard_layer(), 1000); // t shard == 1
            // y, x are single chunks, so each band is exactly one chunk
            EXPECT_EQ(int, d.chunks_per_dim1_band(), 1);
            EXPECT_EQ(int,
                      d.chunks_per_dim1_band() * d.dim1_band_count(),
                      d.number_of_chunks_in_memory());
        }

        // Append chunk size > 1 disables banding (inner chunks span sweeps).
        {
            std::vector<ZarrDimension> dims;
            dims.emplace_back("t", ZarrDimensionType_Time, 0, 4, 1);
            dims.emplace_back("z", ZarrDimensionType_Space, 256, 64, 1);
            dims.emplace_back("y", ZarrDimensionType_Space, 64, 64, 1);
            dims.emplace_back("x", ZarrDimensionType_Space, 64, 64, 1);
            auto d = make_dims(std::move(dims));

            EXPECT(!d.supports_dim1_banding(),
                   "Expected banding disabled for append chunk > 1");
            // append chunk (4) * z array (256)
            EXPECT_EQ(int, d.frames_per_chunk_layer(), 1024);
        }

        // No intermediate dimension ([t, y, x]) disables banding.
        {
            std::vector<ZarrDimension> dims;
            dims.emplace_back("t", ZarrDimensionType_Time, 0, 1, 1);
            dims.emplace_back("y", ZarrDimensionType_Space, 64, 64, 1);
            dims.emplace_back("x", ZarrDimensionType_Space, 64, 64, 1);
            auto d = make_dims(std::move(dims));

            EXPECT(!d.supports_dim1_banding(),
                   "Expected banding disabled without an intermediate dim");
        }

        // a requested transposition disables banding
        {
            std::vector<ZarrDimension> dims;
            dims.emplace_back("t", ZarrDimensionType_Time, 0, 1, 1);
            dims.emplace_back("c", ZarrDimensionType_Channel, 4, 2, 1);
            dims.emplace_back("z", ZarrDimensionType_Space, 256, 64, 1);
            dims.emplace_back("y", ZarrDimensionType_Space, 64, 64, 1);
            dims.emplace_back("x", ZarrDimensionType_Space, 64, 64, 1);
            // store order [t, z, c, y, x] -- a non-identity permutation
            auto d = make_dims(std::move(dims), { 0, 2, 1, 3, 4 });

            EXPECT(!d.supports_dim1_banding(),
                   "Expected banding disabled under transposition");
        }

        // multiple intermediate dims [t, a, b, y, x]: bands run along a (dim 1)
        {
            std::vector<ZarrDimension> dims;
            dims.emplace_back("t", ZarrDimensionType_Time, 0, 1, 1);
            dims.emplace_back("a", ZarrDimensionType_Space, 8, 2, 1);
            dims.emplace_back("b", ZarrDimensionType_Space, 10, 5, 1);
            dims.emplace_back("y", ZarrDimensionType_Space, 64, 64, 1);
            dims.emplace_back("x", ZarrDimensionType_Space, 64, 64, 1);
            auto d = make_dims(std::move(dims));

            EXPECT(d.supports_dim1_banding(), "Expected banding to be enabled");
            EXPECT_EQ(int, d.dim1_band_count(), 4); // ceil(8 / 2) bands along a
            // a chunk (2) * full b array (10)
            EXPECT_EQ(int, d.frames_per_dim1_band(), 20);
            // a array (8) * b array (10)
            EXPECT_EQ(int, d.frames_per_chunk_layer(), 80);
            // chunks_per_band == chunks across b, y, x = 2 * 1 * 1
            EXPECT_EQ(int, d.chunks_per_dim1_band(), 2);
            EXPECT_EQ(int,
                      d.chunks_per_dim1_band() * d.dim1_band_count(),
                      d.number_of_chunks_in_memory());
        }

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Exception: ", exc.what());
    }

    return retval;
}

#include "acquire.zarr.h"
#include "zarr.stream.hh"
#include "unit.test.macros.hh"

#include <filesystem>

namespace fs = std::filesystem;

namespace {
void
configure_stream_dimensions(ZarrArraySettings* settings)
{
    CHECK(ZarrStatusCode_Success ==
          ZarrArraySettings_create_dimension_array(settings, 3));
    ZarrDimensionProperties* dim = settings->dimensions;

    *dim = ZarrDimensionProperties{
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 0,
        .chunk_size_px = 2,
        .shard_size_chunks = 1,
    };

    dim = settings->dimensions + 1;
    *dim = ZarrDimensionProperties{
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 48,
        .chunk_size_px = 16,
        .shard_size_chunks = 1,
    };

    dim = settings->dimensions + 2;
    *dim = ZarrDimensionProperties{
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 64,
        .chunk_size_px = 16,
        .shard_size_chunks = 1,
    };

    settings->is_ngff = false;
}

// An array key that is a strict prefix of another array key is a conflict:
// the shorter key wants to write an array node where the longer key needs a
// group. It must be rejected no matter which order the keys are declared in,
// otherwise one declaration order silently writes a group over the array and
// strands its chunks with no readable metadata.
bool
prefix_key_conflict_is_rejected(const char* first, const char* second)
{
    ZarrStreamSettings settings = { TEST ".zarr" };

    ZarrStreamSettings_create_arrays(&settings, 2);

    settings.arrays[0].output_key = first;
    configure_stream_dimensions(settings.arrays);

    settings.arrays[1].output_key = second;
    configure_stream_dimensions(settings.arrays + 1);

    ZarrStream* stream = ZarrStream_create(&settings);
    const bool retval = stream == nullptr; // impossible to configure this
    ZarrStream_destroy(stream);

    ZarrStreamSettings_destroy_arrays(&settings); // destroys dimensions

    return retval;
}

// Sibling keys sharing a parent directory are fine in either order; make sure
// the fix for the above does not reject legitimate hierarchies.
bool
sibling_keys_are_accepted(const char* first, const char* second)
{
    ZarrStreamSettings settings = { TEST ".zarr" };

    ZarrStreamSettings_create_arrays(&settings, 2);

    settings.arrays[0].output_key = first;
    configure_stream_dimensions(settings.arrays);

    settings.arrays[1].output_key = second;
    configure_stream_dimensions(settings.arrays + 1);

    ZarrStream* stream = ZarrStream_create(&settings);
    const bool retval = stream != nullptr; // configured correctly
    ZarrStream_destroy(stream);

    ZarrStreamSettings_destroy_arrays(&settings); // destroys dimensions

    return retval;
}
} // namespace

int
main()
{
    int retval = 0;

    try {
        // the short-key-first order was always rejected
        if (!prefix_key_conflict_is_rejected("foo", "foo/bar")) {
            LOG_ERROR("Erroneously successful configuration of array key "
                      "'foo' with child array key 'foo/bar'");
            retval = 1;
        }

        // the long-key-first order used to be accepted, writing a group
        // zarr.json over the array node declared by 'foo'
        if (!prefix_key_conflict_is_rejected("foo/bar", "foo")) {
            LOG_ERROR("Erroneously successful configuration of array key "
                      "'foo/bar' with parent array key 'foo'");
            retval = 1;
        }

        // deeper nesting, both orders
        if (!prefix_key_conflict_is_rejected("a/b/c", "a/b")) {
            LOG_ERROR("Erroneously successful configuration of array key "
                      "'a/b/c' with parent array key 'a/b'");
            retval = 1;
        }

        if (!prefix_key_conflict_is_rejected("a/b", "a/b/c")) {
            LOG_ERROR("Erroneously successful configuration of array key "
                      "'a/b' with child array key 'a/b/c'");
            retval = 1;
        }

        if (!sibling_keys_are_accepted("foo/bar", "foo/baz")) {
            LOG_ERROR("Failed to configure sibling array keys 'foo/bar' and "
                      "'foo/baz'");
            retval = 1;
        }

        if (!sibling_keys_are_accepted("foo/bar", "qux")) {
            LOG_ERROR("Failed to configure array keys 'foo/bar' and 'qux'");
            retval = 1;
        }
    } catch (const std::exception& exception) {
        LOG_ERROR(exception.what());
    }

    return retval;
}

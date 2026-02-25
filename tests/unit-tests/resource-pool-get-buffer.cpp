#include "resource.pool.hh"
#include "unit-test-utils.hh"

namespace {
void
get_buffer(zarr::ResourcePool& pool)
{
    uint64_t memory_usage = pool.memory_usage();
    EXPECT(memory_usage == 0,
           "Expected no memory usage initially, got ",
           memory_usage);

    {
        const auto buffer = pool.get_data_buffer(14192);

        CHECK(buffer != nullptr);
        memory_usage = pool.memory_usage();

        EXPECT(memory_usage == buffer->size(),
               "Expected memory usage of ",
               buffer->size(),
               "bytes, got ",
               memory_usage);
    }

    // buffer should be destroyed now
    memory_usage = pool.memory_usage();
    EXPECT(memory_usage == 0,
           "Expected 0 memory usage after destruct, got ",
           memory_usage);
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        zarr::ResourcePool pool(0);
        get_buffer(pool);

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Error: ", exc.what());
    }

    return retval;
}

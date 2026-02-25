#include "resource.pool.hh"
#include "unit-test-utils.hh"

namespace {
const std::string object_key = TEST;

void
get_connection(zarr::ResourcePool& pool, const zarr::S3Settings& settings)
{
    uint64_t active_connections = pool.active_s3_connections();
    EXPECT(active_connections == 0,
           "Expected no active S3 connections initially, got ",
           active_connections);

    {
        const auto buffer = pool.get_s3_connection(object_key, settings);

        CHECK(buffer != nullptr);
        active_connections = pool.active_s3_connections();

        EXPECT(active_connections == 1,
               "Expected 1 active S3 connection, got ",
               active_connections);
    }

    // buffer should be destroyed now
    active_connections = pool.active_s3_connections();
    EXPECT(active_connections == 0,
           "Expected 0 active S3 connections after destruct, got ",
           active_connections);
}
} // namespace

int
main()
{
    zarr::S3Settings settings;
    if (!testing::get_s3_settings(settings)) {
        LOG_INFO("Failed to get credentials. Skipping test.");
        return 0;
    }

    int retval = 1;

    try {
        zarr::ResourcePool pool(0);
        get_connection(pool, settings);

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Error: ", exc.what());
    }

    return retval;
}

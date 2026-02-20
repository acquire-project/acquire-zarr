#include "resource.pool.hh"
#include "unit.test.macros.hh"

#include <filesystem>

void*
make_flags();

void
destroy_flags(void*);

namespace fs = std::filesystem;

namespace {
const std::string file_path = TEST;
void
get_file_handle(zarr::ResourcePool& pool, const std::string& path)
{
    uint64_t active_handle_count = pool.active_file_handles();
    EXPECT(active_handle_count == 0,
           "Expected 0 active handles initially, got ",
           active_handle_count);

    void* flags = make_flags();
    const auto handle = pool.get_file_handle(file_path, flags);

    CHECK(handle != nullptr);
    active_handle_count = pool.active_file_handles();

    EXPECT(active_handle_count == 1,
           "Expected 1 active handle after construction, got ",
           active_handle_count);
    destroy_flags(flags);
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        zarr::ResourcePool pool(0);
        get_file_handle(pool, file_path);

        retval = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Error: ", exc.what());
    }

    if (fs::exists(file_path)) {
        fs::remove(file_path);
    }

    return retval;
}

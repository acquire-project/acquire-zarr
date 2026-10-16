#include "acquire.zarr.h"
#include "unit.test.macros.hh"

#include <string>

/** Zarr_get_api_version() must report the release version from CMake. */
int
main()
{
    try {
        EXPECT_STR_EQ(Zarr_get_api_version(), EXPECTED_API_VERSION);
    } catch (const std::exception& exception) {
        LOG_ERROR(exception.what());
        return 1;
    }
    return 0;
}

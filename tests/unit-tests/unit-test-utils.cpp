#include "unit-test-utils.hh"

bool
testing::get_s3_settings(zarr::S3Settings& settings)
{
    char* env = nullptr;
    if (!(env = std::getenv("ZARR_S3_ENDPOINT"))) {
        LOG_INFO("ZARR_S3_ENDPOINT not set.");
        return false;
    }
    settings.endpoint = env;

    if (!(env = std::getenv("ZARR_S3_BUCKET_NAME"))) {
        LOG_INFO("ZARR_S3_BUCKET_NAME not set.");
        return false;
    }
    settings.bucket_name = env;

    env = std::getenv("ZARR_S3_REGION");
    if (env) {
        settings.region = env;
    }

    return true;
}
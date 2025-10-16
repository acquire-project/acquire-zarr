#include "macros.hh"
#include "s3.multiscale.array.hh"

zarr::S3MultiscaleArray::S3MultiscaleArray(
  std::shared_ptr<ArrayConfig> config,
  std::shared_ptr<ThreadPool> thread_pool,
  std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : MultiscaleArray(config, thread_pool)
  , S3Storage(*config->bucket_name, s3_connection_pool)
{
    // dimensions may be null in the case of intermediate groups, e.g., the
    // A in A/1
    if (config_->dimensions) {
        CHECK(S3MultiscaleArray::create_arrays_());
    }
}

bool
zarr::S3MultiscaleArray::write_metadata_()
{
    std::string metadata;
    if (!make_metadata_(metadata)) {
        LOG_ERROR("Failed to make metadata.");
        return false;
    }
    const std::string path = node_path_() + "/zarr.json";

    return write_string_(path, metadata, 0);
}

bool
zarr::S3MultiscaleArray::create_arrays_()
{
    arrays_.clear();

    try {
        if (downsampler_) {
            const auto& configs = downsampler_->writer_configurations();
            arrays_.resize(configs.size());

            for (const auto& [lod, config] : configs) {
                arrays_[lod] = std::make_unique<S3Array>(
                  config, thread_pool_, s3_connection_pool_);
            }
        } else {
            arrays_.push_back(std::make_unique<S3Array>(
              make_base_array_config_(), thread_pool_, s3_connection_pool_));
        }
    } catch (const std::exception& e) {
        LOG_ERROR(e.what());
        return false;
    }

    return true;
}

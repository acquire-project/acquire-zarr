#include "fs.multiscale.array.hh"
#include "macros.hh"

zarr::FSMultiscaleArray::FSMultiscaleArray(
  std::shared_ptr<ArrayConfig> config,
  std::shared_ptr<ThreadPool> thread_pool,
  std::shared_ptr<FileHandlePool> file_handle_pool)
  : MultiscaleArray(config, thread_pool)
  , FSStorage(file_handle_pool)
{
    // dimensions may be null in the case of intermediate groups, e.g., the
    // A in A/1
    if (config_->dimensions) {
        CHECK(FSMultiscaleArray::create_arrays_());
    }
}

bool
zarr::FSMultiscaleArray::write_metadata_()
{
    std::string metadata;
    if (!make_metadata_(metadata)) {
        LOG_ERROR("Failed to make metadata.");
        return false;
    }

    if (last_written_metadata_ == metadata) {
        return true; // no changes
    }
    const std::string path = node_path_() + "/zarr.json";

    bool success;
    if ((success = write_string(path, metadata, 0))) {
        last_written_metadata_ = metadata;
    }

    return success;
}

bool
zarr::FSMultiscaleArray::create_arrays_()
{
    arrays_.clear();

    try {
        if (downsampler_) {
            const auto& configs = downsampler_->writer_configurations();
            arrays_.resize(configs.size());

            for (const auto& [lod, config] : configs) {
                arrays_[lod] = std::make_unique<FSArray>(
                  config, thread_pool_, file_handle_pool_);
            }
        } else {
            arrays_.push_back(std::make_unique<FSArray>(
              make_base_array_config_(), thread_pool_, file_handle_pool_));
        }
    } catch (const std::exception& e) {
        LOG_ERROR(e.what());
        return false;
    }

    return true;
}

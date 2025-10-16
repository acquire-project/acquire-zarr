#include <utility>

#include "array.hh"
#include "array.base.hh"
#include "macros.hh"
#include "multiscale.array.hh"

zarr::ArrayBase::ArrayBase(std::shared_ptr<ArrayConfig> config,
                           std::shared_ptr<ThreadPool> thread_pool)
  : config_(config)
  , thread_pool_(thread_pool)
{
    CHECK(config_);      // required
    CHECK(thread_pool_); // required
}

std::string
zarr::ArrayBase::node_path_() const
{
    std::string key = config_->store_root;
    if (!config_->node_key.empty()) {
        key += "/" + config_->node_key;
    }

    return key;
}

bool
zarr::finalize_array(std::unique_ptr<ArrayBase>&& array)
{
    if (array == nullptr) {
        LOG_INFO("Array is null. Nothing to finalize.");
        return true;
    }

    try {
        bool result = array->close_();
        return result;
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to close array: ", exc.what());
        return false;
    }
}

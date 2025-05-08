#include <utility>

#include "array.hh"
#include "group.hh"
#include "macros.hh"
#include "node.hh"

zarr::Node::Node(std::shared_ptr<NodeConfig> config,
                 std::shared_ptr<ThreadPool> thread_pool,
                 std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : config_(config)
  , thread_pool_(thread_pool)
  , s3_connection_pool_(s3_connection_pool)
{
    CHECK(config_);      // required
    CHECK(thread_pool_); // required
}

bool
zarr::finalize_node(std::unique_ptr<Node>&& node)
{
    if (!node) {
        LOG_INFO("Node is null, nothing to finalize.");
        return true;
    }

    if (auto group = downcast_node<Group>(std::move(node))) {
        if (!finalize_group(std::move(group))) {
            LOG_ERROR("Failed to finalize group.");
            node.reset(group.release());
            return false;
        }
    } else if (auto array = downcast_node<Array>(std::move(node))) {
        if (!finalize_array(std::move(array))) {
            LOG_ERROR("Failed to finalize array.");
            node.reset(array.release());
            return false;
        }
    } else {
        LOG_ERROR("Unknown node type.");
        return false;
    }

    node.reset();
    return true;
}

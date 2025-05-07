#include <utility>

#include "macros.hh"
#include "node.hh"

zarr::Node::Node(std::shared_ptr<NodeConfig> config,
                 std::shared_ptr<ThreadPool> thread_pool,
                 std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : config_{ std::move(config) }
  , thread_pool_{ std::move(thread_pool) }
  , s3_connection_pool_{ std::move(s3_connection_pool) }
  , is_open_{ false }
{
    CHECK(config_);      // required
    CHECK(thread_pool_); // required
}
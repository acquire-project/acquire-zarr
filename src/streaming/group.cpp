#include "group.hh"

zarr::Group::Group(const zarr::GroupConfig& config,
                   std::shared_ptr<ThreadPool> thread_pool)
  : Group(config, thread_pool, nullptr)
{
}

zarr::Group::Group(const zarr::GroupConfig& config,
                   std::shared_ptr<ThreadPool> thread_pool,
                   std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : config_{ config }
  , thread_pool_{ thread_pool }
  , s3_connection_pool_{ s3_connection_pool }
{
}
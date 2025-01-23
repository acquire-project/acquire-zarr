#pragma once

#include "zarr.dimension.hh"
#include "sink.hh"
#include "thread.pool.hh"
#include "s3.connection.hh"

#include <optional>
#include <memory>
#include <unordered_map>

namespace zarr {
class SinkCreator
{
  public:
    SinkCreator(std::shared_ptr<ThreadPool> thread_pool_,
                std::shared_ptr<S3ConnectionPool> connection_pool);
    ~SinkCreator() noexcept = default;

    /**
     * @brief
     * @param version
     * @param bucket_name
     * @param base_path
     * @param metadata_sinks
     * @return
     * @throws std::runtime_error if @p version is invalid, if @p bucket_name is
     * empty or does not exist, or if @p base_path is empty.
     */
    [[nodiscard]] bool make_metadata_sinks(
      size_t version,
      std::string_view bucket_name,
      std::string_view base_path,
      std::unordered_map<std::string, std::unique_ptr<Sink>>& metadata_sinks);

  private:
    std::shared_ptr<ThreadPool> thread_pool_;
    std::shared_ptr<S3ConnectionPool> connection_pool_; // could be null

    /**
     * @brief Construct the paths for a Zarr dataset.
     * @param base_path The base path for the dataset.
     * @param dimensions The dimensions of the dataset.
     * @param parts_along_dimension Function to determine the number of parts
     * @param create_directories Whether to create intermediate directories.
     * @return A queue of paths to the dataset components.
     * @throws std::runtime_error if intermediate directories cannot be created,
     * or if the number of parts along a dimension is zero.
     */
    std::queue<std::string> make_data_sink_paths_(
      std::string_view base_path,
      const ArrayDimensions* dimensions,
      const DimensionPartsFun& parts_along_dimension,
      bool create_directories);

    std::vector<std::string> make_metadata_sink_paths_(
      size_t version,
      std::string_view base_path,
      bool create_directories);

    /// @brief Parallel create a collection of directories.
    /// @param[in] dir_paths The directories to create.
    /// @return True iff all directories were created successfully.
    [[nodiscard]] bool make_dirs_(std::queue<std::string>& dir_paths);

    /// @brief Check whether an S3 bucket exists.
    /// @param[in] bucket_name The name of the bucket to check.
    /// @return True iff the bucket exists.
    bool bucket_exists_(std::string_view bucket_name);

    /// @brief Create a collection of S3 objects.
    /// @param[in] bucket_name The name of the bucket.
    /// @param[in,out] object_keys The keys of the objects to create.
    /// @param[out] sinks The sinks created.
    /// @return True iff all S3 objects were created successfully.
    [[nodiscard]] bool make_s3_objects_(
      std::string_view bucket_name,
      std::queue<std::string>& object_keys,
      std::vector<std::unique_ptr<Sink>>& sinks);

    /// @brief Create a collection of S3 objects, keyed by object key.
    /// @param[in] bucket_name The name of the bucket.
    /// @param[in] object_keys The keys of the objects to create.
    /// @param[out] sinks The sinks created, keyed by object key.
    /// @return True iff all S3 objects were created successfully.
    [[nodiscard]] bool make_s3_objects_(
      std::string_view bucket_name,
      std::string_view base_path,
      std::vector<std::string>& object_keys,
      std::unordered_map<std::string, std::unique_ptr<Sink>>& sinks);
};
} // namespace zarr

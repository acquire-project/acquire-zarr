#pragma once

#include "definitions.hh"
#include "s3.connection.hh"
#include "thread.pool.hh"
#include "array.dimensions.hh"

#include <cstddef> // size_t
#include <memory>  // std::unique_ptr
#include <span>    // std::span

namespace zarr {
class Sink
{
  public:
    virtual ~Sink() = default;

    /**
     * @brief Write data to the sink.
     * @param offset The offset in the sink to write to.
     * @param data The buffer to write to the sink.
     * @param bytes_of_buf The number of bytes to write from @p buf.
     * @return True if the write was successful, false otherwise.
     */
    [[nodiscard]] virtual bool write(size_t offset, ConstByteSpan data) = 0;

  protected:
    /**
     * @brief Flush any buffered data to the sink.
     * @note This should ONLY be called when finalizing the sink.
     * @return True if the flush was successful, false otherwise.
     */
    [[nodiscard]] virtual bool flush_() = 0;

    friend bool finalize_sink(std::unique_ptr<Sink>&& sink);
};

/**
 * @brief Finalize and destroy the Sink @p sink.
 * @note @p sink is no longer accessible after a successful call to this
 * function.
 * @param[in] sink The Sink to finalize.
 * @return True if and only if the Sink was finalized successfully.
 */
bool
finalize_sink(std::unique_ptr<Sink>&& sink);

/**
 * @brief Construct paths for data sinks, given the dimensions and a function
 * to determine the number of parts along a dimension.
 * @param base_path The base path for the dataset.
 * @param dimensions The dimensions of the dataset.
 * @param parts_along_dimension Function to determine the number of parts along
 * a dimension.
 * @return A vector of paths for the data sinks.
 */
std::vector<std::string>
construct_data_paths(std::string_view base_path,
                     const ArrayDimensions& dimensions,
                     const DimensionPartsFun& parts_along_dimension);

/**
 * @brief Get unique paths to the parent directories of each file in @p
 * file_paths.
 * @param file_paths Collection of paths to files.
 * @return Collection of unique parent directories.
 */
std::vector<std::string>
get_parent_paths(const std::vector<std::string>& file_paths);

/**
 * @brief Parallel create directories for a collection of paths.
 * @param dir_paths The directories to create.
 * @param thread_pool The thread pool to use for parallel creation.
 * @return True iff all directories were created successfully.
 */
bool
make_dirs(const std::vector<std::string>& dir_paths,
          std::shared_ptr<ThreadPool> thread_pool);

/**
 * @brief Create a file sink from a path.
 * @param file_path The path to the file.
 * @return Pointer to the sink created, or nullptr if the file cannot be
 * opened.
 * @throws std::runtime_error if the file path is not valid.
 */
std::unique_ptr<zarr::Sink>
make_file_sink(std::string_view file_path);

/**
 * @brief Create a collection of file sinks for a Zarr dataset.
 * @param[in] base_path The path to the base directory for the dataset.
 * @param[in] dimensions The dimensions of the dataset.
 * @param[in] parts_along_dimension Function to determine the number of
 * parts (i.e., shards or chunks) along a dimension.
 * @param[in] thread_pool Pointer to a thread pool object. Used to create files
 * in parallel.
 * @param[out] part_sinks The sinks created.
 * @return True iff all file sinks were created successfully.
 * @throws std::runtime_error if @p base_path is not valid, or if the number
 * of parts along a dimension is zero.
 */
[[nodiscard]] bool
make_data_file_sinks(std::string_view base_path,
                     const ArrayDimensions& dimensions,
                     const DimensionPartsFun& parts_along_dimension,
                     std::shared_ptr<ThreadPool> thread_pool,
                     std::vector<std::unique_ptr<Sink>>& part_sinks);

/**
 * @brief Create a sink from an S3 bucket name and object key.
 * @param bucket_name The name of the bucket in which the object is stored.
 * @param object_key The key of the object to write to.
 * @param connection_pool Pointer to a pool of existing S3 connections.
 * @return Pointer to the sink created, or nullptr if the bucket does not
 * exist.
 * @throws std::runtime_error if the bucket name or object key is not valid,
 * or if there is no connection pool.
 */
std::unique_ptr<Sink>
make_s3_sink(std::string_view bucket_name,
             std::string_view object_key,
             std::shared_ptr<S3ConnectionPool> connection_pool);

/**
 * @brief Create a collection of S3 sinks for a Zarr dataset.
 * @param[in] bucket_name The name of the bucket in which the dataset is
 * stored.
 * @param[in] base_path The path to the base directory for the dataset.
 * @param[in] dimensions The dimensions of the dataset.
 * @param[in] parts_along_dimension Function to determine the number of
 * parts (i.e., shards or chunks) along a dimension.
 * @param[in] connection_pool Pointer to a pool of existing S3 connections.
 * @param[out] part_sinks The sinks created.
 * @return True iff all file sinks were created successfully.
 */
[[nodiscard]] bool
make_data_s3_sinks(std::string_view bucket_name,
                   std::string_view base_path,
                   const ArrayDimensions& dimensions,
                   const DimensionPartsFun& parts_along_dimension,
                   std::shared_ptr<S3ConnectionPool> connection_pool,
                   std::vector<std::unique_ptr<Sink>>& part_sinks);
} // namespace zarr

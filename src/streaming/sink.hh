#pragma once

#include "definitions.hh"
#include "s3.client.hh"
#include "thread.pool.hh"
#include "array.dimensions.hh"

#include <cstddef> // size_t
#include <file.handle.hh>
#include <memory> // std::unique_ptr

namespace zarr {
class Sink
{
  public:
    virtual ~Sink() = default;

    /**
     * @brief Write data to the sink.
     * @param offset The offset in the sink to write to.
     * @param data The buffer to write to the sink.
     * @return True if the write was successful, false otherwise.
     */
    [[nodiscard]] virtual bool write(size_t offset, ConstByteSpan data) = 0;

    /**
     * @brief Bytes the sink is holding that are not yet in storage.
     * @details Sinks that pass every write straight through report 0.
     * @return The number of bytes buffered.
     */
    virtual size_t memory_usage() const noexcept { return 0; }

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
 * @brief Finalize and destroy @p sink.
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
 * @param make_directories Create intermediate directories if true.
 * @return A vector of paths for the data sinks.
 */
std::vector<std::string>
construct_data_paths(std::string_view base_path,
                     const ArrayDimensions& dimensions,
                     const DimensionPartsFun& parts_along_dimension,
                     bool make_directories);

/**
 * @brief Create a file sink from a path.
 * @param file_path The path to the file.
 * @param file_handle_pool Pointer to a pool of file handles.
 * @return Pointer to the sink created, or nullptr if the file cannot be
 * opened.
 * @throws std::runtime_error if the file path is not valid.
 */
std::unique_ptr<Sink>
make_file_sink(std::string_view file_path,
               std::shared_ptr<FileHandlePool> file_handle_pool,
               bool truncate_to_fit = false);

/**
 * @brief Create a sink from an S3 bucket name and object key.
 * @param bucket_name The name of the bucket in which the object is stored.
 * @param object_key The key of the object to write to.
 * @param client Pointer to the shared S3 client.
 * @return Pointer to the sink created.
 * @throws std::runtime_error if the bucket name or object key is not valid,
 * or if there is no client.
 */
std::unique_ptr<Sink>
make_s3_sink(std::string_view bucket_name,
             std::string_view object_key,
             std::shared_ptr<S3Client> client);
} // namespace zarr

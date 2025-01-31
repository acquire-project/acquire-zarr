#pragma once

#include "thread.pool.hh"
#include "zarr.dimension.hh"

#include <cstddef> // size_t, std::byte
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
     * @param buf The buffer to write to the sink.
     * @param bytes_of_buf The number of bytes to write from @p buf.
     * @return True if the write was successful, false otherwise.
     */
    [[nodiscard]] virtual bool write(size_t offset,
                                     std::span<const std::byte> buf) = 0;

  protected:
    [[nodiscard]] virtual bool flush_() = 0;

    friend bool finalize_sink(std::unique_ptr<Sink>&& sink);
};

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
} // namespace zarr

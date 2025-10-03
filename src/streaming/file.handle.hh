#pragma once

#include <condition_variable>
#include <memory> // for std::unique_ptr
#include <mutex>
#include <string>

namespace zarr {
/**
 * @brief A handle to a file, wrapping the platform-specific file handle.
 * @details This class is not copyable or movable. It is intended to be used
 * with FileHandlePool to manage a pool of file handles.
 */
class FileHandle
{
  public:
    /**
     * @brief Create a new FileHandle. The file is opened with the specified
     * filename and flags.
     * @details The flags parameter is platform-specific and should be created
     * using the appropriate function for the platform (e.g., make_flags()).
     * The FileHandle will be closed when the object is destroyed.
     * @param filename The path to the file to open.
     * @param flags Platform-specific flags for opening the file.
     * @throws std::runtime_error if the file cannot be opened.
     */
    FileHandle(const std::string& filename, void* flags);
    ~FileHandle();

    /**
     * @brief Get the underlying platform-specific file handle.
     * @return A pointer to the platform-specific file handle.
     */
    void* get() const;

  private:
    void* handle_; /**< Platform-specific file handle. */
};

/**
 * @brief A pool of file handles to limit the number of concurrently open files.
 */
class FileHandlePool
{
  public:
    FileHandlePool();
    ~FileHandlePool() = default;

    /**
     * @brief Get a file handle for the specified filename.
     * This function will block if the maximum number of active handles has
     * been reached, until a handle is returned to the pool.
     * @param filename The path to the file to open.
     * @param flags Platform-specific flags for opening the file.
     * @return A unique pointer to a FileHandle, or nullptr on failure.
     */
    std::unique_ptr<FileHandle> get_handle(const std::string& filename,
                                           void* flags);

    /**
     * @brief Return a file handle to the pool.
     * @details This function should be called when a file handle is no longer
     * needed, to allow other threads to acquire a handle.
     * @param handle The file handle to return.
     */
    void return_handle(std::unique_ptr<FileHandle>&& handle);

  private:
    const uint64_t max_active_handles_;
    std::atomic<uint64_t> n_active_handles_;
    std::mutex mutex_;
    std::condition_variable cv_;
};
} // namespace zarr
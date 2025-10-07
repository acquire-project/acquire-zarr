#pragma once

#include <condition_variable>
#include <list>
#include <memory> // for std::unique_ptr
#include <mutex>
#include <string>
#include <tuple>
#include <unordered_map>

namespace zarr {
class FileHandlePool;

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

    const std::string& filename() const { return filename_; }

  private:
    std::string filename_; /**< The path to the file. */
    void* handle_;         /**< Platform-specific file handle. */
};

class FileHandleGuard
{
  public:
    FileHandleGuard(const std::string& filename,
                    void* flags,
                    FileHandlePool* pool);
    FileHandleGuard(const FileHandleGuard&) = delete;
    FileHandleGuard& operator=(const FileHandleGuard&) = delete;

    ~FileHandleGuard();

    /**
     * @brief Get the underlying platform-specific file handle.
     * @return A non-owning pointer to the platform-specific file handle.
     */
    void* get() const;

  private:
    std::unique_ptr<FileHandle> handle_;
    FileHandlePool* pool_; // the pool that created this handle
};

/**
 * @brief A pool of file handles to limit the number of concurrently open files.
 */
class FileHandlePool
{
  public:
    FileHandlePool();
    ~FileHandlePool();

    FileHandleGuard get(const std::string& filename, void* flags);

  private:
    using HandleList = std::list<std::unique_ptr<FileHandle>>;

    const uint64_t max_active_handles_;
    std::atomic<uint64_t> n_active_handles_;
    std::atomic<uint64_t> max_ever_active_handles_{ 0 };
    std::mutex mutex_;
    std::condition_variable cv_;

    // LRU list of active handles
    // Each entry is a pair of filename and FileHandle
    HandleList handles_;

    // Map from filename to iterator in the LRU list
    std::unordered_map<std::string, HandleList::iterator> handle_map_;

    /**
     * @brief Get a file handle for the specified filename.
     * This function will block if the maximum number of active handles has
     * been reached, until a handle is returned to the pool.
     * @param filename The path to the file to open.
     * @param flags Platform-specific flags for opening the file.
     * @return A unique pointer to a FileHandle, or nullptr on failure.
     */
    std::unique_ptr<FileHandle> get_handle_(const std::string& filename,
                                            void* flags);

    /**
     * @brief Return a file handle to the pool.
     * @details This function should be called when a file handle is no longer
     * needed, to allow other threads to acquire a handle.
     * @param handle The file handle to return.
     */
    void return_handle_(std::unique_ptr<FileHandle>&& handle);

    /**
     * @brief Get a file handle from the cache if it exists.
     * @details This function checks if a file handle for the specified filename
     * exists in the cache. If it does, it is removed from the cache and
     * returned. If not, it is created, added to the cache, and returned. If
     * the maximum number of active handles has been reached, this function
     * will block until a handle is returned to the pool.
     * @param filename The path to the file.
     * @param flags Platform-specific flags for opening the file.
     * @return A unique pointer to a FileHandle if found in the cache, else
     * nullptr.
     */
    std::unique_ptr<FileHandle> get_from_cache_(const std::string& filename,
                                                void* flags);

    /**
     * @brief Return a file handle to the cache.
     * @details This function adds the file handle back to the cache and
     * notifies any waiting threads that a handle is available.
     * @param handle The file handle to return to the cache.
     */
    void return_to_cache_(std::unique_ptr<FileHandle>&& handle);

    /**
     * @brief Add a new file handle to the cache.
     * @details Blocks if the maximum number of active handles has been
     * reached, evicting the least recently used handle if necessary.
     * @param filename The path to the file.
     * @param flags Platform-specific flags for opening the file.
     */
    void create_and_add_to_cache_(const std::string& filename, void* flags);

    /**
     * @brief Evict the least recently used file handle(s) from the cache until
     * there is room for a new handle.
     * @details This function is called when the maximum number of active
     * handles has been reached and a new handle is requested.
     */
    void evict_lru_();

    friend class FileHandleGuard;
};
} // namespace zarr
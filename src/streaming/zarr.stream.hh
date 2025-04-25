#pragma once

#include "array.hh"
#include "definitions.hh"
#include "downsampler.hh"
#include "frame.queue.hh"
#include "s3.connection.hh"
#include "sink.hh"
#include "thread.pool.hh"
#include "array.dimensions.hh"
#include "group.hh"

#include <nlohmann/json.hpp>

#include <condition_variable>
#include <cstddef> // size_t
#include <memory>  // unique_ptr
#include <mutex>
#include <optional>
#include <span>
#include <string_view>

struct ZarrStream_s
{
  public:
    ZarrStream_s(struct ZarrStreamSettings_s* settings);

    /**
     * @brief Append data to the named group.
     * @param group_name The name of the group to append to. Empty string for
     * the root group.
     * @param data The data to append.
     * @param nbytes The number of bytes to append.
     * @return The number of bytes appended.
     */
    size_t append_to_group(std::string_view group_name,
                           const void* data,
                           size_t nbytes);

    /**
     * @brief Append data to the named array.
     * @param group_name The name of the array to append to.
     * @param data The data to append.
     * @param nbytes The number of bytes to append.
     * @return The number of bytes appended.
     */
    size_t append_to_array(std::string_view array_name,
                           const void* data,
                           size_t nbytes);

    /**
     * @brief Write custom metadata to the stream.
     * @param custom_metadata JSON-formatted custom metadata to write.
     * @param overwrite If true, overwrite any existing custom metadata.
     * Otherwise, fail if custom metadata has already been written.
     * @return ZarrStatusCode_Success on success, or an error code on failure.
     */
    ZarrStatusCode write_custom_metadata(std::string_view custom_metadata,
                                         bool overwrite);

  private:
    struct CompressionSettings
    {
        ZarrCompressor compressor;
        ZarrCompressionCodec codec;
        uint8_t level;
        uint8_t shuffle;
    };

    std::string error_; // error message. If nonempty, an error occurred.

    ZarrVersion version_;
    std::string store_path_;
    std::optional<zarr::S3Settings> s3_settings_;
    std::optional<CompressionSettings> compression_settings_;
    ZarrDataType dtype_;
    std::shared_ptr<ArrayDimensions> dimensions_;
    bool multiscale_;

    std::unordered_map<std::string, std::unique_ptr<zarr::Group>> groups_;
    std::unordered_map<std::string, std::unique_ptr<zarr::Array>> arrays_;

    std::vector<std::byte> frame_buffer_;
    size_t frame_buffer_offset_;

    std::atomic<bool> process_frames_{ true };
    std::mutex frame_queue_mutex_;
    std::condition_variable frame_queue_not_full_cv_;  // Space is available
    std::condition_variable frame_queue_not_empty_cv_; // Data is available
    std::condition_variable frame_queue_finished_cv_;  // Done processing
    std::unique_ptr<zarr::FrameQueue> frame_queue_;

    std::shared_ptr<zarr::ThreadPool> thread_pool_;
    std::shared_ptr<zarr::S3ConnectionPool> s3_connection_pool_;

    std::unordered_map<std::string, std::unique_ptr<zarr::Sink>>
      metadata_sinks_;

    bool is_s3_acquisition_() const;
    bool is_compressed_acquisition_() const;

    /**
     * @brief Check that the settings are valid.
     * @note Sets the error_ member if settings are invalid.
     * @param settings Struct containing settings to validate.
     * @return true if settings are valid, false otherwise.
     */
    [[nodiscard]] bool validate_settings_(
      const struct ZarrStreamSettings_s* settings);

    /**
     * @brief Copy settings to the stream.
     * @param settings Struct containing settings to copy.
     */
    void commit_settings_(const struct ZarrStreamSettings_s* settings);

    /**
     * @brief Spin up the thread pool.
     */
    void start_thread_pool_(uint32_t max_threads);

    /**
     * @brief Set an error message.
     * @param msg The error message to set.
     */
    void set_error_(const std::string& msg);

    /** @brief Create the data store. */
    [[nodiscard]] bool create_store_();

    /** @brief Initialize the frame queue. */
    [[nodiscard]] bool init_frame_queue_();

    /** @brief Create the root group. */
    [[nodiscard]] bool create_root_group_();

    /** @brief Create the metadata sinks. */
    [[nodiscard]] bool create_metadata_sinks_();

    /** @brief Write per-acquisition metadata. */
    [[nodiscard]] bool write_base_metadata_();

    /** @brief Write Zarr group metadata. */
    [[nodiscard]] bool write_group_metadata_();

    /** @brief Process the frame queue. */
    void process_frame_queue_();

    /** @brief Wait for the frame queue to finish processing. */
    void finalize_frame_queue_();

    friend bool finalize_stream(struct ZarrStream_s* stream);
};

bool
finalize_stream(struct ZarrStream_s* stream);

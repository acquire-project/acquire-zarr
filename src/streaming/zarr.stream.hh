#pragma once

#include "array.hh"
#include "array.dimensions.hh"
#include "definitions.hh"
#include "downsampler.hh"
#include "frame.queue.hh"
#include "group.hh"
#include "s3.connection.hh"
#include "sink.hh"
#include "thread.pool.hh"

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
     * @brief Append data to the named node (i.e., group or array).
     * @param key The name of the node to append to. Empty string for
     * the root group.
     * @param data The data to append.
     * @param nbytes The number of bytes to append.
     * @return The number of bytes appended.
     */
    size_t append_to_node(std::string_view key,
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

    /**
     * @brief
     * @param properties
     * @return
     */
    ZarrStatusCode configure_group(const ZarrGroupProperties* properties);

    /**
     * @brief
     * @param properties
     * @return
     */
    ZarrStatusCode configure_array(const ZarrArrayProperties* properties);

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

    std::optional<std::string> active_node_key_;
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
     * @return True if settings were committed successfully, otherwise false.
     */
    [[nodiscard]] bool create_root_group_(
      const struct ZarrStreamSettings_s* settings);

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
    [[nodiscard]] bool init_frame_queue_(size_t frame_size);

    /** @brief Create the metadata sinks. */
    [[nodiscard]] bool create_metadata_sinks_();

    /** @brief Write per-acquisition metadata. */
    [[nodiscard]] bool write_base_metadata_();

    /** @brief Process the frame queue. */
    void process_frame_queue_();

    /** @brief Wait for the frame queue to finish processing. */
    void finalize_frame_queue_();

    /**
     * @brief Check if the stream has a node with key @p key.
     * @param key A node key.
     * @return True if the stream has a node with the specified key, otherwise
     * false.
     */
    bool has_node_(std::string_view key);

    /** @brief Close the currently active group or array. */
    void close_current_node_();

    /**
     * @brief Switch to a different node in the stream.
     * @param key The name of the node to switch to. Empty string for
     * the root group.
     * @return True if the switch was successful, false otherwise.
     */
    [[nodiscard]] bool switch_node_(std::string_view key);

    [[nodiscard]] static bool validate_compression_settings_(
      const ZarrCompressionSettings* settings,
      std::string& error);

    [[nodiscard]] bool validate_dimension_(
      const ZarrDimensionProperties* dimension,
      bool is_append,
      std::string& error);

    template<typename PropertiesT>
    [[nodiscard]]
    bool validate_node_properties_(const PropertiesT* properties,
                                   ZarrVersion version,
                                   std::string& error)
    {
        if (!properties) {
            error = "Null pointer: properties";
            return false;
        }

        if (!properties->store_key) {
            error = "Null pointer: store_key";
            return false;
        }

        if (properties->data_type >= ZarrDataTypeCount) {
            error =
              "Invalid data type: " + std::to_string(properties->data_type);
            return false;
        }

        if (properties->compression_settings &&
            !validate_compression_settings_(properties->compression_settings,
                                            error)) {
            return false;
        }

        if (!properties->dimensions) {
            error = "Null pointer: dimensions";
            return false;
        }

        const auto ndims = properties->dimension_count;
        if (ndims < 3) {
            error = "Invalid number of dimensions: " + std::to_string(ndims) +
                    ". Must be at least 3";
            return false;
        }

        // check the final dimension (width), must be space
        if (properties->dimensions[ndims - 1].type != ZarrDimensionType_Space) {
            error = "Last dimension must be of type Space";
            return false;
        }

        // check the penultimate dimension (height), must be space
        if (properties->dimensions[ndims - 2].type != ZarrDimensionType_Space) {
            error = "Second to last dimension must be of type Space";
            return false;
        }

        // validate the dimensions individually
        for (size_t i = 0; i < ndims; ++i) {
            if (!validate_dimension_(
                  properties->dimensions + i, i == 0, error)) {
                return false;
            }
        }

        return true;
    };

    friend bool finalize_stream(struct ZarrStream_s* stream);
};

bool
finalize_stream(struct ZarrStream_s* stream);

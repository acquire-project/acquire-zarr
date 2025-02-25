#pragma once

#include "zarr.dimension.hh"
#include "thread.pool.hh"
#include "s3.connection.hh"
#include "sink.hh"
#include "array.writer.hh"
#include "definitions.hh"

#include <nlohmann/json.hpp>

#include <cstddef> // size_t
#include <memory>  // unique_ptr
#include <optional>
#include <span>

struct ZarrStream_s
{
  public:
    ZarrStream_s(struct ZarrStreamSettings_s* settings);

    /**
     * @brief Append data to the stream.
     * @param data The data to append.
     * @param nbytes The number of bytes to append.
     * @return The number of bytes appended.
     */
    size_t append(const void* data, size_t nbytes);

  private:
    struct S3Settings
    {
        std::string endpoint;
        std::string bucket_name;
        std::string access_key_id;
        std::string secret_access_key;
        std::optional<std::string> region;
    };
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
    std::optional<S3Settings> s3_settings_;
    std::optional<CompressionSettings> compression_settings_;
    std::optional<std::string> custom_metadata_;
    ZarrDataType dtype_;
    std::shared_ptr<ArrayDimensions> dimensions_;
    bool multiscale_;

    std::vector<std::byte> frame_buffer_;
    size_t frame_buffer_offset_;

    std::shared_ptr<zarr::ThreadPool> thread_pool_;
    std::shared_ptr<zarr::S3ConnectionPool> s3_connection_pool_;

    std::vector<std::unique_ptr<zarr::ArrayWriter>> writers_;
    std::unordered_map<std::string, std::unique_ptr<zarr::Sink>>
      metadata_sinks_;

    std::unordered_map<size_t, std::optional<ByteVector>> scaled_frames_;

    bool is_s3_acquisition_() const;
    bool is_compressed_acquisition_() const;

    /**
     * @brief Copy settings to the stream.
     * @param settings Struct containing settings to copy.
     */
    void commit_settings_(const struct ZarrStreamSettings_s* settings);

    /**
     * @brief Set an error message.
     * @param msg The error message to set.
     */
    void set_error_(const std::string& msg);

    /** @brief Create the data store. */
    [[nodiscard]] bool create_store_();

    /** @brief Create the writers. */
    [[nodiscard]] bool create_writers_();

    /** @brief Create placeholders for multiscale frames. */
    void create_scaled_frames_();

    /** @brief Create the metadata sinks. */
    [[nodiscard]] bool create_metadata_sinks_();

    /** @brief Write per-acquisition metadata. */
    [[nodiscard]] bool write_base_metadata_();

    /** @brief Write Zarr group metadata. */
    [[nodiscard]] bool write_group_metadata_();

    /** @brief Write external metadata. */
    [[nodiscard]] bool write_external_metadata_();

    /** @brief Construct OME metadata pertaining to the multiscale pyramid. */
    [[nodiscard]] nlohmann::json make_ome_metadata_() const;

    size_t write_frame_(ConstByteSpan data);
    void write_multiscale_frames_(ConstByteSpan data);

    friend bool finalize_stream(struct ZarrStream_s* stream);
};

bool
finalize_stream(struct ZarrStream_s* stream);

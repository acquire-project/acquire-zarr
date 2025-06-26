#pragma once

#include "sink.hh"
#include "s3.connection.hh"

#include <array>
#include <optional>
#include <string>

namespace zarr {
class S3Sink : public Sink
{
  public:
    S3Sink(std::string_view bucket_name,
           std::string_view object_key,
           std::shared_ptr<S3ConnectionPool> connection_pool);

    bool write(size_t offset, ConstByteSpan data) override;

  protected:
    bool flush_() override;

  private:
    struct MultiPartUpload
    {
        std::string upload_id;
        std::vector<S3Part> parts;
    };

    static constexpr size_t max_part_size_ = 5 << 20;
    std::string bucket_name_;
    std::string object_key_;

    std::shared_ptr<S3ConnectionPool> connection_pool_;

    std::array<uint8_t, max_part_size_> part_buffer_;
    size_t nbytes_buffered_{ 0 };
    size_t nbytes_flushed_{ 0 };

    std::optional<MultiPartUpload> multipart_upload_;

    /**
     * @brief Upload the object to S3.
     * @return True if the object was successfully uploaded, otherwise false.
     */
    [[nodiscard]] bool put_object_();

    /**
     * @brief Check if a multipart upload is in progress.
     * @return True if a multipart upload is in progress, otherwise false.
     */
    bool is_multipart_upload_() const;

    /**
     * @brief Create a new multipart upload.
     */
    void create_multipart_upload_();

    /**
     * @brief Flush the current part to S3.
     * @return True if the part was successfully flushed, otherwise false.
     */
    [[nodiscard]] bool flush_part_();

    /**
     * @brief Finalize the multipart upload.
     * @returns True if a multipart upload was successfully finalized,
     * otherwise false.
     */
    [[nodiscard]] bool finalize_multipart_upload_();
};
} // namespace zarr

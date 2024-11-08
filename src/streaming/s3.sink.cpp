#include "macros.hh"
#include "s3.sink.hh"

#include <miniocpp/client.h>

#ifdef min
#undef min
#endif

zarr::S3Sink::S3Sink(std::string_view bucket_name,
                     std::string_view object_key,
                     std::shared_ptr<S3ConnectionPool> connection_pool)
  : bucket_name_{ bucket_name }
  , object_key_{ object_key }
  , connection_pool_{ connection_pool }
{
    EXPECT(!bucket_name_.empty(), "Bucket name must not be empty");
    EXPECT(!object_key_.empty(), "Object key must not be empty");
    EXPECT(connection_pool_, "Null pointer: connection_pool");
}

bool
zarr::S3Sink::flush_()
{
    if (is_multipart_upload_()) {
        const auto& parts = multipart_upload_->parts;
        if (nbytes_buffered_ > 0 && !flush_part_()) {
            LOG_ERROR("Failed to upload part ",
                      parts.size() + 1,
                      " of object ",
                      object_key_);
            return false;
        }
        if (!finalize_multipart_upload_()) {
            LOG_ERROR("Failed to finalize multipart upload of object ",
                      object_key_);
            return false;
        }
    } else if (nbytes_buffered_ > 0) {
        if (!put_object_()) {
            LOG_ERROR("Failed to upload object: ", object_key_);
            return false;
        }
    }

    // cleanup
    nbytes_buffered_ = 0;

    return true;
}

bool
zarr::S3Sink::write(size_t offset, std::span<const std::byte> data)
{
    if (data.data() == nullptr || data.empty()) {
        return true;
    }

    if (offset < nbytes_flushed_) {
        LOG_ERROR("Cannot write data at offset ",
                  offset,
                  ", already flushed to ",
                  nbytes_flushed_);
        return false;
    }
    nbytes_buffered_ = offset - nbytes_flushed_;

    size_t bytes_of_data = data.size();
    const std::byte* data_ptr = data.data();
    while (bytes_of_data > 0) {
        const auto bytes_to_write =
          std::min(bytes_of_data, part_buffer_.size() - nbytes_buffered_);

        if (bytes_to_write) {
            std::copy_n(data_ptr,
                        bytes_to_write,
                        part_buffer_.begin() + nbytes_buffered_);
            nbytes_buffered_ += bytes_to_write;
            data_ptr += bytes_to_write;
            bytes_of_data -= bytes_to_write;
        }

        if (nbytes_buffered_ == part_buffer_.size() && !flush_part_()) {
            return false;
        }
    }

    return true;
}

bool
zarr::S3Sink::put_object_()
{
    if (nbytes_buffered_ == 0) {
        return false;
    }

    auto connection = connection_pool_->get_connection();
    std::span data(reinterpret_cast<uint8_t*>(part_buffer_.data()),
                   nbytes_buffered_);

    bool retval = false;
    try {
        std::string etag =
          connection->put_object(bucket_name_, object_key_, data);
        EXPECT(!etag.empty(), "Failed to upload object: ", object_key_);

        retval = true;

        nbytes_flushed_ = nbytes_buffered_;
        nbytes_buffered_ = 0;
    } catch (const std::exception& exc) {
        LOG_ERROR("Error: ", exc.what());
    }

    // cleanup
    connection_pool_->return_connection(std::move(connection));

    return retval;
}

bool
zarr::S3Sink::is_multipart_upload_() const
{
    return multipart_upload_.has_value();
}

void
zarr::S3Sink::create_multipart_upload_()
{
    multipart_upload_ = MultiPartUpload{};

    auto connection = connection_pool_->get_connection();
    multipart_upload_->upload_id =
      connection->create_multipart_object(bucket_name_, object_key_);

    connection_pool_->return_connection(std::move(connection));
}

bool
zarr::S3Sink::flush_part_()
{
    if (nbytes_buffered_ == 0) {
        return false;
    }

    if (!is_multipart_upload_()) {
        create_multipart_upload_();
    }

    auto connection = connection_pool_->get_connection();

    bool retval = false;
    try {
        auto& parts = multipart_upload_->parts;

        minio::s3::Part part;
        part.number = static_cast<unsigned int>(parts.size()) + 1;

        std::span data(reinterpret_cast<uint8_t*>(part_buffer_.data()),
                       nbytes_buffered_);
        part.etag =
          connection->upload_multipart_object_part(bucket_name_,
                                                   object_key_,
                                                   multipart_upload_->upload_id,
                                                   data,
                                                   part.number);
        EXPECT(!part.etag.empty(),
               "Failed to upload part ",
               part.number,
               " of object ",
               object_key_);

        parts.push_back(part);

        retval = true;
    } catch (const std::exception& exc) {
        LOG_ERROR("Error: ", exc.what());
    }

    // cleanup
    connection_pool_->return_connection(std::move(connection));
    nbytes_flushed_ += nbytes_buffered_;
    nbytes_buffered_ = 0;

    return retval;
}

bool
zarr::S3Sink::finalize_multipart_upload_()
{
    auto connection = connection_pool_->get_connection();

    const auto& upload_id = multipart_upload_->upload_id;
    const auto& parts = multipart_upload_->parts;

    bool retval = connection->complete_multipart_object(
      bucket_name_, object_key_, upload_id, parts);

    connection_pool_->return_connection(std::move(connection));

    return retval;
}

#include "macros.hh"
#include "s3.connection.hh"

#include <miniocpp/client.h>
#include <miniocpp/utils.h>

#include <list>
#include <regex>
#include <sstream>
#include <string_view>

namespace {
bool
has_port(const std::string& endpoint)
{
    return std::regex_search(endpoint, std::regex(":[0-9]+"));
}

minio::s3::BaseUrl
make_url(const std::string& endpoint, std::optional<std::string> region)
{
    minio::s3::BaseUrl url(endpoint);
    url.https = endpoint.starts_with("https");
    if (!has_port(endpoint)) {
        // https://docs.aws.amazon.com/filegateway/latest/files3/Requirements.html#requirements-network
        url.port = url.https ? 443 : 80;
    }

    if (region) {
        url.region = *region;
    }

    return url;
}
} // namespace

struct zarr::S3Connection::Impl
{
    std::unique_ptr<minio::s3::Client> client;
    std::unique_ptr<minio::creds::Provider> provider;
    std::list<minio::creds::Provider*> providers;
};

zarr::S3Connection::S3Connection(const S3Settings& settings)
  : impl_(std::make_unique<Impl>())
{
    auto url = make_url(settings.endpoint, settings.region);

    impl_->providers.push_back(new minio::creds::EnvAwsProvider());
    impl_->providers.push_back(new minio::creds::AwsConfigProvider());
    impl_->providers.push_back(new minio::creds::IamAwsProvider());
    impl_->provider =
      std::make_unique<minio::creds::ChainedProvider>(impl_->providers);
    impl_->client =
      std::make_unique<minio::s3::Client>(url, impl_->provider.get());

    CHECK(impl_->client);
}

zarr::S3Connection::~S3Connection()
{
    for (auto& provider : impl_->providers) {
        delete provider;
    }
}

bool
zarr::S3Connection::bucket_exists(std::string_view bucket_name)
{
    minio::s3::BucketExistsArgs args;
    args.bucket = bucket_name;

    auto response = impl_->client->BucketExists(args);
    return response.exist;
}

bool
zarr::S3Connection::object_exists(std::string_view bucket_name,
                                  std::string_view object_name)
{
    minio::s3::StatObjectArgs args;
    args.bucket = bucket_name;
    args.object = object_name;

    auto response = impl_->client->StatObject(args);
    // casts to true if response code in 200 range and error message is empty
    return static_cast<bool>(response);
}

std::string
zarr::S3Connection::put_object(std::string_view bucket_name,
                               std::string_view object_name,
                               std::span<uint8_t> data)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");
    EXPECT(!data.empty(), "Data must not be empty.");

    minio::utils::CharBuffer buffer(reinterpret_cast<char*>(data.data()),
                                    data.size());
    std::basic_istream stream(&buffer);

    LOG_DEBUG("Putting object ",
              object_name,
              " with ",
              data.size(),
              " bytes into bucket ",
              bucket_name);
    minio::s3::PutObjectArgs args(stream, static_cast<long>(data.size()), 0);
    args.bucket = bucket_name;
    args.object = object_name;

    auto response = impl_->client->PutObject(args);
    if (!response) {
        LOG_ERROR("Failed to put object ",
                  object_name,
                  " in bucket ",
                  bucket_name,
                  ": ",
                  response.Error().String());
        return {};
    }

    return response.etag;
}

bool
zarr::S3Connection::delete_object(std::string_view bucket_name,
                                  std::string_view object_name)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");

    LOG_DEBUG("Deleting object ", object_name, " from bucket ", bucket_name);
    minio::s3::RemoveObjectArgs args;
    args.bucket = bucket_name;
    args.object = object_name;

    auto response = impl_->client->RemoveObject(args);
    if (!response) {
        LOG_ERROR("Failed to delete object ",
                  object_name,
                  " from bucket ",
                  bucket_name,
                  ": ",
                  response.Error().String());
        return false;
    }

    return true;
}

std::string
zarr::S3Connection::create_multipart_object(std::string_view bucket_name,
                                            std::string_view object_name)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");

    LOG_DEBUG(
      "Creating multipart object ", object_name, " in bucket ", bucket_name);
    minio::s3::CreateMultipartUploadArgs args;
    args.bucket = bucket_name;
    args.object = object_name;

    auto response = impl_->client->CreateMultipartUpload(args);
    if (!response) {
        LOG_ERROR("Failed to create multipart object ",
                  object_name,
                  " in bucket ",
                  bucket_name,
                  ": ",
                  response.Error().String());
    }
    EXPECT(!response.upload_id.empty(), "Upload id returned empty.");

    return response.upload_id;
}

std::string
zarr::S3Connection::upload_multipart_object_part(std::string_view bucket_name,
                                                 std::string_view object_name,
                                                 std::string_view upload_id,
                                                 std::span<uint8_t> data,
                                                 unsigned int part_number)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");
    EXPECT(!data.empty(), "Number of bytes must be positive.");
    EXPECT(part_number, "Part number must be positive.");

    LOG_DEBUG("Uploading multipart object part ",
              part_number,
              " for object ",
              object_name,
              " in bucket ",
              bucket_name);

    std::string_view data_buffer(reinterpret_cast<const char*>(data.data()),
                                 data.size());

    minio::s3::UploadPartArgs args;
    args.bucket = bucket_name;
    args.object = object_name;
    args.part_number = part_number;
    args.upload_id = upload_id;
    args.data = data_buffer;

    auto response = impl_->client->UploadPart(args);
    if (!response) {
        LOG_ERROR("Failed to upload part ",
                  part_number,
                  " for object ",
                  object_name,
                  " in bucket ",
                  bucket_name,
                  ": ",
                  response.Error().String());
        return {};
    }

    return response.etag;
}

bool
zarr::S3Connection::complete_multipart_object(std::string_view bucket_name,
                                              std::string_view object_name,
                                              std::string_view upload_id,
                                              const std::vector<S3Part>& parts)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");
    EXPECT(!upload_id.empty(), "Upload id must not be empty.");
    EXPECT(!parts.empty(), "Parts list must not be empty.");

    LOG_DEBUG(
      "Completing multipart object ", object_name, " in bucket ", bucket_name);
    minio::s3::CompleteMultipartUploadArgs args;
    args.bucket = bucket_name;
    args.object = object_name;
    args.upload_id = upload_id;
    args.parts.clear();
    for (const auto& part : parts) {
        args.parts.emplace_back(part.number, part.etag);
        args.parts.back().size = part.size;
    }

    auto response = impl_->client->CompleteMultipartUpload(args);
    if (!response) {
        LOG_ERROR("Failed to complete multipart object ",
                  object_name,
                  " in bucket ",
                  bucket_name,
                  ": ",
                  response.Error().String());
        return false;
    }

    return true;
}

zarr::S3ConnectionPool::S3ConnectionPool(size_t n_connections,
                                         const S3Settings& settings)
{
    if (settings.region) {
        LOG_DEBUG("Setting region to ", *settings.region);
    }

    for (auto i = 0; i < n_connections; ++i) {
        connections_.emplace_back(std::make_unique<S3Connection>(settings));
    }

    CHECK(!connections_.empty());
}

zarr::S3ConnectionPool::~S3ConnectionPool()
{
    is_accepting_connections_ = false;
    cv_.notify_all();
}

std::unique_ptr<zarr::S3Connection>
zarr::S3ConnectionPool::get_connection()
{
    std::unique_lock lock(connections_mutex_);
    cv_.wait(lock, [this] {
        return !is_accepting_connections_ || !connections_.empty();
    });

    if (!is_accepting_connections_ || connections_.empty()) {
        return nullptr;
    }

    auto conn = std::move(connections_.back());
    connections_.pop_back();
    return conn;
}

void
zarr::S3ConnectionPool::return_connection(std::unique_ptr<S3Connection>&& conn)
{
    std::unique_lock lock(connections_mutex_);
    connections_.push_back(std::move(conn));
    cv_.notify_one();
}

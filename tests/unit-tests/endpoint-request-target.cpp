/// Endpoint parsing and bucket addressing. Needs no server, so the name avoids
/// "s3" -- CMake labels any test matching that as requiring MinIO.

#include "unit.test.macros.hh"
#include "s3.client.hh"

#include <string>

namespace {
void
expect_target_for_bucket(const std::string& endpoint,
                         std::string_view bucket_name,
                         std::string_view object_name,
                         const std::string& expected_uri,
                         const std::string& expected_path)
{
    const auto target =
      zarr::s3_request_target(endpoint, bucket_name, object_name);

    EXPECT_STR_EQ(target.uri.c_str(), expected_uri.c_str());
    EXPECT_STR_EQ(target.path.c_str(), expected_path.c_str());
}

void
expect_target(const std::string& endpoint,
              std::string_view object_name,
              const std::string& expected_uri,
              const std::string& expected_path)
{
    expect_target_for_bucket(
      endpoint, "my-bucket", object_name, expected_uri, expected_path);
}

void
expect_rejected(const std::string& endpoint)
{
    bool rejected = false;
    std::string uri;

    try {
        uri = zarr::s3_request_target(endpoint, "my-bucket", "key").uri;
    } catch (const std::runtime_error&) {
        rejected = true;
    }

    EXPECT(rejected,
           "Expected endpoint '",
           endpoint,
           "' to be rejected, but it resolved to ",
           uri);

    // settings validation must agree with the client, or it would accept an
    // endpoint the client then throws on
    std::string error;
    EXPECT(!zarr::is_valid_s3_endpoint(endpoint, error),
           "Expected is_valid_s3_endpoint to reject '",
           endpoint,
           "'");
    EXPECT(!error.empty(), "Expected a reason for rejecting '", endpoint, "'");
}

void
expect_accepted(const std::string& endpoint)
{
    std::string error;
    EXPECT(zarr::is_valid_s3_endpoint(endpoint, error),
           "Expected is_valid_s3_endpoint to accept '",
           endpoint,
           "', but it reported: ",
           error);
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        // S3-compatible servers get path-style addressing
        expect_target("http://localhost:9000",
                      "my-dataset.zarr/c/0/0",
                      "http://localhost:9000",
                      "/my-bucket/my-dataset.zarr/c/0/0");
        expect_target("https://minio.internal",
                      "key",
                      "https://minio.internal",
                      "/my-bucket/key");

        // an empty object name addresses the bucket itself
        expect_target(
          "http://localhost:9000", "", "http://localhost:9000", "/my-bucket");

        // AWS gets virtual-host addressing, port and all
        expect_target("https://s3.amazonaws.com",
                      "my-dataset.zarr/zarr.json",
                      "https://my-bucket.s3.amazonaws.com",
                      "/my-dataset.zarr/zarr.json");
        expect_target("https://s3.us-west-2.amazonaws.com:443",
                      "key",
                      "https://my-bucket.s3.us-west-2.amazonaws.com:443",
                      "/key");

        // a host that merely ends in something similar is not AWS
        expect_target("https://not-amazonaws.com",
                      "key",
                      "https://not-amazonaws.com",
                      "/my-bucket/key");

        // a bucket that is not a DNS label cannot go in the host, so it gets
        // path style even on AWS: a dot would add a label the wildcard
        // certificate cannot match, and the others are not legal hostnames
        expect_target_for_bucket("https://s3.us-west-2.amazonaws.com",
                                 "microscopy.data",
                                 "key",
                                 "https://s3.us-west-2.amazonaws.com",
                                 "/microscopy.data/key");
        expect_target_for_bucket("https://s3.amazonaws.com",
                                 "MyBucket",
                                 "key",
                                 "https://s3.amazonaws.com",
                                 "/MyBucket/key");
        expect_target_for_bucket("https://s3.amazonaws.com",
                                 "my_bucket",
                                 "key",
                                 "https://s3.amazonaws.com",
                                 "/my_bucket/key");
        expect_target_for_bucket("https://s3.amazonaws.com",
                                 "-leading-hyphen",
                                 "key",
                                 "https://s3.amazonaws.com",
                                 "/-leading-hyphen/key");

        // ... and one that is keeps virtual-host addressing
        expect_target_for_bucket("https://s3.amazonaws.com",
                                 "bucket-2",
                                 "key",
                                 "https://bucket-2.s3.amazonaws.com",
                                 "/key");

        // schemes and hostnames are case-insensitive
        expect_target(
          "HTTPS://MinIO:9000", "key", "https://minio:9000", "/my-bucket/key");
        expect_target("Http://LocalHost:9000",
                      "key",
                      "http://localhost:9000",
                      "/my-bucket/key");
        expect_target("HTTPS://S3.US-WEST-2.AMAZONAWS.COM",
                      "key",
                      "https://my-bucket.s3.us-west-2.amazonaws.com",
                      "/key");

        // anything after the authority belongs to us, not the caller
        expect_target("http://localhost:9000/ignored?x=1#f",
                      "key",
                      "http://localhost:9000",
                      "/my-bucket/key");

        // the scheme is required: neither default is safe to guess
        expect_rejected("localhost:9000");
        expect_rejected("s3.amazonaws.com");
        expect_rejected("ftp://localhost:9000");
        expect_rejected("");

        // ... and so is a host
        expect_rejected("http://");
        expect_rejected("https:///my-bucket");

        // the endpoints above are what settings validation accepts, too
        expect_accepted("http://localhost:9000");
        expect_accepted("HTTPS://MinIO:9000");
        expect_accepted("https://s3.us-west-2.amazonaws.com:443");
        expect_accepted("http://localhost:9000/ignored?x=1#f");

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Test failed: ", e.what());
        throw;
    }

    return retval;
}

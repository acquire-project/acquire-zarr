// The CRT headers pull in windows.h transitively; its min/max macros would
// otherwise break <algorithm> below. The sink this replaces did the same job
// with an #undef min after the fact. This has to precede every include, since
// any of them may reach windows.h first.
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#endif

#include "macros.hh"
#include "s3.client.hh"
#include "zarr.common.hh"

#include <aws/crt/Api.h>
#include <aws/crt/auth/Credentials.h>
#include <aws/crt/http/HttpRequestResponse.h>
#include <aws/crt/io/Stream.h>
#include <aws/crt/io/Uri.h>
#include <aws/crt/s3/S3.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <functional>
#include <future>
#include <istream>
#include <mutex>
#include <streambuf>
#include <type_traits>
#include <utility>

namespace crt = Aws::Crt;
namespace crt_s3 = Aws::Crt::S3;

namespace {

/// The CRT's library init is guarded by a plain bool rather than a refcount, and
/// its teardown joins every CRT-managed thread process-wide. Initialize exactly
/// once and never tear down: destroying the ApiHandle at exit would race writer
/// threads still draining, and the CRT cannot be re-initialized afterwards.
struct CrtContext
{
    crt::ApiHandle api_handle;
    std::shared_ptr<crt::Auth::ICredentialsProvider> credentials;

    CrtContext()
    {
        // Bootstrap and TlsContext are left null so the CRT supplies its
        // process-wide defaults (ApiHandle::GetOrCreateStaticDefaultClientBootstrap).
        crt::Auth::CredentialsProviderChainDefaultConfig config;
        credentials =
          crt::Auth::CredentialsProvider::CreateCredentialsProviderChainDefault(
            config);
    }
};

CrtContext&
crt_context()
{
    // Deliberately leaked; see CrtContext.
    static auto* context = new CrtContext();
    return *context;
}

/// Where requests go, and how the bucket is addressed.
struct Endpoint
{
    std::string scheme;    // "http" or "https"
    std::string authority; // host, with port if one was given
    bool aws_endpoint;     // real AWS, which requires virtual-host addressing
};

/// Split @p endpoint into scheme and authority, or set @p error to why it
/// cannot be. Shared by parse_endpoint() and zarr::is_valid_s3_endpoint() so
/// that settings validation and the client cannot disagree about what is
/// acceptable, and so the caller-facing message is written once.
std::optional<Endpoint>
try_parse_endpoint(const std::string& endpoint, std::string& error)
{
    Endpoint parsed;

    // Schemes and hostnames are case-insensitive, and only the scheme and
    // authority survive the parse below, so fold the whole thing once. Error
    // messages still quote what the caller actually passed.
    const auto lowered = zarr::to_lower(endpoint);

    // Neither default is safe to guess: TLS would fail the handshake against a
    // plaintext server, and plaintext would silently downgrade one that
    // expected TLS. Make the caller say which they meant.
    std::string_view rest(lowered);
    if (!rest.starts_with("http://") && !rest.starts_with("https://")) {
        error =
          "S3 endpoint '" + endpoint + "' must begin with http:// or https://";
        return std::nullopt;
    }

    if (rest.starts_with("https://")) {
        parsed.scheme = "https";
        rest.remove_prefix(8);
    } else {
        parsed.scheme = "http";
        rest.remove_prefix(7);
    }

    // drop any path, query or fragment the caller appended
    if (const auto end = rest.find_first_of("/?#");
        end != std::string_view::npos) {
        rest = rest.substr(0, end);
    }

    if (rest.empty()) {
        error = "S3 endpoint '" + endpoint + "' has no host";
        return std::nullopt;
    }
    parsed.authority = std::string(rest);

    // AWS requires virtual-host addressing; most other S3-compatible servers
    // require path style. Decide from the endpoint rather than adding a knob
    // callers would have no way to know they must set.
    std::string_view host = parsed.authority;
    if (const auto colon = host.rfind(':'); colon != std::string_view::npos) {
        host = host.substr(0, colon);
    }
    parsed.aws_endpoint = host.ends_with(".amazonaws.com");

    return parsed;
}

Endpoint
parse_endpoint(const std::string& endpoint)
{
    std::string error;
    auto parsed = try_parse_endpoint(endpoint, error);
    EXPECT(parsed.has_value(), error);

    return *parsed;
}

/// Whether @p bucket_name can be prepended to the endpoint as a DNS label.
/// Virtual-host addressing puts the bucket in the hostname, so a name with a
/// dot adds a label the `*.s3.<region>.amazonaws.com` wildcard certificate
/// cannot match, and one with an underscore or an upper-case letter is not a
/// legal hostname at all. Such buckets fall back to path style, which is what
/// the AWS SDKs do and what AWS documents as the workaround.
bool
is_virtual_host_addressable(std::string_view bucket_name)
{
    if (bucket_name.size() < 3 || bucket_name.size() > 63) {
        return false;
    }

    const auto is_alnum = [](char c) {
        return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
    };

    if (!is_alnum(bucket_name.front()) || !is_alnum(bucket_name.back())) {
        return false;
    }

    return std::all_of(bucket_name.begin(), bucket_name.end(), [&](char c) {
        return is_alnum(c) || c == '-';
    });
}

zarr::S3RequestTarget
make_target(const Endpoint& endpoint,
            std::string_view bucket_name,
            std::string_view object_name)
{
    zarr::S3RequestTarget target;

    if (endpoint.aws_endpoint && is_virtual_host_addressable(bucket_name)) {
        target.uri = endpoint.scheme + "://" + std::string(bucket_name) + "." +
                     endpoint.authority;
        target.path = "/" + std::string(object_name);
    } else {
        target.uri = endpoint.scheme + "://" + endpoint.authority;
        target.path = "/" + std::string(bucket_name);
        if (!object_name.empty()) {
            target.path += "/" + std::string(object_name);
        }
    }

    return target;
}

/// aws-crt-cpp 0.43.0 declares the meta-request callbacks as int-returning,
/// where AWS_OP_SUCCESS (0) means "keep going"; 0.43.5 changes them to
/// bool-returning, where 0 means "abort". Returning the wrong one silently
/// truncates every download, so pin the convention we compiled against.
static_assert(
  std::is_same_v<
    crt_s3::S3MetaRequestOptions::BodyCallback,
    std::function<int(crt::ByteCursor, uint64_t)>>,
  "aws-crt-cpp changed its meta-request callback convention (0.43.0 returns "
  "int/AWS_OP_SUCCESS to continue, 0.43.5 returns bool). Audit every callback "
  "return in this file before bumping the vcpkg baseline.");

/// What a callback returns to let the meta request proceed.
constexpr int kContinue = AWS_OP_SUCCESS;

/// Outcome of a meta request that we waited on.
struct Outcome
{
    int error_code{ AWS_ERROR_UNKNOWN };
    int response_status{ 0 };
    std::string error_body;

    [[nodiscard]] bool ok() const { return error_code == AWS_ERROR_SUCCESS; }

    [[nodiscard]] std::string describe() const
    {
        std::string described(aws_error_debug_str(error_code));
        if (response_status != 0) {
            described += " (HTTP " + std::to_string(response_status) + ")";
        }
        if (!error_body.empty()) {
            described += ": " + error_body;
        }
        return described;
    }
};

/// Install a finish callback that fulfils @p promise.
/// @note The promise is captured by value so it outlives the caller's frame even
/// if we stop waiting on it.
void
capture_outcome(crt_s3::S3MetaRequestOptions& options,
                const std::shared_ptr<std::promise<Outcome>>& promise)
{
    options.SetFinishCallback(
      [promise](const crt_s3::S3MetaRequestResult& result) {
          Outcome outcome;
          outcome.error_code = result.errorCode;
          outcome.response_status = result.responseStatus;
          if (result.errorResponseBody.len > 0) {
              // borrowed for the duration of this callback only
              outcome.error_body.assign(
                reinterpret_cast<const char*>(result.errorResponseBody.ptr),
                result.errorResponseBody.len);
          }
          promise->set_value(std::move(outcome));
      });
}

std::shared_ptr<crt::Http::HttpRequest>
make_request(std::string_view method, const zarr::S3RequestTarget& target)
{
    auto request = crt::MakeShared<crt::Http::HttpRequest>(crt::ApiAllocator());
    EXPECT(request, "Failed to allocate an HTTP request.");

    // No Host header: the CRT derives it from the endpoint override, and a Host
    // that disagrees with the endpoint authority fails the request outright.
    if (!request->SetMethod(crt::ByteCursorFromArray(
          reinterpret_cast<const uint8_t*>(method.data()), method.size())) ||
        !request->SetPath(crt::ByteCursorFromCString(target.path.c_str()))) {
        return nullptr;
    }

    return request;
}

/// Non-owning istream over caller memory, so PutObject streams from the sink's
/// buffer without copying it.
class MemoryStreamBuf : public std::streambuf
{
  public:
    MemoryStreamBuf(const char* data, size_t size)
    {
        auto* begin = const_cast<char*>(data);
        setg(begin, begin, begin + size);
    }

  protected:
    pos_type seekoff(off_type off,
                     std::ios_base::seekdir dir,
                     std::ios_base::openmode which) override
    {
        if (!(which & std::ios_base::in)) {
            return pos_type(off_type(-1));
        }

        const off_type size = egptr() - eback();
        off_type target = off;
        if (dir == std::ios_base::cur) {
            target += gptr() - eback();
        } else if (dir == std::ios_base::end) {
            target += size;
        }

        if (target < 0 || target > size) {
            return pos_type(off_type(-1));
        }

        setg(eback(), eback() + target, egptr());
        return pos_type(target);
    }

    pos_type seekpos(pos_type pos, std::ios_base::openmode which) override
    {
        return seekoff(off_type(pos), std::ios_base::beg, which);
    }
};

struct MemoryStream : std::istream
{
    MemoryStreamBuf buffer;

    MemoryStream(const char* data, size_t size)
      : std::istream(nullptr)
      , buffer(data, size)
    {
        rdbuf(&buffer);
    }
};

} // namespace

zarr::S3RequestTarget
zarr::s3_request_target(const std::string& endpoint,
                        std::string_view bucket_name,
                        std::string_view object_name)
{
    return make_target(parse_endpoint(endpoint), bucket_name, object_name);
}

bool
zarr::is_valid_s3_endpoint(const std::string& endpoint, std::string& error)
{
    return try_parse_endpoint(endpoint, error).has_value();
}

struct zarr::S3Client::Impl
{
    Endpoint endpoint;
    std::string region;
    std::unique_ptr<crt_s3::S3Client> client;

    /// Run @p options to completion on this client and report the result.
    Outcome run(crt_s3::S3MetaRequestOptions& options)
    {
        auto promise = std::make_shared<std::promise<Outcome>>();
        auto future = promise->get_future();
        capture_outcome(options, promise);

        auto request = client->MakeMetaRequest(options);
        if (!request) {
            Outcome outcome;
            outcome.error_code = client->LastError();
            return outcome;
        }

        return future.get();
    }

    /// Issue a request with no body and report the result.
    Outcome run_simple(std::string_view method,
                       std::string_view operation,
                       std::string_view bucket_name,
                       std::string_view object_name)
    {
        const auto target = make_target(endpoint, bucket_name, object_name);
        auto request = make_request(method, target);
        if (!request) {
            return {};
        }

        auto options = crt_s3::S3DefaultObjectMetaRequestOptions::Create(
          request, crt::String(operation.data(), operation.size()));
        if (!options) {
            return {};
        }

        const crt::Io::Uri uri(crt::ByteCursorFromCString(target.uri.c_str()));
        options->SetEndpoint(uri);

        return run(*options);
    }
};

zarr::S3Client::S3Client(const S3Settings& settings)
  : impl_(std::make_unique<Impl>())
{
    impl_->endpoint = parse_endpoint(settings.endpoint);

    // SigV4 always needs a region. Use us-east-1 when none is configured;
    // endpoints that validate the signing region require it to be set
    // explicitly, so this is a starting point rather than a safe default.
    if (settings.region && !settings.region->empty()) {
        impl_->region = *settings.region;
    } else {
        impl_->region = "us-east-1";
        LOG_DEBUG("No S3 region configured; signing for ", impl_->region);
    }

    // path style against real AWS is the workaround for a bucket that cannot be
    // a DNS label, not a choice; say so rather than leave it to be inferred
    // from a rejected request
    if (impl_->endpoint.aws_endpoint &&
        !is_virtual_host_addressable(settings.bucket_name)) {
        LOG_DEBUG("Bucket '",
                  settings.bucket_name,
                  "' is not a DNS label, so requests to ",
                  settings.endpoint,
                  " use path-style addressing");
    }

    auto& context = crt_context();
    EXPECT(context.credentials, "Failed to create an S3 credentials provider.");

    crt_s3::S3ClientConfig config(context.credentials);
    config.SetRegion(crt::String(impl_->region.c_str()))
      .SetTlsMode(impl_->endpoint.scheme == "https"
                    ? crt_s3::S3TlsMode::Enabled
                    : crt_s3::S3TlsMode::Disabled)
      // directory buckets are not a use case here, and leaving S3 Express on
      // makes the client probe for a session token it will never need
      .SetEnableS3Express(false);

    impl_->client = std::make_unique<crt_s3::S3Client>(config);
    EXPECT(impl_->client && static_cast<bool>(*impl_->client),
           "Failed to create an S3 client for endpoint ",
           settings.endpoint,
           ": ",
           aws_error_debug_str(impl_->client ? impl_->client->LastError()
                                             : AWS_ERROR_UNKNOWN));
}

zarr::S3Client::~S3Client() = default;

bool
zarr::S3Client::bucket_exists(std::string_view bucket_name)
{
    if (bucket_name.empty()) {
        return false;
    }

    return impl_->run_simple("HEAD", "HeadBucket", bucket_name, "").ok();
}

bool
zarr::S3Client::object_exists(std::string_view bucket_name,
                              std::string_view object_name)
{
    if (bucket_name.empty() || object_name.empty()) {
        return false;
    }

    return impl_->run_simple("HEAD", "HeadObject", bucket_name, object_name)
      .ok();
}

bool
zarr::S3Client::put_object(std::string_view bucket_name,
                           std::string_view object_name,
                           ConstByteSpan data)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");
    EXPECT(!data.empty(), "Data must not be empty.");

    LOG_DEBUG("Putting object ",
              object_name,
              " with ",
              data.size(),
              " bytes into bucket ",
              bucket_name);

    const auto target = make_target(impl_->endpoint, bucket_name, object_name);
    auto request = make_request("PUT", target);
    if (!request) {
        return false;
    }

    const auto length = std::to_string(data.size());
    crt::Http::HttpHeader content_length;
    content_length.name = crt::ByteCursorFromCString("Content-Length");
    content_length.value = crt::ByteCursorFromCString(length.c_str());
    if (!request->AddHeader(content_length)) {
        return false;
    }

    auto body = std::make_shared<MemoryStream>(
      reinterpret_cast<const char*>(data.data()), data.size());
    if (!request->SetBody(std::static_pointer_cast<crt::Io::IStream>(body))) {
        return false;
    }

    auto options = crt_s3::S3PutObjectMetaRequestOptions::Create(request);
    if (!options) {
        return false;
    }

    const crt::Io::Uri uri(crt::ByteCursorFromCString(target.uri.c_str()));
    options->SetEndpoint(uri);

    const auto outcome = impl_->run(*options);
    if (!outcome.ok()) {
        LOG_ERROR("Failed to put object ",
                  object_name,
                  " in bucket ",
                  bucket_name,
                  ": ",
                  outcome.describe());
        return false;
    }

    return true;
}

bool
zarr::S3Client::delete_object(std::string_view bucket_name,
                              std::string_view object_name)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");

    LOG_DEBUG("Deleting object ", object_name, " from bucket ", bucket_name);

    const auto outcome =
      impl_->run_simple("DELETE", "DeleteObject", bucket_name, object_name);
    if (!outcome.ok()) {
        LOG_ERROR("Failed to delete object ",
                  object_name,
                  " from bucket ",
                  bucket_name,
                  ": ",
                  outcome.describe());
        return false;
    }

    return true;
}

std::optional<size_t>
zarr::S3Client::object_size(std::string_view bucket_name,
                            std::string_view object_name)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");

    const auto target = make_target(impl_->endpoint, bucket_name, object_name);
    auto request = make_request("HEAD", target);
    if (!request) {
        return std::nullopt;
    }

    auto options = crt_s3::S3DefaultObjectMetaRequestOptions::Create(
      request, crt::String("HeadObject"));
    if (!options) {
        return std::nullopt;
    }

    const crt::Io::Uri uri(crt::ByteCursorFromCString(target.uri.c_str()));
    options->SetEndpoint(uri);

    auto size = std::make_shared<std::optional<size_t>>();
    options->SetHeadersCallback(
      [size](const crt::Vector<crt::Http::HttpHeader>& headers, int) -> int {
          for (const auto& header : headers) {
              const std::string_view name(
                reinterpret_cast<const char*>(header.name.ptr), header.name.len);
              if (name.size() == 14 &&
                  std::equal(name.begin(),
                             name.end(),
                             "content-length",
                             [](char a, char b) {
                                 return std::tolower(
                                          static_cast<unsigned char>(a)) == b;
                             })) {
                  // this runs on a CRT thread, called from C: from_chars
                  // reports a malformed value instead of throwing through the
                  // C frames the way stoull would. An unparseable length is
                  // left unset, as an absent one is.
                  const auto* value =
                    reinterpret_cast<const char*>(header.value.ptr);
                  size_t length = 0;
                  if (std::from_chars(value, value + header.value.len, length)
                        .ec == std::errc{}) {
                      *size = length;
                  }
                  break;
              }
          }
          return kContinue;
      });

    const auto outcome = impl_->run(*options);
    if (!outcome.ok()) {
        LOG_ERROR("Failed to stat object ",
                  object_name,
                  " in bucket ",
                  bucket_name,
                  ": ",
                  outcome.describe());
        return std::nullopt;
    }

    return *size;
}

std::optional<ByteVector>
zarr::S3Client::get_object(std::string_view bucket_name,
                           std::string_view object_name)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");

    const auto target = make_target(impl_->endpoint, bucket_name, object_name);
    auto request = make_request("GET", target);
    if (!request) {
        return std::nullopt;
    }

    // The CRT may split a GetObject into parallel ranged requests, so chunks can
    // arrive out of order and on different threads. Place each at its offset.
    struct Body
    {
        std::mutex mutex;
        ByteVector bytes;
    };
    auto body = std::make_shared<Body>();

    auto options = crt_s3::S3GetObjectMetaRequestOptions::Create(
      request,
      [body](crt::ByteCursor chunk, uint64_t range_start) -> int {
          const auto offset = static_cast<size_t>(range_start);
          std::lock_guard lock(body->mutex);
          if (body->bytes.size() < offset + chunk.len) {
              body->bytes.resize(offset + chunk.len);
          }
          std::copy_n(chunk.ptr, chunk.len, body->bytes.begin() + offset);
          return kContinue;
      });
    if (!options) {
        return std::nullopt;
    }

    const crt::Io::Uri uri(crt::ByteCursorFromCString(target.uri.c_str()));
    options->SetEndpoint(uri);

    const auto outcome = impl_->run(*options);
    if (!outcome.ok()) {
        LOG_ERROR("Failed to get object ",
                  object_name,
                  " from bucket ",
                  bucket_name,
                  ": ",
                  outcome.describe());
        return std::nullopt;
    }

    std::lock_guard lock(body->mutex);
    return std::move(body->bytes);
}

struct zarr::S3Upload::Impl
{
    std::string object_key;

    std::shared_ptr<crt::Http::HttpRequest> request;
    crt::ScopedResource<crt_s3::S3MetaRequestOptions> options;
    std::shared_ptr<crt_s3::S3MetaRequest> meta_request;

    std::future<Outcome> completion;

    bool eof_sent{ false };
    bool finished{ false };
    bool ok{ false };

    /// Wait for the CRT to finish tearing the upload down.
    void await_completion()
    {
        if (completion.valid()) {
            (void)completion.get();
        }
    }
};

zarr::S3Upload::S3Upload(std::unique_ptr<Impl> impl)
  : impl_(std::move(impl))
{
}

zarr::S3Upload::~S3Upload()
{
    if (!impl_ || impl_->finished) {
        return;
    }

    // dropped without finishing: don't leave parts behind in the bucket
    abort();
}

bool
zarr::S3Upload::append(ConstByteSpan data)
{
    if (data.empty()) {
        return true;
    }

    EXPECT(!impl_->eof_sent,
           "Cannot append to object ",
           impl_->object_key,
           " after it has been finished.");

    auto pending = impl_->meta_request->Write(
      crt::ByteCursorFromArray(data.data(), data.size()), false);
    const int status = pending.get();
    if (status != AWS_ERROR_SUCCESS) {
        LOG_ERROR("Failed to upload ",
                  data.size(),
                  " bytes of object ",
                  impl_->object_key,
                  ": ",
                  aws_error_debug_str(status));
        return false;
    }

    return true;
}

bool
zarr::S3Upload::finish()
{
    if (impl_->finished) {
        return impl_->ok;
    }
    impl_->finished = true;

    if (!impl_->eof_sent) {
        impl_->eof_sent = true;
        auto pending =
          impl_->meta_request->Write(crt::ByteCursorFromArray(nullptr, 0), true);
        const int status = pending.get();
        if (status != AWS_ERROR_SUCCESS) {
            LOG_ERROR("Failed to finalize upload of object ",
                      impl_->object_key,
                      ": ",
                      aws_error_debug_str(status));
            impl_->await_completion();
            return false;
        }
    }

    const auto outcome = impl_->completion.get();
    if (!outcome.ok()) {
        LOG_ERROR("Failed to upload object ",
                  impl_->object_key,
                  ": ",
                  outcome.describe());
        return false;
    }

    impl_->ok = true;
    return true;
}

void
zarr::S3Upload::abort()
{
    if (impl_->finished) {
        return;
    }
    impl_->finished = true;

    LOG_DEBUG("Aborting upload of object ", impl_->object_key);
    impl_->meta_request->Cancel();

    // the finish callback still fires on cancellation, so this returns promptly
    impl_->await_completion();
}

std::unique_ptr<zarr::S3Upload>
zarr::S3Client::create_upload(std::string_view bucket_name,
                              std::string_view object_name)
{
    EXPECT(!bucket_name.empty(), "Bucket name must not be empty.");
    EXPECT(!object_name.empty(), "Object name must not be empty.");

    LOG_DEBUG(
      "Starting upload of object ", object_name, " to bucket ", bucket_name);

    const auto target = make_target(impl_->endpoint, bucket_name, object_name);

    auto impl = std::make_unique<S3Upload::Impl>();
    impl->object_key = std::string(object_name);

    impl->request = make_request("PUT", target);
    if (!impl->request) {
        return nullptr;
    }

    // No body and no Content-Length: the CRT drives the multipart upload and the
    // total size is not known until finish().
    impl->options =
      crt_s3::S3PutObjectMetaRequestOptions::CreateWithAsyncWrites(
        impl->request);
    if (!impl->options) {
        LOG_ERROR("Failed to create upload options for object ", object_name);
        return nullptr;
    }

    const crt::Io::Uri uri(crt::ByteCursorFromCString(target.uri.c_str()));
    impl->options->SetEndpoint(uri);

    auto promise = std::make_shared<std::promise<Outcome>>();
    impl->completion = promise->get_future();
    capture_outcome(*impl->options, promise);

    impl->meta_request = impl_->client->MakeMetaRequest(*impl->options);
    if (!impl->meta_request) {
        LOG_ERROR("Failed to start upload of object ",
                  object_name,
                  ": ",
                  aws_error_debug_str(impl_->client->LastError()));
        return nullptr;
    }

    return std::unique_ptr<S3Upload>(new S3Upload(std::move(impl)));
}

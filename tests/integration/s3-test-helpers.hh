// Header-only S3 test helpers for validation against MinIO/S3.
//
// Two backends:
//   - miniocpp (default) — used by the baseline vcpkg build, portable to
//     all platforms that can link miniocpp.
//   - aws CLI via popen — opt-in via -DS3_TEST_HELPERS_USE_AWS_CLI, used by
//     the shim build where miniocpp is intentionally not available.
#pragma once

#include <cstdint>
#include <cstdlib>
#include <string>
#include <vector>

#ifdef S3_TEST_HELPERS_USE_AWS_CLI

#include <cstdio>

#ifdef _WIN32
#define popen _popen
#define pclose _pclose
#endif

namespace s3 {

namespace detail {

inline std::string
env(const char* name)
{
    const char* v = std::getenv(name);
    return v ? v : "";
}

inline int
run(const std::string& cmd)
{
    int rc = std::system(cmd.c_str());
#ifdef _WIN32
    return rc;
#else
    return WIFEXITED(rc) ? WEXITSTATUS(rc) : 1;
#endif
}

inline std::string
capture(const std::string& cmd)
{
    std::string out;
    FILE* f = popen(cmd.c_str(), "r");
    if (!f)
        return out;
    char buf[4096];
    while (size_t n = fread(buf, 1, sizeof(buf), f))
        out.append(buf, n);
    pclose(f);
    return out;
}

inline std::vector<uint8_t>
capture_bytes(const std::string& cmd)
{
    std::vector<uint8_t> out;
    FILE* f = popen(cmd.c_str(), "r");
    if (!f)
        return out;
    uint8_t buf[4096];
    while (size_t n = fread(buf, 1, sizeof(buf), f))
        out.insert(out.end(), buf, buf + n);
    pclose(f);
    return out;
}

inline std::string
aws_prefix()
{
    return "aws --endpoint-url " + env("ZARR_S3_ENDPOINT");
}

} // namespace detail

inline std::string
endpoint()
{
    return detail::env("ZARR_S3_ENDPOINT");
}

inline std::string
bucket()
{
    return detail::env("ZARR_S3_BUCKET_NAME");
}

inline bool
object_exists(const std::string& key)
{
    std::string cmd = detail::aws_prefix() + " s3api head-object --bucket " +
                      bucket() + " --key " + key + " > /dev/null 2>&1";
    return detail::run(cmd) == 0;
}

inline size_t
get_object_size(const std::string& key)
{
    std::string cmd = detail::aws_prefix() + " s3api head-object --bucket " +
                      bucket() + " --key " + key +
                      " --query ContentLength --output text 2>/dev/null";
    std::string out = detail::capture(cmd);
    if (out.empty())
        return 0;
    return std::stoull(out);
}

inline std::string
get_object_contents(const std::string& key)
{
    std::string cmd = detail::aws_prefix() + " s3 cp s3://" + bucket() + "/" +
                      key + " - 2>/dev/null";
    return detail::capture(cmd);
}

inline std::vector<uint8_t>
get_object_bytes(const std::string& key)
{
    std::string cmd = detail::aws_prefix() + " s3 cp s3://" + bucket() + "/" +
                      key + " - 2>/dev/null";
    return detail::capture_bytes(cmd);
}

inline bool
remove_prefix(const std::string& prefix)
{
    std::string cmd = detail::aws_prefix() + " s3 rm --recursive s3://" +
                      bucket() + "/" + prefix + " > /dev/null 2>&1";
    return detail::run(cmd) == 0;
}

inline bool
remove_items(const std::vector<std::string>& keys)
{
    for (const auto& key : keys) {
        std::string cmd = detail::aws_prefix() + " s3 rm s3://" + bucket() +
                          "/" + key + " > /dev/null 2>&1";
        if (detail::run(cmd) != 0)
            return false;
    }
    return true;
}

} // namespace s3

#else // !S3_TEST_HELPERS_USE_AWS_CLI — miniocpp backend

#include <miniocpp/client.h>

#include <list>
#include <sstream>

namespace s3 {

namespace detail {

inline std::string
env(const char* name)
{
    const char* v = std::getenv(name);
    return v ? v : "";
}

struct Context
{
    std::string endpoint_url = env("ZARR_S3_ENDPOINT");
    std::string bucket_name = env("ZARR_S3_BUCKET_NAME");
    minio::s3::BaseUrl url = [this] {
        minio::s3::BaseUrl u(endpoint_url);
        u.https = endpoint_url.rfind("https://", 0) == 0;
        return u;
    }();
    minio::creds::StaticProvider provider{ env("AWS_ACCESS_KEY_ID"),
                                           env("AWS_SECRET_ACCESS_KEY") };
    minio::s3::Client client{ url, &provider };
};

inline Context&
ctx()
{
    static Context c;
    return c;
}

} // namespace detail

inline std::string
endpoint()
{
    return detail::env("ZARR_S3_ENDPOINT");
}

inline std::string
bucket()
{
    return detail::env("ZARR_S3_BUCKET_NAME");
}

inline bool
object_exists(const std::string& key)
{
    minio::s3::StatObjectArgs args;
    args.bucket = detail::ctx().bucket_name;
    args.object = key;
    return (bool)detail::ctx().client.StatObject(args);
}

inline size_t
get_object_size(const std::string& key)
{
    minio::s3::StatObjectArgs args;
    args.bucket = detail::ctx().bucket_name;
    args.object = key;
    auto resp = detail::ctx().client.StatObject(args);
    return resp ? resp.size : 0;
}

inline std::string
get_object_contents(const std::string& key)
{
    std::stringstream ss;
    minio::s3::GetObjectArgs args;
    args.bucket = detail::ctx().bucket_name;
    args.object = key;
    args.datafunc = [&ss](minio::http::DataFunctionArgs a) -> bool {
        ss << a.datachunk;
        return true;
    };
    (void)detail::ctx().client.GetObject(args);
    return ss.str();
}

inline std::vector<uint8_t>
get_object_bytes(const std::string& key)
{
    std::vector<uint8_t> out;
    minio::s3::GetObjectArgs args;
    args.bucket = detail::ctx().bucket_name;
    args.object = key;
    args.datafunc = [&out](minio::http::DataFunctionArgs a) -> bool {
        const auto* p = reinterpret_cast<const uint8_t*>(a.datachunk.data());
        out.insert(out.end(), p, p + a.datachunk.size());
        return true;
    };
    (void)detail::ctx().client.GetObject(args);
    return out;
}

inline bool
remove_items(const std::vector<std::string>& keys)
{
    std::list<minio::s3::DeleteObject> objs;
    for (const auto& k : keys) {
        minio::s3::DeleteObject o;
        o.name = k;
        objs.push_back(o);
    }
    minio::s3::RemoveObjectsArgs args;
    args.bucket = detail::ctx().bucket_name;
    auto it = objs.begin();
    args.func = [&objs, &it](minio::s3::DeleteObject& out) -> bool {
        if (it == objs.end())
            return false;
        out = *it++;
        return true;
    };
    auto result = detail::ctx().client.RemoveObjects(args);
    for (; result; result++) {
        auto err = *result;
        if (!err)
            return false;
    }
    return true;
}

inline bool
remove_prefix(const std::string& prefix)
{
    std::vector<std::string> keys;
    minio::s3::ListObjectsArgs args;
    args.bucket = detail::ctx().bucket_name;
    args.prefix = prefix;
    args.recursive = true;
    auto result = detail::ctx().client.ListObjects(args);
    for (; result; result++) {
        auto item = *result;
        if (!item)
            return false;
        keys.push_back(item.name);
    }
    if (keys.empty())
        return true;
    return remove_items(keys);
}

} // namespace s3

#endif // S3_TEST_HELPERS_USE_AWS_CLI

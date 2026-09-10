#include "acquire.zarr.h"
#include "zarr.stream.hh"
#include "unit.test.macros.hh"

#include <cstdlib>
#include <filesystem>
#include <optional>
#include <string>
#include <thread>

namespace fs = std::filesystem;

namespace {
#ifdef _WIN32
void
set_env(const char* name, const char* value)
{
    _putenv_s(name, value);
}

void
unset_env(const char* name)
{
    // An empty value removes the variable from the environment on Windows.
    _putenv_s(name, "");
}
#else
void
set_env(const char* name, const char* value)
{
    setenv(name, value, 1);
}

void
unset_env(const char* name)
{
    unsetenv(name);
}
#endif

class ScopedEnvVar
{
  public:
    ScopedEnvVar(const char* name, const char* value)
      : name_{ name }
    {
        if (const char* existing = std::getenv(name)) {
            previous_value_ = existing;
        }

        if (value == nullptr) {
            unset_env(name);
        } else {
            set_env(name, value);
        }
    }

    ~ScopedEnvVar()
    {
        if (previous_value_) {
            set_env(name_.c_str(), previous_value_->c_str());
        } else {
            unset_env(name_.c_str());
        }
    }

  private:
    std::string name_;
    std::optional<std::string> previous_value_;
};
} // namespace

void
configure_stream_dimensions(ZarrArraySettings* settings)
{
    CHECK(ZarrStatusCode_Success ==
          ZarrArraySettings_create_dimension_array(settings, 3));
    ZarrDimensionProperties* dim = settings->dimensions;

    *dim = ZarrDimensionProperties{
        .name = "t",
        .type = ZarrDimensionType_Time,
        .array_size_px = 100,
        .chunk_size_px = 10,
        .shard_size_chunks = 1,
    };

    dim = settings->dimensions + 1;
    *dim = ZarrDimensionProperties{
        .name = "y",
        .type = ZarrDimensionType_Space,
        .array_size_px = 200,
        .chunk_size_px = 20,
        .shard_size_chunks = 1,
    };

    dim = settings->dimensions + 2;
    *dim = ZarrDimensionProperties{
        .name = "x",
        .type = ZarrDimensionType_Space,
        .array_size_px = 300,
        .chunk_size_px = 30,
        .shard_size_chunks = 1,
    };
}

int
main()
{
    int retval = 1;

    ZarrStream* stream = nullptr;
    ZarrStreamSettings settings = {};
    settings.max_threads = std::thread::hardware_concurrency();

    try {
        // try to create a stream with no store path
        stream = ZarrStream_create(&settings);
        CHECK(nullptr == stream);

        // try to create a stream with no dimensions
        settings.store_path = static_cast<const char*>(TEST ".zarr");
        stream = ZarrStream_create(&settings);
        CHECK(nullptr == stream);
        CHECK(!fs::exists(settings.store_path));

        // allocate dimensions
        CHECK(ZarrStatusCode_Success ==
              ZarrStreamSettings_create_arrays(&settings, 1));
        configure_stream_dimensions(settings.arrays);
        stream = ZarrStream_create(&settings);
        CHECK(nullptr != stream);
        CHECK(fs::is_directory(settings.store_path));

        // ZARR_MAX_THREADS is picked up when max_threads is not explicitly
        // set
        {
            ScopedEnvVar env("ZARR_MAX_THREADS", "2");

            ZarrStreamSettings env_settings = {};
            env_settings.store_path =
              static_cast<const char*>(TEST "-env-max-threads.zarr");
            env_settings.max_threads = 0;
            CHECK(ZarrStatusCode_Success ==
                  ZarrStreamSettings_create_arrays(&env_settings, 1));
            configure_stream_dimensions(env_settings.arrays);

            ZarrStream* env_stream = ZarrStream_create(&env_settings);
            CHECK(nullptr != env_stream);

            const uint32_t expected_threads =
              std::thread::hardware_concurrency() <= 1 ? 1 : 2;
            EXPECT_EQ(uint32_t, stream_thread_count(env_stream),
                      expected_threads);

            ZarrStream_destroy(env_stream);
            ZarrStreamSettings_destroy_arrays(&env_settings);

            std::error_code ec;
            fs::remove_all(env_settings.store_path, ec);
        }

        retval = 0;
    } catch (const std::exception& exception) {
        LOG_ERROR(exception.what());
    }

    // cleanup
    ZarrStream_destroy(stream);

    if (std::error_code ec; fs::is_directory(settings.store_path) &&
                            !fs::remove_all(settings.store_path, ec)) {
        LOG_ERROR("Failed to remove store path: ", ec.message().c_str());
    }

    ZarrStreamSettings_destroy_arrays(&settings);
    return retval;
}
#include "zarr.common.hh"
#include "unit.test.macros.hh"

#include <cstdlib>
#include <optional>
#include <string>

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

int
main()
{
    int retval = 1;

    try {
        // explicit non-zero value wins, even when the env var is also set
        {
            ScopedEnvVar env("ZARR_MAX_THREADS", "2");
            EXPECT_EQ(uint32_t, zarr::resolve_max_threads(5), 5);
        }

        // explicit non-zero value, env var unset -> explicit value used
        {
            ScopedEnvVar env("ZARR_MAX_THREADS", nullptr);
            EXPECT_EQ(uint32_t, zarr::resolve_max_threads(5), 5);
        }

        // env var unset, no explicit value -> defer to caller (0)
        {
            ScopedEnvVar env("ZARR_MAX_THREADS", nullptr);
            EXPECT_EQ(uint32_t, zarr::resolve_max_threads(0), 0);
        }

        // env var set to a valid positive integer, no explicit value
        {
            ScopedEnvVar env("ZARR_MAX_THREADS", "4");
            EXPECT_EQ(uint32_t, zarr::resolve_max_threads(0), 4);
        }

        // env var set to a non-numeric value -> defer to caller (0)
        {
            ScopedEnvVar env("ZARR_MAX_THREADS", "abc");
            EXPECT_EQ(uint32_t, zarr::resolve_max_threads(0), 0);
        }

        // env var set to an empty string -> defer to caller (0)
        {
            ScopedEnvVar env("ZARR_MAX_THREADS", "");
            EXPECT_EQ(uint32_t, zarr::resolve_max_threads(0), 0);
        }

        // env var set to a value with trailing garbage -> defer to caller (0)
        {
            ScopedEnvVar env("ZARR_MAX_THREADS", "4x");
            EXPECT_EQ(uint32_t, zarr::resolve_max_threads(0), 0);
        }

        // env var set to zero -> defer to caller (0)
        {
            ScopedEnvVar env("ZARR_MAX_THREADS", "0");
            EXPECT_EQ(uint32_t, zarr::resolve_max_threads(0), 0);
        }

        // env var set to a negative value -> defer to caller (0)
        {
            ScopedEnvVar env("ZARR_MAX_THREADS", "-1");
            EXPECT_EQ(uint32_t, zarr::resolve_max_threads(0), 0);
        }

        // env var set to a value that overflows uint32_t -> defer to caller (0)
        {
            ScopedEnvVar env("ZARR_MAX_THREADS", "99999999999");
            EXPECT_EQ(uint32_t, zarr::resolve_max_threads(0), 0);
        }

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Exception: ", e.what());
    }

    return retval;
}

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
    // An empty value removes the variable on Windows.
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

void
expect_tile_copy_threads(const char* value, uint32_t expected)
{
    ScopedEnvVar env("ZARR_TILE_COPY_THREADS", value);
    EXPECT_EQ(uint32_t, zarr::resolve_tile_copy_threads(), expected);
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        // unset / empty -> 0 (use the OpenMP default team size)
        expect_tile_copy_threads(nullptr, 0);
        expect_tile_copy_threads("", 0);

        // valid positive integers pass through
        expect_tile_copy_threads("1", 1);
        expect_tile_copy_threads("8", 8);
        expect_tile_copy_threads("128", 128);

        // zero and negatives are invalid -> 0
        expect_tile_copy_threads("0", 0);
        expect_tile_copy_threads("-1", 0);

        // non-numeric / trailing / leading junk is invalid -> 0
        expect_tile_copy_threads("abc", 0);
        expect_tile_copy_threads("8x", 0);
        expect_tile_copy_threads(" 8", 0);
        expect_tile_copy_threads("8 ", 0);

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Exception: ", e.what());
    }

    return retval;
}

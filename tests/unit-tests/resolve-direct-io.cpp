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

void
expect_direct_io(const char* value, bool expected)
{
    ScopedEnvVar env("ZARR_DIRECT_IO", value);
    EXPECT_EQ(bool, zarr::resolve_direct_io(), expected);
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        // unset -> disabled
        expect_direct_io(nullptr, false);

        // empty -> disabled
        expect_direct_io("", false);

        // recognized true spellings
        expect_direct_io("1", true);
        expect_direct_io("true", true);
        expect_direct_io("on", true);
        expect_direct_io("yes", true);

        // recognized false spellings
        expect_direct_io("0", false);
        expect_direct_io("false", false);
        expect_direct_io("off", false);
        expect_direct_io("no", false);

        // matching is case-insensitive
        expect_direct_io("TRUE", true);
        expect_direct_io("On", true);
        expect_direct_io("YeS", true);
        expect_direct_io("FALSE", false);
        expect_direct_io("Off", false);

        // unrecognized values fail closed
        expect_direct_io("maybe", false);
        expect_direct_io("2", false);
        expect_direct_io("1x", false);
        expect_direct_io(" 1", false);

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Exception: ", e.what());
    }

    return retval;
}

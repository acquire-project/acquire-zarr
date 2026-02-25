#pragma once

#include "logger.hh"
#include "s3.connection.hh"

#define EXPECT(e, ...)                                                         \
    do {                                                                       \
        if (!(e)) {                                                            \
            const std::string __err = LOG_ERROR(__VA_ARGS__);                  \
            throw std::runtime_error(__err);                                   \
        }                                                                      \
    } while (0)
#define CHECK(e) EXPECT(e, "Expression evaluated as false:\n\t", #e)

/// Check that a==b
/// example: `ASSERT_EQ(int,42,meaning_of_life())`
#define EXPECT_EQ(T, a, b)                                                     \
    do {                                                                       \
        T a_ = (T)(a);                                                         \
        T b_ = (T)(b);                                                         \
        EXPECT(                                                                \
          a_ == b_, "Expected ", #a, " == ", #b, " but ", a_, " != ", b_);     \
    } while (0)

#define EXPECT_STR_EQ(a, b)                                                    \
    do {                                                                       \
        std::string a_ = (a) ? (a) : "";                                       \
        std::string b_ = (b) ? (b) : "";                                       \
        EXPECT(a_ == b_,                                                       \
               "Expected ",                                                    \
               #a,                                                             \
               " == ",                                                         \
               #b,                                                             \
               " but ",                                                        \
               a_,                                                             \
               " != ",                                                         \
               b_,                                                             \
               #a,                                                             \
               #b,                                                             \
               a_,                                                             \
               b_);                                                            \
    } while (0)

namespace testing {
bool
get_s3_settings(zarr::S3Settings& settings);
} // namespace testing
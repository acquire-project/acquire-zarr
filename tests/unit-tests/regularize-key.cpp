#include "zarr.common.hh"
#include "unit.test.macros.hh"

#include <regex>
#include <string>
#include <string_view>
#include <vector>

namespace {
// the regex implementation that zarr::regularize_key replaced
std::string
regularize_key_regex(std::string_view key)
{
    std::string out{ key };
    out = std::regex_replace(out, std::regex(R"(^(\s|\/)+|(\s|\/)+$)"), "");
    out = std::regex_replace(out, std::regex(R"(\/+)"), "/");
    return out;
}

void
expect_regularized(std::string_view key, std::string_view expected)
{
    const auto actual = zarr::regularize_key(key);
    EXPECT(actual == expected,
           "regularize_key('",
           key,
           "') returned '",
           actual,
           "', expected '",
           expected,
           "'");
}

void
expect_matches_regex(const std::string& key)
{
    const auto actual = zarr::regularize_key(key);
    const auto expected = regularize_key_regex(key);
    EXPECT(actual == expected,
           "regularize_key('",
           key,
           "') returned '",
           actual,
           "', regex version returned '",
           expected,
           "'");
}
} // namespace

int
main()
{
    int retval = 1;

    try {
        EXPECT(zarr::regularize_key(static_cast<const char*>(nullptr)).empty(),
               "null key should regularize to an empty string");

        expect_regularized("", "");
        expect_regularized("/", "");
        expect_regularized("///", "");
        expect_regularized(" /\t\n/ ", "");
        expect_regularized("a", "a");
        expect_regularized("/a/", "a");
        expect_regularized("a//b", "a/b");
        expect_regularized("//a///b//", "a/b");
        expect_regularized("\t/a//b/\n", "a/b");
        expect_regularized(" a / b ", "a / b");
        expect_regularized("a/ /b", "a/ /b");
        expect_regularized("pos/0", "pos/0");
        expect_regularized("A1-site-0001/0", "A1-site-0001/0");
        expect_regularized(std::string_view("a\0b", 3),
                           std::string_view("a\0b", 3));

        // interior line feeds are pinned here rather than compared with the
        // regex below: before microsoft/STL#5535, MSVC's std::regex let `^`
        // and `$` match at line feeds, so the reference disagrees there
        expect_regularized("\na\n", "a");
        expect_regularized("a/\nb", "a/\nb");
        expect_regularized("a\n/b", "a\n/b");
        expect_regularized("a\n\nb", "a\n\nb");

        // every string up to length 7 over an alphabet of key characters,
        // slashes and whitespace
        const std::string alphabet = { 'a', '/', ' ', '\t' };
        std::vector<std::string> frontier{ "" };
        for (int len = 0; len <= 7; ++len) {
            std::vector<std::string> next;
            for (const auto& key : frontier) {
                expect_matches_regex(key);
                if (len < 7) {
                    for (char c : alphabet) {
                        next.push_back(key + c);
                    }
                }
            }
            frontier.swap(next);
        }

        // every byte value, alone and next to key characters and slashes
        for (int b = 0; b < 256; ++b) {
            const std::string c(1, static_cast<char>(b));
            for (const auto& key :
                 { c, c + "a", "a" + c, "a" + c + "b", "/" + c + "/a/" + c }) {
                expect_matches_regex(key);
            }
        }

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Exception: ", e.what());
    }

    return retval;
}

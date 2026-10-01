#include "parallel.for.hh"
#include "unit.test.macros.hh"

#include <numeric>
#include <stdexcept>
#include <vector>

namespace {

// Each index must be visited exactly once, and the per-block sums must add up to
// the serial reduction -- across both the inline-shortcut path (small n) and the
// fanned-out path (large n).
void
test_covers_range_once(int n)
{
    std::vector<int> visits(n > 0 ? n : 0, 0);

    const size_t total =
      zarr::parallel_for_reduce(n, [&](int begin, int end) -> size_t {
          size_t local = 0;
          for (int i = begin; i < end; ++i) {
              visits[i] += 1; // disjoint blocks: no data race
              local += static_cast<size_t>(i);
          }
          return local;
      });

    const size_t expected =
      static_cast<size_t>(n) * static_cast<size_t>(n - 1) / 2;
    EXPECT_EQ(size_t, total, expected);

    for (int i = 0; i < n; ++i) {
        EXPECT(visits[i] == 1,
               "index ",
               i,
               " visited ",
               visits[i],
               " times (expected 1)");
    }
}

// An exception thrown inside a block must surface on the calling thread.
void
test_exception_propagates()
{
    bool threw = false;
    try {
        // large n so the fan-out path runs
        zarr::parallel_for_reduce(100000, [](int begin, int end) -> size_t {
            for (int i = begin; i < end; ++i) {
                if (i == 54321) {
                    throw std::runtime_error("boom");
                }
            }
            return 0;
        });
    } catch (const std::runtime_error&) {
        threw = true;
    }
    EXPECT(threw, "expected the block's exception to propagate to the caller");
}

} // namespace

int
main()
{
    int retval = 1;

    try {
        test_covers_range_once(0);     // empty
        test_covers_range_once(1);     // single
        test_covers_range_once(10);    // below threshold -> inline shortcut
        test_covers_range_once(63);    // just below threshold
        test_covers_range_once(64);    // at threshold
        test_covers_range_once(65);    // just above
        test_covers_range_once(1024);  // fanned out
        test_covers_range_once(100003); // fanned out, not divisible by team

        test_exception_propagates();

        CHECK(zarr::tile_copy_team_size() >= 1);

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Exception: ", e.what());
    }

    return retval;
}

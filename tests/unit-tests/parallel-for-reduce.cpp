#include "parallel.for.hh"
#include "unit.test.macros.hh"

#include <numeric>
#include <stdexcept>
#include <string>
#include <thread>
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

// Several streams in one process share the team. Concurrent callers must each
// get their own sum, and none may hang.
void
test_concurrent_callers()
{
    constexpr int n_callers = 4;
    constexpr int n_rounds = 2000;
    constexpr int n = 4096;

    std::vector<std::string> failures(n_callers);
    std::vector<std::thread> callers;
    for (int c = 0; c < n_callers; ++c) {
        callers.emplace_back([c, &failures] {
            const size_t offset = static_cast<size_t>(c) * 1000003;
            const size_t expected =
              static_cast<size_t>(n) * (n - 1) / 2 + n * offset;
            for (int r = 0; r < n_rounds && failures[c].empty(); ++r) {
                const size_t total =
                  zarr::parallel_for_reduce(n, [offset](int begin, int end) {
                      size_t local = 0;
                      for (int i = begin; i < end; ++i) {
                          local += static_cast<size_t>(i) + offset;
                      }
                      return local;
                  });
                if (total != expected) {
                    failures[c] = "caller " + std::to_string(c) + " round " +
                                  std::to_string(r) + ": got " +
                                  std::to_string(total) + ", expected " +
                                  std::to_string(expected);
                }
            }
        });
    }
    for (auto& t : callers) {
        t.join();
    }
    for (const auto& f : failures) {
        EXPECT(f.empty(), f);
    }
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
        test_concurrent_callers();

        CHECK(zarr::tile_copy_team_size() >= 1);

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Exception: ", e.what());
    }

    return retval;
}

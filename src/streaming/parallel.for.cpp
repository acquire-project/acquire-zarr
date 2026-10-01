#include "parallel.for.hh"

#include <algorithm>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <thread>
#include <vector>

namespace {

// Worker-team size for the tile scatter. Measured per-frame copy latency
// saturates by ~4 threads (and a hardware-width team regresses throughput at
// high core counts via oversubscription), so cap it here rather than per-core.
constexpr int kTileCopyTeamSize = 4;

// Run the scatter inline (no fork/join) below this many iterations: the team
// wake-up cost is not worth it for a small frame.
constexpr int kMinTilesForParallel = 64;

// A persistent fork-join team. Workers block on a condition variable between
// rounds; a round splits [0, n) into kTileCopyTeamSize contiguous blocks, one
// per worker, and sums the per-block byte counts. Created lazily on first real
// (non-shortcut) use and torn down at process exit.
class TileCopyPool
{
  public:
    static TileCopyPool& instance()
    {
        static TileCopyPool pool;
        return pool;
    }

    int team_size() const { return team_; }

    // Execute block over [0, n) across the team and return the summed result.
    // Rethrows the first exception any worker captured.
    size_t run(int n, const std::function<size_t(int, int)>& block)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        n_ = n;
        block_ = &block;
        std::fill(partials_.begin(), partials_.end(), size_t{ 0 });
        std::fill(errors_.begin(), errors_.end(), std::exception_ptr{});
        pending_ = team_;
        ++generation_;
        work_cv_.notify_all();
        done_cv_.wait(lock, [this] { return pending_ == 0; });
        block_ = nullptr;

        for (auto& err : errors_) {
            if (err) {
                std::rethrow_exception(err);
            }
        }
        size_t total = 0;
        for (auto p : partials_) {
            total += p;
        }
        return total;
    }

  private:
    TileCopyPool()
    {
        const unsigned hw = std::thread::hardware_concurrency();
        team_ = std::max(1, std::min(kTileCopyTeamSize, static_cast<int>(hw)));
        partials_.resize(team_, 0);
        errors_.resize(team_);
        threads_.reserve(team_);
        for (int w = 0; w < team_; ++w) {
            threads_.emplace_back([this, w] { worker_loop(w); });
        }
    }

    ~TileCopyPool()
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            stop_ = true;
            work_cv_.notify_all();
        }
        for (auto& t : threads_) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    void worker_loop(int w)
    {
        uint64_t seen = 0;
        std::unique_lock<std::mutex> lock(mutex_);
        for (;;) {
            work_cv_.wait(lock,
                          [this, seen] { return stop_ || generation_ != seen; });
            if (stop_) {
                return;
            }
            seen = generation_;
            const int n = n_;
            const auto* block = block_;
            lock.unlock();

            size_t local = 0;
            std::exception_ptr err;
            try {
                const int per = (n + team_ - 1) / team_;
                const int begin = std::min(n, w * per);
                const int end = std::min(n, begin + per);
                if (begin < end) {
                    local = (*block)(begin, end);
                }
            } catch (...) {
                err = std::current_exception();
            }

            lock.lock();
            partials_[w] = local;
            errors_[w] = err;
            if (--pending_ == 0) {
                done_cv_.notify_one();
            }
        }
    }

    int team_ = 1;
    std::vector<std::thread> threads_;
    std::mutex mutex_;
    std::condition_variable work_cv_; // workers wait for the next round
    std::condition_variable done_cv_; // caller waits for the round to finish
    uint64_t generation_ = 0;
    int pending_ = 0;
    bool stop_ = false;

    int n_ = 0;
    const std::function<size_t(int, int)>* block_ = nullptr;
    std::vector<size_t> partials_;
    std::vector<std::exception_ptr> errors_;
};

} // namespace

namespace zarr {

size_t
parallel_for_reduce(int n, const std::function<size_t(int, int)>& block)
{
#ifdef ACQUIRE_ZARR_SERIAL_TILE_COPY
    // Benchmark/A-B build: always serial, no team.
    return n > 0 ? block(0, n) : 0;
#else
    if (n <= kMinTilesForParallel) {
        return n > 0 ? block(0, n) : 0; // small-workload shortcut
    }
    auto& pool = TileCopyPool::instance();
    if (pool.team_size() <= 1) {
        return block(0, n);
    }
    return pool.run(n, block);
#endif
}

int
tile_copy_team_size()
{
#ifdef ACQUIRE_ZARR_SERIAL_TILE_COPY
    return 1;
#else
    return TileCopyPool::instance().team_size();
#endif
}

} // namespace zarr

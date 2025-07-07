#include "thread.pool.hh"

#include <algorithm>

zarr::ThreadPool::ThreadPool(unsigned int n_threads, ErrorCallback&& err)
  : error_handler_{ std::move(err) }
{
    // hardware_concurrency() can return 0 if not computable
    const auto max_threads = std::max(std::thread::hardware_concurrency(), 1u);

    // On multi-core systems, enforce minimum 2 threads: user requested + frame queue thread
    n_threads = max_threads == 1 ? 1 : std::clamp(n_threads, 2u, max_threads);

    for (auto i = 0; i < n_threads; ++i) {
        threads_.emplace_back([this] { process_tasks_(); });
    }
}

zarr::ThreadPool::~ThreadPool() noexcept
{
    {
        std::unique_lock lock(jobs_mutex_);
        while (!jobs_.empty()) {
            jobs_.pop();
        }
    }

    await_stop();
}

bool
zarr::ThreadPool::push_job(Task&& job)
{
    std::unique_lock lock(jobs_mutex_);
    if (!accepting_jobs) {
        return false;
    }

    jobs_.push(std::move(job));
    jobs_cv_.notify_one();

    return true;
}

void
zarr::ThreadPool::await_stop() noexcept
{
    {
        std::scoped_lock lock(jobs_mutex_);
        accepting_jobs = false;

        jobs_cv_.notify_all();
    }

    // spin down threads
    for (auto& thread : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

std::optional<zarr::ThreadPool::Task>
zarr::ThreadPool::pop_from_job_queue_() noexcept
{
    if (jobs_.empty()) {
        return std::nullopt;
    }

    auto job = std::move(jobs_.front());
    jobs_.pop();
    return job;
}

bool
zarr::ThreadPool::should_stop_() const noexcept
{
    return !accepting_jobs && jobs_.empty();
}

void
zarr::ThreadPool::process_tasks_()
{
    while (true) {
        std::unique_lock lock(jobs_mutex_);
        jobs_cv_.wait(lock, [&] { return should_stop_() || !jobs_.empty(); });

        if (should_stop_()) {
            break;
        }

        if (auto job = pop_from_job_queue_(); job.has_value()) {
            lock.unlock();
            if (std::string err_msg; !job.value()(err_msg)) {
                error_handler_(err_msg);
            }
        }
    }
}

uint32_t
zarr::ThreadPool::n_threads() const
{
    return threads_.size();
}
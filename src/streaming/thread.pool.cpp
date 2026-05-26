#include "logger.hh"
#include "thread.pool.hh"

#include <algorithm>

zarr::ThreadPool::ThreadPool(unsigned int n_threads, ErrorCallback&& err)
  : error_handler_{ std::move(err) }
  , main_thread_id_(std::this_thread::get_id())
{
    // hardware_concurrency() can return 0 if not computable
    const auto max_threads = std::max(std::thread::hardware_concurrency(), 1u);

    // On multi-core systems, enforce minimum 2 threads: user requested + frame
    // queue thread
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
zarr::ThreadPool::push_job(Task&& job, std::optional<uint32_t> max_retries)
{
    std::unique_lock lock(jobs_mutex_);
    if (!accepting_jobs) {
        return false;
    }

    TaskWrapper wrapper{
        .task = std::move(job),
        .max_retries = max_retries,
        .attempt = 0,
    };

    push_to_queue_(std::move(wrapper));
    return true;
}

bool
zarr::ThreadPool::execute_job(Task&& job)
{
    if (std::string err_msg; job(err_msg) != TaskResult::Success) {
        LOG_ERROR(err_msg);
        return false;
    }

    return true;
}

bool
zarr::ThreadPool::execute_job_with_retry(Task&& job, uint32_t max_retries)
{
    std::string err_msg;

    for (uint32_t retry = 0; retry < max_retries; ++retry) {
        switch (job(err_msg)) {
            case TaskResult::Success:
                return true;
            case TaskResult::Retry:
                continue;
            case TaskResult::Fatal:
                LOG_ERROR("Error while executing job: ", err_msg);
                return false;
        }
    }

    LOG_ERROR("Failed to execute job after ",
              max_retries,
              " retries",
              err_msg.empty() ? err_msg : ": " + err_msg);
    return false;
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

uint32_t
zarr::ThreadPool::n_threads() const
{
    return threads_.size();
}

void
zarr::ThreadPool::push_to_queue_(TaskWrapper&& job)
{
    jobs_.push(std::move(job));
    jobs_cv_.notify_one();
}

std::optional<zarr::ThreadPool::TaskWrapper>
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

            switch (std::string err_msg; job->task(err_msg)) {
                case TaskResult::Success:
                    break;
                case TaskResult::Retry:
                    if (const auto max_retries =
                          job->max_retries ? *job->max_retries : 1;
                        job->attempt < max_retries) {
                        ++job->attempt;
                        lock.lock();
                        push_to_queue_(std::move(*job)); // requeue
                        break;
                    } else {
                        err_msg = "Max retries (" +
                                  std::to_string(max_retries) + ") exceeded";
                        [[fallthrough]];
                    }
                case TaskResult::Fatal:
                    lock.lock();
                    error_messages_.push_back(err_msg);
                    accepting_jobs = false; // drain and stop
                    lock.unlock();
                    jobs_cv_.notify_all();
                    error_handler_(err_msg);
                    break;
            }
        }
    }
}
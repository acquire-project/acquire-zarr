#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>

namespace zarr {
class ThreadPool
{
  public:
    enum class TaskResult
    {
        Success,
        Retry,   // requeue the job
        Fatal,   // log + call error_handler_, stop pool
    };

    using Task = std::function<TaskResult(std::string&)>;
    using ErrorCallback = std::function<void(const std::string&)>;

    // The error handler `err` is called when a job returns false. This
    // can happen when the job encounters an error, or otherwise fails. The
    // std::string& argument to the error handler is a diagnostic message from
    // the failing job and is logged to the error stream by the Zarr driver when
    // the next call to `append()` is made.
    ThreadPool(unsigned int n_threads, ErrorCallback&& err);
    ~ThreadPool() noexcept;

    /**
     * @brief Push a job onto the job queue.
     *
     * @param job The job to push onto the queue.
     * @param max_retries
     * @return true if the job was successfully pushed onto the queue, false
     * otherwise.
     */
    [[nodiscard]] bool push_job(
      Task&& job,
      std::optional<uint32_t> max_retries = std::nullopt);

    // TODO (aliddell: docstring)
    [[nodiscard]] static bool execute_job(Task&& job);

    // TODO (aliddell: docstring)
    [[nodiscard]] static bool execute_job_with_retry(Task&& job,
                                                     uint32_t max_retries = 3);

    /**
     * @brief Block until all jobs on the queue have processed, then spin down
     * the threads.
     * @note After calling this function, the job queue no longer accepts jobs.
     */
    void await_stop() noexcept;

    /**
     * @brief Get the number of threads running.
     * @return The number of threads running.
     */
    uint32_t n_threads() const;

  private:
    struct TaskWrapper
    {
        Task task;
        std::optional<uint32_t> max_retries;
        uint32_t attempt;
    };

    ErrorCallback error_handler_;

    std::thread::id main_thread_id_;

    std::vector<std::thread> threads_;

    std::atomic<bool> accepting_jobs{ true };
    std::mutex jobs_mutex_;
    std::condition_variable jobs_cv_;
    std::queue<TaskWrapper> jobs_;

    std::vector<std::string> error_messages_;

    void push_to_queue_(TaskWrapper&& job);
    std::optional<TaskWrapper> pop_from_job_queue_() noexcept;
    [[nodiscard]] bool should_stop_() const noexcept;
    void process_tasks_();
};
} // zarr

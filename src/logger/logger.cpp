#include "logger.hh"

#include <iomanip>
#include <string>
#include <thread>

LogLevel Logger::current_level_ = LogLevel_Info;
std::mutex Logger::log_mutex_{};

void
Logger::set_log_level(LogLevel level)
{
    if (level < LogLevel_Debug || level > LogLevel_None) {
        throw std::invalid_argument("Invalid log level");
    }

    current_level_ = level;
}

LogLevel
Logger::get_log_level()
{
    return current_level_;
}

std::string
Logger::get_timestamp_()
{

    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) %
              1000;

    std::tm tm{};
#if defined(_WIN32)
    localtime_s(&tm, &time);
#else
    localtime_r(&time, &tm);
#endif

    std::ostringstream ss;
    ss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << '.' << std::setfill('0')
       << std::setw(3) << ms.count();

    return ss.str();
}
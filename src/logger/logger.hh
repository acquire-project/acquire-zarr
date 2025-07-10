#include "logger.types.h"

#include <filesystem>
#include <iostream>
#include <mutex>
#include <sstream>

class Logger
{
  public:
    static void set_log_level(LogLevel level);
    static LogLevel get_log_level();

    template<typename... Args>
    static std::string log(LogLevel level,
                           const char* file,
                           int line,
                           const char* func,
                           Args&&... args)
    {
        namespace fs = std::filesystem;

        if (level < current_level_) {
            return "";
        }

        std::scoped_lock lock(log_mutex_);

        std::string prefix;
        auto stream = &std::cout;

        switch (level) {
            case LogLevel_Debug:
                prefix = "[DEBUG] ";
                break;
            case LogLevel_Info:
                prefix = "[INFO] ";
                break;
            case LogLevel_Warning:
                prefix = "[WARNING] ";
                break;
            default:
                prefix = "[ERROR] ";
                stream = &std::cerr;
                break;
        }

        fs::path filepath(file);
        std::string filename = filepath.filename().string();

        std::ostringstream ss;
        ss << get_timestamp_() << " " << prefix << filename << ":" << line
           << " " << func << ": ";

        format_arg_(ss, std::forward<Args>(args)...);

        std::string message = ss.str();
        *stream << message << std::endl;

        return message;
    }

  private:
    static LogLevel current_level_;
    static std::mutex log_mutex_;

    static void format_arg_(std::ostream& ss) {}; // base case
    template<typename T, typename... Args>
    static void format_arg_(std::ostream& ss, T&& arg, Args&&... args) {
        ss << std::forward<T>(arg);
        format_arg_(ss, std::forward<Args>(args)...);
    }

    static std::string get_timestamp_();
};

#define LOG_DEBUG(...)                                                         \
    Logger::log(LogLevel_Debug, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define LOG_INFO(...)                                                          \
    Logger::log(LogLevel_Info, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define LOG_WARNING(...)                                                       \
    Logger::log(LogLevel_Warning, __FILE__, __LINE__, __func__, __VA_ARGS__)
#define LOG_ERROR(...)                                                         \
    Logger::log(LogLevel_Error, __FILE__, __LINE__, __func__, __VA_ARGS__)

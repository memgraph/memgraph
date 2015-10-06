#ifndef MEMGRAPH_DEBUG_LOG_HPP
#define MEMGRAPH_DEBUG_LOG_HPP

#include <sstream>
#include <iostream>
#include <thread>
#include <atomic>
#include <array>

#include "data_structures/queue/mpsc_queue.hpp"
#include "utils/bash_colors.hpp"

class Log
{
public:
    enum class Level : std::uint_fast8_t { Debug, Info, Warn, Error };

private:
    static std::array<std::string, 4> level_strings;

    static std::string to_string(Log::Level level)
    {
        return level_strings[static_cast<std::size_t>(level)];
    }

    Log() : alive(true), worker([this]() { work(); }) {}

    ~Log()
    {
        alive.store(false, std::memory_order_seq_cst);
        worker.join();
    }

    static Log& instance()
    {
        static Log log;
        return log;
    }

public:
    Log(Log&) = delete;
    Log(Log&&) = delete;

    static void log(Level level,
                    std::string text,
                    std::string file,
                    std::string function,
                    size_t line)
    {
        using namespace std::chrono;

        auto time_point = system_clock::now();
        auto t = system_clock::to_time_t(time_point);
        auto time_string = std::string(std::ctime(&t));

        std::stringstream ss;
    
        ss << bash_color::green << "[" << to_string(level) << "] "
           << bash_color::end << text << std::endl
           << bash_color::yellow << "    on " << bash_color::end
           << time_string.substr(0, time_string.size() - 1)
           << bash_color::yellow << " in file " << bash_color::end
           << file
           << bash_color::yellow << " in function " << bash_color::end
           << function
           << bash_color::yellow << " at line " << bash_color::end
           << line;

        auto& log = Log::instance();
        log.messages.push(std::make_unique<std::string>(ss.str()));
    }

private:
    lockfree::MpscQueue<std::string> messages;
    std::atomic<bool> alive;
    std::thread worker;

    void work()
    {
        using namespace std::chrono_literals;

        while(true)
        {
            auto message = messages.pop();

            if(message != nullptr)
            {
                std::cerr << *message << std::endl;
                continue;
            }

            if(!alive)
                return;

            std::this_thread::sleep_for(10ms);
        }
    }
};

std::array<std::string, 4> Log::level_strings {{
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR"
}};

#define LOG(_LEVEL_, _MESSAGE_)                 \
  Log::log(                                     \
    _LEVEL_,                                    \
    static_cast<std::ostringstream&>(           \
      std::ostringstream().flush() << _MESSAGE_ \
    ).str(),                                    \
    __FILE__,                                   \
    __PRETTY_FUNCTION__,                        \
    __LINE__                                    \
  );

#ifdef NDEBUG
#  define LOG_DEBUG(_) do {} while(0);
#else
#  define LOG_DEBUG(_MESSAGE_) LOG(Log::Level::Debug, _MESSAGE_)
#endif

#endif

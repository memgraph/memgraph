#pragma once

#include <string>
#include <iostream>
#include <ctime>
#include <iomanip>

namespace logger
{

class Logger
{
public:
    Logger(Logger& other) = delete;
    Logger(Logger&& other) = delete;

private:
    Logger() = default;

    //  TODO logger name support

    //  TODO level support

    //  TODO handlers support:
    //      * log format support

    //  TODO merge with debug/log.hpp
   
public: 
    static Logger& instance()
    {
        static Logger logger;
        return logger;
    }

    void info(const std::string& text)
    {
        stdout_log(text);
    }

private:
    void stdout_log(const std::string& text)
    {
        auto now = std::time(nullptr);
        std::cout << std::put_time(std::gmtime(&now), "[%F %T]: ")
                  << text << std::endl; 
    }
};

}

#ifdef NOT_LOG_INFO
#   define LOG_INFO(_)
#else
#   define LOG_INFO(_MESSAGE_) logger::Logger::instance().info(_MESSAGE_);
#endif



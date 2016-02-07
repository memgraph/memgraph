#pragma once

#include <string>
#include <iostream>
#include <ctime>
#include <iomanip>

class Logger
{
public:
    Logger() = default;

    //  TODO logger name support

    //  TODO level support

    //  TODO handlers support:
    //      * log format support
    
    void info(const std::string& text)
    {
        stdout_log(text);
    }

    void debug(const std::string& text)
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

#pragma once

#include "logging/default.hpp"

class Loggable
{
public:
    Loggable(std::string &&name)
        : logger(logging::log->logger(std::forward<std::string>(name)))
    {
    }

    virtual ~Loggable() {}

protected:
    Logger logger;
};

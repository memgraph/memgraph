#pragma once

#include "logging/default.hpp"

class Loggable
{
public:
    Loggable(const std::string& name) : logger(logging::log->logger(name)) {}

    virtual ~Loggable() {}

protected:
    Logger logger;
};

#include <iostream>

#include "logging/log.hpp"
#include "logging/logger.hpp"

Logger Log::logger(const std::string& name)
{
    return Logger(this, name);
}

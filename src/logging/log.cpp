#include <iostream>

#include "log.hpp"
#include "logger.hpp"

Logger Log::logger(const std::string& name)
{
    return Logger(*this, name);
}

#pragma once

#include "log.hpp"
#include "logger.hpp"

namespace logging
{

extern std::unique_ptr<Log> log;

extern Logger debug_logger;

template <class... Args>
void debug(Args&&... args)
{
    debug_logger.debug(std::forward<Args>(args)...);
}

void init_async();
void init_sync();

}

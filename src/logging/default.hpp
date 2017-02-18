#pragma once

#include "logging/log.hpp"
#include "logging/logger.hpp"

namespace logging {

extern std::unique_ptr<Log> log;

extern Logger debug_logger;

template <class... Args>
void debug(Args&&... args) {
  debug_logger.debug(std::forward<Args>(args)...);
}

extern Logger info_logger;

template <class... Args>
void info(Args&&... args) {
  info_logger.info(std::forward<Args>(args)...);
}

void init_async();
void init_sync();
}

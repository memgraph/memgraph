#include "logging/default.hpp"

#include "logging/logs/async_log.hpp"
#include "logging/logs/sync_log.hpp"

#include "logging/streams/stdout.hpp"

namespace logging
{

std::unique_ptr<Log> log;

std::unique_ptr<Log> debug_log = std::make_unique<SyncLog>();

Logger init_debug_logger()
{
    debug_log->pipe(std::make_unique<Stdout>());
    return debug_log->logger("DEBUG");
}

Logger debug_logger = init_debug_logger();

std::unique_ptr<Log> info_log = std::make_unique<SyncLog>();

Logger init_info_logger()
{
    info_log->pipe(std::make_unique<Stdout>());
    return info_log->logger("INFO");
}

Logger info_logger = init_info_logger();

void init_async()
{
    log = std::make_unique<AsyncLog>();
}

void init_sync()
{
    log = std::make_unique<SyncLog>();
}

}

#include "logging/logger.hpp"
#include "logging/logs/async_log.hpp"
#include "logging/logs/sync_log.hpp"

#include "logging/streams/stdout.hpp"

int main(void) {
  // Log::uptr log = std::make_unique<SyncLog>();
  Log::uptr log = std::make_unique<AsyncLog>();

  log->pipe(std::make_unique<Stdout>());

  auto logger = log->logger("main");

  logger.info("This is very {}!", "awesome");
  logger.warn("This is very {}!", "awesome");
  logger.error("This is very {}!", "awesome");
  logger.trace("This is very {}!", "awesome");
  logger.debug("This is very {}!", "awesome");

  using namespace std::chrono;
  /* std::this_thread::sleep_for(1s); */

  return 0;
}

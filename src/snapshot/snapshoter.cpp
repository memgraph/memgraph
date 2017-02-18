#include "snapshot/snapshoter.hpp"

#include "database/db_accessor.hpp"
#include "logging/default.hpp"
#include "snapshot/snapshot_decoder.hpp"
#include "snapshot/snapshot_encoder.hpp"
#include "storage/indexes/indexes.hpp"
#include "threading/thread.hpp"
#include "utils/sys.hpp"

Snapshoter::Snapshoter(ConcurrentMap<std::string, Db> &dbs,
                       size_t snapshot_cycle)
    : snapshot_cycle(snapshot_cycle), dbms(dbs) {
  // Start snapshoter thread.
  thread = std::make_unique<Thread>([&]() {
    logger = logging::log->logger("Snapshoter");
    logger.info("Started with snapshoot cycle of {} sec", this->snapshot_cycle);

    try {
      run();
    } catch (const std::exception &e) {
      logger.error("Irreversible error occured in snapshoter");
      logger.error("{}", e.what());
    }

    logger.info("Shutting down snapshoter");
  });
}

Snapshoter::~Snapshoter() {
  snapshoting.store(false, std::memory_order_release);
  thread.get()->join();
}

void Snapshoter::run() {
  std::time_t last_snapshot = std::time(nullptr);

  while (snapshoting.load(std::memory_order_acquire)) {
    std::time_t now = std::time(nullptr);

    if (now >= last_snapshot + snapshot_cycle) {
      // It's time for snapshot
      make_snapshots();

      last_snapshot = now;

    } else {
      // It isn't time for snapshot so i should wait.
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

void Snapshoter::make_snapshots() {
  logger.info("Started snapshoting cycle");

  for (auto &db : dbms.access()) {
    db.second.snap_engine.make_snapshot();
  }

  logger.info("Finished snapshoting cycle");
}

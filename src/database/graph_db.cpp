#include <functional>

#include "config/config.hpp"
#include "database/creation_exception.hpp"
#include "database/graph_db.hpp"
#include "logging/logger.hpp"
#include "storage/edge.hpp"
#include "storage/garbage_collector.hpp"
//#include "snapshot/snapshoter.hpp"

const int DEFAULT_CLEANING_CYCLE_SEC = 30;  // 30 seconds

GraphDb::GraphDb(const std::string &name, bool import_snapshot)
    : name_(name),
      gc_vertices_(&vertices_, &tx_engine),
      gc_edges_(&edges_, &tx_engine) {
  const std::string time_str = CONFIG(config::CLEANING_CYCLE_SEC);
  int pause = DEFAULT_CLEANING_CYCLE_SEC;
  if (!time_str.empty()) pause = CONFIG_INTEGER(config::CLEANING_CYCLE_SEC);
  // Pause of -1 means we shouldn't run the GC.
  if (pause != -1) {
    gc_vertices_scheduler_.Run(
        std::chrono::seconds(pause),
        std::bind(&GarbageCollector<Vertex>::Run, gc_vertices_));
    gc_edges_scheduler_.Run(std::chrono::seconds(pause),
                            std::bind(&GarbageCollector<Edge>::Run, gc_edges_));
  }
  //  if (import_snapshot)
  //    snap_engine.import();
}

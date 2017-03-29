#include "database/graph_db.hpp"
#include "config/config.hpp"
#include "database/creation_exception.hpp"
#include "logging/logger.hpp"
#include "storage/edge.hpp"
#include "storage/garbage_collector.hpp"
//#include "snapshot/snapshoter.hpp"

const int DEFAULT_CLEANING_CYCLE_SEC = 30;  // 30 seconds

GraphDb::GraphDb(const std::string &name, bool import_snapshot)
    : name_(name),
      gc_vertices_(&vertices_, &tx_engine),
      gc_edges_(&edges_, &tx_engine) {
  const std::string timeStr = CONFIG(config::CLEANING_CYCLE_SEC);
  int pause = DEFAULT_CLEANING_CYCLE_SEC;
  if (!timeStr.empty()) pause = stoll(timeStr);
  this->gc_edges_.Run(std::chrono::seconds(pause));
  this->gc_vertices_.Run(std::chrono::seconds(pause));
  //  if (import_snapshot)
  //    snap_engine.import();
}

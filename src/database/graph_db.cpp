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
      gc_vertices_(vertices_, vertex_record_deleter_,
                   vertex_version_list_deleter_),
      gc_edges_(edges_, edge_record_deleter_, edge_version_list_deleter_) {
  const std::string time_str = CONFIG(config::CLEANING_CYCLE_SEC);
  int pause = DEFAULT_CLEANING_CYCLE_SEC;
  if (!time_str.empty()) pause = CONFIG_INTEGER(config::CLEANING_CYCLE_SEC);
  // Pause of -1 means we shouldn't run the GC.
  if (pause != -1) {
    gc_scheduler_.Run(std::chrono::seconds(pause), [this]() {
      // main garbage collection logic
      // see wiki documentation for logic explanation
      const auto next_id = this->tx_engine.count() + 1;
      const auto id = this->tx_engine.oldest_active().get_or(next_id);
      {
        // This can be run concurrently
        this->gc_vertices_.Run(id, this->tx_engine);
        this->gc_edges_.Run(id, this->tx_engine);
      }
      // This has to be run sequentially after gc because gc modifies
      // version_lists and changes the oldest visible record, on which Refresh
      // depends.
      {
        // This can be run concurrently
        this->labels_index_.Refresh(id, this->tx_engine);
        this->edge_types_index_.Refresh(id, this->tx_engine);
      }
      this->edge_record_deleter_.FreeExpiredObjects(id);
      this->vertex_record_deleter_.FreeExpiredObjects(id);
      this->edge_version_list_deleter_.FreeExpiredObjects(id);
      this->vertex_version_list_deleter_.FreeExpiredObjects(id);
    });
  }
  //  if (import_snapshot)
  //    snap_engine.import();
}

GraphDb::~GraphDb() {
  // Stop the gc scheduler to not run into race conditions for deletions.
  gc_scheduler_.Stop();

  // Delete vertices and edges which weren't collected before, also deletes
  // records inside version list
  for (auto &vertex : this->vertices_.access()) delete vertex;
  for (auto &edge : this->edges_.access()) delete edge;

  // Free expired records with the maximal possible id from all the deleters.
  this->edge_record_deleter_.FreeExpiredObjects(Id::MaximalId());
  this->vertex_record_deleter_.FreeExpiredObjects(Id::MaximalId());
  this->edge_version_list_deleter_.FreeExpiredObjects(Id::MaximalId());
  this->vertex_version_list_deleter_.FreeExpiredObjects(Id::MaximalId());
}

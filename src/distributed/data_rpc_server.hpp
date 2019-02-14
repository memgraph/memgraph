#pragma once

#include "database/distributed/graph_db.hpp"
#include "distributed/coordination.hpp"

namespace database {
class GraphDb;
}

namespace distributed {

/// Serves this worker's data to others.
class DataRpcServer {
 public:
  DataRpcServer(database::GraphDb *db,
                distributed::Coordination *coordination);

 private:
  database::GraphDb *db_;
};

}  // namespace distributed

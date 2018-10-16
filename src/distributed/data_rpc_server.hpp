#pragma once

#include "database/distributed/graph_db.hpp"
#include "distributed/coordination.hpp"

namespace database {
class DistributedGraphDb;
}

namespace distributed {

/// Serves this worker's data to others.
class DataRpcServer {
 public:
  DataRpcServer(database::DistributedGraphDb *db,
                distributed::Coordination *coordination);

 private:
  database::DistributedGraphDb *db_;
};

}  // namespace distributed

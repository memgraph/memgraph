#pragma once

#include "communication/rpc/server.hpp"
#include "database/distributed/graph_db.hpp"

namespace database {
class DistributedGraphDb;
}

namespace distributed {

/// Serves this worker's data to others.
class DataRpcServer {
 public:
  DataRpcServer(database::DistributedGraphDb *db,
                communication::rpc::Server *server);

 private:
  database::DistributedGraphDb *db_;
  communication::rpc::Server *rpc_server_;
};

}  // namespace distributed

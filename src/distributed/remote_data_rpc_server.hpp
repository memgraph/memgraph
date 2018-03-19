#pragma once

#include "communication/rpc/server.hpp"
#include "database/graph_db.hpp"

namespace distributed {

/// Serves this worker's data to others.
class RemoteDataRpcServer {
 public:
  RemoteDataRpcServer(database::GraphDb &db,
                      communication::rpc::Server &server);

 private:
  database::GraphDb &db_;
  communication::rpc::Server &rpc_server_;
};
}  // namespace distributed

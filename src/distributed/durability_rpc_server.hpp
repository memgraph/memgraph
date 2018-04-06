#pragma once

#include "communication/rpc/server.hpp"

namespace database {
class GraphDb;
};

namespace distributed {

class DurabilityRpcServer {
 public:
  DurabilityRpcServer(database::GraphDb &db,
                      communication::rpc::Server &server);

 private:
  database::GraphDb &db_;
  communication::rpc::Server &rpc_server_;
};

}  // namespace distributed

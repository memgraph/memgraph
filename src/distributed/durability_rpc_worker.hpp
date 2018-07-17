#pragma once

#include "communication/rpc/server.hpp"

namespace database {
class Worker;
};  // namespace database

namespace distributed {

class DurabilityRpcWorker {
 public:
  DurabilityRpcWorker(database::Worker *db, communication::rpc::Server *server);

 private:
  database::Worker *db_;
  communication::rpc::Server *rpc_server_;
};

}  // namespace distributed

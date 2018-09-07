/// @file
#pragma once

#include <atomic>
#include <string>
#include <vector>

#include "communication/rpc/client_pool.hpp"
#include "communication/rpc/server.hpp"

namespace database {
class DistributedGraphDb;
}  // namespace database

namespace distributed {
using Server = communication::rpc::Server;
using ClientPool = communication::rpc::ClientPool;

class DynamicWorkerAddition final {
 public:
  DynamicWorkerAddition(database::DistributedGraphDb *db, Server *server);

  /// Enable dynamic worker addition.
  void Enable();

 private:
  database::DistributedGraphDb *db_{nullptr};
  Server *server_;

  std::atomic<bool> enabled_{false};

  /// Return the indices a dynamically added worker needs to create.
  std::vector<std::pair<std::string, std::string>> GetIndicesToCreate();
};

class DynamicWorkerRegistration final {
 public:
  explicit DynamicWorkerRegistration(ClientPool *client_pool);

  /// Make a RPC call to master to get indices to create.
  std::vector<std::pair<std::string, std::string>> GetIndicesToCreate();

 private:
  ClientPool *client_pool_;
};

}  // namespace distributed

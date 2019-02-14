/// @file
#pragma once

#include <atomic>
#include <string>
#include <vector>

#include "communication/rpc/client_pool.hpp"
#include "distributed/coordination.hpp"

namespace database {
class GraphDb;
}  // namespace database

namespace distributed {
class DynamicWorkerAddition final {
 public:
  DynamicWorkerAddition(database::GraphDb *db,
                        distributed::Coordination *coordination);

  /// Enable dynamic worker addition.
  void Enable();

 private:
  database::GraphDb *db_{nullptr};
  distributed::Coordination *coordination_;

  std::atomic<bool> enabled_{false};

  /// Return the indices a dynamically added worker needs to create.
  std::vector<std::pair<std::string, std::string>> GetIndicesToCreate();
};

class DynamicWorkerRegistration final {
 public:
  explicit DynamicWorkerRegistration(
      communication::rpc::ClientPool *client_pool);

  /// Make a RPC call to master to get indices to create.
  std::vector<std::pair<std::string, std::string>> GetIndicesToCreate();

 private:
  communication::rpc::ClientPool *client_pool_;
};

}  // namespace distributed

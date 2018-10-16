#pragma once

#include "distributed/coordination_master.hpp"

namespace distributed {
using Server = communication::rpc::Server;

/** Handle cluster discovery on master.
 *
 * Cluster discovery on master handles worker registration and broadcasts new
 * worker information to already registered workers, and already registered
 * worker information to the new worker.
 */
class ClusterDiscoveryMaster final {
 public:
  ClusterDiscoveryMaster(MasterCoordination *coordination,
                         const std::string &durability_directory);

 private:
  MasterCoordination *coordination_;
  std::string durability_directory_;
};

}  // namespace distributed

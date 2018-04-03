#include "distributed/cluster_discovery_worker.hpp"
#include "distributed/coordination_rpc_messages.hpp"

namespace distributed {
using Server = communication::rpc::Server;

ClusterDiscoveryWorker::ClusterDiscoveryWorker(
    Server &server, WorkerCoordination &coordination,
    communication::rpc::ClientPool &client_pool)
    : server_(server), coordination_(coordination), client_pool_(client_pool) {
  server_.Register<ClusterDiscoveryRpc>([this](const ClusterDiscoveryReq &req) {
    this->coordination_.RegisterWorker(req.worker_id, req.endpoint);
    return std::make_unique<ClusterDiscoveryRes>();
  });
}

int ClusterDiscoveryWorker::RegisterWorker(int desired_worker_id) {
  auto result = client_pool_.Call<RegisterWorkerRpc>(desired_worker_id,
                                                     server_.endpoint());
  CHECK(result) << "RegisterWorkerRpc failed";

  for (auto &kv : result->workers) {
    coordination_.RegisterWorker(kv.first, kv.second);
  }

  return result->assigned_worker_id;
}

}  // namespace distributed

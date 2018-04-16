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

void ClusterDiscoveryWorker::RegisterWorker(int worker_id) {
  auto result =
      client_pool_.Call<RegisterWorkerRpc>(worker_id, server_.endpoint());
  CHECK(result) << "RegisterWorkerRpc failed";
  CHECK(result->registration_successful) << "Unable to assign requested ID ("
                                         << worker_id << ") to worker!";

  for (auto &kv : result->workers) {
    coordination_.RegisterWorker(kv.first, kv.second);
  }
  recovery_info_ = result->recovery_info;
}

}  // namespace distributed

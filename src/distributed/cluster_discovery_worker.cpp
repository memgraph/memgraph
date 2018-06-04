#include "distributed/cluster_discovery_worker.hpp"
#include "distributed/coordination_rpc_messages.hpp"

namespace distributed {
using Server = communication::rpc::Server;

ClusterDiscoveryWorker::ClusterDiscoveryWorker(
    Server &server, WorkerCoordination &coordination,
    communication::rpc::ClientPool &client_pool)
    : server_(server), coordination_(coordination), client_pool_(client_pool) {
  server_.Register<ClusterDiscoveryRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        ClusterDiscoveryReq req;
        req.Load(req_reader);
        this->coordination_.RegisterWorker(req.worker_id, req.endpoint);
      });
}

void ClusterDiscoveryWorker::RegisterWorker(int worker_id) {
  auto result =
      client_pool_.Call<RegisterWorkerRpc>(worker_id, server_.endpoint());
  CHECK(result) << "RegisterWorkerRpc failed";
  CHECK(result->registration_successful)
      << "Unable to assign requested ID (" << worker_id << ") to worker!";

  worker_id_ = worker_id;
  for (auto &kv : result->workers) {
    coordination_.RegisterWorker(kv.first, kv.second);
  }
  recovery_info_ = result->recovery_info;
}

void ClusterDiscoveryWorker::NotifyWorkerRecovered() {
  CHECK(worker_id_ >= 0)
      << "Workers id is not yet assigned, preform registration before "
         "notifying that the recovery finished";
  auto result = client_pool_.Call<NotifyWorkerRecoveredRpc>(worker_id_);
  CHECK(result) << "NotifyWorkerRecoveredRpc failed";
}

}  // namespace distributed

#include "distributed/cluster_discovery_worker.hpp"

#include <experimental/filesystem>

#include "distributed/coordination_rpc_messages.hpp"
#include "utils/file.hpp"

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

void ClusterDiscoveryWorker::RegisterWorker(
    int worker_id, const std::string &durability_directory) {
  // Create and find out what is our durability directory.
  CHECK(utils::EnsureDir(durability_directory))
      << "Couldn't create durability directory '" << durability_directory
      << "'!";
  auto full_durability_directory =
      std::experimental::filesystem::canonical(durability_directory);

  // Register to the master.
  try {
    auto result = client_pool_.Call<RegisterWorkerRpc>(
        worker_id, server_.endpoint().port(), full_durability_directory);
    CHECK(!result.durability_error)
        << "This worker was started on the same machine and with the same "
           "durability directory as the master! Please change the durability "
           "directory for this worker.";
    CHECK(result.registration_successful)
        << "Unable to assign requested ID (" << worker_id << ") to worker!";

    worker_id_ = worker_id;
    for (auto &kv : result.workers) {
      coordination_.RegisterWorker(kv.first, kv.second);
    }
    snapshot_to_recover_ = result.snapshot_to_recover;
  } catch (const communication::rpc::RpcFailedException &e) {
    LOG(FATAL) << "Couldn't register to the master!";
  }
}

void ClusterDiscoveryWorker::NotifyWorkerRecovered(
    const std::experimental::optional<durability::RecoveryInfo>
        &recovery_info) {
  CHECK(worker_id_ >= 0)
      << "Workers id is not yet assigned, preform registration before "
         "notifying that the recovery finished";
  try {
    client_pool_.Call<NotifyWorkerRecoveredRpc>(worker_id_, recovery_info);
  } catch (const communication::rpc::RpcFailedException &e) {
    LOG(FATAL) << "Couldn't notify the master that we finished recovering!";
  }
}

}  // namespace distributed

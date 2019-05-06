#include "distributed/cluster_discovery_master.hpp"

#include <filesystem>

#include "distributed/coordination_rpc_messages.hpp"
#include "io/network/endpoint.hpp"
#include "utils/file.hpp"
#include "utils/string.hpp"

namespace distributed {

ClusterDiscoveryMaster::ClusterDiscoveryMaster(
    MasterCoordination *coordination, const std::string &durability_directory)
    : coordination_(coordination), durability_directory_(durability_directory) {
  coordination_->Register<RegisterWorkerRpc>([this](const auto &endpoint,
                                                    auto *req_reader,
                                                    auto *res_builder) {
    bool registration_successful = false;
    bool durability_error = false;

    RegisterWorkerReq req;
    slk::Load(&req, req_reader);

    // Compose the worker's endpoint from its connecting address and its
    // advertised port.
    io::network::Endpoint worker_endpoint(endpoint.address(), req.port);

    // Create and find out what is our durability directory.
    utils::EnsureDirOrDie(durability_directory_);
    auto full_durability_directory =
        std::filesystem::canonical(durability_directory_);

    // Check whether the worker is running on the same host (detected when it
    // connects to us over the loopback interface) and whether it has the same
    // durability directory as us.
    // TODO (mferencevic): This check should also be done for all workers in
    // between them because this check only verifies that the worker and master
    // don't collide, there can still be a collision between workers.
    if ((utils::StartsWith(endpoint.address(), "127.") ||
         endpoint.address() == "::1") &&
        req.durability_directory == full_durability_directory) {
      durability_error = true;
      LOG(WARNING)
          << "The worker at " << worker_endpoint
          << " was started with the same durability directory as the master!";
    }

    // Register the worker if the durability check succeeded.
    if (!durability_error) {
      registration_successful =
          coordination_->RegisterWorker(req.desired_worker_id, worker_endpoint);
    }

    // Notify the cluster of the new worker if the registration succeeded.
    if (registration_successful) {
      coordination_->ExecuteOnWorkers<
          void>(0, [req, worker_endpoint](
                       int worker_id,
                       communication::rpc::ClientPool &client_pool) {
        try {
          client_pool.Call<ClusterDiscoveryRpc>(req.desired_worker_id,
                                                worker_endpoint);
        } catch (const communication::rpc::RpcFailedException &) {
          LOG(FATAL)
              << "Couldn't notify the cluster of the changed configuration!";
        }
      });
    }

    RegisterWorkerRes res(registration_successful, durability_error,
                          coordination_->RecoveredSnapshotTx(),
                          coordination_->GetWorkers());
    slk::Save(res, res_builder);
  });

  coordination_->Register<NotifyWorkerRecoveredRpc>(
      [this](auto *req_reader, auto *res_builder) {
        NotifyWorkerRecoveredReq req;
        slk::Load(&req, req_reader);
        coordination_->WorkerRecoveredSnapshot(req.worker_id,
                                               req.recovery_info);
        NotifyWorkerRecoveredRes res;
        slk::Save(res, res_builder);
      });
}

}  // namespace distributed

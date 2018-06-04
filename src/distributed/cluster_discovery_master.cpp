#include "communication/rpc/client_pool.hpp"
#include "distributed/cluster_discovery_master.hpp"
#include "distributed/coordination_rpc_messages.hpp"

namespace distributed {
using Server = communication::rpc::Server;

ClusterDiscoveryMaster::ClusterDiscoveryMaster(
    Server &server, MasterCoordination &coordination,
    RpcWorkerClients &rpc_worker_clients)
    : server_(server),
      coordination_(coordination),
      rpc_worker_clients_(rpc_worker_clients) {
  server_.Register<RegisterWorkerRpc>([this](const auto &req_reader,
                                             auto *res_builder) {
    RegisterWorkerReq req;
    req.Load(req_reader);
    bool registration_successful =
        this->coordination_.RegisterWorker(req.desired_worker_id, req.endpoint);

    if (registration_successful) {
      rpc_worker_clients_.ExecuteOnWorkers<void>(
          0, [req](int worker_id, communication::rpc::ClientPool &client_pool) {
            auto result = client_pool.Call<ClusterDiscoveryRpc>(
                req.desired_worker_id, req.endpoint);
            CHECK(result) << "ClusterDiscoveryRpc failed";
          });
    }

    RegisterWorkerRes res(registration_successful,
                          this->coordination_.RecoveryInfo(),
                          this->coordination_.GetWorkers());
    res.Save(res_builder);
  });

  server_.Register<NotifyWorkerRecoveredRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        this->coordination_.WorkerRecovered(req_reader.getMember());
      });
}

}  // namespace distributed

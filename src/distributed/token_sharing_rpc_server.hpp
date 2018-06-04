#pragma once

#include "distributed/rpc_worker_clients.hpp"
#include "storage/dynamic_graph_partitioner/dgp.hpp"

namespace communication::rpc {
class Server;
}

namespace database {
class GraphDb;
};

namespace distributed {

/// Shares the token between dynamic graph partitioners instances across workers
/// by passing the token from one worker to another, in a circular fashion. This
/// guarantees that no two workers will execute the dynamic graph partitioner
/// step in the same time.
class TokenSharingRpcServer {
 public:
  TokenSharingRpcServer(database::GraphDb *db, int worker_id,
                        distributed::Coordination *coordination,
                        communication::rpc::Server *server,
                        distributed::TokenSharingRpcClients *clients)
      : worker_id_(worker_id),
        coordination_(coordination),
        server_(server),
        clients_(clients),
        dgp_(db) {
    server_->Register<distributed::TokenTransferRpc>(
        [this](const auto &req_reader, auto *res_builder) { token_ = true; });

    runner_ = std::thread([this]() {
      while (true) {
        // Wait till we get the token
        while (!token_) {
          if (shutting_down_) break;
          std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        if (shutting_down_) break;

        token_ = false;
        dgp_.Run();

        // Transfer token to next
        auto workers = coordination_->GetWorkerIds();
        sort(workers.begin(), workers.end());

        int next_worker = -1;
        auto pos = std::upper_bound(workers.begin(), workers.end(), worker_id_);
        if (pos != workers.end()) {
          next_worker = *pos;
        } else {
          next_worker = workers[0];
        }

        clients_->TransferToken(next_worker);
      }
    });
  }

  /// Starts the token sharing server which in turn starts the dynamic graph
  /// partitioner.
  void StartTokenSharing() {
    started_ = true;
    token_ = true;
  }

  ~TokenSharingRpcServer() {
    shutting_down_ = true;
    if (runner_.joinable()) runner_.join();
    if (started_ && worker_id_ == 0) {
      // Wait till we get the token back otherwise some worker might try to
      // migrate to another worker while that worker is shutting down or
      // something else bad might happen
      // TODO(dgleich): Solve this better in the future since this blocks
      // shutting down until spinner steps complete
      while (!token_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }
    }
  }

 private:
  int worker_id_;
  distributed::Coordination *coordination_;
  communication::rpc::Server *server_;
  distributed::TokenSharingRpcClients *clients_;

  std::atomic<bool> started_{false};
  std::atomic<bool> token_{false};
  std::atomic<bool> shutting_down_{false};
  std::thread runner_;

  DynamicGraphPartitioner dgp_;
};

}  // namespace distributed

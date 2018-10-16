/// @file

#pragma once

#include "distributed/coordination.hpp"
#include "distributed/dgp/partitioner.hpp"

namespace database {
class DistributedGraphDb;
};

namespace distributed {

// TODO (buda): dgp_.Run() should be injected. This server shouldn't know
// anything about the partitioning.
// TODO (buda): It makes more sense to have centralized server which will assign
// tokens because error handling would be much easier.
// TODO (buda): Broken by design.

/// Shares the token between dynamic graph partitioners instances across workers
/// by passing the token from one worker to another, in a circular fashion. This
/// guarantees that no two workers will execute the dynamic graph partitioner
/// step in the same time.
class TokenSharingRpcServer {
 public:
  TokenSharingRpcServer(database::DistributedGraphDb *db, int worker_id,
                        distributed::Coordination *coordination)
      : worker_id_(worker_id), coordination_(coordination), dgp_(db) {
    coordination_->Register<distributed::TokenTransferRpc>(
        [this](const auto &req_reader, auto *res_builder) { token_ = true; });
    // TODO (buda): It's not trivial to move this part in the Start method
    // because worker then doesn't run the step. Will resolve that with
    // a different implementation of the token assignment.
    runner_ = std::thread([this]() {
      while (!shutting_down_) {
        // If no other instances are connected just wait. It doesn't make sense
        // to migrate anything because only one machine is available.
        auto workers = coordination_->GetWorkerIds();
        if (!(workers.size() > 1)) {
          std::this_thread::sleep_for(std::chrono::seconds(1));
          continue;
        }

        // Wait till we get the token.
        while (!token_) {
          if (shutting_down_) break;
          std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        if (shutting_down_) break;

        token_ = false;
        dgp_.Partition();

        // Transfer token to next.
        sort(workers.begin(), workers.end());

        int next_worker = -1;
        auto pos = std::upper_bound(workers.begin(), workers.end(), worker_id_);
        if (pos != workers.end()) {
          next_worker = *pos;
        } else {
          next_worker = workers[0];
        }

        // Try to transfer the token until successful.
        while (true) {
          try {
            coordination_->GetClientPool(next_worker)->Call<TokenTransferRpc>();
            break;
          } catch (const communication::rpc::RpcFailedException &e) {
            DLOG(WARNING) << "Unable to transfer token to worker "
                          << next_worker;
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
          }
        }
      }
    });
  }

  /// Starts the token sharing server which in turn starts the dynamic graph
  /// partitioner.
  void Start() {
    started_ = true;
    token_ = true;
  }

  ~TokenSharingRpcServer() {
    shutting_down_ = true;
    if (runner_.joinable()) runner_.join();
    if (started_ && worker_id_ == 0) {
      // Wait till we get the token back otherwise some worker might try to
      // migrate to another worker while that worker is shutting down or
      // something else bad might happen.
      // TODO (buda): Solve this better in the future since this blocks
      // shutting down until spinner steps complete.
      while (!token_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }
    }
  }

 private:
  int worker_id_;
  distributed::Coordination *coordination_;

  std::atomic<bool> started_{false};
  std::atomic<bool> token_{false};
  std::atomic<bool> shutting_down_{false};
  std::thread runner_;

  distributed::dgp::Partitioner dgp_;
};

}  // namespace distributed

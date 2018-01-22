#pragma once

#include "communication/messaging/distributed.hpp"
#include "communication/rpc/rpc.hpp"
#include "distributed/coordination.hpp"

namespace distributed {

/** A cache of RPC clients (of the given name/kind) per MG distributed worker.
 * Thread safe. */
class RpcWorkerClients {
 public:
  RpcWorkerClients(communication::messaging::System &system,
                   Coordination &coordination,
                   const std::string &rpc_client_name)
      : system_(system),
        coordination_(coordination),
        rpc_client_name_(rpc_client_name) {}

  RpcWorkerClients(const RpcWorkerClients &) = delete;
  RpcWorkerClients(RpcWorkerClients &&) = delete;
  RpcWorkerClients &operator=(const RpcWorkerClients &) = delete;
  RpcWorkerClients &operator=(RpcWorkerClients &&) = delete;

  auto &GetClient(int worker_id) {
    std::lock_guard<std::mutex> guard{lock_};
    auto found = clients_.find(worker_id);
    if (found != clients_.end()) return found->second;
    return clients_
        .emplace(
            std::piecewise_construct, std::forward_as_tuple(worker_id),
            std::forward_as_tuple(system_, coordination_.GetEndpoint(worker_id),
                                  rpc_client_name_))
        .first->second;
  }

  auto GetWorkerIds() { return coordination_.GetWorkerIds(); }

 private:
  communication::messaging::System &system_;
  // TODO make Coordination const, it's member GetEndpoint must be const too.
  Coordination &coordination_;
  const std::string rpc_client_name_;
  std::unordered_map<int, communication::rpc::Client> clients_;
  std::mutex lock_;
};

}  // namespace distributed

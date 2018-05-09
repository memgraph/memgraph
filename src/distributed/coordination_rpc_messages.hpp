#pragma once

#include <experimental/optional>
#include <unordered_map>

#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"
#include "boost/serialization/unordered_map.hpp"

#include "communication/rpc/messages.hpp"
#include "durability/recovery.hpp"
#include "io/network/endpoint.hpp"

namespace distributed {

using communication::rpc::Message;
using Endpoint = io::network::Endpoint;

struct RegisterWorkerReq : public Message {
  // Set desired_worker_id to -1 to get an automatically assigned ID.
  RegisterWorkerReq(int desired_worker_id, const Endpoint &endpoint)
      : desired_worker_id(desired_worker_id), endpoint(endpoint) {}
  int desired_worker_id;
  Endpoint endpoint;

 private:
  friend class boost::serialization::access;
  RegisterWorkerReq() {}

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<Message>(*this);
    ar &desired_worker_id;
    ar &endpoint;
  }
};

struct RegisterWorkerRes : public Message {
  RegisterWorkerRes(
      bool registration_successful,
      std::experimental::optional<durability::RecoveryInfo> recovery_info,
      std::unordered_map<int, Endpoint> workers)
      : registration_successful(registration_successful),
        recovery_info(recovery_info),
        workers(std::move(workers)) {}

  bool registration_successful;
  std::experimental::optional<durability::RecoveryInfo> recovery_info;
  std::unordered_map<int, Endpoint> workers;

 private:
  friend class boost::serialization::access;
  RegisterWorkerRes() {}

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<Message>(*this);
    ar &registration_successful;
    ar &recovery_info;
    ar &workers;
  }
};

struct ClusterDiscoveryReq : public Message {
  ClusterDiscoveryReq(int worker_id, Endpoint endpoint)
      : worker_id(worker_id), endpoint(endpoint) {}

  int worker_id;
  Endpoint endpoint;

 private:
  friend class boost::serialization::access;
  ClusterDiscoveryReq() {}

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<Message>(*this);
    ar &worker_id;
    ar &endpoint;
  }
};

RPC_NO_MEMBER_MESSAGE(ClusterDiscoveryRes);

RPC_NO_MEMBER_MESSAGE(StopWorkerReq);
RPC_NO_MEMBER_MESSAGE(StopWorkerRes);

RPC_SINGLE_MEMBER_MESSAGE(NotifyWorkerRecoveredReq, int);
RPC_NO_MEMBER_MESSAGE(NotifyWorkerRecoveredRes);

using RegisterWorkerRpc =
    communication::rpc::RequestResponse<RegisterWorkerReq, RegisterWorkerRes>;
using StopWorkerRpc =
    communication::rpc::RequestResponse<StopWorkerReq, StopWorkerRes>;
using NotifyWorkerRecoveredRpc =
    communication::rpc::RequestResponse<NotifyWorkerRecoveredReq,
                                        NotifyWorkerRecoveredRes>;
using ClusterDiscoveryRpc =
    communication::rpc::RequestResponse<ClusterDiscoveryReq,
                                        ClusterDiscoveryRes>;
}  // namespace distributed

#pragma once

#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"

#include "communication/messaging/local.hpp"
#include "communication/rpc/rpc.hpp"
#include "io/network/endpoint.hpp"
#include "utils/rpc_pimp.hpp"

namespace distributed {

const std::string kCoordinationServerName = "CoordinationRpc";

using communication::messaging::Message;
using Endpoint = io::network::Endpoint;

struct RegisterWorkerReq : public Message {
  RegisterWorkerReq() {}
  // Set desired_worker_id to -1 to get an automatically assigned ID.
  RegisterWorkerReq(int desired_worker_id, const Endpoint &endpoint)
      : desired_worker_id(desired_worker_id), endpoint(endpoint) {}
  int desired_worker_id;
  Endpoint endpoint;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<Message>(*this);
    ar &desired_worker_id;
    ar &endpoint;
  }
};

RPC_SINGLE_MEMBER_MESSAGE(RegisterWorkerRes, int);
RPC_SINGLE_MEMBER_MESSAGE(GetEndpointReq, int);
RPC_SINGLE_MEMBER_MESSAGE(GetEndpointRes, Endpoint);
RPC_NO_MEMBER_MESSAGE(StopWorkerReq);
RPC_NO_MEMBER_MESSAGE(StopWorkerRes);

using RegisterWorkerRpc =
    communication::rpc::RequestResponse<RegisterWorkerReq, RegisterWorkerRes>;
using GetEndpointRpc =
    communication::rpc::RequestResponse<GetEndpointReq, GetEndpointRes>;
using StopWorkerRpc =
    communication::rpc::RequestResponse<StopWorkerReq, StopWorkerRes>;

}  // namespace distributed

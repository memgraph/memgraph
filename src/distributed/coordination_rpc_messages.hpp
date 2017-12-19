#pragma once

#include "communication/messaging/local.hpp"
#include "communication/rpc/rpc.hpp"
#include "io/network/network_endpoint.hpp"
#include "utils/rpc_pimp.hpp"

namespace distributed {

const std::string kCoordinationServerName = "CoordinationRpc";

using communication::messaging::Message;
using Endpoint = io::network::NetworkEndpoint;

struct RegisterWorkerReq : public Message {
  RegisterWorkerReq() {}
  // Set desired_worker_id to -1 to get an automatically assigned ID.
  RegisterWorkerReq(int desired_worker_id, const Endpoint &endpoint)
      : desired_worker_id(desired_worker_id), endpoint(endpoint) {}
  int desired_worker_id;
  Endpoint endpoint;

  template <class Archive>
  void serialize(Archive &ar) {
    ar(cereal::virtual_base_class<Message>(this), desired_worker_id, endpoint);
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

CEREAL_REGISTER_TYPE(distributed::RegisterWorkerReq);
CEREAL_REGISTER_TYPE(distributed::RegisterWorkerRes);
CEREAL_REGISTER_TYPE(distributed::GetEndpointReq);
CEREAL_REGISTER_TYPE(distributed::GetEndpointRes);
CEREAL_REGISTER_TYPE(distributed::StopWorkerReq);
CEREAL_REGISTER_TYPE(distributed::StopWorkerRes);

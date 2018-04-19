#pragma once

#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"

#include "communication/rpc/messages.hpp"
#include "io/network/endpoint.hpp"
#include "transactions/transaction.hpp"

namespace distributed {

using communication::rpc::Message;
using Endpoint = io::network::Endpoint;

struct GcClearedStatusReq : public Message {
  GcClearedStatusReq() {}
  GcClearedStatusReq(tx::TransactionId local_oldest_active, int worker_id)
      : local_oldest_active(local_oldest_active), worker_id(worker_id) {}

  tx::TransactionId local_oldest_active;
  int worker_id;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<Message>(*this);
    ar &local_oldest_active;
    ar &worker_id;
  }
};

RPC_NO_MEMBER_MESSAGE(GcClearedStatusRes);

using RanLocalGcRpc =
    communication::rpc::RequestResponse<GcClearedStatusReq, GcClearedStatusRes>;

}  // namespace distributed

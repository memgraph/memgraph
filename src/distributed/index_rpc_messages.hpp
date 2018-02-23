#pragma once

#include <memory>
#include <string>

#include "communication/rpc/messages.hpp"
#include "distributed/serialization.hpp"

namespace distributed {

struct IndexLabelPropertyTx {
  storage::Label label;
  storage::Property property;
  tx::transaction_id_t tx_id;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &label;
    ar &property;
    ar &tx_id;
  }
};

RPC_SINGLE_MEMBER_MESSAGE(BuildIndexReq, IndexLabelPropertyTx);
RPC_NO_MEMBER_MESSAGE(BuildIndexRes);

using BuildIndexRpc =
    communication::rpc::RequestResponse<BuildIndexReq, BuildIndexRes>;
}  // namespace distributed

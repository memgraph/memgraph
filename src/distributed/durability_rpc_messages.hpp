#pragma once

#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"

#include "communication/rpc/messages.hpp"
#include "transactions/transaction.hpp"

namespace distributed {

RPC_SINGLE_MEMBER_MESSAGE(MakeSnapshotReq, tx::transaction_id_t);
RPC_SINGLE_MEMBER_MESSAGE(MakeSnapshotRes, bool);

using MakeSnapshotRpc =
    communication::rpc::RequestResponse<MakeSnapshotReq, MakeSnapshotRes>;

}  // namespace distributed

#pragma once

#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"

#include "communication/rpc/messages.hpp"
#include "transactions/transaction.hpp"

namespace distributed {

RPC_SINGLE_MEMBER_MESSAGE(MakeSnapshotReq, tx::TransactionId);
RPC_SINGLE_MEMBER_MESSAGE(MakeSnapshotRes, bool);

using MakeSnapshotRpc =
    communication::rpc::RequestResponse<MakeSnapshotReq, MakeSnapshotRes>;

}  // namespace distributed

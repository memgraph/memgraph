#pragma once

#include "communication/rpc/messages.hpp"
#include "database/state_delta.hpp"
#include "transactions/type.hpp"

namespace distributed {

const std::string kRemoteUpdatesRpc = "RemoteUpdatesRpc";

/// The result of sending or applying a deferred update to a worker.
enum class RemoteUpdateResult {
  DONE,
  SERIALIZATION_ERROR,
  LOCK_TIMEOUT_ERROR,
  UPDATE_DELETED_ERROR
};

RPC_SINGLE_MEMBER_MESSAGE(RemoteUpdateReq, database::StateDelta);
RPC_SINGLE_MEMBER_MESSAGE(RemoteUpdateRes, RemoteUpdateResult);
using RemoteUpdateRpc =
    communication::rpc::RequestResponse<RemoteUpdateReq, RemoteUpdateRes>;

RPC_SINGLE_MEMBER_MESSAGE(RemoteUpdateApplyReq, tx::transaction_id_t);
RPC_SINGLE_MEMBER_MESSAGE(RemoteUpdateApplyRes, RemoteUpdateResult);
using RemoteUpdateApplyRpc =
    communication::rpc::RequestResponse<RemoteUpdateApplyReq,
                                        RemoteUpdateApplyRes>;
}  // namespace distributed

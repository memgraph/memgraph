#pragma once

#include "utils/rpc_pimp.hpp"

#include "communication/rpc/rpc.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/snapshot.hpp"
#include "transactions/type.hpp"

namespace tx {

RPC_SINGLE_MEMBER_MESSAGE(SnapshotReq, transaction_id_t)
RPC_SINGLE_MEMBER_MESSAGE(SnapshotRes, Snapshot)
RPC_NO_MEMBER_MESSAGE(GcSnapshotReq)
RPC_SINGLE_MEMBER_MESSAGE(ClogInfoReq, transaction_id_t)
RPC_SINGLE_MEMBER_MESSAGE(ClogInfoRes, CommitLog::Info)
RPC_SINGLE_MEMBER_MESSAGE(ActiveTransactionsReq, transaction_id_t)
RPC_SINGLE_MEMBER_MESSAGE(IsActiveReq, transaction_id_t)
RPC_SINGLE_MEMBER_MESSAGE(IsActiveRes, bool)

using SnapshotRpc =
    communication::rpc::RequestResponse<SnapshotReq, SnapshotRes>;
using GcSnapshotRpc =
    communication::rpc::RequestResponse<GcSnapshotReq, SnapshotRes>;
using GcSnapshotRpc =
    communication::rpc::RequestResponse<GcSnapshotReq, SnapshotRes>;
using ClogInfoRpc =
    communication::rpc::RequestResponse<ClogInfoReq, ClogInfoRes>;
using ActiveTransactionsRpc =
    communication::rpc::RequestResponse<ActiveTransactionsReq, SnapshotRes>;
using IsActiveRpc =
    communication::rpc::RequestResponse<IsActiveReq, IsActiveRes>;
}  // namespace tx

CEREAL_REGISTER_TYPE(tx::SnapshotReq);
CEREAL_REGISTER_TYPE(tx::SnapshotRes);
CEREAL_REGISTER_TYPE(tx::GcSnapshotReq);
CEREAL_REGISTER_TYPE(tx::ClogInfoReq);
CEREAL_REGISTER_TYPE(tx::ClogInfoRes);
CEREAL_REGISTER_TYPE(tx::ActiveTransactionsReq);
CEREAL_REGISTER_TYPE(tx::IsActiveReq);
CEREAL_REGISTER_TYPE(tx::IsActiveRes);

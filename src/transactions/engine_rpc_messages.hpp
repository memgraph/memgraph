#pragma once

#include "communication/rpc/messages.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/snapshot.hpp"
#include "transactions/type.hpp"

namespace tx {

RPC_NO_MEMBER_MESSAGE(BeginReq);
struct TxAndSnapshot {
  TransactionId tx_id;
  Snapshot snapshot;

 private:
  friend class boost::serialization::access;
  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &tx_id;
    ar &snapshot;
  }
};
RPC_SINGLE_MEMBER_MESSAGE(BeginRes, TxAndSnapshot);
using BeginRpc = communication::rpc::RequestResponse<BeginReq, BeginRes>;

RPC_SINGLE_MEMBER_MESSAGE(AdvanceReq, TransactionId);
RPC_SINGLE_MEMBER_MESSAGE(AdvanceRes, CommandId);
using AdvanceRpc = communication::rpc::RequestResponse<AdvanceReq, AdvanceRes>;

RPC_SINGLE_MEMBER_MESSAGE(CommitReq, TransactionId);
RPC_NO_MEMBER_MESSAGE(CommitRes);
using CommitRpc = communication::rpc::RequestResponse<CommitReq, CommitRes>;

RPC_SINGLE_MEMBER_MESSAGE(AbortReq, TransactionId);
RPC_NO_MEMBER_MESSAGE(AbortRes);
using AbortRpc = communication::rpc::RequestResponse<AbortReq, AbortRes>;

RPC_SINGLE_MEMBER_MESSAGE(SnapshotReq, TransactionId);
RPC_SINGLE_MEMBER_MESSAGE(SnapshotRes, Snapshot);
using SnapshotRpc =
    communication::rpc::RequestResponse<SnapshotReq, SnapshotRes>;

RPC_SINGLE_MEMBER_MESSAGE(CommandReq, TransactionId);
RPC_SINGLE_MEMBER_MESSAGE(CommandRes, CommandId);
using CommandRpc = communication::rpc::RequestResponse<CommandReq, CommandRes>;

RPC_NO_MEMBER_MESSAGE(GcSnapshotReq);
using GcSnapshotRpc =
    communication::rpc::RequestResponse<GcSnapshotReq, SnapshotRes>;

RPC_SINGLE_MEMBER_MESSAGE(ClogInfoReq, TransactionId);
RPC_SINGLE_MEMBER_MESSAGE(ClogInfoRes, CommitLog::Info);
using ClogInfoRpc =
    communication::rpc::RequestResponse<ClogInfoReq, ClogInfoRes>;

RPC_NO_MEMBER_MESSAGE(ActiveTransactionsReq);
using ActiveTransactionsRpc =
    communication::rpc::RequestResponse<ActiveTransactionsReq, SnapshotRes>;

RPC_SINGLE_MEMBER_MESSAGE(EnsureNextIdGreaterReq, TransactionId);
RPC_NO_MEMBER_MESSAGE(EnsureNextIdGreaterRes);
using EnsureNextIdGreaterRpc =
    communication::rpc::RequestResponse<EnsureNextIdGreaterReq,
                                        EnsureNextIdGreaterRes>;

RPC_NO_MEMBER_MESSAGE(GlobalLastReq);
RPC_SINGLE_MEMBER_MESSAGE(GlobalLastRes, TransactionId);
using GlobalLastRpc =
    communication::rpc::RequestResponse<GlobalLastReq, GlobalLastRes>;
}  // namespace tx

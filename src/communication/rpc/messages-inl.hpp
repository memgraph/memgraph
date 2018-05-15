#pragma once

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/export.hpp"

#include "database/state_delta.hpp"
#include "distributed/bfs_rpc_messages.hpp"
#include "distributed/coordination_rpc_messages.hpp"
#include "distributed/data_rpc_messages.hpp"
#include "distributed/durability_rpc_messages.hpp"
#include "distributed/index_rpc_messages.hpp"
#include "distributed/plan_rpc_messages.hpp"
#include "distributed/pull_produce_rpc_messages.hpp"
#include "distributed/storage_gc_rpc_messages.hpp"
#include "distributed/transactional_cache_cleaner_rpc_messages.hpp"
#include "distributed/updates_rpc_messages.hpp"
#include "durability/recovery.hpp"
#include "stats/stats_rpc_messages.hpp"
#include "storage/concurrent_id_mapper_rpc_messages.hpp"
#include "transactions/engine_rpc_messages.hpp"

#define ID_VALUE_EXPORT_BOOST_TYPE(type)      \
  BOOST_CLASS_EXPORT(storage::type##IdReq);   \
  BOOST_CLASS_EXPORT(storage::type##IdRes);   \
  BOOST_CLASS_EXPORT(storage::Id##type##Req); \
  BOOST_CLASS_EXPORT(storage::Id##type##Res);

ID_VALUE_EXPORT_BOOST_TYPE(Label)
ID_VALUE_EXPORT_BOOST_TYPE(EdgeType)
ID_VALUE_EXPORT_BOOST_TYPE(Property)

#undef ID_VALUE_EXPORT_BOOST_TYPE

// Distributed transaction engine.
BOOST_CLASS_EXPORT(tx::TxAndSnapshot);
BOOST_CLASS_EXPORT(tx::BeginReq);
BOOST_CLASS_EXPORT(tx::BeginRes);
BOOST_CLASS_EXPORT(tx::AdvanceReq);
BOOST_CLASS_EXPORT(tx::AdvanceRes);
BOOST_CLASS_EXPORT(tx::CommitReq);
BOOST_CLASS_EXPORT(tx::CommitRes);
BOOST_CLASS_EXPORT(tx::AbortReq);
BOOST_CLASS_EXPORT(tx::AbortRes);
BOOST_CLASS_EXPORT(tx::SnapshotReq);
BOOST_CLASS_EXPORT(tx::SnapshotRes);
BOOST_CLASS_EXPORT(tx::CommandReq);
BOOST_CLASS_EXPORT(tx::CommandRes);
BOOST_CLASS_EXPORT(tx::GcSnapshotReq);
BOOST_CLASS_EXPORT(tx::ClogInfoReq);
BOOST_CLASS_EXPORT(tx::ClogInfoRes);
BOOST_CLASS_EXPORT(tx::ActiveTransactionsReq);
BOOST_CLASS_EXPORT(tx::EnsureNextIdGreaterReq);
BOOST_CLASS_EXPORT(tx::EnsureNextIdGreaterRes);
BOOST_CLASS_EXPORT(tx::GlobalLastReq);
BOOST_CLASS_EXPORT(tx::GlobalLastRes);

// Distributed coordination.
BOOST_CLASS_EXPORT(durability::RecoveryInfo);
BOOST_CLASS_EXPORT(distributed::RegisterWorkerReq);
BOOST_CLASS_EXPORT(distributed::RegisterWorkerRes);
BOOST_CLASS_EXPORT(distributed::ClusterDiscoveryReq);
BOOST_CLASS_EXPORT(distributed::ClusterDiscoveryRes);
BOOST_CLASS_EXPORT(distributed::StopWorkerReq);
BOOST_CLASS_EXPORT(distributed::StopWorkerRes);
BOOST_CLASS_EXPORT(distributed::NotifyWorkerRecoveredReq);
BOOST_CLASS_EXPORT(distributed::NotifyWorkerRecoveredRes);

// Distributed data exchange.
BOOST_CLASS_EXPORT(distributed::EdgeReq);
BOOST_CLASS_EXPORT(distributed::EdgeRes);
BOOST_CLASS_EXPORT(distributed::VertexReq);
BOOST_CLASS_EXPORT(distributed::VertexRes);
BOOST_CLASS_EXPORT(distributed::TxGidPair);

// Distributed plan exchange.
BOOST_CLASS_EXPORT(distributed::DispatchPlanReq);
BOOST_CLASS_EXPORT(distributed::DispatchPlanRes);
BOOST_CLASS_EXPORT(distributed::RemovePlanReq);
BOOST_CLASS_EXPORT(distributed::RemovePlanRes);

// Pull.
BOOST_CLASS_EXPORT(distributed::PullReq);
BOOST_CLASS_EXPORT(distributed::PullRes);
BOOST_CLASS_EXPORT(distributed::TransactionCommandAdvancedReq);
BOOST_CLASS_EXPORT(distributed::TransactionCommandAdvancedRes);

// Distributed indexes.
BOOST_CLASS_EXPORT(distributed::BuildIndexReq);
BOOST_CLASS_EXPORT(distributed::BuildIndexRes);
BOOST_CLASS_EXPORT(distributed::IndexLabelPropertyTx);

// Stats.
BOOST_CLASS_EXPORT(stats::StatsReq);
BOOST_CLASS_EXPORT(stats::StatsRes);
BOOST_CLASS_EXPORT(stats::BatchStatsReq);
BOOST_CLASS_EXPORT(stats::BatchStatsRes);

// Updates.
BOOST_CLASS_EXPORT(database::StateDelta);
BOOST_CLASS_EXPORT(distributed::UpdateReq);
BOOST_CLASS_EXPORT(distributed::UpdateRes);
BOOST_CLASS_EXPORT(distributed::UpdateApplyReq);
BOOST_CLASS_EXPORT(distributed::UpdateApplyRes);

// Creates.
BOOST_CLASS_EXPORT(distributed::CreateResult);
BOOST_CLASS_EXPORT(distributed::CreateVertexReq);
BOOST_CLASS_EXPORT(distributed::CreateVertexReqData);
BOOST_CLASS_EXPORT(distributed::CreateVertexRes);
BOOST_CLASS_EXPORT(distributed::CreateEdgeReqData);
BOOST_CLASS_EXPORT(distributed::CreateEdgeReq);
BOOST_CLASS_EXPORT(distributed::CreateEdgeRes);
BOOST_CLASS_EXPORT(distributed::AddInEdgeReqData);
BOOST_CLASS_EXPORT(distributed::AddInEdgeReq);
BOOST_CLASS_EXPORT(distributed::AddInEdgeRes);

// Removes.
BOOST_CLASS_EXPORT(distributed::RemoveVertexReq);
BOOST_CLASS_EXPORT(distributed::RemoveVertexRes);
BOOST_CLASS_EXPORT(distributed::RemoveEdgeReq);
BOOST_CLASS_EXPORT(distributed::RemoveEdgeRes);
BOOST_CLASS_EXPORT(distributed::RemoveInEdgeData);
BOOST_CLASS_EXPORT(distributed::RemoveInEdgeReq);
BOOST_CLASS_EXPORT(distributed::RemoveInEdgeRes);

// Durability
BOOST_CLASS_EXPORT(distributed::MakeSnapshotReq);
BOOST_CLASS_EXPORT(distributed::MakeSnapshotRes);

// Storage Gc.
BOOST_CLASS_EXPORT(distributed::GcClearedStatusReq);
BOOST_CLASS_EXPORT(distributed::GcClearedStatusRes);

// Transactional Cache Cleaner.
BOOST_CLASS_EXPORT(distributed::WaitOnTransactionEndReq);
BOOST_CLASS_EXPORT(distributed::WaitOnTransactionEndRes);

// Cursor.
BOOST_CLASS_EXPORT(distributed::CreateBfsSubcursorReq);
BOOST_CLASS_EXPORT(distributed::CreateBfsSubcursorRes);
BOOST_CLASS_EXPORT(distributed::RegisterSubcursorsReq);
BOOST_CLASS_EXPORT(distributed::RegisterSubcursorsRes);
BOOST_CLASS_EXPORT(distributed::RemoveBfsSubcursorReq);
BOOST_CLASS_EXPORT(distributed::RemoveBfsSubcursorRes);
BOOST_CLASS_EXPORT(distributed::SetSourceReq);
BOOST_CLASS_EXPORT(distributed::SetSourceRes);
BOOST_CLASS_EXPORT(distributed::ExpandLevelReq);
BOOST_CLASS_EXPORT(distributed::ExpandLevelRes);
BOOST_CLASS_EXPORT(distributed::SubcursorPullReq);
BOOST_CLASS_EXPORT(distributed::SubcursorPullRes);
BOOST_CLASS_EXPORT(distributed::ExpandToRemoteVertexReq);
BOOST_CLASS_EXPORT(distributed::ExpandToRemoteVertexRes);
BOOST_CLASS_EXPORT(distributed::ReconstructPathReq);
BOOST_CLASS_EXPORT(distributed::ReconstructPathRes);
BOOST_CLASS_EXPORT(distributed::PrepareForExpandReq);
BOOST_CLASS_EXPORT(distributed::PrepareForExpandRes);

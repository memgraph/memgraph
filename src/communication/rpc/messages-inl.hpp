#pragma once

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/export.hpp"

#include "distributed/coordination_rpc_messages.hpp"
#include "distributed/index_rpc_messages.hpp"
#include "distributed/plan_rpc_messages.hpp"
#include "distributed/remote_data_rpc_messages.hpp"
#include "distributed/remote_pull_produce_rpc_messages.hpp"
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
BOOST_CLASS_EXPORT(tx::GcSnapshotReq);
BOOST_CLASS_EXPORT(tx::ClogInfoReq);
BOOST_CLASS_EXPORT(tx::ClogInfoRes);
BOOST_CLASS_EXPORT(tx::ActiveTransactionsReq);
BOOST_CLASS_EXPORT(tx::IsActiveReq);
BOOST_CLASS_EXPORT(tx::IsActiveRes);

// Distributed coordination.
BOOST_CLASS_EXPORT(distributed::RegisterWorkerReq);
BOOST_CLASS_EXPORT(distributed::RegisterWorkerRes);
BOOST_CLASS_EXPORT(distributed::GetEndpointReq);
BOOST_CLASS_EXPORT(distributed::GetEndpointRes);
BOOST_CLASS_EXPORT(distributed::StopWorkerReq);
BOOST_CLASS_EXPORT(distributed::StopWorkerRes);

// Distributed data exchange.
BOOST_CLASS_EXPORT(distributed::RemoteEdgeReq);
BOOST_CLASS_EXPORT(distributed::RemoteEdgeRes);
BOOST_CLASS_EXPORT(distributed::RemoteVertexReq);
BOOST_CLASS_EXPORT(distributed::RemoteVertexRes);
BOOST_CLASS_EXPORT(distributed::TxGidPair);

// Distributed plan exchange.
BOOST_CLASS_EXPORT(distributed::DispatchPlanReq);
BOOST_CLASS_EXPORT(distributed::ConsumePlanRes);

// Remote pull.
BOOST_CLASS_EXPORT(distributed::RemotePullReq);
BOOST_CLASS_EXPORT(distributed::RemotePullRes);
BOOST_CLASS_EXPORT(distributed::EndRemotePullReq);
BOOST_CLASS_EXPORT(distributed::EndRemotePullRes);

// Distributed indexes.
BOOST_CLASS_EXPORT(distributed::BuildIndexReq);
BOOST_CLASS_EXPORT(distributed::BuildIndexRes);
BOOST_CLASS_EXPORT(distributed::IndexLabelPropertyTx);

// Stats.
BOOST_CLASS_EXPORT(stats::StatsReq);
BOOST_CLASS_EXPORT(stats::StatsRes);
BOOST_CLASS_EXPORT(stats::BatchStatsReq);
BOOST_CLASS_EXPORT(stats::BatchStatsRes);

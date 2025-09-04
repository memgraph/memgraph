// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/event_counter.hpp"

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define GenerateHARpcCounters(NAME)                                                     \
  M(NAME##Success, HighAvailability, "Number of times " #NAME " finished successfully") \
  M(NAME##Fail, HighAvailability, "Number of times " #NAME " finished unsuccessfully")

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
// clang-format off
#define APPLY_FOR_COUNTERS(M)                                                                                          \
  M(ReadQuery, QueryType, "Number of read-only queries executed.")                                                     \
  M(WriteQuery, QueryType, "Number of write-only queries executed.")                                                   \
  M(ReadWriteQuery, QueryType, "Number of read-write queries executed.")                                               \
                                                                                                                       \
  M(OnceOperator, Operator, "Number of times Once operator was used.")                                                 \
  M(CreateNodeOperator, Operator, "Number of times CreateNode operator was used.")                                     \
  M(CreateExpandOperator, Operator, "Number of times CreateExpand operator was used.")                                 \
  M(ScanAllOperator, Operator, "Number of times ScanAll operator was used.")                                           \
  M(ScanAllByLabelOperator, Operator, "Number of times ScanAllByLabel operator was used.")                             \
  M(ScanAllByLabelPropertiesOperator, Operator, "Number of times ScanAllByLabelProperties operator was used.")         \
  M(ScanAllByIdOperator, Operator, "Number of times ScanAllById operator was used.")                                   \
  M(ScanAllByEdgeOperator, Operator, "Number of times ScanAllByEdgeOperator operator was used.")                       \
  M(ScanAllByEdgeTypeOperator, Operator, "Number of times ScanAllByEdgeTypeOperator operator was used.")               \
  M(ScanAllByEdgeTypePropertyOperator, Operator,                                                                       \
    "Number of times ScanAllByEdgeTypePropertyOperator operator was used.")                                            \
  M(ScanAllByEdgeTypePropertyValueOperator, Operator,                                                                  \
    "Number of times ScanAllByEdgeTypePropertyValueOperator operator was used.")                                       \
  M(ScanAllByEdgeTypePropertyRangeOperator, Operator,                                                                  \
    "Number of times ScanAllByEdgeTypePropertyRangeOperator operator was used.")                                       \
  M(ScanAllByEdgePropertyOperator, Operator, "Number of times ScanAllByEdgePropertyOperator operator was used.")       \
  M(ScanAllByEdgePropertyValueOperator, Operator,                                                                      \
    "Number of times ScanAllByEdgePropertyValueOperator operator was used.")                                           \
  M(ScanAllByEdgePropertyRangeOperator, Operator,                                                                      \
    "Number of times ScanAllByEdgePropertyRangeOperator operator was used.")                                           \
  M(ScanAllByEdgeIdOperator, Operator, "Number of times ScanAllByEdgeIdOperator operator was used.")                   \
  M(ScanAllByPointDistanceOperator, Operator, "Number of times ScanAllByPointDistanceOperator operator was used.")     \
  M(ScanAllByPointWithinbboxOperator, Operator, "Number of times ScanAllByPointWithinbboxOperator operator was used.") \
  M(ExpandOperator, Operator, "Number of times Expand operator was used.")                                             \
  M(ExpandVariableOperator, Operator, "Number of times ExpandVariable operator was used.")                             \
  M(ConstructNamedPathOperator, Operator, "Number of times ConstructNamedPath operator was used.")                     \
  M(FilterOperator, Operator, "Number of times Filter operator was used.")                                             \
  M(ProduceOperator, Operator, "Number of times Produce operator was used.")                                           \
  M(DeleteOperator, Operator, "Number of times Delete operator was used.")                                             \
  M(SetPropertyOperator, Operator, "Number of times SetProperty operator was used.")                                   \
  M(SetPropertiesOperator, Operator, "Number of times SetProperties operator was used.")                               \
  M(SetLabelsOperator, Operator, "Number of times SetLabels operator was used.")                                       \
  M(RemovePropertyOperator, Operator, "Number of times RemoveProperty operator was used.")                             \
  M(RemoveLabelsOperator, Operator, "Number of times RemoveLabels operator was used.")                                 \
  M(EdgeUniquenessFilterOperator, Operator, "Number of times EdgeUniquenessFilter operator was used.")                 \
  M(EmptyResultOperator, Operator, "Number of times EmptyResult operator was used.")                                   \
  M(AccumulateOperator, Operator, "Number of times Accumulate operator was used.")                                     \
  M(AggregateOperator, Operator, "Number of times Aggregate operator was used.")                                       \
  M(SkipOperator, Operator, "Number of times Skip operator was used.")                                                 \
  M(LimitOperator, Operator, "Number of times Limit operator was used.")                                               \
  M(OrderByOperator, Operator, "Number of times OrderBy operator was used.")                                           \
  M(MergeOperator, Operator, "Number of times Merge operator was used.")                                               \
  M(OptionalOperator, Operator, "Number of times Optional operator was used.")                                         \
  M(UnwindOperator, Operator, "Number of times Unwind operator was used.")                                             \
  M(DistinctOperator, Operator, "Number of times Distinct operator was used.")                                         \
  M(UnionOperator, Operator, "Number of times Union operator was used.")                                               \
  M(CartesianOperator, Operator, "Number of times Cartesian operator was used.")                                       \
  M(CallProcedureOperator, Operator, "Number of times CallProcedure operator was used.")                               \
  M(ForeachOperator, Operator, "Number of times Foreach operator was used.")                                           \
  M(EvaluatePatternFilterOperator, Operator, "Number of times EvaluatePatternFilter operator was used.")               \
  M(ApplyOperator, Operator, "Number of times ApplyOperator operator was used.")                                       \
  M(IndexedJoinOperator, Operator, "Number of times IndexedJoin operator was used.")                                   \
  M(HashJoinOperator, Operator, "Number of times HashJoin operator was used.")                                         \
  M(RollUpApplyOperator, Operator, "Number of times RollUpApply operator was used.")                                   \
  M(PeriodicCommitOperator, Operator, "Number of times PeriodicCommit operator was used.")                             \
  M(PeriodicSubqueryOperator, Operator, "Number of times PeriodicSubquery operator was used.")                         \
  M(SetNestedPropertyOperator, Operator, "Number of times SetNestedProperty operator was used.")                       \
  M(RemoveNestedPropertyOperator, Operator, "Number of times RemoveNestedProperty operator was used.")                 \
                                                                                                                       \
  M(ActiveLabelIndices, Index, "Number of active label indices in the system.")                                        \
  M(ActiveLabelPropertyIndices, Index, "Number of active label property indices in the system.")                       \
  M(ActiveEdgeTypeIndices, Index, "Number of active edge type indices in the system.")                                 \
  M(ActiveEdgeTypePropertyIndices, Index, "Number of active edge type property indices in the system.")                \
  M(ActiveEdgePropertyIndices, Index, "Number of active edge property indices in the system.")                         \
  M(ActivePointIndices, Index, "Number of active point indices in the system.")                                        \
  M(ActiveTextIndices, Index, "Number of active text indices in the system.")                                          \
  M(ActiveVectorIndices, Index, "Number of active vector indices in the system.")                                      \
  M(ActiveVectorEdgeIndices, Index, "Number of active vector edge indices in the system.")                             \
                                                                                                                       \
  M(StreamsCreated, Stream, "Number of Streams created.")                                                              \
  M(MessagesConsumed, Stream, "Number of consumed streamed messages.")                                                 \
                                                                                                                       \
  M(TriggersCreated, Trigger, "Number of Triggers created.")                                                           \
  M(TriggersExecuted, Trigger, "Number of Triggers executed.")                                                         \
                                                                                                                       \
  M(ActiveSessions, Session, "Number of active connections.")                                                          \
  M(ActiveBoltSessions, Session, "Number of active Bolt connections.")                                                 \
  M(ActiveTCPSessions, Session, "Number of active TCP connections.")                                                   \
  M(ActiveSSLSessions, Session, "Number of active SSL connections.")                                                   \
  M(ActiveWebSocketSessions, Session, "Number of active websocket connections.")                                       \
  M(BoltMessages, Session, "Number of Bolt messages sent.")                                                            \
                                                                                                                       \
  M(ActiveTransactions, Transaction, "Number of active transactions.")                                                 \
  M(CommitedTransactions, Transaction, "Number of committed transactions.")                                            \
  M(RollbackedTransactions, Transaction, "Number of rollbacked transactions.")                                         \
  M(FailedQuery, Transaction, "Number of times executing a query failed.")                                             \
  M(FailedPrepare, Transaction, "Number of times preparing a query failed.")                                           \
  M(FailedPull, Transaction, "Number of times executing a prepared query failed.")                                     \
  M(SuccessfulQuery, Transaction, "Number of successful queries.")                                                     \
  M(WriteWriteConflicts, Transaction, "Number of times a write-write conflict happened.")                              \
  M(TransientErrors, Transaction, "Number of times a transient error happened.")                                        \
  M(UnreleasedDeltaObjects, Memory, "Total number of unreleased delta objects in memory.")                             \
                                                                                                                       \
  M(DeletedNodes, TTL, "Number of nodes deleted via TTL")                                                              \
  M(DeletedEdges, TTL, "Number of edges deleted via TTL")                                                              \
                                                                                                                       \
  M(ShowSchema, SchemaInfo, "Number of times the user called \"SHOW SCHEMA INFO\" query")                              \
                                                                                                                       \
  M(SuccessfulFailovers, HighAvailability, "Number of successful failovers performed on the coordinator.")             \
  M(RaftFailedFailovers, HighAvailability, "Number of failed failovers because of Raft on the coordinator.")           \
  M(NoAliveInstanceFailedFailovers, HighAvailability, "Number of failed failovers, no data instance was alive.")       \
  M(BecomeLeaderSuccess, HighAvailability, "Number of times the coordinator successfuly became the leader.")           \
  M(FailedToBecomeLeader, HighAvailability, "Number of times the coordinator failed to become the leader.")            \
  M(ShowInstance, HighAvailability, "Number of times the user called \"SHOW INSTANCE\" query.")                        \
  M(ShowInstances, HighAvailability, "Number of times the user called \"SHOW INSTANCES\" query.")                      \
  M(DemoteInstance, HighAvailability, "Number of times the user called \"DEMOTE INSTANCE ...\" query.")                \
  M(UnregisterReplInstance, HighAvailability, "Number of times the user called \"UNREGISTER INSTANCE ...\" query.")    \
  M(RemoveCoordInstance, HighAvailability, "Number of times the user called \"REMOVE COORDINATOR ...\" query.")        \
  M(ReplicaRecoverySuccess, HighAvailability, "Number of times the replica recovery finished successfully")            \
  M(ReplicaRecoveryFail, HighAvailability, "Number of times the replica recovery finished unsuccessfully")             \
  M(ReplicaRecoverySkip, HighAvailability, "Number of times the replica recovery task was skipped")                    \
  GenerateHARpcCounters(StateCheckRpc)                                                                                 \
  GenerateHARpcCounters(UnregisterReplicaRpc)                                                                          \
  GenerateHARpcCounters(EnableWritingOnMainRpc)                                                                        \
  GenerateHARpcCounters(PromoteToMainRpc)                                                                              \
  GenerateHARpcCounters(DemoteMainToReplicaRpc)                                                                        \
  GenerateHARpcCounters(RegisterReplicaOnMainRpc)                                                                      \
  GenerateHARpcCounters(SwapMainUUIDRpc)                                                                               \
  GenerateHARpcCounters(GetDatabaseHistoriesRpc)
// clang-format on
namespace memgraph::metrics {

// define every Event as an index in the array of counters

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, TYPE, DOCUMENTATION) extern const Event NAME = __COUNTER__;

APPLY_FOR_COUNTERS(M)

#undef M

inline constexpr Event END = __COUNTER__;

// Initialize array for the global counter with all values set to 0
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
Counter global_counters_array[END]{};

// Initialize global counters
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
EventCounters global_counters(global_counters_array);

const Event EventCounters::num_counters = END;

void EventCounters::Increment(const Event event, Count const amount) {
  counters_[event].fetch_add(amount, std::memory_order_relaxed);
}

void EventCounters::Decrement(const Event event, Count const amount) {
  counters_[event].fetch_sub(amount, std::memory_order_relaxed);
}

void IncrementCounter(const Event event, Count const amount) { global_counters.Increment(event, amount); }
void DecrementCounter(const Event event, Count const amount) { global_counters.Decrement(event, amount); }
Count GetCounterValue(const Event event) { return global_counters.GetCount(event); }

const char *GetCounterName(const Event event) {
  static const char *strings[] = {
  // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, TYPE, DOCUMENTATION) #NAME,
      APPLY_FOR_COUNTERS(M)
#undef M
  };

  return strings[event];
}

const char *GetCounterDocumentation(const Event event) {
  static const char *strings[] = {
  // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, TYPE, DOCUMENTATION) DOCUMENTATION,
      APPLY_FOR_COUNTERS(M)
#undef M
  };

  return strings[event];
}

const char *GetCounterType(const Event event) {
  static const char *strings[] = {
  // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define M(NAME, TYPE, DOCUMENTATION) #TYPE,
      APPLY_FOR_COUNTERS(M)
#undef M
  };

  return strings[event];
}

Event CounterEnd() { return END; }
}  // namespace memgraph::metrics

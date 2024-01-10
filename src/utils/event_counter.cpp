// Copyright 2024 Memgraph Ltd.
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
#define APPLY_FOR_COUNTERS(M)                                                                                        \
  M(ReadQuery, QueryType, "Number of read-only queries executed.")                                                   \
  M(WriteQuery, QueryType, "Number of write-only queries executed.")                                                 \
  M(ReadWriteQuery, QueryType, "Number of read-write queries executed.")                                             \
                                                                                                                     \
  M(OnceOperator, Operator, "Number of times Once operator was used.")                                               \
  M(CreateNodeOperator, Operator, "Number of times CreateNode operator was used.")                                   \
  M(CreateExpandOperator, Operator, "Number of times CreateExpand operator was used.")                               \
  M(ScanAllOperator, Operator, "Number of times ScanAll operator was used.")                                         \
  M(ScanAllByLabelOperator, Operator, "Number of times ScanAllByLabel operator was used.")                           \
  M(ScanAllByLabelPropertyRangeOperator, Operator, "Number of times ScanAllByLabelPropertyRange operator was used.") \
  M(ScanAllByLabelPropertyValueOperator, Operator, "Number of times ScanAllByLabelPropertyValue operator was used.") \
  M(ScanAllByLabelPropertyOperator, Operator, "Number of times ScanAllByLabelProperty operator was used.")           \
  M(ScanAllByIdOperator, Operator, "Number of times ScanAllById operator was used.")                                 \
  M(ExpandOperator, Operator, "Number of times Expand operator was used.")                                           \
  M(ExpandVariableOperator, Operator, "Number of times ExpandVariable operator was used.")                           \
  M(ConstructNamedPathOperator, Operator, "Number of times ConstructNamedPath operator was used.")                   \
  M(FilterOperator, Operator, "Number of times Filter operator was used.")                                           \
  M(ProduceOperator, Operator, "Number of times Produce operator was used.")                                         \
  M(DeleteOperator, Operator, "Number of times Delete operator was used.")                                           \
  M(SetPropertyOperator, Operator, "Number of times SetProperty operator was used.")                                 \
  M(SetPropertiesOperator, Operator, "Number of times SetProperties operator was used.")                             \
  M(SetLabelsOperator, Operator, "Number of times SetLabels operator was used.")                                     \
  M(RemovePropertyOperator, Operator, "Number of times RemoveProperty operator was used.")                           \
  M(RemoveLabelsOperator, Operator, "Number of times RemoveLabels operator was used.")                               \
  M(EdgeUniquenessFilterOperator, Operator, "Number of times EdgeUniquenessFilter operator was used.")               \
  M(EmptyResultOperator, Operator, "Number of times EmptyResult operator was used.")                                 \
  M(AccumulateOperator, Operator, "Number of times Accumulate operator was used.")                                   \
  M(AggregateOperator, Operator, "Number of times Aggregate operator was used.")                                     \
  M(SkipOperator, Operator, "Number of times Skip operator was used.")                                               \
  M(LimitOperator, Operator, "Number of times Limit operator was used.")                                             \
  M(OrderByOperator, Operator, "Number of times OrderBy operator was used.")                                         \
  M(MergeOperator, Operator, "Number of times Merge operator was used.")                                             \
  M(OptionalOperator, Operator, "Number of times Optional operator was used.")                                       \
  M(UnwindOperator, Operator, "Number of times Unwind operator was used.")                                           \
  M(DistinctOperator, Operator, "Number of times Distinct operator was used.")                                       \
  M(UnionOperator, Operator, "Number of times Union operator was used.")                                             \
  M(CartesianOperator, Operator, "Number of times Cartesian operator was used.")                                     \
  M(CallProcedureOperator, Operator, "Number of times CallProcedure operator was used.")                             \
  M(ForeachOperator, Operator, "Number of times Foreach operator was used.")                                         \
  M(EvaluatePatternFilterOperator, Operator, "Number of times EvaluatePatternFilter operator was used.")             \
  M(ApplyOperator, Operator, "Number of times ApplyOperator operator was used.")                                     \
  M(IndexedJoinOperator, Operator, "Number of times IndexedJoin operator was used.")                                 \
  M(HashJoinOperator, Operator, "Number of times HashJoin operator was used.")                                       \
                                                                                                                     \
  M(ActiveLabelIndices, Index, "Number of active label indices in the system.")                                      \
  M(ActiveLabelPropertyIndices, Index, "Number of active label property indices in the system.")                     \
                                                                                                                     \
  M(StreamsCreated, Stream, "Number of Streams created.")                                                            \
  M(MessagesConsumed, Stream, "Number of consumed streamed messages.")                                               \
                                                                                                                     \
  M(TriggersCreated, Trigger, "Number of Triggers created.")                                                         \
  M(TriggersExecuted, Trigger, "Number of Triggers executed.")                                                       \
                                                                                                                     \
  M(ActiveSessions, Session, "Number of active connections.")                                                        \
  M(ActiveBoltSessions, Session, "Number of active Bolt connections.")                                               \
  M(ActiveTCPSessions, Session, "Number of active TCP connections.")                                                 \
  M(ActiveSSLSessions, Session, "Number of active SSL connections.")                                                 \
  M(ActiveWebSocketSessions, Session, "Number of active websocket connections.")                                     \
  M(BoltMessages, Session, "Number of Bolt messages sent.")                                                          \
                                                                                                                     \
  M(ActiveTransactions, Transaction, "Number of active transactions.")                                               \
  M(CommitedTransactions, Transaction, "Number of committed transactions.")                                          \
  M(RollbackedTransactions, Transaction, "Number of rollbacked transactions.")                                       \
  M(FailedQuery, Transaction, "Number of times executing a query failed.")                                           \
  M(FailedPrepare, Transaction, "Number of times preparing a query failed.")                                         \
  M(FailedPull, Transaction, "Number of times executing a prepared query failed.")                                   \
  M(SuccessfulQuery, Transaction, "Number of successful queries.")

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

void EventCounters::Increment(const Event event, Count amount) {
  counters_[event].fetch_add(amount, std::memory_order_relaxed);
}

void EventCounters::Decrement(const Event event, Count amount) {
  counters_[event].fetch_sub(amount, std::memory_order_relaxed);
}

void IncrementCounter(const Event event, Count amount) { global_counters.Increment(event, amount); }
void DecrementCounter(const Event event, Count amount) { global_counters.Decrement(event, amount); }

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

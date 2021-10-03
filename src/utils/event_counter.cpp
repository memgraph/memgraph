// Copyright 2021 Memgraph Ltd.
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

#define APPLY_FOR_EVENTS(M)                                                                                \
  M(ReadQuery, "Number of read-only queries executed.")                                                    \
  M(WriteQuery, "Number of write-only queries executed.")                                                  \
  M(ReadWriteQuery, "Number of read-write queries executed.")                                              \
                                                                                                           \
  M(OnceOperator, "Number of times Once operator was used.")                                               \
  M(CreateNodeOperator, "Number of times CreateNode operator was used.")                                   \
  M(CreateExpandOperator, "Number of times CreateExpand operator was used.")                               \
  M(ScanAllOperator, "Number of times ScanAll operator was used.")                                         \
  M(ScanAllByLabelOperator, "Number of times ScanAllByLabel operator was used.")                           \
  M(ScanAllByLabelPropertyRangeOperator, "Number of times ScanAllByLabelPropertyRange operator was used.") \
  M(ScanAllByLabelPropertyValueOperator, "Number of times ScanAllByLabelPropertyValue operator was used.") \
  M(ScanAllByLabelPropertyOperator, "Number of times ScanAllByLabelProperty operator was used.")           \
  M(ScanAllByIdOperator, "Number of times ScanAllById operator was used.")                                 \
  M(ExpandOperator, "Number of times Expand operator was used.")                                           \
  M(ExpandVariableOperator, "Number of times ExpandVariable operator was used.")                           \
  M(ConstructNamedPathOperator, "Number of times ConstructNamedPath operator was used.")                   \
  M(FilterOperator, "Number of times Filter operator was used.")                                           \
  M(ProduceOperator, "Number of times Produce operator was used.")                                         \
  M(DeleteOperator, "Number of times Delete operator was used.")                                           \
  M(SetPropertyOperator, "Number of times SetProperty operator was used.")                                 \
  M(SetPropertiesOperator, "Number of times SetProperties operator was used.")                             \
  M(SetLabelsOperator, "Number of times SetLabels operator was used.")                                     \
  M(RemovePropertyOperator, "Number of times RemoveProperty operator was used.")                           \
  M(RemoveLabelsOperator, "Number of times RemoveLabels operator was used.")                               \
  M(EdgeUniquenessFilterOperator, "Number of times EdgeUniquenessFilter operator was used.")               \
  M(AccumulateOperator, "Number of times Accumulate operator was used.")                                   \
  M(AggregateOperator, "Number of times Aggregate operator was used.")                                     \
  M(SkipOperator, "Number of times Skip operator was used.")                                               \
  M(LimitOperator, "Number of times Limit operator was used.")                                             \
  M(OrderByOperator, "Number of times OrderBy operator was used.")                                         \
  M(MergeOperator, "Number of times Merge operator was used.")                                             \
  M(OptionalOperator, "Number of times Optional operator was used.")                                       \
  M(UnwindOperator, "Number of times Unwind operator was used.")                                           \
  M(DistinctOperator, "Number of times Distinct operator was used.")                                       \
  M(UnionOperator, "Number of times Union operator was used.")                                             \
  M(CartesianOperator, "Number of times Cartesian operator was used.")                                     \
  M(CallProcedureOperator, "Number of times CallProcedure operator was used.")                             \
                                                                                                           \
  M(FailedQuery, "Number of times executing a query failed.")                                              \
  M(LabelIndexCreated, "Number of times a label index was created.")                                       \
  M(LabelPropertyIndexCreated, "Number of times a label property index was created.")                      \
  M(StreamsCreated, "Number of Streams created.")                                                          \
  M(MessagesConsumed, "Number of consumed streamed messages.")                                             \
  M(TriggersCreated, "Number of Triggers created.")                                                        \
  M(TriggersExecuted, "Number of Triggers executed.")

namespace EventCounter {

// define every Event as an index in the array of counters
#define M(NAME, DOCUMENTATION) extern const Event NAME = __COUNTER__;
APPLY_FOR_EVENTS(M)
#undef M

constexpr Event END = __COUNTER__;

// Initialize array for the global counter with all values set to 0
Counter global_counters_array[END]{};
// Initialize global counters
EventCounters global_counters(global_counters_array);

const Event EventCounters::num_counters = END;

void EventCounters::Increment(const Event event, Count amount) {
  counters_[event].fetch_add(amount, std::memory_order_relaxed);
}

void IncrementCounter(const Event event, Count amount) { global_counters.Increment(event, amount); }

const char *GetName(const Event event) {
  static const char *strings[] = {
#define M(NAME, DOCUMENTATION) #NAME,
      APPLY_FOR_EVENTS(M)
#undef M
  };

  return strings[event];
}

const char *GetDocumentation(const Event event) {
  static const char *strings[] = {
#define M(NAME, DOCUMENTATION) DOCUMENTATION,
      APPLY_FOR_EVENTS(M)
#undef M
  };

  return strings[event];
}

Event End() { return END; }

}  // namespace EventCounter

// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <type_traits>

#include "io/local_transport/local_transport.hpp"
#include "query/v2/bindings/symbol_table.hpp"
#include "query/v2/common.hpp"
#include "query/v2/metadata.hpp"
#include "query/v2/parameters.hpp"
#include "query/v2/plan/profile.hpp"
//#include "query/v2/trigger.hpp"
#include "query/v2/shard_request_manager.hpp"
#include "utils/async_timer.hpp"

namespace memgraph::query::v2 {

struct EvaluationContext {
  /// Memory for allocations during evaluation of a *single* Pull call.
  ///
  /// Although the assigned memory may live longer than the duration of a Pull
  /// (e.g. memory is the same as the whole execution memory), you have to treat
  /// it as if the lifetime is only valid during the Pull.
  utils::MemoryResource *memory{utils::NewDeleteResource()};
  int64_t timestamp{-1};
  Parameters parameters;
  /// All properties indexable via PropertyIx
  std::vector<storage::v3::PropertyId> properties;
  /// All labels indexable via LabelIx
  std::vector<storage::v3::LabelId> labels;
  /// All counters generated by `counter` function, mutable because the function
  /// modifies the values
  mutable std::unordered_map<std::string, int64_t> counters;
};

inline std::vector<storage::v3::PropertyId> NamesToProperties(
    const std::vector<std::string> &property_names, msgs::ShardRequestManagerInterface *shard_request_manager) {
  std::vector<storage::v3::PropertyId> properties;
  properties.reserve(property_names.size());
  for (const auto &name : property_names) {
    properties.push_back(shard_request_manager->NameToProperty(name));
  }
  return properties;
}

inline std::vector<storage::v3::LabelId> NamesToLabels(const std::vector<std::string> &label_names,
                                                       msgs::ShardRequestManagerInterface *shard_request_manager) {
  std::vector<storage::v3::LabelId> labels;
  labels.reserve(label_names.size());
  for (const auto &name : label_names) {
    if (shard_request_manager != nullptr) {
      labels.push_back(shard_request_manager->LabelNameToLabelId(name));
    }
  }
  return labels;
}

struct ExecutionContext {
  DbAccessor *db_accessor{nullptr};
  SymbolTable symbol_table;
  EvaluationContext evaluation_context;
  std::atomic<bool> *is_shutting_down{nullptr};
  bool is_profile_query{false};
  std::chrono::duration<double> profile_execution_time;
  plan::ProfilingStats stats;
  plan::ProfilingStats *stats_root{nullptr};
  ExecutionStats execution_stats;
  //  TriggerContextCollector *trigger_context_collector{nullptr};
  utils::AsyncTimer timer;
  msgs::ShardRequestManagerInterface *shard_request_manager{nullptr};
};

static_assert(std::is_move_assignable_v<ExecutionContext>, "ExecutionContext must be move assignable!");
static_assert(std::is_move_constructible_v<ExecutionContext>, "ExecutionContext must be move constructible!");

inline bool MustAbort(const ExecutionContext &context) noexcept {
  return (context.is_shutting_down != nullptr && context.is_shutting_down->load(std::memory_order_acquire)) ||
         context.timer.IsExpired();
}

inline plan::ProfilingStatsWithTotalTime GetStatsWithTotalTime(const ExecutionContext &context) {
  return plan::ProfilingStatsWithTotalTime{context.stats, context.profile_execution_time};
}

}  // namespace memgraph::query::v2

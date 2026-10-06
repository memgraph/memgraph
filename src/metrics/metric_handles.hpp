// Copyright 2026 Memgraph Ltd.
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

#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>

#include "utils/logging.hpp"

namespace memgraph::metrics {

struct GaugeHandle {
  prometheus::Gauge *gauge{nullptr};

  void Increment(double v = 1.0) const noexcept {
    if (gauge) gauge->Increment(v);
  }

  void Decrement(double v = 1.0) const noexcept {
    if (gauge) gauge->Decrement(v);
  }

  void Set(double v) const noexcept {
    if (gauge) gauge->Set(v);
  }

  double Value() const noexcept { return gauge ? gauge->Value() : 0.0; }

  prometheus::Gauge *get() const {
    DMG_ASSERT(gauge);
    return gauge;
  }
};

struct CounterHandle {
  prometheus::Counter *counter{nullptr};

  void Increment(double v = 1.0) const noexcept {
    if (counter) counter->Increment(v);
  }

  double Value() const noexcept { return counter ? counter->Value() : 0.0; }

  prometheus::Counter *get() const {
    DMG_ASSERT(counter);
    return counter;
  }
};

struct HistogramHandle {
  prometheus::Histogram *histogram{nullptr};

  void Observe(double v) const {
    if (histogram) histogram->Observe(v);
  }

  auto Collect() const {
    DMG_ASSERT(histogram);
    return histogram->Collect();
  }

  prometheus::Histogram *get() const {
    DMG_ASSERT(histogram);
    return histogram;
  }
};

struct DatabaseMetricHandles {
  // Storage
  GaugeHandle vertex_count;
  GaugeHandle edge_count;
  GaugeHandle disk_usage_bytes;

  // Per-database memory tracking
  GaugeHandle db_memory_tracked_bytes;
  GaugeHandle db_peak_memory_tracked_bytes;
  GaugeHandle db_storage_memory_tracked_bytes;
  GaugeHandle db_embedding_memory_tracked_bytes;
  GaugeHandle db_query_memory_tracked_bytes;

  // Operators
  CounterHandle once_operator;
  CounterHandle create_node_operator;
  CounterHandle create_expand_operator;
  CounterHandle scan_all_operator;
  CounterHandle scan_all_by_label_operator;
  CounterHandle scan_all_by_label_properties_operator;
  CounterHandle scan_all_by_id_operator;
  CounterHandle scan_all_by_edge_operator;
  CounterHandle scan_all_by_edge_type_operator;
  CounterHandle scan_all_by_edge_type_property_operator;
  CounterHandle scan_all_by_edge_property_operator;
  CounterHandle scan_all_by_edge_id_operator;
  CounterHandle scan_all_by_vertex_property_operator;
  CounterHandle scan_all_by_point_distance_operator;
  CounterHandle scan_all_by_point_withinbbox_operator;
  CounterHandle expand_operator;
  CounterHandle expand_variable_operator;
  CounterHandle construct_named_path_operator;
  CounterHandle filter_operator;
  CounterHandle produce_operator;
  CounterHandle delete_operator;
  CounterHandle set_property_operator;
  CounterHandle set_properties_operator;
  CounterHandle set_labels_operator;
  CounterHandle remove_property_operator;
  CounterHandle remove_labels_operator;
  CounterHandle edge_uniqueness_filter_operator;
  CounterHandle empty_result_operator;
  CounterHandle accumulate_operator;
  CounterHandle aggregate_operator;
  CounterHandle skip_operator;
  CounterHandle limit_operator;
  CounterHandle order_by_operator;
  CounterHandle merge_operator;
  CounterHandle optional_operator;
  CounterHandle unwind_operator;
  CounterHandle distinct_operator;
  CounterHandle union_operator;
  CounterHandle cartesian_operator;
  CounterHandle call_procedure_operator;
  CounterHandle foreach_operator;
  CounterHandle evaluate_pattern_filter_operator;
  CounterHandle apply_operator;
  CounterHandle indexed_join_operator;
  CounterHandle hash_join_operator;
  CounterHandle roll_up_apply_operator;
  CounterHandle periodic_commit_operator;
  CounterHandle periodic_subquery_operator;
  CounterHandle set_nested_property_operator;
  CounterHandle remove_nested_property_operator;

  // Index
  GaugeHandle active_label_indices;
  GaugeHandle active_label_property_indices;
  GaugeHandle active_edge_type_indices;
  GaugeHandle active_edge_type_property_indices;
  GaugeHandle active_edge_property_indices;
  GaugeHandle active_vertex_property_indices;
  GaugeHandle active_point_indices;
  GaugeHandle active_text_indices;
  GaugeHandle active_text_edge_indices;
  GaugeHandle active_vector_indices;
  GaugeHandle active_vector_edge_indices;

  // Constraint
  GaugeHandle active_existence_constraints;
  GaugeHandle active_unique_constraints;
  GaugeHandle active_type_constraints;

  // Stream
  CounterHandle streams_created;
  CounterHandle messages_consumed;

  // Trigger
  CounterHandle triggers_created;
  CounterHandle triggers_executed;

  // Transaction
  GaugeHandle active_transactions;
  CounterHandle committed_transactions;
  CounterHandle rolled_back_transactions;
  CounterHandle failed_query;
  CounterHandle failed_prepare;
  CounterHandle failed_pull;
  CounterHandle successful_query;
  CounterHandle write_write_conflicts;
  CounterHandle transient_errors;
  GaugeHandle unreleased_delta_objects;

  // Query type
  CounterHandle read_query;
  CounterHandle write_query;
  CounterHandle read_write_query;

  // TTL
  CounterHandle deleted_nodes;
  CounterHandle deleted_edges;

  // SchemaInfo
  CounterHandle show_schema;

  // StorageInfo database specific
  CounterHandle show_storage_info;

  // Histograms
  HistogramHandle query_execution_latency_seconds;
  HistogramHandle snapshot_creation_latency_seconds;
  HistogramHandle snapshot_recovery_latency_seconds;
  HistogramHandle gc_latency_seconds;
  HistogramHandle gc_skiplist_cleanup_latency_seconds;
  // Counts individual indexes swept, not collection cycles, because what a cycle costs depends on
  // how many indexes it had any reason to look through.
  CounterHandle gc_index_sweeps;
};

}  // namespace memgraph::metrics

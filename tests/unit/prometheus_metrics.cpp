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

#include "metrics/prometheus_metrics.hpp"

#include <algorithm>

#include <gtest/gtest.h>
#include <prometheus/metric_family.h>

namespace {

// @TODO: can improve this!
std::optional<double> FindSample(std::vector<prometheus::MetricFamily> const &families, std::string_view name,
                                 std::string_view db_name) {
  for (auto const &family : families) {
    if (family.name != name) continue;
    for (auto const &metric : family.metric) {
      for (auto const &label : metric.label) {
        if (label.name == "database" && label.value == db_name) {
          if (!metric.gauge.value && !metric.counter.value) return std::nullopt;
          if (metric.gauge.value) return metric.gauge.value;
          return metric.counter.value;
        }
      }
    }
  }
  return std::nullopt;
}

}  // namespace

TEST(PrometheusMetrics, GetOrAddDatabaseRegistersMetrics) {
  memgraph::metrics::PrometheusMetrics pm;
  auto handles = pm.AddDatabase("db1");

  ASSERT_NE(handles.vertex_count, nullptr);
  ASSERT_NE(handles.committed_transactions, nullptr);

  handles.vertex_count->Set(42.0);
  handles.committed_transactions->Increment(5.0);

  auto const families = pm.registry().Collect();
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "db1"), 42.0);
  EXPECT_EQ(FindSample(families, "memgraph_committed_transactions_total", "db1"), 5.0);
}

TEST(PrometheusMetrics, MultipleDatabasesAreIsolated) {
  memgraph::metrics::PrometheusMetrics pm;
  auto h1 = pm.AddDatabase("db1");
  auto h2 = pm.AddDatabase("db2");

  h1.vertex_count->Set(10.0);
  h2.vertex_count->Set(20.0);

  auto const families = pm.registry().Collect();
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "db1"), 10.0);
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "db2"), 20.0);
}

TEST(PrometheusMetrics, RemoveDatabaseRemovesMetrics) {
  memgraph::metrics::PrometheusMetrics pm;
  auto handles = pm.AddDatabase("db1");
  handles.vertex_count->Set(99.0);

  pm.RemoveDatabase(handles);

  auto const families = pm.registry().Collect();
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "db1"), std::nullopt);
}

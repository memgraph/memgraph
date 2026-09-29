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

#include "metrics/sharded_metrics.hpp"

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

using memgraph::metrics::ShardedMetricSet;

namespace {

constexpr int kThreads = 8;

template <typename F>
void RunThreads(int n, F const &f) {
  std::vector<std::jthread> threads;
  threads.reserve(n);
  for (int t = 0; t < n; ++t) threads.emplace_back([&f, t] { f(t); });
}

class ShardedMetricsTest : public ::testing::Test {
 protected:
  prometheus::Counter &NewCounter(std::string const &name) {
    return prometheus::BuildCounter().Name(name).Help(name).Register(registry_).Add({});
  }

  prometheus::Gauge &NewGauge(std::string const &name) {
    return prometheus::BuildGauge().Name(name).Help(name).Register(registry_).Add({});
  }

  prometheus::Histogram &NewHistogram(std::string const &name, std::vector<double> const &boundaries) {
    return prometheus::BuildHistogram().Name(name).Help(name).Register(registry_).Add({}, boundaries);
  }

  prometheus::Registry registry_;
};

}  // namespace

TEST_F(ShardedMetricsTest, CounterExactAcrossThreads) {
  constexpr int kAddsPerThread = 100'000;
  auto &counter = NewCounter("c");
  ShardedMetricSet set;
  auto const idx = set.BindCounter(&counter);
  set.Finalize();

  RunThreads(kThreads, [&](int) {
    for (int i = 0; i < kAddsPerThread; ++i) set.Add(idx, 1.0);
  });

  set.Fold();
  EXPECT_EQ(counter.Value(), static_cast<double>(kThreads) * kAddsPerThread);
  set.Fold();
  EXPECT_EQ(counter.Value(), static_cast<double>(kThreads) * kAddsPerThread);
}

TEST_F(ShardedMetricsTest, ConcurrentFoldLosesNothing) {
  constexpr int kAddsPerThread = 100'000;
  auto &counter = NewCounter("c");
  ShardedMetricSet set;
  auto const idx = set.BindCounter(&counter);
  set.Finalize();

  std::atomic<bool> writers_done{false};
  std::atomic<uint64_t> folds{0};
  std::jthread folder([&] {
    while (!writers_done.load(std::memory_order_acquire)) {
      set.Fold();
      folds.fetch_add(1, std::memory_order_relaxed);
    }
  });

  RunThreads(kThreads, [&](int) {
    for (int i = 0; i < kAddsPerThread; ++i) set.Add(idx, 1.0);
  });
  writers_done.store(true, std::memory_order_release);
  folder.join();

  // Folds racing with the writers only ever publish a prefix of the total.
  EXPECT_LE(counter.Value(), static_cast<double>(kThreads) * kAddsPerThread);
  set.Fold();
  EXPECT_EQ(counter.Value(), static_cast<double>(kThreads) * kAddsPerThread);
  EXPECT_EQ(set.Pending(idx), 0.0);
  EXPECT_GT(folds.load(), 0U);
}

TEST_F(ShardedMetricsTest, PendingTracksUnfoldedAdds) {
  auto &counter = NewCounter("c");
  ShardedMetricSet set;
  auto const idx = set.BindCounter(&counter);
  set.Finalize();

  EXPECT_EQ(set.Pending(idx), 0.0);
  set.Add(idx, 1.0);
  set.Add(idx, 2.5);
  set.Add(idx, 4.0);
  EXPECT_EQ(set.Pending(idx), 7.5);
  EXPECT_EQ(counter.Value(), 0.0);

  set.Fold();
  EXPECT_EQ(set.Pending(idx), 0.0);
  EXPECT_EQ(counter.Value(), 7.5);

  set.Add(idx, 3.0);
  EXPECT_EQ(set.Pending(idx), 3.0);
  set.Fold();
  EXPECT_EQ(set.Pending(idx), 0.0);
  EXPECT_EQ(counter.Value(), 10.5);
}

TEST_F(ShardedMetricsTest, GaugeIncDec) {
  constexpr int kIterations = 10'000;
  auto &balanced = NewGauge("balanced");
  auto &unbalanced = NewGauge("unbalanced");
  ShardedMetricSet set;
  auto const b_idx = set.BindGauge(&balanced);
  auto const u_idx = set.BindGauge(&unbalanced);
  set.Finalize();

  RunThreads(kThreads, [&](int) {
    for (int i = 0; i < kIterations; ++i) {
      set.Add(b_idx, 1.0);
      set.Add(b_idx, -1.0);
    }
    for (int i = 0; i < 3; ++i) set.Add(u_idx, 1.0);
  });
  set.Fold();
  EXPECT_EQ(balanced.Value(), 0.0);
  EXPECT_EQ(unbalanced.Value(), 3.0 * kThreads);
}

TEST_F(ShardedMetricsTest, GaugeNegativeNetAcrossFolds) {
  auto &gauge = NewGauge("g");
  ShardedMetricSet set;
  auto const idx = set.BindGauge(&gauge);
  set.Finalize();

  for (int i = 0; i < 5; ++i) set.Add(idx, 1.0);
  set.Fold();
  EXPECT_EQ(gauge.Value(), 5.0);

  RunThreads(kThreads, [&](int) { set.Add(idx, -1.0); });
  set.Fold();
  EXPECT_EQ(gauge.Value(), 5.0 - kThreads);
  EXPECT_LT(gauge.Value(), 0.0);
}

TEST_F(ShardedMetricsTest, HistogramMatchesReference) {
  std::vector<double> const boundaries{1.0, 2.5, 5.0, 10.0};
  // Below the first boundary, 0, exactly on each boundary, between boundaries, and above the last.
  std::vector<double> const values{-1.0, 0.0, 0.5, 1.0, 2.0, 2.5, 3.0, 5.0, 7.5, 10.0, 10.5, 100.0};

  auto &reference = NewHistogram("reference", boundaries);
  auto &sharded = NewHistogram("sharded", boundaries);
  ShardedMetricSet set;
  auto const base = set.BindHistogram(&sharded, boundaries);
  set.Finalize();

  auto const expect_equal = [&] {
    auto const ref = reference.Collect().histogram;
    auto const got = sharded.Collect().histogram;
    EXPECT_EQ(got.sample_count, ref.sample_count);
    EXPECT_NEAR(got.sample_sum, ref.sample_sum, 1e-9);
    ASSERT_EQ(got.bucket.size(), boundaries.size() + 1);
    ASSERT_EQ(got.bucket.size(), ref.bucket.size());
    for (std::size_t i = 0; i < ref.bucket.size(); ++i) {
      EXPECT_EQ(got.bucket[i].cumulative_count, ref.bucket[i].cumulative_count) << "bucket " << i;
      EXPECT_EQ(got.bucket[i].upper_bound, ref.bucket[i].upper_bound) << "bucket " << i;
    }
  };

  // Two rounds so the second fold is checked against deltas, not the first fold's absolute values.
  for (int round = 0; round < 2; ++round) {
    for (int t = 0; t < kThreads; ++t) {
      for (auto const v : values) reference.Observe(v);
    }
    RunThreads(kThreads, [&](int) {
      for (auto const v : values) set.Observe(base, v);
    });
    set.Fold();
    expect_equal();
  }

  auto const got = sharded.Collect().histogram;
  EXPECT_EQ(got.sample_count, 2U * kThreads * values.size());
  // le="1" holds -1, 0, 0.5 and 1 (upper-inclusive).
  EXPECT_EQ(got.bucket.front().cumulative_count, 2U * kThreads * 4);
}

TEST_F(ShardedMetricsTest, MultipleBindingsAreIsolated) {
  std::vector<double> const boundaries{1.0, 2.0};
  auto &c1 = NewCounter("c1");
  auto &c2 = NewCounter("c2");
  auto &gauge = NewGauge("g");
  auto &hist = NewHistogram("h", boundaries);
  auto &c3 = NewCounter("c3");

  ShardedMetricSet set;
  auto const i1 = set.BindCounter(&c1);
  auto const i2 = set.BindCounter(&c2);
  auto const ig = set.BindGauge(&gauge);
  auto const ih = set.BindHistogram(&hist, boundaries);
  auto const i3 = set.BindCounter(&c3);
  set.Finalize();

  RunThreads(kThreads, [&](int) {
    set.Add(i1, 1.0);
    set.Add(i2, 10.0);
    set.Add(ig, -2.0);
    set.Observe(ih, 1.5);
    set.Add(i3, 100.0);
  });

  EXPECT_EQ(set.Pending(i1), 1.0 * kThreads);
  EXPECT_EQ(set.Pending(i2), 10.0 * kThreads);
  EXPECT_EQ(set.Pending(ig), -2.0 * kThreads);
  EXPECT_EQ(set.Pending(i3), 100.0 * kThreads);

  set.Fold();
  EXPECT_EQ(c1.Value(), 1.0 * kThreads);
  EXPECT_EQ(c2.Value(), 10.0 * kThreads);
  EXPECT_EQ(gauge.Value(), -2.0 * kThreads);
  EXPECT_EQ(c3.Value(), 100.0 * kThreads);

  auto const h = hist.Collect().histogram;
  EXPECT_EQ(h.sample_count, static_cast<uint64_t>(kThreads));
  EXPECT_DOUBLE_EQ(h.sample_sum, 1.5 * kThreads);
  ASSERT_EQ(h.bucket.size(), 3U);
  EXPECT_EQ(h.bucket[0].cumulative_count, 0U);
  EXPECT_EQ(h.bucket[1].cumulative_count, static_cast<uint64_t>(kThreads));
  EXPECT_EQ(h.bucket[2].cumulative_count, static_cast<uint64_t>(kThreads));
}

TEST_F(ShardedMetricsTest, UnfinalizedSetIsNoOp) {
  ShardedMetricSet empty;
  empty.Add(0, 1.0);
  empty.Observe(0, 1.0);
  empty.Fold();
  EXPECT_EQ(empty.Pending(0), 0.0);

  auto &counter = NewCounter("c");
  auto &hist = NewHistogram("h", {1.0});
  ShardedMetricSet bound_only;
  auto const idx = bound_only.BindCounter(&counter);
  auto const base = bound_only.BindHistogram(&hist, {1.0});
  bound_only.Add(idx, 5.0);
  bound_only.Observe(base, 0.5);
  bound_only.Fold();
  EXPECT_EQ(bound_only.Pending(idx), 0.0);
  EXPECT_EQ(counter.Value(), 0.0);
  EXPECT_EQ(hist.Collect().histogram.sample_count, 0U);
}

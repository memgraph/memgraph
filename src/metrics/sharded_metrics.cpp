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

#include <functional>
#include <ranges>

namespace memgraph::metrics {

namespace detail {
std::size_t AssignThisThreadMetricShard() noexcept {
  // Relaxed: only uniqueness of the ticket matters, it publishes no data.
  static std::atomic<std::size_t> next_shard{0};
  auto const shard = next_shard.fetch_add(1, std::memory_order_relaxed) & (kMetricShards - 1);
  this_thread_metric_shard = shard;
  return shard;
}
}  // namespace detail

uint32_t ShardedMetricSet::BindCounter(prometheus::Counter *counter) {
  MG_ASSERT(!lines_ && counter, "BindCounter requires a counter and must precede Finalize");
  auto const idx = num_slots_++;
  counters_.push_back({counter, idx});
  return idx;
}

uint32_t ShardedMetricSet::BindGauge(prometheus::Gauge *gauge) {
  MG_ASSERT(!lines_ && gauge, "BindGauge requires a gauge and must precede Finalize");
  auto const idx = num_slots_++;
  gauges_.push_back({gauge, idx});
  return idx;
}

uint32_t ShardedMetricSet::BindHistogram(prometheus::Histogram *histogram, std::vector<double> bucket_boundaries) {
  MG_ASSERT(!lines_ && histogram, "BindHistogram requires a histogram and must precede Finalize");
  MG_ASSERT(std::ranges::adjacent_find(bucket_boundaries, std::greater_equal{}) == bucket_boundaries.end(),
            "Histogram bucket boundaries must be strictly increasing");
  auto const base = num_slots_;
  num_slots_ += static_cast<uint32_t>(bucket_boundaries.size()) + 2;
  histograms_.push_back({histogram, base, std::move(bucket_boundaries)});
  return base;
}

void ShardedMetricSet::Finalize() {
  MG_ASSERT(!lines_, "ShardedMetricSet finalized twice");
  lines_per_shard_ = std::max<std::size_t>(1, (num_slots_ + kSlotsPerLine - 1) / kSlotsPerLine);
  lines_ = std::make_unique<CacheLine[]>(kMetricShards * lines_per_shard_);
  folded_ = std::make_unique<std::atomic<double>[]>(num_slots_);
  slot_histogram_.assign(num_slots_, kNoHistogram);
  for (auto const &[i, h] : std::views::enumerate(histograms_)) {
    slot_histogram_[h.base] = static_cast<uint32_t>(i);
  }
}

void ShardedMetricSet::Add(uint32_t idx, double v) noexcept {
  if (!lines_) [[unlikely]] {
    return;
  }
  DMG_ASSERT(idx < num_slots_, "Sharded metric slot out of range");
  Slot(ThisThreadMetricShard(), idx).fetch_add(v, std::memory_order_relaxed);
}

void ShardedMetricSet::Observe(uint32_t base, double v) noexcept {
  if (!lines_) [[unlikely]] {
    return;
  }
  DMG_ASSERT(base < num_slots_ && slot_histogram_[base] != kNoHistogram, "Not a sharded histogram base slot");
  auto const &boundaries = histograms_[slot_histogram_[base]].boundaries;
  // Same bucket selection as prometheus::Histogram::Observe (upper-inclusive `le`; NaN lands in bucket 0).
  auto const bucket =
      static_cast<uint32_t>(std::lower_bound(boundaries.begin(), boundaries.end(), v) - boundaries.begin());
  auto const sum_slot = base + static_cast<uint32_t>(boundaries.size()) + 1;
  auto const shard = ThisThreadMetricShard();
  Slot(shard, base + bucket).fetch_add(1.0, std::memory_order_relaxed);
  Slot(shard, sum_slot).fetch_add(v, std::memory_order_relaxed);
}

void ShardedMetricSet::Fold() {
  if (!lines_) return;
  auto const lock = std::lock_guard{fold_mutex_};

  // Shard-major pass so each shard's lines are read sequentially.
  std::vector<double> sums(num_slots_, 0.0);
  for (std::size_t shard = 0; shard < kMetricShards; ++shard) {
    for (uint32_t idx = 0; idx < num_slots_; ++idx) {
      sums[idx] += Slot(shard, idx).load(std::memory_order_relaxed);
    }
  }

  auto take_delta = [&](uint32_t idx) {
    auto const delta = sums[idx] - folded_[idx].load(std::memory_order_relaxed);
    folded_[idx].store(sums[idx], std::memory_order_relaxed);
    return delta;
  };

  for (auto const &[counter, idx] : counters_) {
    if (auto const delta = take_delta(idx); delta > 0.0) counter->Increment(delta);
  }
  for (auto const &[gauge, idx] : gauges_) {
    if (auto const delta = take_delta(idx); delta != 0.0) gauge->Increment(delta);
  }
  std::vector<double> bucket_deltas;
  for (auto const &[histogram, base, boundaries] : histograms_) {
    // ObserveMultiple expects boundaries.size() + 1 per-bucket (non-cumulative) increments, +Inf last.
    auto const num_buckets = static_cast<uint32_t>(boundaries.size()) + 1;
    bucket_deltas.resize(num_buckets);
    bool changed = false;
    for (uint32_t b = 0; b < num_buckets; ++b) {
      bucket_deltas[b] = take_delta(base + b);
      changed |= bucket_deltas[b] != 0.0;
    }
    auto const sum_delta = take_delta(base + num_buckets);
    if (changed || sum_delta != 0.0) histogram->ObserveMultiple(bucket_deltas, sum_delta);
  }
}

double ShardedMetricSet::Pending(uint32_t idx) const noexcept {
  if (!lines_) return 0.0;
  DMG_ASSERT(idx < num_slots_, "Sharded metric slot out of range");
  double sum = 0.0;
  for (std::size_t shard = 0; shard < kMetricShards; ++shard) {
    sum += Slot(shard, idx).load(std::memory_order_relaxed);
  }
  return sum - folded_[idx].load(std::memory_order_relaxed);
}

}  // namespace memgraph::metrics

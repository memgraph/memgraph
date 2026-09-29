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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>

#include "utils/logging.hpp"

namespace memgraph::metrics {

inline constexpr std::size_t kMetricShards = 64;
static_assert((kMetricShards & (kMetricShards - 1)) == 0, "kMetricShards must be a power of two");

namespace detail {
inline constexpr std::size_t kUnassignedMetricShard = ~std::size_t{0};
// Constant-initialized so the hot path has no TLS init guard, only a sentinel compare.
inline thread_local std::size_t this_thread_metric_shard = kUnassignedMetricShard;
std::size_t AssignThisThreadMetricShard() noexcept;
}  // namespace detail

inline std::size_t ThisThreadMetricShard() noexcept {
  auto const shard = detail::this_thread_metric_shard;
  if (shard != detail::kUnassignedMetricShard) [[likely]] {
    return shard;
  }
  return detail::AssignThisThreadMetricShard();
}

// Per-thread-sharded accumulators for increment-only counters, inc/dec gauges and histograms, folded into the
// bound prometheus objects by Fold(). Each shard occupies whole cache lines, so a writer only touches lines of
// its own shard. Bind*/Finalize must complete before the set is shared with other threads.
class ShardedMetricSet {
 public:
  ShardedMetricSet() = default;
  ShardedMetricSet(ShardedMetricSet const &) = delete;
  ShardedMetricSet &operator=(ShardedMetricSet const &) = delete;
  ShardedMetricSet(ShardedMetricSet &&) = delete;
  ShardedMetricSet &operator=(ShardedMetricSet &&) = delete;
  ~ShardedMetricSet() = default;

  uint32_t BindCounter(prometheus::Counter *counter);
  // Only for gauges changed via Increment/Decrement; Set() cannot be sharded.
  uint32_t BindGauge(prometheus::Gauge *gauge);
  // `bucket_boundaries` must equal the boundaries `histogram` was built with. Occupies boundaries.size() + 2
  // slots starting at the returned base: one per finite bucket, +Inf, then the sum of observed values.
  uint32_t BindHistogram(prometheus::Histogram *histogram, std::vector<double> bucket_boundaries);
  void Finalize();

  void Add(uint32_t idx, double v) noexcept {
    if (!lines_) [[unlikely]] {
      return;
    }
    DMG_ASSERT(idx < num_slots_, "Sharded metric slot out of range");
    Slot(ThisThreadMetricShard(), idx).fetch_add(v, std::memory_order_relaxed);
  }

  void Observe(uint32_t base, double v) noexcept {
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

  // Pushes everything accumulated since the previous Fold() into the bound prometheus objects. Serialized
  // against other folders; safe concurrently with Add/Observe (a racing update is picked up by the next fold).
  void Fold();

  // Accumulated but not yet folded amount of `idx`. Approximate while a Fold() is in progress.
  double Pending(uint32_t idx) const noexcept;

 private:
  static constexpr std::size_t kSlotsPerLine = 8;
  static constexpr uint32_t kNoHistogram = ~uint32_t{0};

  struct alignas(64) CacheLine {
    std::atomic<double> slots[kSlotsPerLine]{};
  };

  static_assert(sizeof(CacheLine) == 64);
  static_assert(std::atomic<double>::is_always_lock_free);

  struct CounterBinding {
    prometheus::Counter *counter;
    uint32_t idx;
  };

  struct GaugeBinding {
    prometheus::Gauge *gauge;
    uint32_t idx;
  };

  struct HistogramBinding {
    prometheus::Histogram *histogram;
    uint32_t base;
    std::vector<double> boundaries;
  };

  std::atomic<double> &Slot(std::size_t shard, uint32_t idx) const noexcept {
    return lines_[(shard * lines_per_shard_) + (idx / kSlotsPerLine)].slots[idx % kSlotsPerLine];
  }

  uint32_t num_slots_{0};
  std::size_t lines_per_shard_{0};
  std::unique_ptr<CacheLine[]> lines_;  // kMetricShards * lines_per_shard_, shard-major

  std::vector<CounterBinding> counters_;
  std::vector<GaugeBinding> gauges_;
  std::vector<HistogramBinding> histograms_;
  std::vector<uint32_t> slot_histogram_;  // slot -> index into histograms_ for base slots, else kNoHistogram

  std::mutex fold_mutex_;
  // Written only under fold_mutex_; atomic so Pending() may read it without the lock.
  std::unique_ptr<std::atomic<double>[]> folded_;
};

}  // namespace memgraph::metrics

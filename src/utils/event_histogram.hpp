// Copyright 2023 Memgraph Ltd.
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

#include <cmath>
#include <utility>

#include "utils/logging.hpp"

namespace memgraph::metrics {
using Event = uint64_t;
using Value = uint64_t;
using Measurement = std::atomic<uint64_t>;

// This is a logarithmically bucketing histogram optimized
// for collecting network response latency distributions.
// It "compresses" values by mapping them to a point on a
// logarithmic curve, which serves as the bucket index. This
// compression technique allows for very accurate histograms
// (unlike what is the case for sampling or lossy probabilistic
// approaches) with the trade-off that we sacrifice around 1%
// precision.
//
// properties:
// * roughly 1% precision loss - can be higher for values
//   less than 100, so if measuring latency, generally do
//   so in microseconds.
// * ~32kb constant space, single allocation per Histogram.
// * Histogram::Percentile() will return 0 if there were no
//   samples measured yet.
class Histogram {
  // This is the number of buckets that observed values
  // will be logarithmically compressed into.
  constexpr static auto kSampleLimit = 4096;

  // This is roughly 1/error rate, where 100.0 is roughly
  // a 1% error bound for measurements. This is less true
  // for tiny measurements, but because we tend to measure
  // microseconds, it is usually over 100, which is where
  // the error bound starts to stabilize a bit. This has
  // been tuned to allow the maximum uint64_t to compress
  // within 4096 samples while still achieving a high accuracy.
  constexpr static auto kPrecision = 92.0;

  // samples_ stores per-bucket counts for measurements
  // that have been mapped to a specific uint64_t in
  // the "compression" logic below.
  std::vector<uint64_t> samples_ = {};

  std::vector<uint8_t> percentiles_;

  // count_ is the number of measurements that have been
  // included in this Histogram.
  Measurement count_ = 0;

  // sum_ is the summed value of all measurements that
  // have been included in this Histogram.
  Measurement sum_ = 0;

  std::mutex samples_mutex_;

 public:
  Histogram() {
    samples_.resize(kSampleLimit, 0);
    percentiles_ = {0, 25, 50, 75, 90, 100};
  }

  explicit Histogram(std::vector<uint8_t> percentiles) : percentiles_(std::move(percentiles)) {
    samples_.resize(kSampleLimit, 0);
  }

  uint64_t Count() const { return count_.load(std::memory_order_relaxed); }

  uint64_t Sum() const { return sum_.load(std::memory_order_relaxed); }

  std::vector<uint8_t> Percentiles() const { return percentiles_; }

  void Measure(uint64_t value) {
    // "compression" logic
    double boosted = 1.0 + static_cast<double>(value);
    double ln = std::log(boosted);
    double compressed = (kPrecision * ln) + 0.5;

    MG_ASSERT(compressed < kSampleLimit, "compressing value {} to {} is invalid", value, compressed);
    auto sample_index = static_cast<uint16_t>(compressed);

    count_.fetch_add(1, std::memory_order_relaxed);
    sum_.fetch_add(value, std::memory_order_relaxed);

    {
      std::lock_guard<std::mutex> lock(samples_mutex_);
      samples_[sample_index]++;
    }
  }

  std::vector<std::pair<uint64_t, uint64_t>> YieldPercentiles() const {
    std::vector<std::pair<uint64_t, uint64_t>> percentile_yield;
    percentile_yield.reserve(percentiles_.size());

    for (const auto percentile : percentiles_) {
      percentile_yield.emplace_back(percentile, Percentile(percentile));
    }

    return percentile_yield;
  }

  uint64_t Percentile(double percentile) const {
    MG_ASSERT(percentile <= 100.0, "percentiles must not exceed 100.0");
    MG_ASSERT(percentile >= 0.0, "percentiles must be greater than or equal to 0.0");

    auto count = Count();

    if (count == 0) {
      return 0;
    }

    const auto floated_count = static_cast<double>(count);
    const auto target = std::max(floated_count * percentile / 100.0, 1.0);

    auto scanned = 0.0;

    for (int i = 0; i < kSampleLimit; i++) {
      const auto samples_at_index = samples_[i];
      scanned += static_cast<double>(samples_at_index);
      if (scanned >= target) {
        // "decompression" logic
        auto floated = static_cast<double>(i);
        auto unboosted = floated / kPrecision;
        auto decompressed = std::exp(unboosted) - 1.0;
        return static_cast<uint64_t>(decompressed);
      }
    }

    LOG_FATAL("bug in Histogram::Percentile where it failed to return the {} percentile", percentile);
    return 0;
  }
};

class EventHistograms {
 public:
  explicit EventHistograms(Histogram *allocated_histograms) noexcept : histograms_(allocated_histograms) {}

  auto &operator[](const Event event) { return histograms_[event]; }

  const auto &operator[](const Event event) const { return histograms_[event]; }

  void Measure(Event event, Value value);

  static const Event num_histograms;

 private:
  Histogram *histograms_;
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern EventHistograms global_histograms;

void Measure(Event event, Value value);

const char *GetHistogramName(Event event);
const char *GetHistogramDocumentation(Event event);
const char *GetHistogramType(Event event);

Event HistogramEnd();
}  // namespace memgraph::metrics

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

// a histogram suitable for

#include <cmath>

namespace memgraph::utils {

// This is a logarithmically bucketing histogram optimized
// for collecting network response latency distributions.
//
// properties:
// * roughly 1% precision loss - can be higher for values
//   less than 100, so if measuring latency, generally do
//   so in microseconds.
// * ~40kb constant space, single allocation
// * Histogram::Percentile() can return NaN if there were no
//   collected samples yet.
class Histogram {
  std::vector<uint64_t> samples = {};

  // 5442 is what numeric_limits<double>::max() compresses to,
  // so by making 5443 sample slots, we have enough room for the
  // entire range of double measurements.
  constexpr static auto sample_limit = 5443;

  // This is roughly 1/error rate, where 100.0 is roughly
  // a 1% error bound for measurements. This is less true
  // for tiny measurements, but because we tend to measure
  // microseconds, it is usually over 100, which is where
  // the error bound starts to stabilize a bit.
  constexpr static auto precision = 100.0;

 public:
  uint64_t count = 0;
  uint64_t sum = 0;

  Histogram() {
    // set samples_ to 16k 0's
    samples.resize(sample_limit, 0);
  }

  void Measure(double value) {
    // "compression" logic
    auto boosted = 1.0 + value;
    auto ln = std::log(boosted);
    auto compressed = precision * ln + 0.5;
    auto sample_index = static_cast<uint16_t>(compressed);
    MG_ASSERT(sample_index < sample_limit);

    count++;
    samples[sample_index]++;
    sum += count;
  }

  double Percentile(double percentile) const {
    MG_ASSERT(percentile <= 100.0, "percentiles must not exceed 100.0");

    if (count == 0) {
      return NAN;
    }

    const auto floated_count = static_cast<double>(count);
    const auto target = std::max(floated_count * percentile / 100.0, 1.0);

    auto scanned = 0.0;

    for (int i = 0; i < sample_limit; i++) {
      const auto samples_at_index = samples[i];
      scanned += static_cast<double>(samples_at_index);
      if (scanned >= target) {
        // "decompression" logic
        auto floated = static_cast<double>(i);
        auto unboosted = floated / precision;
        auto decompressed = std::exp(unboosted) - 1.0;
        return decompressed;
      }
    }

    MG_ASSERT(false, "bug in Histogram::Percentile where it failed to return the {} percentile", percentile);
    return NAN;
  }
};

}  // namespace memgraph::utils

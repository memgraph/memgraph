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

#include <cmath>

#include "utils/logging.hpp"

namespace memgraph::utils {

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
  std::vector<uint64_t> samples = {};

  // This is the number of buckets that observed values
  // will be logarithmically compressed into.
  constexpr static auto sample_limit = 4096;

  // This is roughly 1/error rate, where 100.0 is roughly
  // a 1% error bound for measurements. This is less true
  // for tiny measurements, but because we tend to measure
  // microseconds, it is usually over 100, which is where
  // the error bound starts to stabilize a bit. This has
  // been tuned to allow the maximum uint64_t to compress
  // within 4096 samples while still achieving a high accuracy.
  constexpr static auto precision = 92.0;

 public:
  // count is the number of measurements that have been
  // included in this Histogram.
  uint64_t count = 0;

  // sum is the summed value of all measurements that
  // have been included in this Histogram.
  uint64_t sum = 0;

  Histogram() { samples.resize(sample_limit, 0); }

  void Measure(uint64_t value) {
    // "compression" logic
    double boosted = 1.0 + static_cast<double>(value);
    double ln = std::log(boosted);
    double compressed = (precision * ln) + 0.5;

    MG_ASSERT(compressed < sample_limit, "compressing value {} to {} is invalid", value, compressed);
    auto sample_index = static_cast<uint16_t>(compressed);

    count++;
    samples[sample_index]++;
    sum += value;
  }

  uint64_t Percentile(double percentile) const {
    MG_ASSERT(percentile <= 100.0, "percentiles must not exceed 100.0");
    MG_ASSERT(percentile >= 0.0, "percentiles must be greater than or equal to 0.0");

    if (count == 0) {
      return 0;
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
        return static_cast<uint64_t>(decompressed);
      }
    }

    MG_ASSERT(false, "bug in Histogram::Percentile where it failed to return the {} percentile", percentile);
    return 0;
  }
};

}  // namespace memgraph::utils

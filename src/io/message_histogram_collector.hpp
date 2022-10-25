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

#include <chrono>
#include <cmath>
#include <functional>
#include <typeinfo>
#include <unordered_map>

#include <boost/core/demangle.hpp>

#include "io/time.hpp"
#include "utils/logging.hpp"

namespace memgraph::io {

constexpr auto sample_limit = (16 * 1024) - 1;
constexpr auto precision = 100.0;

struct LatencyHistogramSummary {
  uint64_t count;
  Duration p0;
  Duration p50;
  Duration p75;
  Duration p90;
  Duration p95;
  Duration p975;
  Duration p99;
  Duration p999;
  Duration p9999;
  Duration p100;
  Duration sum;

  friend std::ostream &operator<<(std::ostream &in, const LatencyHistogramSummary &histo) {
    in << "LatencyHistogramSummary { \"count\": " << histo.count;
    in << ", \"p0\": " << histo.p0.count();
    in << ", \"p50\": " << histo.p50.count();
    in << ", \"p75\": " << histo.p75.count();
    in << ", \"p90\": " << histo.p90.count();
    in << ", \"p95\": " << histo.p95.count();
    in << ", \"p975\": " << histo.p975.count();
    in << ", \"p99\": " << histo.p99.count();
    in << ", \"p999\": " << histo.p999.count();
    in << ", \"p9999\": " << histo.p9999.count();
    in << ", \"p100\": " << histo.p100.count();
    in << ", \"sum\": " << histo.sum.count();
    in << " }";

    return in;
  }
};

struct Histogram {
  uint64_t count = 0;
  uint64_t sum = 0;
  std::vector<uint64_t> samples = {};

  Histogram() {
    // set samples_ to 16k 0's
    samples.resize(sample_limit, 0);
  }

  Duration Percentile(double percentile) const {
    MG_ASSERT(percentile <= 100.0, "percentiles must not exceed 100.0");

    if (count == 0) {
      return Duration::min();
    }

    const auto floated_count = static_cast<double>(count);
    const auto target = std::max(floated_count * percentile / 100.0, 1.0);

    auto scanned = 0.0;

    for (int i = 0; i <= sample_limit; i++) {
      const auto samples_at_index = samples[i];
      scanned += static_cast<double>(samples_at_index);
      if (scanned >= target) {
        auto floated = static_cast<double>(i);
        auto unboosted = floated / precision;
        auto microseconds = static_cast<int64_t>(std::exp(unboosted) - 1.0);

        return Duration(microseconds);
      }
    }

    return Duration::min();
  }
};

using TypeInfoRef = std::reference_wrapper<const std::type_info>;
struct Hasher {
  std::size_t operator()(TypeInfoRef code) const { return code.get().hash_code(); }
};

struct EqualTo {
  bool operator()(TypeInfoRef lhs, TypeInfoRef rhs) const { return lhs.get() == rhs.get(); }
};

class MessageHistogramCollector {
  std::unordered_map<TypeInfoRef, Histogram, Hasher, EqualTo> histograms_;

 public:
  void Measure(const std::type_info &type_info, const Duration &duration) {
    // TODO(tyler)
    auto count = duration.count();
    auto floated = static_cast<double>(count);
    auto boosted = 1.0 + floated;
    auto ln = std::log(boosted);
    auto compressed = precision * ln + 0.5;
    auto sample_index = static_cast<uint16_t>(compressed);
    MG_ASSERT(sample_index < sample_limit);

    auto &histo = histograms_[type_info];

    histo.count++;
    histo.samples[sample_index]++;
    histo.sum += count;
  }

  std::unordered_map<std::string, LatencyHistogramSummary> ResponseLatencies() {
    std::unordered_map<std::string, LatencyHistogramSummary> ret{};

    for (const auto &[type_id, histo] : histograms_) {
      std::string demangled_name = boost::core::demangle(type_id.get().name());

      LatencyHistogramSummary latency_histogram_summary{
          .count = histo.count,
          .p0 = histo.Percentile(0.0),
          .p50 = histo.Percentile(50.0),
          .p75 = histo.Percentile(75.0),
          .p90 = histo.Percentile(90.0),
          .p95 = histo.Percentile(95.0),
          .p975 = histo.Percentile(97.5),
          .p99 = histo.Percentile(99.0),
          .p999 = histo.Percentile(99.9),
          .p9999 = histo.Percentile(99.99),
          .p100 = histo.Percentile(100.0),
          .sum = Duration(histo.sum),
      };

      ret.emplace(demangled_name, latency_histogram_summary);
    }

    return ret;
  }
};

}  // namespace memgraph::io

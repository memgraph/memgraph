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
#include <unordered_map>

#include <boost/core/demangle.hpp>

#include "io/time.hpp"
#include "utils/histogram.hpp"
#include "utils/logging.hpp"
#include "utils/print_helpers.hpp"
#include "utils/type_info_ref.hpp"

namespace memgraph::io {

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
    in << "{ \"count\": " << histo.count;
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

struct LatencyHistogramSummaries {
  std::unordered_map<std::string, LatencyHistogramSummary> latencies;

  std::string SummaryTable() {
    std::string output = "";

    const auto row = [&output](const auto &c1, const auto &c2, const auto &c3, const auto &c4, const auto &c5,
                               const auto &c6, const auto &c7) {
      output +=
          fmt::format("{: >50} | {: >8} | {: >8} | {: >8} | {: >8} | {: >8} | {: >8}\n", c1, c2, c3, c4, c5, c6, c7);
    };
    row("name", "count", "min (μs)", "med (μs)", "p99 (μs)", "max (μs)", "sum (μs)");

    for (const auto &[name, histo] : latencies) {
      row(name, histo.count, histo.p0.count(), histo.p50.count(), histo.p99.count(), histo.p100.count(),
          histo.sum.count());
    }

    output += "\n";
    return output;
  }

  friend std::ostream &operator<<(std::ostream &in, const LatencyHistogramSummaries &histo) {
    using memgraph::utils::print_helpers::operator<<;
    in << histo.latencies;
    return in;
  }
};

class MessageHistogramCollector {
  std::unordered_map<utils::TypeInfoRef, utils::Histogram, utils::TypeInfoHasher, utils::TypeInfoEqualTo> histograms_;

 public:
  void Measure(const std::type_info &type_info, const Duration &duration) {
    auto &histo = histograms_[type_info];
    histo.Measure(duration.count());
  }

  LatencyHistogramSummaries ResponseLatencies() {
    std::unordered_map<std::string, LatencyHistogramSummary> ret{};

    for (const auto &[type_id, histo] : histograms_) {
      std::string demangled_name = "\"" + boost::core::demangle(type_id.get().name()) + "\"";

      LatencyHistogramSummary latency_histogram_summary{
          .count = histo.Count(),
          .p0 = Duration(static_cast<int64_t>(histo.Percentile(0.0))),
          .p50 = Duration(static_cast<int64_t>(histo.Percentile(50.0))),
          .p75 = Duration(static_cast<int64_t>(histo.Percentile(75.0))),
          .p90 = Duration(static_cast<int64_t>(histo.Percentile(90.0))),
          .p95 = Duration(static_cast<int64_t>(histo.Percentile(95.0))),
          .p975 = Duration(static_cast<int64_t>(histo.Percentile(97.5))),
          .p99 = Duration(static_cast<int64_t>(histo.Percentile(99.0))),
          .p999 = Duration(static_cast<int64_t>(histo.Percentile(99.9))),
          .p9999 = Duration(static_cast<int64_t>(histo.Percentile(99.99))),
          .p100 = Duration(static_cast<int64_t>(histo.Percentile(100.0))),
          .sum = Duration(histo.Sum()),
      };

      ret.emplace(demangled_name, latency_histogram_summary);
    }

    return LatencyHistogramSummaries{.latencies = ret};
  }
};

}  // namespace memgraph::io

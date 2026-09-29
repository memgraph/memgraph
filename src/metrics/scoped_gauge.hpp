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

#include <prometheus/gauge.h>

#include <utility>

#include "metrics/metric_handles.hpp"

namespace memgraph::metrics {

/// Non-owning RAII wrapper over a gauge. The gauge is incremented on construction and decremented when the
/// wrapper is destructed. A handle bound to a ShardedMetricSet goes through the set.
class ScopedGauge {
 public:
  ScopedGauge() = default;

  explicit ScopedGauge(prometheus::Gauge *gauge) : ScopedGauge(GaugeHandle{.gauge = gauge}) {}

  explicit ScopedGauge(GaugeHandle handle) : handle_(handle) { handle_.Increment(); }

  ~ScopedGauge() { handle_.Decrement(); }

  ScopedGauge(ScopedGauge const &) = delete;
  ScopedGauge &operator=(ScopedGauge const &) = delete;

  ScopedGauge(ScopedGauge &&other) noexcept : handle_(std::exchange(other.handle_, {})) {}

  ScopedGauge &operator=(ScopedGauge &&other) noexcept {
    if (this != &other) {
      handle_.Decrement();
      handle_ = std::exchange(other.handle_, {});
    }
    return *this;
  }

 private:
  GaugeHandle handle_{};
};

}  // namespace memgraph::metrics

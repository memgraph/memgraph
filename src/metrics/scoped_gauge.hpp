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

#include "metrics/metric_ptr.hpp"

namespace memgraph::metrics {

/// RAII wrapper that increments a prometheus::Gauge on construction and
/// decrements on destruction. Holds a shared reference to the gauge's
/// control block (MetricPtr), so the gauge pointer can be atomically
/// invalidated during metric rebind — the destructor becomes a safe no-op.
class ScopedGauge {
 public:
  ScopedGauge() = default;

  /// For per-database gauges (rebind-safe via shared control block).
  explicit ScopedGauge(MetricPtr<prometheus::Gauge> ref) : ref_(std::move(ref)) {
    if (auto *g = Load()) g->Increment();
  }

  /// For global gauges (process-lifetime, never rebound).
  explicit ScopedGauge(prometheus::Gauge *gauge) : ref_(gauge ? MakeMetricPtr(gauge) : nullptr) {
    if (gauge) gauge->Increment();
  }

  ~ScopedGauge() {
    if (auto *g = Load()) g->Decrement();
  }

  ScopedGauge(ScopedGauge const &) = delete;
  ScopedGauge &operator=(ScopedGauge const &) = delete;

  ScopedGauge(ScopedGauge &&other) noexcept : ref_(std::move(other.ref_)) {}

  ScopedGauge &operator=(ScopedGauge &&other) noexcept {
    if (this != &other) {
      if (auto *g = Load()) g->Decrement();
      ref_ = std::move(other.ref_);
    }
    return *this;
  }

 private:
  prometheus::Gauge *Load() const noexcept { return ref_ ? ref_->load(std::memory_order_acquire) : nullptr; }

  MetricPtr<prometheus::Gauge> ref_;
};

}  // namespace memgraph::metrics

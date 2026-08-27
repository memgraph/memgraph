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

#include <memory>

#include <prometheus/gauge.h>

namespace memgraph::metrics {

/// RAII wrapper over a prometheus::Gauge. The gauge is incremented on
/// construction and decremented when the wrapper is destructed. An optional
/// shared_ptr anchor keeps the owning object alive for the lifetime of the
/// wrapper, preventing use-after-free when metric handles are rebound.
class ScopedGauge {
 public:
  ScopedGauge() = default;

  explicit ScopedGauge(prometheus::Gauge *gauge) : gauge_(gauge) {
    if (gauge_) gauge_->Increment();
  }

  ScopedGauge(prometheus::Gauge *gauge, std::shared_ptr<void> anchor) : gauge_(gauge), anchor_(std::move(anchor)) {
    if (gauge_) gauge_->Increment();
  }

  ~ScopedGauge() {
    if (gauge_) gauge_->Decrement();
  }

  ScopedGauge(ScopedGauge const &) = delete;
  ScopedGauge &operator=(ScopedGauge const &) = delete;

  ScopedGauge(ScopedGauge &&other) noexcept : gauge_(other.gauge_), anchor_(std::move(other.anchor_)) {
    other.gauge_ = nullptr;
  }

  ScopedGauge &operator=(ScopedGauge &&other) noexcept {
    if (this != &other) {
      if (gauge_) gauge_->Decrement();
      gauge_ = other.gauge_;
      anchor_ = std::move(other.anchor_);
      other.gauge_ = nullptr;
    }
    return *this;
  }

 private:
  prometheus::Gauge *gauge_{nullptr};
  std::shared_ptr<void> anchor_;
};

}  // namespace memgraph::metrics

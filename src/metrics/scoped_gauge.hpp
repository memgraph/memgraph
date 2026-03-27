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

#include <utility>

#include <prometheus/gauge.h>

namespace metrics {

class ScopedGauge {
 public:
  explicit ScopedGauge(prometheus::Gauge *gauge) : gauge_(gauge) {
    if (gauge_) gauge_->Increment();
  }

  ScopedGauge() = default;

  ~ScopedGauge() { release(); }

  ScopedGauge(ScopedGauge &&other) noexcept : gauge_(std::exchange(other.gauge_, nullptr)) {}

  ScopedGauge &operator=(ScopedGauge &&other) noexcept {
    release();
    gauge_ = std::exchange(other.gauge_, nullptr);
    return *this;
  }

  ScopedGauge(ScopedGauge const &) = delete;
  ScopedGauge &operator=(ScopedGauge const &) = delete;

  void release() {
    if (gauge_) {
      gauge_->Decrement();
      gauge_ = nullptr;
    }
  }

 private:
  prometheus::Gauge *gauge_{nullptr};
};

}  // namespace metrics

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

#include <chrono>

#include <prometheus/histogram.h>

namespace memgraph::metrics {

class ScopedHistogramTimer {
 public:
  explicit ScopedHistogramTimer(prometheus::Histogram *histogram)
      : histogram_(histogram), start_time_(std::chrono::high_resolution_clock::now()) {}

  ~ScopedHistogramTimer() {
    if (histogram_) {
      histogram_->Observe(
          std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - start_time_).count());
    }
  }

  ScopedHistogramTimer(ScopedHistogramTimer const &) = delete;
  ScopedHistogramTimer &operator=(ScopedHistogramTimer const &) = delete;

  ScopedHistogramTimer(ScopedHistogramTimer &&other) noexcept
      : histogram_(other.histogram_), start_time_(other.start_time_) {
    other.histogram_ = nullptr;
  }

  ScopedHistogramTimer &operator=(ScopedHistogramTimer &&other) noexcept {
    if (this != &other) {
      histogram_ = other.histogram_;
      start_time_ = other.start_time_;
      other.histogram_ = nullptr;
    }
    return *this;
  }

 private:
  prometheus::Histogram *histogram_;
  std::chrono::high_resolution_clock::time_point start_time_;
};

}  // namespace memgraph::metrics

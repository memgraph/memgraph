// Copyright 2025 Memgraph Ltd.
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

#include "utils/event_histogram.hpp"

#include <chrono>

namespace memgraph::utils {

template <typename TDuration = std::chrono::microseconds>
class MetricsTimer {
 public:
  explicit MetricsTimer(const metrics::Event event_type)
      : event_type_(event_type), start_time_(std::chrono::high_resolution_clock::now()) {}

  ~MetricsTimer() {
    metrics::Measure(
        event_type_,
        std::chrono::duration_cast<TDuration>(std::chrono::high_resolution_clock::now() - start_time_).count());
  }

 private:
  metrics::Event event_type_;
  std::chrono::high_resolution_clock::time_point start_time_;
};

}  // namespace memgraph::utils

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
#include <functional>
#include <optional>
#include <thread>

#include "utils/logging.hpp"
#include "utils/timer.hpp"

namespace utils {

// TSC stands for Time-Stamp Counter

uint64_t ReadTSC();

std::optional<double> GetTSCFrequency();

/// Class that is used to measure elapsed time using the TSC directly. It has
/// almost zero overhead and is appropriate for use in performance critical
/// paths.
class TSCTimer {
 public:
  TSCTimer() {}

  explicit TSCTimer(std::optional<double> frequency) : frequency_(frequency) {
    if (!frequency_) return;
    start_value_ = utils::ReadTSC();
  }

  double Elapsed() const {
    if (!frequency_) return 0.0;
    auto current_value = utils::ReadTSC();
    auto delta = current_value - start_value_;
    return static_cast<double>(delta) / *frequency_;
  }

 private:
  std::optional<double> frequency_;
  unsigned long long start_value_{0};
};

}  // namespace utils

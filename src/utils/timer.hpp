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

namespace memgraph::utils {

// This class is threadsafe.
class Timer {
 public:
  Timer() : start_time_(std::chrono::steady_clock::now()) {}

  template <typename TDuration = std::chrono::duration<double>>
  TDuration Elapsed() const {
    return std::chrono::duration_cast<TDuration>(std::chrono::steady_clock::now() - start_time_);
  }

 private:
  std::chrono::steady_clock::time_point start_time_;
};

}  // namespace memgraph::utils

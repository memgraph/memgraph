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

#include <algorithm>
#include <chrono>

class Timer {
 public:
  void Start() {
    duration_ = duration_.zero();
    start_time_ = std::chrono::steady_clock::now();
  }

  void Pause() {
    if (pause_ == 0) {
      duration_ += std::chrono::steady_clock::now() - start_time_;
    }
    ++pause_;
  }

  void Resume() {
    if (pause_ == 1) {
      start_time_ = std::chrono::steady_clock::now();
    }
    pause_ = std::max(0, pause_ - 1);
  }

  template <class TFun>
  auto WithPause(const TFun &fun) {
    Pause();
    auto ret = fun();
    Resume();
    return std::move(ret);
  }

  std::chrono::duration<double> Elapsed() {
    if (pause_ == 0) {
      return duration_ + (std::chrono::steady_clock::now() - start_time_);
    }
    return duration_;
  }

 private:
  std::chrono::duration<double> duration_;
  std::chrono::time_point<std::chrono::steady_clock> start_time_;
  int pause_ = 0;
};

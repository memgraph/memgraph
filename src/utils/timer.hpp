// Copyright 2023 Memgraph Ltd.
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
template <typename TTime = std::chrono::duration<double>>
class Timer {
 public:
  Timer(std::function<void(decltype(std::declval<TTime>().count()) elapsed)> destroy_callback = nullptr)
      : start_time_(std::chrono::steady_clock::now()), destroy_callback_(destroy_callback) {}

  template <typename TDuration = std::chrono::duration<double>>
  TDuration Elapsed() const {
    return std::chrono::duration_cast<TDuration>(std::chrono::steady_clock::now() - start_time_);
  }

  ~Timer() {
    if (destroy_callback_) {
      destroy_callback_(Elapsed<TTime>().count());
    }
  }

 private:
  std::chrono::steady_clock::time_point start_time_;
  std::function<void(decltype(std::declval<TTime>().count()))> destroy_callback_ = nullptr;
};

#define __MG_RAII_TIMER(name, message) \
  memgraph::utils::Timer<> name([](auto elapsed) { spdlog::critical("{} {}s", message, elapsed); })
// TODO(gitbuda): Swap MG_TIMER defines
#ifdef MG_TIMER
#define MG_RAII_TIMER(name, message)
#else
#define MG_RAII_TIMER(name, message) __MG_RAII_TIMER(name, message)
#endif

}  // namespace memgraph::utils

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
#include <functional>

namespace memgraph::utils {

// TODO(gitbuda): Figure out the relation with tsc.hpp
// TODO(gitbuda): Consider adding system_clock as an option

// This class is threadsafe.
template <typename TTime = std::chrono::duration<double>>
class Timer {
 public:
  // TODO(gitbuda): Timer(TFun&& destroy_callback...
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
  // TODO(gitbuda): std::function is an overhead, replace with TFun
  std::function<void(decltype(std::declval<TTime>().count()))> destroy_callback_ = nullptr;
};

// The intention with the macro is to have an easy way to probe how long a
// certain code block takes. It should be a short living piece of code. For
// something that has to stay longer in the code, please use the Timer
// directly.
// TODO(gitbuda): Maybe a desired property would be to at any given time, turn
// some of them ON/OFF (not all).
//
#define MG_RAII_TIMER(name, message) \
  memgraph::utils::Timer<> name([&](auto elapsed) { spdlog::critical("{} {}s", message, elapsed); })

}  // namespace memgraph::utils

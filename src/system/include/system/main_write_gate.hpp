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
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>

namespace memgraph::system {

// Admits system writes, each from its local write through its replication, while the instance may write as main.
// A demotion closes it and waits for the writes already admitted, so none of them commits after the role changes.
class MainWriteGate {
  struct Leave {
    void operator()(MainWriteGate *gate) const {
      auto const lock = std::lock_guard{gate->mtx_};
      if (--gate->admitted_ == 0) gate->drained_.notify_all();
    }
  };

 public:
  using Pass = std::unique_ptr<MainWriteGate, Leave>;

  auto TryEnter() -> Pass {
    auto const lock = std::lock_guard{mtx_};
    if (!open_) return nullptr;
    ++admitted_;
    return Pass{this};
  }

  // The gate stays closed when the drain times out: the demotion is already decided and is retried.
  auto CloseAndDrain(std::chrono::milliseconds const timeout) -> bool {
    auto lock = std::unique_lock{mtx_};
    open_ = false;
    return drained_.wait_for(lock, timeout, [this] { return admitted_ == 0; });
  }

  void Open() {
    auto const lock = std::lock_guard{mtx_};
    open_ = true;
  }

 private:
  std::mutex mtx_;
  std::condition_variable drained_;
  bool open_{true};
  uint32_t admitted_{0};
};

}  // namespace memgraph::system

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

#include "rpc/progress_heartbeat.hpp"

#include "rpc/protocol.hpp"
#include "rpc/utils.hpp"

#include <spdlog/spdlog.h>
#include <stdexcept>

namespace memgraph::rpc {

ProgressHeartbeat::ProgressHeartbeat() = default;

ProgressHeartbeat::ProgressHeartbeat(slk::Builder *res_builder, std::chrono::milliseconds const interval)
    : ProgressHeartbeat() {
  Start(res_builder, interval);
}

ProgressHeartbeat::~ProgressHeartbeat() {
  worker_.request_stop();
  cv_.notify_all();
  if (worker_.joinable()) worker_.join();
}

void ProgressHeartbeat::Start(slk::Builder *res_builder, std::chrono::milliseconds const interval) {
  auto lock = std::lock_guard{mtx_};
  if (active_) throw std::logic_error("ProgressHeartbeat is already active");
  if (!worker_.joinable()) {
    worker_ = std::jthread{[this](std::stop_token token) { Run(std::move(token)); }};
  }

  res_builder_ = res_builder;
  interval_ = interval;
  work_done_.store(false, std::memory_order_relaxed);
  peer_gone_.store(false, std::memory_order_relaxed);
  active_ = true;
  cv_.notify_all();
}

void ProgressHeartbeat::Stop() noexcept {
  auto lock = std::lock_guard{mtx_};
  if (!active_) return;

  active_ = false;
  res_builder_ = nullptr;
  work_done_.store(false, std::memory_order_relaxed);
  cv_.notify_all();
}

void ProgressHeartbeat::Run(std::stop_token token) {
  auto lock = std::unique_lock{mtx_};
  while (!token.stop_requested()) {
    cv_.wait(lock, token, [this] { return active_; });
    if (token.stop_requested()) return;

    auto const interval = interval_;
    // A notification caused by Stop() or a subsequent Start() resets the interval for the active RPC.
    if (cv_.wait_for(lock, token, interval, [this] { return !active_; })) continue;

    // Clearing as we read means the next tick only fires if the handler recorded something new in between. No new
    // work therefore means silence, which is what lets the peer's timeout still catch a wedged handler.
    if (!work_done_.exchange(false, std::memory_order_relaxed)) continue;

    try {
      SendInProgressMsg(res_builder_);
    } catch (SessionException const &e) {
      // Peer is gone, so there is nothing left to keep alive. Latch it so in-flight work can abandon early instead of
      // finishing a job whose result can no longer be delivered. Leave the worker idle for reuse by the next RPC.
      peer_gone_.store(true, std::memory_order_relaxed);
      active_ = false;
      res_builder_ = nullptr;
      spdlog::trace("Progress heartbeat stopped, peer connection is broken: {}", e.what());
    } catch (std::exception const &e) {
      // Heartbeats are advisory: a failed send must never abort the work it is reporting on. The builder resets its
      // write position even when the flush throws (see slk::Builder::FlushInternal), so the next tick starts clean.
      spdlog::trace("Failed to send progress heartbeat: {}", e.what());
    }
  }
}

}  // namespace memgraph::rpc

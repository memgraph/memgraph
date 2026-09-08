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

namespace memgraph::rpc {

ProgressHeartbeat::ProgressHeartbeat(slk::Builder *res_builder, std::chrono::milliseconds const interval)
    : res_builder_{res_builder} {
  worker_ = std::jthread{[this, interval](std::stop_token token) { Run(std::move(token), interval); }};
}

ProgressHeartbeat::~ProgressHeartbeat() { Stop(); }

void ProgressHeartbeat::Stop() noexcept {
  if (!worker_.joinable()) return;
  worker_.request_stop();
  cv_.notify_all();
  worker_.join();
}

void ProgressHeartbeat::Run(std::stop_token token, std::chrono::milliseconds const interval) {
  auto lock = std::unique_lock{mtx_};
  // wait_for returns the predicate's value: true means a stop was requested, false means the interval elapsed.
  while (!cv_.wait_for(lock, token, interval, [&token] { return token.stop_requested(); })) {
    // Clearing as we read means the next tick only fires if the handler recorded something new in between. No new
    // work therefore means silence, which is what lets the peer's timeout still catch a wedged handler.
    if (!work_done_.exchange(false, std::memory_order_relaxed)) continue;

    try {
      SendInProgressMsg(res_builder_);
    } catch (SessionException const &e) {
      // Peer is gone, so there is nothing left to keep alive. Latch it so in-flight work can abandon early instead of
      // finishing a job whose result can no longer be delivered.
      peer_gone_.store(true, std::memory_order_relaxed);
      spdlog::trace("Progress heartbeat stopped, peer connection is broken: {}", e.what());
      return;
    } catch (std::exception const &e) {
      // Heartbeats are advisory: a failed send must never abort the work it is reporting on. The builder resets its
      // write position even when the flush throws (see slk::Builder::FlushInternal), so the next tick starts clean.
      spdlog::trace("Failed to send progress heartbeat: {}", e.what());
    }
  }
}

}  // namespace memgraph::rpc

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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stop_token>
#include <thread>

namespace memgraph::slk {
class Builder;
}  // namespace memgraph::slk

namespace memgraph::rpc {

// Keeps a long-running RPC alive while its handler is still doing work.
//
// A background thread emits InProgressRes on a fixed interval, but only when the handler recorded progress since the
// previous tick. A handler that stops progressing stops heartbeating, so the peer's read timeout still fires and a
// stuck peer is detected rather than masked by an unconditional keepalive.
//
// This thread is the only writer of InProgressRes for its builder. The socket write behind slk::Builder is
// unsynchronized, so a handler writing its final response at the same time would interleave segments on the wire.
// Stop() must therefore run before the final response is written; the destructor calls it as a backstop.
class ProgressHeartbeat {
 public:
  // min timeout of PrepareCommit,WalFiles,CurrentWal,SnapshotReq - 5s (buffer for network)
  static constexpr std::chrono::milliseconds kDefaultInterval{25000};

  explicit ProgressHeartbeat(slk::Builder *res_builder, std::chrono::milliseconds interval = kDefaultInterval);
  ~ProgressHeartbeat();

  ProgressHeartbeat(ProgressHeartbeat const &) = delete;
  ProgressHeartbeat &operator=(ProgressHeartbeat const &) = delete;
  ProgressHeartbeat(ProgressHeartbeat &&) = delete;
  ProgressHeartbeat &operator=(ProgressHeartbeat &&) = delete;

  // Records that the handler made progress. Sits on per-item paths (every delta, every vertex visited while
  // populating an index or validating a constraint.
  void RecordProgress() noexcept { work_done_.store(true, std::memory_order_relaxed); }

  // True once a heartbeat failed to reach the peer. Long-running work polls this to abandon early rather than finish
  // a job whose result can no longer be delivered.
  bool PeerGone() const noexcept { return peer_gone_.load(std::memory_order_relaxed); }

  // Joins the heartbeat thread. Idempotent. Must run before the handler writes its final response.
  void Stop() noexcept;

 private:
  void Run(std::stop_token token, std::chrono::milliseconds interval);

  slk::Builder *res_builder_;

  // Separate cache lines: the heartbeat thread clears work_done_ once per tick while population threads read
  // peer_gone_ on a per-item path, so sharing a line would let each tick invalidate the cancel check.
  alignas(64) std::atomic<bool> work_done_{false};
  alignas(64) std::atomic<bool> peer_gone_{false};

  std::mutex mtx_;
  std::condition_variable_any cv_;
  // Declared last so it starts only after every member it touches is initialised, and is joined first on destruction.
  std::jthread worker_;
};

}  // namespace memgraph::rpc

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

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <thread>
#include <vector>

#include "utils/priority_thread_pool.hpp"

namespace memgraph::communication::v2 {

// Object a slot keeps alive and hands to whoever claims its readiness.
class PollTarget : public utils::IdleRunnable {
 public:
  // The caller owns the claimed target. Submits it to the worker pool, which then runs it.
  virtual void Dispatch() = 0;
};

// Epoll set over adopted fds. Exactly-once delivery: EPOLLONESHOT plus a generation-checked CAS
// ARMED -> RUNNING. Whoever holds a target in RUNNING owns its fd until it re-arms or closes it.
//
// Slot states: RUNNING (owner is a task), ARMED (waiting for readiness), CLOSING (terminator won
// ARMED -> CLOSING), FREE. Only ARMED slots can be claimed or closed by a thread that is not the owner.
class EpollPoller final : public utils::IdlePoller {
 public:
  using Slot = uint64_t;  // (index << 32) | generation
  static constexpr Slot kInvalid = ~Slot{0};
  static constexpr size_t kMaxEventsPerPoll = 8;

  EpollPoller();
  ~EpollPoller() override;

  EpollPoller(const EpollPoller &) = delete;
  EpollPoller &operator=(const EpollPoller &) = delete;
  EpollPoller(EpollPoller &&) = delete;
  EpollPoller &operator=(EpollPoller &&) = delete;

  // Takes over fd (must be non-blocking). The slot starts RUNNING, owned by the caller, and disarmed.
  // On failure returns kInvalid and does not touch fd.
  [[nodiscard]] Slot Adopt(int fd, std::shared_ptr<PollTarget> target);

  // Owner only (RUNNING -> ARMED). Owner's last touch of the session: once this returns true another
  // thread may own it. Returns false (and stays RUNNING) if the kernel refused the re-arm.
  [[nodiscard]] bool Arm(Slot slot);

  // Non-owner close: succeeds only if the slot is ARMED. On success the caller must call Close().
  [[nodiscard]] bool TryBeginClose(Slot slot);

  // Owner (RUNNING) or the TryBeginClose winner: deregisters, shuts down and closes the fd, frees the
  // slot. Returns the keep-alive reference so the caller decides where the last ref drops.
  std::shared_ptr<PollTarget> Close(Slot slot);

  // Waits up to timeout_ms (-1: forever) and claims every ready slot (up to out.size() <= kMaxEventsPerPoll).
  // drain_wake: consume the wake eventfd (only the blocking poller does). Returns the number claimed.
  size_t PollOnce(int timeout_ms, bool drain_wake, std::span<std::shared_ptr<PollTarget>> out);

  // utils::IdlePoller: non-blocking, at most one caller polls at a time. The first claimed target is
  // returned; the others are handed to the pool.
  std::shared_ptr<utils::IdleRunnable> TryClaim() override;

  // Fallback poller: one thread blocked in epoll_wait that dispatches claimed targets to the pool.
  void Start();
  // Idempotent; joins the fallback thread.
  void Stop();

  // Force-closes every live slot, whatever its state. Only valid once no worker or poller thread can
  // run a session any more (pool joined, Stop() done).
  void CloseAll();

  void Wake();

 private:
  static constexpr uint8_t kFree = 0;
  static constexpr uint8_t kRunning = 1;
  static constexpr uint8_t kArmed = 2;
  static constexpr uint8_t kClosing = 3;

  static constexpr uint32_t kChunkShift = 10;
  static constexpr uint32_t kChunkSize = 1U << kChunkShift;
  static constexpr uint32_t kMaxChunks = 1U << 12;

  static constexpr uint64_t kWakeTag = ~uint64_t{0};

  static constexpr uint64_t Pack(uint32_t gen, uint8_t state) { return (uint64_t{gen} << 8) | state; }

  static constexpr uint32_t Gen(Slot slot) { return static_cast<uint32_t>(slot); }

  static constexpr uint32_t Index(Slot slot) { return static_cast<uint32_t>(slot >> 32); }

  struct alignas(64) Entry {
    std::atomic<uint64_t> word{0};  // generation << 8 | state; zero is (generation 0, kFree)
    int fd{-1};
    std::shared_ptr<PollTarget> keep;
  };

  // Chunks are never freed or moved until the poller is destroyed, so a stale event can always be
  // resolved to an Entry and rejected by its generation.
  Entry *Lookup(uint32_t index) const;
  std::shared_ptr<PollTarget> ClaimEvent(uint64_t tag);
  std::shared_ptr<PollTarget> CloseEntry(Entry &entry, uint32_t index, uint32_t gen);

  int epfd_{-1};
  int wake_fd_{-1};

  std::array<std::atomic<Entry *>, kMaxChunks> chunks_{};
  std::mutex alloc_mtx_;  // guards free_, next_index_ and chunk creation
  std::vector<uint32_t> free_;
  uint32_t next_index_{0};

  std::atomic_bool nb_token_{false};

  std::mutex stop_mtx_;
  std::atomic_bool stopping_{false};
  std::thread thread_;
};

}  // namespace memgraph::communication::v2

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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "utils/priority_thread_pool.hpp"

namespace memgraph::communication::v2 {

// Object a slot keeps alive and hands to whoever claims its readiness; the claimant owns it and runs it (RunInline)
// or submits it to the worker pool (Dispatch).
class PollTarget : public utils::IdleRunnable {
 public:
  // Called by CloseAll before the fd is closed so the session stops reporting itself connected.
  virtual void OnForceClosed() {}
};

// Epoll set over adopted fds. Exactly-once delivery: EPOLLONESHOT plus a generation-checked CAS ARMED -> RUNNING.
// Slot states: RUNNING (owner is a task), ARMED (awaiting readiness), CLOSING (a closer won), FREE; non-owners
// may only claim or close ARMED slots (CloseAll aside). Destroy only after the pool is joined and CloseAll ran.
class EpollPoller final : public utils::IdlePoller {
 public:
  using Slot = uint64_t;  // (index << 32) | generation
  static constexpr Slot kInvalid = ~Slot{0};

  EpollPoller();
  ~EpollPoller() override;

  EpollPoller(const EpollPoller &) = delete;
  EpollPoller &operator=(const EpollPoller &) = delete;
  EpollPoller(EpollPoller &&) = delete;
  EpollPoller &operator=(EpollPoller &&) = delete;

  // Takes over fd (must be non-blocking); the slot starts RUNNING, owned by the caller. Registered with EPOLLONESHOT
  // only, so ERR/HUP before the first Arm are dropped while RUNNING (Arm re-evaluates readiness).
  // On failure returns kInvalid and does not touch fd.
  [[nodiscard]] Slot Adopt(int fd, std::shared_ptr<PollTarget> target);

  // Owner only (RUNNING -> ARMED). True: the caller's last touch, another thread may own the session now (also if a
  // claimant took it first on a queued ERR/HUP). False: kernel refused, slot stays RUNNING. Reads entry.fd, so
  // serialise against Close/TryBeginClose of the slot (Session::arm_lock_) or fd reuse could retarget another session.
  [[nodiscard]] bool Arm(Slot slot);

  // Non-owner close: succeeds only if the slot is ARMED. On success the caller must call Close().
  [[nodiscard]] bool TryBeginClose(Slot slot);

  // Owner (RUNNING) or the TryBeginClose winner: deregisters, shuts down and closes the fd, frees the
  // slot. Returns the keep-alive reference so the caller decides where the last ref drops.
  std::shared_ptr<PollTarget> Close(Slot slot);

  // Waits up to timeout_ms (-1: forever) for one epoll event and claims its slot. Returns null on timeout, EINTR, a
  // wake event (drained iff drain_wake; only the blocking poller does), or a stale / not-ARMED tag.
  std::shared_ptr<PollTarget> PollOne(int timeout_ms, bool drain_wake);

  std::shared_ptr<utils::IdleRunnable> TryClaim() override;

  // Dispatches at most one claimed target per call.
  void WaitAndDispatch(std::chrono::milliseconds max) override;

  void Wake() override;

  // Force-closes every RUNNING/ARMED slot (OnForceClosed first); a slot already CLOSING is left to its winner, awaited
  // for up to ~1 s. RUNNING slots are taken without the owner's consent: only valid once the pool is joined.
  // Does not decrement the session metrics.
  void CloseAll();

  void LogStats() const;

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
  friend struct EpollPollerTestAccess;
  std::shared_ptr<PollTarget> CloseEntry(Entry &entry, uint32_t index, uint32_t gen);

  int epfd_{-1};
  int wake_fd_{-1};

  std::array<std::atomic<Entry *>, kMaxChunks> chunks_{};
  std::mutex alloc_mtx_;  // guards free_, next_index_ and chunk creation
  std::vector<uint32_t> free_;
  uint32_t next_index_{0};

  std::atomic<uint64_t> adopted_{0};
  std::atomic<uint64_t> inline_claims_{0};
  std::atomic<uint64_t> monitor_claims_{0};
};

}  // namespace memgraph::communication::v2

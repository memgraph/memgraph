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

#include "communication/v2/epoll_poller.hpp"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <thread>

#include <spdlog/spdlog.h>

#include "utils/logging.hpp"

namespace memgraph::communication::v2 {

namespace {
constexpr uint32_t kReArmEvents = EPOLLIN | EPOLLRDHUP | EPOLLONESHOT;
}  // namespace

EpollPoller::EpollPoller() {
  epfd_ = ::epoll_create1(EPOLL_CLOEXEC);
  MG_ASSERT(epfd_ >= 0, "epoll_create1 failed: {}", errno);
  wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  MG_ASSERT(wake_fd_ >= 0, "eventfd failed: {}", errno);
  epoll_event ev{};
  ev.events = EPOLLIN;  // level-triggered: stays readable until the blocking poller drains it
  ev.data.u64 = kWakeTag;
  MG_ASSERT(::epoll_ctl(epfd_, EPOLL_CTL_ADD, wake_fd_, &ev) == 0, "epoll_ctl(wake) failed: {}", errno);
}

EpollPoller::~EpollPoller() {
  CloseAll();
  for (auto &chunk : chunks_) {
    delete[] chunk.load(std::memory_order_relaxed);
  }
  ::close(wake_fd_);
  ::close(epfd_);
}

EpollPoller::Entry *EpollPoller::Lookup(const uint32_t index) const {
  const auto chunk_index = index >> kChunkShift;
  if (chunk_index >= kMaxChunks) return nullptr;
  auto *chunk = chunks_[chunk_index].load(std::memory_order_acquire);
  return chunk ? chunk + (index & (kChunkSize - 1)) : nullptr;
}

EpollPoller::Slot EpollPoller::Adopt(const int fd, std::shared_ptr<PollTarget> target) {
  const std::scoped_lock lock{alloc_mtx_};
  uint32_t index;
  if (!free_.empty()) {
    index = free_.back();
    free_.pop_back();
  } else {
    index = next_index_;
    const auto chunk_index = index >> kChunkShift;
    if (chunk_index >= kMaxChunks) return kInvalid;
    if (chunks_[chunk_index].load(std::memory_order_relaxed) == nullptr) {
      chunks_[chunk_index].store(new Entry[kChunkSize], std::memory_order_release);
    }
    ++next_index_;
  }

  auto &entry = *Lookup(index);
  const auto gen = static_cast<uint32_t>(entry.word.load(std::memory_order_relaxed) >> 8);
  entry.fd = fd;
  entry.keep = std::move(target);
  const Slot slot = (Slot{index} << 32) | gen;

  // Registered but disarmed: with no interest bits only the always-reported ERR/HUP can fire, and
  // an event that arrives while RUNNING is dropped (the next Arm() re-evaluates readiness).
  epoll_event ev{};
  ev.events = EPOLLONESHOT;
  ev.data.u64 = slot;
  if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) != 0) {
    entry.keep.reset();
    entry.fd = -1;
    free_.push_back(index);
    return kInvalid;
  }
  entry.word.store(Pack(gen, kRunning), std::memory_order_release);
  adopted_.fetch_add(1, std::memory_order_relaxed);
  return slot;
}

bool EpollPoller::Arm(const Slot slot) {
  auto &entry = *Lookup(Index(slot));
  const auto gen = Gen(slot);
  DMG_ASSERT(entry.word.load(std::memory_order_relaxed) == Pack(gen, kRunning), "Arm of a slot not owned by caller");

  // Publish ARMED before the MOD: the event may fire (and be claimed) the instant the MOD returns.
  entry.word.store(Pack(gen, kArmed), std::memory_order_release);
  epoll_event ev{};
  ev.events = kReArmEvents;
  ev.data.u64 = slot;
  if (::epoll_ctl(epfd_, EPOLL_CTL_MOD, entry.fd, &ev) == 0) return true;

  // An ERR/HUP queued while RUNNING can be claimed as soon as ARMED is published; then the claimant owns the slot
  // and this arm counts as done.
  auto expected = Pack(gen, kArmed);
  return !entry.word.compare_exchange_strong(
      expected, Pack(gen, kRunning), std::memory_order_acq_rel, std::memory_order_relaxed);
}

bool EpollPoller::TryBeginClose(const Slot slot) {
  auto &entry = *Lookup(Index(slot));
  const auto gen = Gen(slot);
  auto expected = Pack(gen, kArmed);
  return entry.word.compare_exchange_strong(
      expected, Pack(gen, kClosing), std::memory_order_acq_rel, std::memory_order_relaxed);
}

std::shared_ptr<PollTarget> EpollPoller::CloseEntry(Entry &entry, const uint32_t index, const uint32_t gen) {
  ::epoll_ctl(epfd_, EPOLL_CTL_DEL, entry.fd, nullptr);
  ::shutdown(entry.fd, SHUT_RDWR);
  ::close(entry.fd);
  entry.fd = -1;
  auto keep = std::move(entry.keep);
  entry.word.store(Pack(gen + 1, kFree), std::memory_order_release);
  const std::scoped_lock lock{alloc_mtx_};
  free_.push_back(index);
  return keep;
}

std::shared_ptr<PollTarget> EpollPoller::Close(const Slot slot) {
  auto &entry = *Lookup(Index(slot));
  const auto gen = Gen(slot);
  DMG_ASSERT(entry.word.load(std::memory_order_relaxed) == Pack(gen, kRunning) ||
                 entry.word.load(std::memory_order_relaxed) == Pack(gen, kClosing),
             "Close of a slot not owned by caller");
  return CloseEntry(entry, Index(slot), gen);
}

std::shared_ptr<PollTarget> EpollPoller::ClaimEvent(const uint64_t tag) {
  auto *entry = Lookup(Index(tag));
  if (!entry) return nullptr;
  const auto gen = Gen(tag);
  auto expected = Pack(gen, kArmed);
  if (!entry->word.compare_exchange_strong(
          expected, Pack(gen, kRunning), std::memory_order_acq_rel, std::memory_order_acquire)) {
    return nullptr;
  }
  return entry->keep;
}

std::shared_ptr<PollTarget> EpollPoller::PollOne(const int timeout_ms, const bool drain_wake) {
  epoll_event ev;
  if (::epoll_wait(epfd_, &ev, 1, timeout_ms) <= 0) return nullptr;  // timeout, or EINTR (callers re-check their loop)
  if (ev.data.u64 == kWakeTag) {
    if (drain_wake) {
      uint64_t value;
      [[maybe_unused]] const auto r = ::read(wake_fd_, &value, sizeof(value));
    }
    return nullptr;
  }
  return ClaimEvent(ev.data.u64);
}

std::shared_ptr<utils::IdleRunnable> EpollPoller::TryClaim() {
  // One event at most: other ready fds stay in epoll for the other pollers instead of queueing behind this one.
  // A readable (undrained) wake fd can take that slot; it is level-triggered and requeues behind the sessions.
  auto ready = PollOne(0, false);
  if (!ready) return nullptr;
  inline_claims_.fetch_add(1, std::memory_order_relaxed);
  return ready;
}

void EpollPoller::WaitAndDispatch(const std::chrono::milliseconds max) {
  // One event per wake (see TryClaim): a batch would serialise dispatch while spinning workers sit idle.
  const auto timeout_ms = static_cast<int>(std::clamp<std::chrono::milliseconds::rep>(max.count(), 0, INT_MAX));
  auto ready = PollOne(timeout_ms, true);
  if (!ready) return;
  monitor_claims_.fetch_add(1, std::memory_order_relaxed);
  ready->Dispatch();
}

void EpollPoller::LogStats() const {
  spdlog::info("Bolt poller stats: adopted={} inline_claims={} monitor_claims={}",
               adopted_.load(),
               inline_claims_.load(),
               monitor_claims_.load());
}

void EpollPoller::Wake() {
  const uint64_t one = 1;
  [[maybe_unused]] const auto r = ::write(wake_fd_, &one, sizeof(one));
}

void EpollPoller::CloseAll() {
  uint32_t end;
  {
    const std::scoped_lock lock{alloc_mtx_};
    end = next_index_;
  }
  for (uint32_t index = 0; index < end; ++index) {
    auto &entry = *Lookup(index);
    // Claim the slot against a concurrent TryBeginClose before touching fd/keep.
    auto word = entry.word.load(std::memory_order_acquire);
    bool won = false;
    while (static_cast<uint8_t>(word) == kRunning || static_cast<uint8_t>(word) == kArmed) {
      if (entry.word.compare_exchange_strong(word,
                                             Pack(static_cast<uint32_t>(word >> 8), kClosing),
                                             std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
        won = true;
        break;
      }
    }
    if (!won) continue;
    const auto gen = static_cast<uint32_t>(word >> 8);
    if (entry.keep) entry.keep->OnForceClosed();
    // Keep dropped after the slot is free so a ~Session cannot observe a half-closed slot.
    auto keep = CloseEntry(entry, index, gen);
    keep.reset();
  }

  // Foreign terminators (TryBeginClose winners) may still be inside CloseEntry; the chunks must outlive them.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
  for (uint32_t index = 0; index < end; ++index) {
    while (static_cast<uint8_t>(Lookup(index)->word.load(std::memory_order_acquire)) == kClosing &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::yield();
    }
  }
}

}  // namespace memgraph::communication::v2

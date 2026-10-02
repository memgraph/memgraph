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

#include <spdlog/spdlog.h>

#include "utils/logging.hpp"
#include "utils/priorities.hpp"
#include "utils/thread.hpp"

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
  Stop();
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
  std::lock_guard lock{alloc_mtx_};
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

  // No event can have fired for this arm, and callers serialize the non-owner close with Arm().
  auto expected = Pack(gen, kArmed);
  const bool reverted = entry.word.compare_exchange_strong(
      expected, Pack(gen, kRunning), std::memory_order_acq_rel, std::memory_order_relaxed);
  DMG_ASSERT(reverted, "Failed re-arm raced with a claim");
  (void)reverted;
  return false;
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
  std::lock_guard lock{alloc_mtx_};
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
    return nullptr;  // stale generation, or the session is not ARMED (already claimed / closed)
  }
  return entry->keep;
}

size_t EpollPoller::PollOnce(const int timeout_ms, const bool drain_wake, std::span<std::shared_ptr<PollTarget>> out) {
  std::array<epoll_event, kMaxEventsPerPoll> events;
  const auto cap = static_cast<int>(std::min(out.size(), events.size()));
  if (cap == 0) return 0;
  const int n = ::epoll_wait(epfd_, events.data(), cap, timeout_ms);
  if (n <= 0) return 0;  // timeout, or EINTR (callers re-check their loop condition)

  size_t claimed = 0;
  for (int i = 0; i < n; ++i) {
    const auto tag = events[i].data.u64;
    if (tag == kWakeTag) {
      if (drain_wake) {
        uint64_t value;
        [[maybe_unused]] const auto r = ::read(wake_fd_, &value, sizeof(value));
      }
      continue;
    }
    if (auto target = ClaimEvent(tag)) out[claimed++] = std::move(target);
  }
  return claimed;
}

std::shared_ptr<utils::IdleRunnable> EpollPoller::TryClaim() {
  // One event at most: other ready fds stay in epoll for the other pollers instead of queueing behind this one.
  std::array<std::shared_ptr<PollTarget>, 1> ready;
  const auto n = PollOnce(0, false, ready);
  if (n == 0) return nullptr;
  inline_claims_.fetch_add(1, std::memory_order_relaxed);
  return std::move(ready[0]);
}

void EpollPoller::Start(utils::HotMask *hot_mask) {
  std::lock_guard lock{stop_mtx_};
  if (thread_.joinable() || stopping_.load(std::memory_order_acquire)) return;
  hot_mask_ = (hot_mask && hot_mask->SingleWord()) ? hot_mask : nullptr;
  spdlog::info("Bolt poller fallback thread: {}",
               hot_mask_ ? "parks while a worker is HOT" : "always blocks in epoll_wait");
  thread_ = std::thread([this] {
    utils::ThreadSetName("bolt poll");
    std::array<std::shared_ptr<PollTarget>, kMaxEventsPerPoll> ready;
    while (!stopping_.load(std::memory_order_acquire)) {
      if (hot_mask_ && hot_mask_->AnyHot()) {
        fallback_parks_.fetch_add(1, std::memory_order_relaxed);
        hot_mask_->WaitUntilEmpty(stopping_);
        continue;
      }
      const auto n = PollOnce(-1, true, ready);
      fallback_claims_.fetch_add(n, std::memory_order_relaxed);
      for (size_t i = 0; i < n; ++i) {
        ready[i]->Dispatch();
        ready[i].reset();
      }
    }
  });
}

void EpollPoller::Stop() {
  std::lock_guard lock{stop_mtx_};
  stopping_.store(true, std::memory_order_release);
  if (thread_.joinable()) {
    if (hot_mask_) hot_mask_->WakeWaiter();
    Wake();
    thread_.join();
  }
}

void EpollPoller::LogStats() const {
  spdlog::info("Bolt poller claims: inline by workers {}, by fallback thread {}, fallback parks {}",
               inline_claims_.load(),
               fallback_claims_.load(),
               fallback_parks_.load());
}

void EpollPoller::Wake() {
  const uint64_t one = 1;
  [[maybe_unused]] const auto r = ::write(wake_fd_, &one, sizeof(one));
}

void EpollPoller::CloseAll() {
  uint32_t end;
  {
    std::lock_guard lock{alloc_mtx_};
    end = next_index_;
  }
  for (uint32_t index = 0; index < end; ++index) {
    auto &entry = *Lookup(index);
    const auto word = entry.word.load(std::memory_order_acquire);
    if (static_cast<uint8_t>(word) == kFree) continue;
    // Keep dropped after the slot is free so a ~Session cannot observe a half-closed slot.
    auto keep = CloseEntry(entry, index, static_cast<uint32_t>(word >> 8));
    keep.reset();
  }
}

}  // namespace memgraph::communication::v2

// Copyright 2024 Memgraph Ltd.
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

#include <time.h>
#include <algorithm>
#include <atomic>
#include <cstdint>

namespace memgraph::utils {
namespace {
/// A helper for RWSpinLock, allows a contended spin lock to yield to another thread.
struct yeilder {
  void operator()() noexcept {
#if defined(__i386__) || defined(__x86_64__)
    // TODO: make portable
    __builtin_ia32_pause();
#endif
    ++count;
    if (count > 8) [[unlikely]] {
      count = 0;
      nanosleep(&shortpause, nullptr);
      // Increase the backoff
      shortpause.tv_nsec = std::min<decltype(shortpause.tv_nsec)>(shortpause.tv_nsec << 1, 512);
    }
  }

 private:
  uint_fast32_t count{0};
  timespec shortpause = {.tv_sec = 0, .tv_nsec = 1};
};
}  // namespace

/**
 * A reader/writer spin lock.
 * Stores in a uint32_t,
 * 0x0000'0001 - is the write bit
 * rest of the bits hold the count for the number of current writers.
 * The lock is friendly to writers.
 * - writer lock() will wait for all readers to leave unlock_shared()
 * - new reader lock_shared() will wait until writer has unlock()
 **/
struct RWSpinLock {
  RWSpinLock() = default;

  void lock() {
    // spin: to grant the UNIQUE_LOCKED bit
    while (true) {
      // optimistic: assume we will be granted the lock
      auto const phase1 = std::atomic_ref{lock_status_}.fetch_or(UNIQUE_LOCKED, std::memory_order_acq_rel);
      // check: we were granted UNIQUE_LOCK and no current readers
      if (phase1 == 0) [[likely]]
        return;
      // check: we were granted UNIQUE_LOCK, but need to wait for readers
      if ((phase1 & UNIQUE_LOCKED) != UNIQUE_LOCKED) [[likely]]
        break;

      // spin: to wait for UNIQUE_LOCKED to be available
      auto maybe_yield = yeilder{};
      while (true) {
        auto const phase2 = std::atomic_ref{lock_status_}.load(std::memory_order_relaxed);
        // check: we are able to obtain UNIQUE_LOCK
        if ((phase2 & UNIQUE_LOCKED) != UNIQUE_LOCKED) [[likely]]
          break;
        maybe_yield();
      }
    }

    // spin: to wait for readers to leave
    auto maybe_yield = yeilder{};
    while (true) {
      auto const phase3 = std::atomic_ref{lock_status_}.load(std::memory_order_relaxed);
      // check: all readers have gone (leaving only the UNIQUE_LOCKED bit set)
      if (phase3 == UNIQUE_LOCKED) return;
      maybe_yield();
    }
  }

  void unlock() { std::atomic_ref{lock_status_}.fetch_and(~UNIQUE_LOCKED, std::memory_order_release); }

  void lock_shared() {
    while (true) {
      // optimistic: assume we will be granted the lock
      auto const phase1 = std::atomic_ref{lock_status_}.fetch_add(READER, std::memory_order_acquire);
      // check: we incremented reader count without the UNIQUE_LOCK already being held
      if ((phase1 & UNIQUE_LOCKED) != UNIQUE_LOCKED) [[likely]]
        return;
      // correct for our optimism, we shouldn't have modified the reader count
      std::atomic_ref{lock_status_}.fetch_sub(READER, std::memory_order_release);

      // spin: to wait for UNIQUE_LOCKED to be available
      auto maybe_yield = yeilder{};
      while (true) {
        auto const phase2 = std::atomic_ref{lock_status_}.load(std::memory_order_relaxed);
        // check: UNIQUE_LOCK was released
        if ((phase2 & UNIQUE_LOCKED) != UNIQUE_LOCKED) [[likely]]
          break;
        maybe_yield();
      }
    }
  }

  void unlock_shared() { std::atomic_ref{lock_status_}.fetch_sub(READER, std::memory_order_release); }

 private:
  using status_t = uint32_t;
  enum FLAGS : status_t {
    UNIQUE_LOCKED = 1,
    READER = 2,
  };

  // TODO: ATM not atomic, just used via atomic_ref, because the type needs to be movable into skip_list
  //       fix the design flaw and then make RWSpinLock a non-copy/non-move type
  status_t lock_status_ = 0;
};
}  // namespace memgraph::utils

// Copyright 2025 Memgraph Ltd.
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

#include <algorithm>
#include <cstdint>
#include <ctime>
#include <thread>

namespace memgraph::utils {

#if defined(__i386__) || defined(__x86_64__)
#define PAUSE __builtin_ia32_pause()
#elif defined(__aarch64__)
#define PAUSE asm volatile("YIELD")
#else
#error("no PAUSE/YIELD instructions for unknown architecture");
#endif

struct yielder {
  void operator()(const uint_fast32_t sleep_after = 8, const uint8_t sleep_increment = 2) noexcept {
    PAUSE;
    ++count;
    if (count > sleep_after) [[unlikely]] {
      count = 0;
      sleep(sleep_increment);
    }
  }

  bool operator()(auto &&f, const uint_fast32_t sleep_after = 8, const uint8_t sleep_increment = 2) noexcept {
    count = 0;
    while (++count < sleep_after) {
      if (f()) return true;
      PAUSE;
    }
    sleep(sleep_increment);
    return false;
  }

 private:
  void sleep(const uint8_t sleep_increment = 2) {
    if (sleep_increment == 0) {
      std::this_thread::yield();
      return;
    }
    nanosleep(&shortpause, nullptr);
    // Increase backoff
    shortpause.tv_nsec = std::min<decltype(shortpause.tv_nsec)>(shortpause.tv_nsec * sleep_increment, 512);
  }

  uint_fast32_t count{0};
  timespec shortpause = {.tv_sec = 0, .tv_nsec = 1};
};

#undef PAUSE

}  // namespace memgraph::utils

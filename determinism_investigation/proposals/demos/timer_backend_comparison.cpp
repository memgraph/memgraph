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

// Timer Backend Comparison Demo
// Compare different timer mechanisms for non-determinism event counts
//
// Build: g++ -std=c++20 -O2 -g -pthread -o timer_backend_comparison timer_backend_comparison.cpp -lrt
// Record: live-record -o cv_timer.undo ./timer_backend_comparison cv
// Analyze: udb cv_timer.undo -ex "info events" -ex "quit"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

// Linux headers for timerfd and POSIX timers
#include <signal.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <time.h>
#include <unistd.h>

using namespace std::chrono;
using namespace std::chrono_literals;

// Test parameters: 5 tasks at 100ms interval
constexpr int NUM_ITERATIONS = 5;
constexpr auto INTERVAL = 100ms;

//=============================================================================
// Approach 1: Condition Variable with steady_clock (current Memgraph approach)
//=============================================================================
class CVTimer {
 public:
  void Run(std::function<void()> callback, milliseconds interval, int iterations) {
    std::mutex mtx;
    std::condition_variable cv;
    bool stop = false;

    auto next = steady_clock::now() + interval;

    for (int i = 0; i < iterations && !stop; ++i) {
      {
        std::unique_lock lock(mtx);
        cv.wait_until(lock, next, [&] { return stop; });
      }
      callback();
      next += interval;
    }
  }
};

//=============================================================================
// Approach 2: timerfd with epoll (kernel-driven wake-ups)
//=============================================================================
class TimerFdTimer {
 public:
  void Run(std::function<void()> callback, milliseconds interval, int iterations) {
    // Create timerfd
    int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC);
    if (tfd == -1) {
      std::cerr << "timerfd_create failed: " << strerror(errno) << "\n";
      return;
    }

    // Set interval timer
    struct itimerspec its {};
    its.it_value.tv_sec = interval.count() / 1000;
    its.it_value.tv_nsec = (interval.count() % 1000) * 1000000;
    its.it_interval = its.it_value;  // Repeating

    if (timerfd_settime(tfd, 0, &its, nullptr) == -1) {
      std::cerr << "timerfd_settime failed: " << strerror(errno) << "\n";
      close(tfd);
      return;
    }

    // Read expirations
    for (int i = 0; i < iterations; ++i) {
      uint64_t expirations = 0;
      ssize_t s = read(tfd, &expirations, sizeof(expirations));
      if (s != sizeof(expirations)) {
        std::cerr << "timerfd read failed\n";
        break;
      }
      callback();
    }

    close(tfd);
  }
};

//=============================================================================
// Approach 3: POSIX timer with sigwaitinfo (similar to AsyncTimer)
//=============================================================================
class PosixTimer {
 public:
  void Run(std::function<void()> callback, milliseconds interval, int iterations) {
    // Block SIGRTMIN
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGRTMIN);
    pthread_sigmask(SIG_BLOCK, &set, nullptr);

    // Create timer
    timer_t timer_id;
    struct sigevent sev {};
    sev.sigev_notify = SIGEV_SIGNAL;
    sev.sigev_signo = SIGRTMIN;
    sev.sigev_value.sival_ptr = &timer_id;

    if (timer_create(CLOCK_MONOTONIC, &sev, &timer_id) == -1) {
      std::cerr << "timer_create failed: " << strerror(errno) << "\n";
      return;
    }

    // Set interval
    struct itimerspec its {};
    its.it_value.tv_sec = interval.count() / 1000;
    its.it_value.tv_nsec = (interval.count() % 1000) * 1000000;
    its.it_interval = its.it_value;  // Repeating

    if (timer_settime(timer_id, 0, &its, nullptr) == -1) {
      std::cerr << "timer_settime failed: " << strerror(errno) << "\n";
      timer_delete(timer_id);
      return;
    }

    // Wait for signals
    for (int i = 0; i < iterations; ++i) {
      siginfo_t info;
      int sig = sigwaitinfo(&set, &info);
      if (sig == -1) {
        std::cerr << "sigwaitinfo failed: " << strerror(errno) << "\n";
        break;
      }
      callback();
    }

    timer_delete(timer_id);
  }
};

//=============================================================================
// Main - run selected backend
//=============================================================================
int main(int argc, char *argv[]) {
  if (argc < 2) {
    std::cout << "Usage: " << argv[0] << " <cv|timerfd|posix>\n";
    std::cout << "\nBackends:\n";
    std::cout << "  cv     - condition_variable with steady_clock (current approach)\n";
    std::cout << "  timerfd - timerfd_create + read (kernel-driven)\n";
    std::cout << "  posix  - POSIX timer_create + sigwaitinfo\n";
    return 1;
  }

  std::string mode = argv[1];
  std::atomic<int> counter{0};
  auto callback = [&counter]() { counter++; };

  std::cout << "Running " << mode << " backend: " << NUM_ITERATIONS << " iterations at " << INTERVAL.count()
            << "ms interval\n";

  auto start = steady_clock::now();

  if (mode == "cv") {
    CVTimer timer;
    timer.Run(callback, INTERVAL, NUM_ITERATIONS);
  } else if (mode == "timerfd") {
    TimerFdTimer timer;
    timer.Run(callback, INTERVAL, NUM_ITERATIONS);
  } else if (mode == "posix") {
    PosixTimer timer;
    timer.Run(callback, INTERVAL, NUM_ITERATIONS);
  } else {
    std::cerr << "Unknown mode: " << mode << "\n";
    return 1;
  }

  auto elapsed = duration_cast<milliseconds>(steady_clock::now() - start);

  std::cout << "Completed " << counter.load() << " callbacks in " << elapsed.count() << "ms\n";

  return 0;
}

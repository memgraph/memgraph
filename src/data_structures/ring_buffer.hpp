// Copyright 2022 Memgraph Ltd.
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
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

#include "utils/logging.hpp"
#include "utils/spin_lock.hpp"

/**
 * A thread-safe ring buffer. Multi-producer, multi-consumer. Producers get
 * blocked if the buffer is full. Consumers get returnd a nullopt. First in
 * first out.
 *
 * @tparam TElement - type of element the buffer tracks.
 */
template <typename TElement>
class RingBuffer {
 public:
  explicit RingBuffer(int capacity) : capacity_(capacity) { buffer_ = std::make_unique<TElement[]>(capacity_); }

  RingBuffer(const RingBuffer &) = delete;
  RingBuffer(RingBuffer &&) = delete;
  RingBuffer &operator=(const RingBuffer &) = delete;
  RingBuffer &operator=(RingBuffer &&) = delete;

  ~RingBuffer() = default;

  /**
   * Emplaces a new element into the buffer. This call blocks until space in the
   * buffer is available. If multiple threads are waiting for space to become
   * available, there are no order-of-entrance guarantees.
   */
  template <typename... TArgs>
  void emplace(TArgs &&...args) {
    while (true) {
      {
        std::lock_guard<memgraph::utils::SpinLock> guard(lock_);
        if (size_ < capacity_) {
          buffer_[write_pos_++] = TElement(std::forward<TArgs>(args)...);
          write_pos_ %= capacity_;
          size_++;
          return;
        }
      }

      SPDLOG_WARN("RingBuffer full: worker waiting");

      // Sleep time determined using tests/benchmark/ring_buffer.cpp
      std::this_thread::sleep_for(std::chrono::microseconds(250));
    }
  }

  /**
   * Removes and returns the oldest element from the buffer. If the buffer is
   * empty, nullopt is returned.
   */
  std::optional<TElement> pop() {
    std::lock_guard<memgraph::utils::SpinLock> guard(lock_);
    if (size_ == 0) return std::nullopt;
    size_--;
    std::optional<TElement> result(std::move(buffer_[read_pos_++]));
    read_pos_ %= capacity_;
    return result;
  }

  /** Removes all elements from the buffer. */
  void clear() {
    std::lock_guard<memgraph::utils::SpinLock> guard(lock_);
    read_pos_ = 0;
    write_pos_ = 0;
    size_ = 0;
  }

 private:
  int capacity_;
  std::unique_ptr<TElement[]> buffer_;
  memgraph::utils::SpinLock lock_;
  int read_pos_{0};
  int write_pos_{0};
  int size_{0};
};

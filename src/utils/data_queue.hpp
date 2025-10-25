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

#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>

// Same as rocksdb/util/work_queue.h but moves in the pop()

namespace memgraph::utils {

template <typename T>
class DataQueue {
  std::mutex mutex_;
  std::condition_variable readerCv_;
  std::condition_variable writerCv_;
  std::queue<T> queue_;
  bool done_{false};
  std::size_t maxSize_;

  bool full() const {
    DMG_ASSERT(!mutex_.try_lock(), "Lock should be taken before full() is invoked");
    if (maxSize_ == 0) {
      return false;
    }
    return queue_.size() >= maxSize_;
  }

 public:
  /**
   * Constructs an empty work queue with an optional max size.
   * If `maxSize == 0` the queue size is unbounded.
   *
   * @param maxSize The maximum allowed size of the work queue.
   */
  explicit DataQueue(std::size_t const maxSize = 0) : maxSize_(maxSize) {}

  /**
   * Push an item onto the work queue.  Notify a single thread that work is
   * available.  If `finish()` has been called, do nothing and return false.
   * If `push()` returns false, then `item` has not been copied from.
   *
   * @param item  Item to push onto the queue.
   * @returns     True upon success, false if `finish()` has been called.  An
   *               item was pushed iff `push()` returns true.
   */
  template <typename U>
  bool push(U &&item) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      writerCv_.wait(lock, [&] { return !full() || done_; });

      if (done_) {
        return false;
      }
      queue_.push(std::forward<U>(item));
    }
    readerCv_.notify_one();
    return true;
  }

  /**
   * Attempts to pop an item off the work queue.  It will block until data is
   * available or `finish()` has been called.
   *
   * @param[out] item  If `pop` returns `true`, it contains the popped item.
   *                    If `pop` returns `false`, it is unmodified.
   * @returns          True upon success.  False if the queue is empty and
   *                    `finish()` has been called.
   */
  bool pop(T &item) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      readerCv_.wait(lock, [&]() { return !queue_.empty() || done_; });

      if (queue_.empty()) {
        MG_ASSERT(done_, "Done is true but queue is not empty");
        return false;
      }

      std::swap(item, queue_.front());
      queue_.pop();
    }
    writerCv_.notify_one();
    return true;
  }

  /**
   * Sets the maximum queue size.  If `maxSize == 0` then it is unbounded.
   *
   * @param maxSize The new maximum queue size.
   */
  void setMaxSize(std::size_t maxSize) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      maxSize_ = maxSize;
    }
    writerCv_.notify_all();
  }

  /**
   * Promise that `push()` won't be called again, so once the queue is empty
   * there will never any more work.
   */
  void finish() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      MG_ASSERT(!done_, "Queue already finished");
      done_ = true;
    }
    readerCv_.notify_all();
    writerCv_.notify_all();
  }
};

}  // namespace memgraph::utils

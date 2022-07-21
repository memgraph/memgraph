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

#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

#include "utils/logging.hpp"

#include "io/errors.hpp"

namespace memgraph::io {

// Shared is in an anonymous namespace, and the only way to
// construct a Promise or Future is to pass a Shared in. This
// ensures that Promises and Futures can only be constructed
// in this translation unit.
namespace {
template <typename T>
class Shared {
  std::condition_variable cv_;
  std::mutex mu_;
  std::optional<T> item_;
  bool consumed_ = false;
  bool waiting_ = false;
  std::optional<std::function<bool()>> simulator_notifier_;

 public:
  explicit Shared(std::function<bool()> simulator_notifier) : simulator_notifier_(simulator_notifier) {}
  Shared() = default;
  Shared(Shared &&) = delete;
  Shared &operator=(Shared &&) = delete;
  Shared(const Shared &) = delete;
  Shared &operator=(const Shared &) = delete;
  ~Shared() = default;

  T Wait() {
    std::unique_lock<std::mutex> lock(mu_);
    waiting_ = true;

    while (!item_) {
      bool simulator_progressed = false;
      if (simulator_notifier_) {
        // We can't hold our own lock while notifying
        // the simulator because notifying the simulator
        // involves acquiring the simulator's mutex
        // to guarantee that our notification linearizes
        // with the simulator's condition variable.
        // However, the simulator may acquire our
        // mutex to check if we are being awaited,
        // while determining system quiescence,
        // so we have to get out of its way to avoid
        // a cyclical deadlock.
        lock.unlock();
        simulator_progressed = (*simulator_notifier_)();
        lock.lock();
        if (item_) {
          // item may have been filled while we
          // had dropped our mutex while notifying
          // the simulator of our waiting_ status.
          break;
        }
      }
      if (!simulator_progressed) {
        cv_.wait(lock);
      }
      MG_ASSERT(!consumed_, "Future consumed twice!");
    }

    T ret = std::move(item_).value();
    item_.reset();

    waiting_ = false;
    consumed_ = true;

    return ret;
  }

  bool IsReady() {
    std::unique_lock<std::mutex> lock(mu_);
    return item_;
  }

  std::optional<T> TryGet() {
    std::unique_lock<std::mutex> lock(mu_);

    if (item_) {
      T ret = std::move(item_).value();
      item_.reset();

      waiting_ = false;
      consumed_ = true;

      return ret;
    } else {
      return std::nullopt;
    }
  }

  void Fill(T item) {
    {
      std::unique_lock<std::mutex> lock(mu_);

      MG_ASSERT(!consumed_, "Promise filled after it was already consumed!");
      MG_ASSERT(!item_, "Promise filled twice!");

      item_ = item;
    }  // lock released before condition variable notification

    cv_.notify_all();
  }

  bool IsAwaited() {
    std::unique_lock<std::mutex> lock(mu_);
    return waiting_;
  }
};
}  // namespace

template <typename T>
class Future {
  bool consumed_or_moved_ = false;
  std::shared_ptr<Shared<T>> shared_;

 public:
  explicit Future(std::shared_ptr<Shared<T>> shared) : shared_(shared) {}

  Future() = delete;
  Future(Future &&old) {
    shared_ = std::move(old.shared_);
    consumed_or_moved_ = old.consumed_or_moved_;
    MG_ASSERT(!old.consumed_or_moved_, "Future moved from after already being moved from or consumed.");
    old.consumed_or_moved_ = true;
  }
  Future &operator=(Future &&old) {
    shared_ = std::move(old.shared_);
    MG_ASSERT(!old.consumed_or_moved_, "Future moved from after already being moved from or consumed.");
    old.consumed_or_moved_ = true;
  }
  Future(const Future &) = delete;
  Future &operator=(const Future &) = delete;
  ~Future() = default;

  /// Returns true if the Future is ready to
  /// be consumed using TryGet or Wait (prefer Wait
  /// if you know it's ready, because it doesn't
  /// return an optional.
  bool IsReady() {
    MG_ASSERT(!consumed_or_moved_, "Called IsReady after Future already consumed!");
    return shared_->IsReady();
  }

  /// Non-blocking method that returns the inner
  /// item if it's already ready, or std::nullopt
  /// if it is not ready yet.
  std::optional<T> TryGet() {
    MG_ASSERT(!consumed_or_moved_, "Called TryGet after Future already consumed!");
    std::optional<T> ret = shared_->TryGet();
    if (ret) {
      consumed_or_moved_ = true;
    }
    return ret;
  }

  /// Block on the corresponding promise to be filled,
  /// returning the inner item when ready.
  T Wait() {
    MG_ASSERT(!consumed_or_moved_, "Future should only be consumed with Wait once!");
    T ret = shared_->Wait();
    consumed_or_moved_ = true;
    return ret;
  }

  /// Marks this Future as canceled.
  void Cancel() {
    MG_ASSERT(!consumed_or_moved_, "Future::Cancel called on a future that was already moved or consumed!");
    consumed_or_moved_ = true;
  }
};

template <typename T>
class Promise {
  std::shared_ptr<Shared<T>> shared_;
  bool filled_or_moved_ = false;

 public:
  explicit Promise(std::shared_ptr<Shared<T>> shared) : shared_(shared) {}

  Promise() = delete;
  Promise(Promise &&old) {
    shared_ = std::move(old.shared_);
    MG_ASSERT(!old.filled_or_moved_, "Promise moved from after already being moved from or filled.");
    old.filled_or_moved_ = true;
  }
  Promise &operator=(Promise &&old) {
    shared_ = std::move(old.shared_);
    MG_ASSERT(!old.filled_or_moved_, "Promise moved from after already being moved from or filled.");
    old.filled_or_moved_ = true;
  }
  Promise(const Promise &) = delete;
  Promise &operator=(const Promise &) = delete;

  ~Promise() { MG_ASSERT(filled_or_moved_, "Promise destroyed before its associated Future was filled!"); }

  // Fill the expected item into the Future.
  void Fill(T item) {
    MG_ASSERT(!filled_or_moved_, "Promise::Fill called on a promise that is already filled or moved!");
    shared_->Fill(item);
    filled_or_moved_ = true;
  }

  bool IsAwaited() { return shared_->IsAwaited(); }

  /// Moves this Promise into a unique_ptr.
  std::unique_ptr<Promise<T>> ToUnique() && {
    std::unique_ptr<Promise<T>> up = std::make_unique<Promise<T>>(std::move(shared_));

    filled_or_moved_ = true;

    return up;
  }
};

template <typename T>
std::pair<Future<T>, Promise<T>> FuturePromisePair() {
  std::shared_ptr<Shared<T>> shared = std::make_shared<Shared<T>>();

  Future<T> future = Future<T>(shared);
  Promise<T> promise = Promise<T>(shared);

  return std::make_pair(std::move(future), std::move(promise));
}

template <typename T>
std::pair<Future<T>, Promise<T>> FuturePromisePairWithNotifier(std::function<bool()> simulator_notifier) {
  std::shared_ptr<Shared<T>> shared = std::make_shared<Shared<T>>(simulator_notifier);

  Future<T> future = Future<T>(shared);
  Promise<T> promise = Promise<T>(shared);

  return std::make_pair(std::move(future), std::move(promise));
}

};  // namespace memgraph::io

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

#include "errors.hpp"

template <typename T>
class MgPromise;

template <typename T>
class MgFuture;

template <typename T>
std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePair();

template <typename T>
std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePairWithNotifier(std::function<void()>);

template <typename T>
class Shared {
  friend std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePair<T>();
  friend std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePairWithNotifier<T>(std::function<void()>);
  friend MgPromise<T>;
  friend MgFuture<T>;

 public:
  Shared(std::function<void()> simulator_notifier) : simulator_notifier_(simulator_notifier) {}
  Shared() = default;
  Shared(Shared &&) = delete;
  Shared &operator=(Shared &&) = delete;
  Shared(const Shared &) = delete;
  Shared &operator=(const Shared &) = delete;
  ~Shared() = default;

 private:
  T Wait() {
    std::unique_lock<std::mutex> lock(mu_);

    while (!item_) {
      waiting_ = true;
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
        (*simulator_notifier_)();
        lock.lock();
        if (item_) {
          // item may have been filled while we
          // had dropped our mutex while notifying
          // the simulator of our waiting_ status.
          break;
        }
      }
      cv_.wait(lock);
      waiting_ = false;
      MG_ASSERT(!consumed_, "MgFuture consumed twice!");
    }

    T ret = std::move(item_).value();
    item_.reset();

    consumed_ = true;

    return ret;
  }

  void Fill(T item) {
    {
      std::unique_lock<std::mutex> lock(mu_);

      MG_ASSERT(!consumed_, "MgPromise filled after it was already consumed!");
      MG_ASSERT(!item_, "MgPromise filled twice!");

      item_ = item;
    }  // lock released before condition variable notification

    cv_.notify_all();
  }

  bool IsAwaited() {
    std::unique_lock<std::mutex> lock(mu_);
    return waiting_;
  }

  std::condition_variable cv_;
  std::mutex mu_;
  std::optional<T> item_;
  bool consumed_;
  bool waiting_;
  std::optional<std::function<void()>> simulator_notifier_;
};

template <typename T>
class MgFuture {
  friend std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePair<T>();
  friend std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePairWithNotifier<T>(std::function<void()>);

 public:
  MgFuture(MgFuture &&old) {
    shared_ = std::move(old.shared_);
    consumed_or_moved_ = old.consumed_or_moved_;
    MG_ASSERT(!old.consumed_or_moved_, "MgFuture moved from after already being moved from or consumed.");
    old.consumed_or_moved_ = true;
  }
  MgFuture &operator=(MgFuture &&old) {
    shared_ = std::move(old.shared_);
    MG_ASSERT(!old.consumed_or_moved_, "MgFuture moved from after already being moved from or consumed.");
    old.consumed_or_moved_ = true;
  }
  MgFuture(const MgFuture &) = delete;
  MgFuture &operator=(const MgFuture &) = delete;
  ~MgFuture() = default;

  // Block on the corresponding promise to be filled,
  // returning the inner item when ready.
  T Wait() {
    MG_ASSERT(!consumed_or_moved_, "MgFuture should only be consumed with Wait once!");
    T ret = shared_->Wait();
    consumed_or_moved_ = true;
    return ret;
  }

 private:
  MgFuture(std::shared_ptr<Shared<T>> shared) : shared_(shared) {}

  bool consumed_or_moved_ = false;
  std::shared_ptr<Shared<T>> shared_;
};

template <typename T>
class MgPromise {
  friend std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePair<T>();
  friend std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePairWithNotifier<T>(std::function<void()>);

 public:
  MgPromise(MgPromise &&old) {
    shared_ = std::move(old.shared_);
    MG_ASSERT(!old.filled_or_moved_, "MgPromise moved from after already being moved from or filled.");
    old.filled_or_moved_ = true;
  }
  MgPromise &operator=(MgPromise &&old) {
    shared_ = std::move(old.shared_);
    MG_ASSERT(!old.filled_or_moved_, "MgPromise moved from after already being moved from or filled.");
    old.filled_or_moved_ = true;
  }
  MgPromise(const MgPromise &) = delete;
  MgPromise &operator=(const MgPromise &) = delete;

  ~MgPromise() { MG_ASSERT(filled_or_moved_, "MgPromise destroyed before its associated MgFuture was filled!"); }

  // Fill the expected item into the Future.
  void Fill(T item) {
    MG_ASSERT(!filled_or_moved_, "MgPromise::Fill called twice on the same promise!");
    shared_->Fill(item);
    filled_or_moved_ = true;
  }

  bool IsAwaited() { return shared_->IsAwaited(); }

 private:
  MgPromise(std::shared_ptr<Shared<T>> shared) : shared_(shared) {}

  std::shared_ptr<Shared<T>> shared_;
  bool filled_or_moved_ = false;
};

template <typename T>
std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePair() {
  std::shared_ptr<Shared<T>> shared = std::make_shared<Shared<T>>();

  MgFuture<T> future = MgFuture<T>(shared);
  MgPromise<T> promise = MgPromise<T>(shared);

  return std::make_pair(std::move(future), std::move(promise));
}

template <typename T>
std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePairWithNotifier(std::function<void()> simulator_notifier) {
  std::shared_ptr<Shared<T>> shared = std::make_shared<Shared<T>>(simulator_notifier);

  MgFuture<T> future = MgFuture<T>(shared);
  MgPromise<T> promise = MgPromise<T>(shared);

  return std::make_pair(std::move(future), std::move(promise));
}

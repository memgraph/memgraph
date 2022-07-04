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
#include "simulator.hpp"

template <typename T>
class MgPromise;

template <typename T>
class MgFuture;

template <typename T>
std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePair();

template <typename T>
std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePair(SimulatorHandle);

template <typename T>
class Shared {
  friend std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePair<T>();
  friend MgPromise<T>;
  friend MgFuture<T>;

 public:
  Shared(Shared &&) = default;
  Shared &operator=(Shared &&) = default;
  Shared(const Shared &) = delete;
  Shared &operator=(const Shared &) = delete;
  ~Shared() = default;

 private:
  T Wait() {
    std::unique_lock<std::mutex> lock(mu_);

    while (!item_) {
      waiting_ = true;
      if (simulator_handle_) {
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
        (*simulator_handle_)->NotifySimulator();
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
  std::optional<std::shared_ptr<SimulatorHandle>> simulator_handle_;
};

template <typename T>
class MgFuture {
  friend std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePair<T>();
  friend std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePair<T>(SimulatorHandle);

 public:
  MgFuture(MgFuture &&) = default;
  MgFuture &operator=(MgFuture &&) = default;
  MgFuture(const MgFuture &) = delete;
  MgFuture &operator=(const MgFuture &) = delete;
  ~MgFuture() = default;

  // Block on the corresponding promise to be filled,
  // returning the inner item when ready.
  T Wait() { return shared_->Wait(); }

 private:
  MgFuture(std::shared_ptr<Shared<T>> shared) : shared_(shared) {}

  std::shared_ptr<Shared<T>> shared_;
};

template <typename T>
class MgPromise {
  friend std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePair<T>();
  friend std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePair<T>(SimulatorHandle);

 public:
  MgPromise(MgPromise &&) = default;
  MgPromise &operator=(MgPromise &&) = default;
  MgPromise(const MgPromise &) = delete;
  MgPromise &operator=(const MgPromise &) = delete;

  ~MgPromise() {
    MG_ASSERT(filled_,
              "MgPromise destroyed before its \
              associated MgFuture was filled!");
  }

  // Fill the expected item into the Future.
  void Fill(T item) {
    shared_->Fill(item);
    filled_ = true;
  }

  bool IsAwaited() { return shared_->IsAwaited(); }

 private:
  MgPromise(std::shared_ptr<Shared<T>> shared) : shared_(shared) {}

  std::shared_ptr<Shared<T>> shared_;
  bool filled_;
};

template <typename T>
std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePair() {
  std::shared_ptr<Shared<T>> shared = std::make_shared<Shared<T>>();
  MgFuture<T> future = MgFuture<T>(shared);
  MgPromise<T> promise = MgPromise<T>(shared);

  return std::make_pair(std::move(future), std::move(promise));
}

template <typename T>
std::pair<MgFuture<T>, MgPromise<T>> FuturePromisePair(SimulatorHandle simulator_handle) {
  auto [future, promise] = FuturePromisePair<T>();
  future.simulator_handle_ = simulator_handle;
  return std::make_pair(std::move(future), std::move(promise));
}

namespace _compile_test {
void _templatization_smoke_test() {
  auto [future, promise] = FuturePromisePair<bool>();
  promise.Fill(true);
  MG_ASSERT(future.Wait() == true);
}
}  // namespace _compile_test

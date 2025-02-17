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

#include <memory>
#include <mutex>
#include <set>

namespace memgraph::utils {

// Observer interface
template <typename T>
class Observer {
 public:
  virtual ~Observer() = default;
  virtual void Update(const T &) = 0;
};

template <>
// Specialize for T = void
class Observer<void> {
 public:
  virtual ~Observer() = default;
  virtual void Update() = 0;
};

template <typename T>
class Observable {
 public:
  virtual ~Observable() = default;

  void Attach(std::shared_ptr<Observer<T>> observer) {
    auto l = std::unique_lock{mtx_};
    observers.insert(std::move(observer));
  }

  void Detach(const std::shared_ptr<Observer<T>> &observer) {
    auto l = std::unique_lock{mtx_};
    observers.erase(observer);
  }

 protected:
  void Notify() {
    auto l = std::unique_lock{mtx_};
    for (const auto &observer : observers) {
      Accept(observer);
    }
  }

  virtual void Accept(std::shared_ptr<Observer<T>>) = 0;

  std::set<std::shared_ptr<Observer<T>>> observers;
  mutable std::mutex mtx_;
};

}  // namespace memgraph::utils

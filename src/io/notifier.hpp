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
#include <mutex>
#include <vector>

namespace memgraph::io {

class ReadinessToken {
  size_t id_;

 public:
  explicit ReadinessToken(size_t id) : id_(id) {}
  size_t GetId() const { return id_; }
};

class Inner {
  std::condition_variable cv_;
  std::mutex mu_;
  std::vector<ReadinessToken> ready_;

 public:
  void Notify(ReadinessToken readiness_token) {
    {
      std::unique_lock<std::mutex> lock(mu_);
      ready_.emplace_back(readiness_token);
    }  // mutex dropped

    cv_.notify_all();
  }

  ReadinessToken Await() {
    std::unique_lock<std::mutex> lock(mu_);

    while (ready_.empty()) {
      cv_.wait(lock);
    }

    ReadinessToken ret = ready_.back();
    ready_.pop_back();
    return ret;
  }
};

class Notifier {
  std::shared_ptr<Inner> inner_;

 public:
  Notifier() : inner_(std::make_shared<Inner>()) {}
  Notifier(const Notifier &) = default;
  Notifier &operator=(const Notifier &) = default;
  Notifier(Notifier &&old) = default;
  Notifier &operator=(Notifier &&old) = default;
  ~Notifier() = default;

  void Notify(ReadinessToken readiness_token) const { inner_->Notify(readiness_token); }

  ReadinessToken Await() const { return inner_->Await(); }
};

}  // namespace memgraph::io

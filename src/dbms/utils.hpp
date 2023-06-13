// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
// TODO: Check if comment above is ok
#pragma once

#include <condition_variable>
#include <functional>
#include <iostream>
#include <memory>

namespace memgraph::dbms {

template <typename TContext, typename TConfig>
struct SyncPtr {
  template <typename... TArgs>
  explicit SyncPtr(TConfig config, TArgs &&...args)
      : config_{config},
        in_use_(true),
        ptr_{new TContext(std::forward<TArgs>(args)...), std::bind(&SyncPtr::OnDelete, this, std::placeholders::_1)} {}

  ~SyncPtr() {
    ptr_.~shared_ptr<TContext>();
    SyncOnDelete();
  }

  SyncPtr(const SyncPtr &) = delete;
  SyncPtr &operator=(const SyncPtr &) = delete;
  SyncPtr(SyncPtr &&) noexcept = delete;
  SyncPtr &operator=(SyncPtr &&) noexcept = delete;

  void SyncOnDelete() {
    std::unique_lock<std::mutex> lock(in_use_mtx_);
    while (in_use_) {
      in_use_cv_.wait(lock);
      std::this_thread::yield();
    }
  }

  void OnDelete(TContext *p) {
    std::lock_guard<std::mutex> lock(in_use_mtx_);
    in_use_ = false;
    in_use_cv_.notify_one();
    delete p;
  }

  //  private:
  TConfig config_;
  bool in_use_;
  std::mutex in_use_mtx_;
  std::condition_variable in_use_cv_;
  std::shared_ptr<TContext> ptr_;
};

}  // namespace memgraph::dbms

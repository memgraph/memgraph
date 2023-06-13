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

#include <cstddef>
#include <thread>
#include <vector>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

#include "utils/logging.hpp"

namespace memgraph::communication::v2 {

class IOContextThreadPool final {
 private:
  using IOContext = boost::asio::io_context;
  using IOContextGuard = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;

 public:
  explicit IOContextThreadPool(size_t pool_size) : guard_{io_context_.get_executor()}, pool_size_{pool_size} {
    MG_ASSERT(pool_size != 0, "Pool size must be greater than 0!");
  }

  IOContextThreadPool(const IOContextThreadPool &) = delete;
  IOContextThreadPool &operator=(const IOContextThreadPool &) = delete;
  IOContextThreadPool(IOContextThreadPool &&) = delete;
  IOContextThreadPool &operator=(IOContextThreadPool &&) = delete;
  ~IOContextThreadPool() = default;

  void Run() {
    background_threads_.reserve(pool_size_);
    for (size_t i = 0; i < pool_size_; ++i) {
      background_threads_.emplace_back([this]() { io_context_.run(); });
    }
    running_ = true;
  }

  void Shutdown() {
    io_context_.stop();
    running_ = false;
  }

  void AwaitShutdown() { background_threads_.clear(); }

  bool IsRunning() const noexcept { return running_; }

  IOContext &GetIOContext() noexcept { return io_context_; }

 private:
  /// The pool of io_context.
  IOContext io_context_;
  IOContextGuard guard_;
  size_t pool_size_;
  std::vector<std::jthread> background_threads_;
  bool running_{false};
};
}  // namespace memgraph::communication::v2

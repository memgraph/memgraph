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

#include <memory>
#include <thread>
#include <vector>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

#include "utils/logging.hpp"

namespace memgraph::communication::v2 {

class IOContextPool {
 private:
  using IOContext = boost::asio::io_context;
  using IOContextGuard = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;

 public:
  explicit IOContextPool(size_t pool_size) : next_io_context_(0) {
    MG_ASSERT(pool_size != 0, "Pool size must be bigger then 0!");

    // Give all the io_contexts work to do so that their Run() functions will not
    // exit until they are explicitly stopped.
    for (size_t i = 0; i < pool_size; ++i) {
      io_contexts_.emplace_back(std::make_shared<IOContext>());
      guards_.emplace_back(std::make_shared<IOContextGuard>(io_contexts_.back()->get_executor()));
    }
  }

  IOContextPool(const IOContextPool &) = delete;
  IOContextPool &operator=(const IOContextPool &) = delete;
  IOContextPool(IOContextPool &&) = delete;
  IOContextPool &operator=(IOContextPool &&) = delete;
  ~IOContextPool() { MG_ASSERT(background_threads_.empty(), "Error while destructing thread pool"); };

  void Run() {
    // Create a pool of threads to run all of the io_services.
    background_threads_.reserve(io_contexts_.size());
    for (const auto &io_context : io_contexts_) {
      background_threads_.emplace_back([io_context] { io_context->run(); });
    }
    running_ = true;
  }

  void Shutdown() {
    for (const auto &io_context : io_contexts_) {
      io_context->stop();
    }
    running_ = false;
  }

  void AwaitShutdown() {
    for (auto &background_thread : background_threads_)
      if (background_thread.joinable()) {
        background_thread.join();
      }
    background_threads_.clear();
  }

  boost::asio::io_context &GetIOContext() {
    // Use a round-robin scheme to choose the next io_service to use.
    auto &io_context = io_contexts_[next_io_context_++];
    if (next_io_context_ == io_contexts_.size()) {
      next_io_context_ = 0;
    }
    return *io_context;
  }

  bool IsRunning() const noexcept { return running_; }

 private:
  /// The pool of io_context.
  std::vector<std::shared_ptr<IOContext>> io_contexts_;
  std::vector<std::shared_ptr<IOContextGuard>> guards_;
  std::vector<std::thread> background_threads_;
  std::size_t next_io_context_;
  bool running_{false};
};
}  // namespace memgraph::communication::v2

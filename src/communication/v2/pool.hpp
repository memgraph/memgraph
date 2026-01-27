// Copyright 2026 Memgraph Ltd.
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
#include <exception>
#include <optional>
#include <thread>
#include <vector>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

#include "utils/logging.hpp"
#include "utils/numa.hpp"
#include "utils/thread.hpp"

namespace memgraph::communication::v2 {

class IOContextThreadPool final {
 private:
  using IOContext = boost::asio::io_context;
  using IOContextGuard = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;

  struct NUMAIOContext {
    IOContext io_context_;
    IOContextGuard guard_;
    int numa_node_;
    std::vector<std::jthread> threads_;

    explicit NUMAIOContext(int numa_node) : guard_{io_context_.get_executor()}, numa_node_{numa_node} {}

    // Non-copyable, non-movable (IOContextGuard is not movable)
    NUMAIOContext(const NUMAIOContext &) = delete;
    NUMAIOContext &operator=(const NUMAIOContext &) = delete;
    NUMAIOContext(NUMAIOContext &&) = delete;
    NUMAIOContext &operator=(NUMAIOContext &&) = delete;
    ~NUMAIOContext() = default;
  };

 public:
  // Legacy constructor: single shared io_context
  explicit IOContextThreadPool(size_t pool_size) : pool_size_{pool_size}, numa_aware_{false} {
    MG_ASSERT(pool_size != 0, "Pool size must be greater then 0!");
    shared_io_context_ = std::make_unique<IOContext>(pool_size);
    shared_guard_ = std::make_unique<IOContextGuard>(shared_io_context_->get_executor());
  }

  // NUMA-aware constructor: one io_context per NUMA node
  explicit IOContextThreadPool(const utils::numa::NUMATopology &topology)
      : pool_size_{0}, numa_aware_{true}, numa_topology_{topology} {
    // Create one io_context per NUMA node
    for (const auto &node : topology.nodes) {
      numa_io_contexts_.push_back(std::make_unique<NUMAIOContext>(node.node_id));
      spdlog::info("Created IO context for NUMA node {}", node.node_id);
    }
  }

  IOContextThreadPool(const IOContextThreadPool &) = delete;
  IOContextThreadPool &operator=(const IOContextThreadPool &) = delete;
  IOContextThreadPool(IOContextThreadPool &&) = delete;
  IOContextThreadPool &operator=(IOContextThreadPool &&) = delete;
  ~IOContextThreadPool() = default;

  void Run() {
    if (numa_aware_) {
      // Run one thread per NUMA node, pinned to that node
      running_ = true;
      for (auto &numa_ctx : numa_io_contexts_) {
        numa_ctx->threads_.reserve(1);
        const int numa_node = numa_ctx->numa_node_;
        numa_ctx->threads_.emplace_back([numa_ctx = numa_ctx.get(), numa_node, this]() {
          utils::ThreadSetName("io context numa");
          // Pin thread to NUMA node
          if (!utils::numa::PinThreadToNUMANode(numa_node)) {
            spdlog::warn("Failed to pin IO thread to NUMA node {}", numa_node);
          }
          while (running_) {
            try {
              numa_ctx->io_context_.run();
              spdlog::trace("IOContextThreadPool (NUMA {}) exited", numa_node);
              break;  // exited normally
            } catch (const std::exception &e) {
              spdlog::trace("IOContextThreadPool (NUMA {}) exception: {}", numa_node, e.what());
            }
          }
        });
      }
    } else {
      // Legacy: shared io_context
      background_threads_.reserve(pool_size_);
      running_ = true;
      for (size_t i = 0; i < pool_size_; ++i) {
        background_threads_.emplace_back([this]() {
          utils::ThreadSetName("io context");
          while (running_) {
            try {
              shared_io_context_->run();
              spdlog::trace("IOContextThreadPool exited");
              break;  // exited normally
            } catch (const std::exception &e) {
              spdlog::trace("IOContextThreadPool exception: {}", e.what());
            }
          }
        });
      }
    }
  }

  void Shutdown() {
    if (numa_aware_) {
      for (auto &numa_ctx : numa_io_contexts_) {
        numa_ctx->io_context_.stop();
      }
    } else {
      shared_io_context_->stop();
    }
    running_ = false;
  }

  void AwaitShutdown() {
    if (numa_aware_) {
      for (auto &numa_ctx : numa_io_contexts_) {
        numa_ctx->threads_.clear();
      }
    } else {
      background_threads_.clear();
    }
  }

  bool IsRunning() const noexcept { return running_; }

  // Get IO context (legacy: returns shared context)
  IOContext &GetIOContext() noexcept {
    if (numa_aware_) {
      // Return first NUMA node's context as default (for acceptor)
      return numa_io_contexts_[0]->io_context_;
    }
    return *shared_io_context_;
  }

  // Get IO context for a specific NUMA node
  IOContext &GetIOContextForNUMA(int numa_node) {
    if (numa_aware_) {
      for (auto &numa_ctx : numa_io_contexts_) {
        if (numa_ctx->numa_node_ == numa_node) {
          return numa_ctx->io_context_;
        }
      }
      // Fallback to first NUMA node
      spdlog::warn("NUMA node {} not found, using first NUMA node's IO context", numa_node);
      return numa_io_contexts_[0]->io_context_;
    }
    return *shared_io_context_;
  }

  // Get IO context based on incoming CPU (for client location detection)
  IOContext &GetIOContextForIncomingCPU(int socket_fd) {
    if (numa_aware_) {
      // Try to detect which CPU received the connection
      int incoming_cpu = utils::numa::GetIncomingCPU(socket_fd);
      if (incoming_cpu >= 0) {
        int numa_node = utils::numa::GetNUMANodeForCPU(incoming_cpu);
        if (numa_node >= 0) {
          return GetIOContextForNUMA(numa_node);
        }
      }
      // Fallback: use current thread's NUMA node
      int current_numa = utils::numa::GetCurrentNUMANode();
      if (current_numa >= 0) {
        return GetIOContextForNUMA(current_numa);
      }
      // Final fallback: first NUMA node
      return numa_io_contexts_[0]->io_context_;
    }
    return *shared_io_context_;
  }

 private:
  size_t pool_size_;
  bool numa_aware_;
  std::optional<utils::numa::NUMATopology> numa_topology_;

  // Legacy: single shared io_context
  std::unique_ptr<IOContext> shared_io_context_;
  std::unique_ptr<IOContextGuard> shared_guard_;
  std::vector<std::jthread> background_threads_;

  // NUMA-aware: one io_context per NUMA node (use unique_ptr since it's not movable)
  std::vector<std::unique_ptr<NUMAIOContext>> numa_io_contexts_;

  std::atomic_bool running_{false};
};
}  // namespace memgraph::communication::v2

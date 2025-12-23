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

#include <array>
#include <atomic>
#include <concepts>
#include <iostream>
#include <new>
#include <type_traits>
#include <version>

#ifdef __cpp_lib_hardware_interference_size
inline constexpr size_t kHardwareInterferenceSize = std::hardware_destructive_interference_size;
#else
inline constexpr size_t kHardwareInterferenceSize = 64;
#endif

namespace memgraph::utils {

/**
 * @brief Base class for objects that can be offloaded for garbage collection.
 *
 * Objects to be garbage collected must inherit from this class to enable
 * intrusive linking in the GCFifoOffloader.
 */
struct GCNode {
  virtual ~GCNode() = default;
  GCNode() = default;
  GCNode(const GCNode &) = delete;
  GCNode &operator=(const GCNode &) = delete;
  GCNode(GCNode &&) = delete;
  GCNode &operator=(GCNode &&) = delete;
};

/**
 * @brief A GCNode that wraps a function/lambda to be executed upon destruction.
 */
template <typename F>
struct GCFunctionNode final : public GCNode {
  explicit GCFunctionNode(F &&func) : func_(std::forward<F>(func)) {}
  ~GCFunctionNode() override { func_(); }

  GCFunctionNode(const GCFunctionNode &) = delete;
  GCFunctionNode &operator=(const GCFunctionNode &) = delete;
  GCFunctionNode(GCFunctionNode &&) = delete;
  GCFunctionNode &operator=(GCFunctionNode &&) = delete;

 private:
  F func_;
};

/**
 * @brief A fast, wait-free FIFO offloader for heterogeneous object destruction.
 *
 * Designed for Multi-Producer Single-Consumer (MPSC) scenarios where the
 * producers (e.g., query threads) offload heavy destruction tasks to a
 * background consumer (e.g., GC thread).
 *
 * This implementation uses a circular buffer of atomic pointers to ensure
 * wait-free push and thread-safe collection without the race conditions of
 * the previous intrusive linked list implementation.
 */
class GCFifoOffloader {
 public:
  static constexpr size_t kGCBufferSize = 16U * 1024U;

  GCFifoOffloader() = default;

  // Non-copyable/movable for safety in concurrent context
  GCFifoOffloader(const GCFifoOffloader &) = delete;
  GCFifoOffloader &operator=(const GCFifoOffloader &) = delete;
  GCFifoOffloader(GCFifoOffloader &&) = delete;
  GCFifoOffloader &operator=(GCFifoOffloader &&) = delete;

  ~GCFifoOffloader() { Collect(); }

  /**
   * @brief Push a node to the offloader (FIFO).
   *
   * Multiple producers can call this concurrently (MPSC).
   * Wait-free: 1 atomic fetch-add + 1 atomic exchange.
   *
   * @param node Pointer to the node to be offloaded. Ownership is transferred.
   */
  void Push(GCNode *node) {
    if (!node) return;

    auto idx = tail_idx_.fetch_add(1, std::memory_order_acquire);
    idx = idx & (kGCBufferSize - 1);

    GCNode *expected = nullptr;
    if (!buffer_[idx].compare_exchange_strong(expected, node, std::memory_order_acq_rel)) {
      // Failed to push, destroy the node
      delete node;
      return;
    }
  }

  /**
   * @brief Helper to push an arbitrary function/lambda to the offloader.
   *
   * @tparam F Function type.
   * @param func The function/lambda to offload.
   */
  template <typename F>
  requires std::invocable<F> &&(!std::is_convertible_v<F, GCNode *>)void Push(F &&func) {
    Push(new GCFunctionNode<std::decay_t<F>>(std::forward<F>(func)));
  }

  /**
   * @brief Collect and destroy nodes in FIFO order.
   *
   * GC thread (consumer) operation.
   * Iterates through pending nodes and calls their virtual destructors.
   */
  void Collect() {
    // Try to sync with the writer (run a single loop until we hit non-null)
    for (size_t i = 0; i < kGCBufferSize; ++i) {
      GCNode *node = buffer_[head_idx_].load(std::memory_order_acquire);
      if (node) break;
      head_idx_ = (head_idx_ + 1) & (kGCBufferSize - 1);
    }
    // Follow the indices from head_idx_ up to current_tail
    while (true) {
      GCNode *node = buffer_[head_idx_].exchange(nullptr, std::memory_order_acq_rel);
      if (node) {
        delete node;
        head_idx_ = (head_idx_ + 1) & (kGCBufferSize - 1);
      } else {
        // Nullptr does not mean we necessarily consumed all nodes
        // If the writer cirucled, the reader could stop prematurally and leave a gap to the next element
        // Break now and try to sync next time
        break;
      }
    }
  }

 private:
  alignas(kHardwareInterferenceSize) std::array<std::atomic<GCNode *>, kGCBufferSize> buffer_{};
  alignas(kHardwareInterferenceSize) std::atomic<size_t> tail_idx_{0};
  alignas(kHardwareInterferenceSize) size_t head_idx_{0};  // single consumer (no need for atomic)
};

}  // namespace memgraph::utils

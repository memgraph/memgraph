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

#include "query/virtual_edge_store.hpp"
#include "query/virtual_node_store.hpp"
#include "utils/logging.hpp"
#include "utils/memory.hpp"

namespace memgraph::query {

class VirtualGraph final {
 public:
  using allocator_type = utils::Allocator<VirtualGraph>;

  explicit VirtualGraph(allocator_type alloc);
  VirtualGraph(const VirtualGraph &other, allocator_type alloc);
  VirtualGraph(const VirtualGraph &other);
  VirtualGraph(VirtualGraph &&other) noexcept;
  VirtualGraph(VirtualGraph &&other, allocator_type alloc);

  VirtualGraph &operator=(const VirtualGraph &) = default;
  VirtualGraph &operator=(VirtualGraph &&) = default;
  ~VirtualGraph() = default;

  [[nodiscard]] VirtualNodeStore &node_store() noexcept { return node_store_; }

  [[nodiscard]] const VirtualNodeStore &node_store() const noexcept { return node_store_; }

  [[nodiscard]] VirtualEdgeStore &edge_store() noexcept { return edge_store_; }

  [[nodiscard]] const VirtualEdgeStore &edge_store() const noexcept { return edge_store_; }

  [[nodiscard]] auto get_allocator() const -> allocator_type {
    DMG_ASSERT(node_store_.get_allocator().resource() == edge_store_.get_allocator().resource(),
               "VirtualGraph sub-store allocators diverged");
    return node_store_.get_allocator();
  }

  // Absorb another graph's nodes and edges into this one (used by parallel aggregate reduce).
  void Merge(const VirtualGraph &other);
  void Merge(VirtualGraph &&other);

 private:
  VirtualNodeStore node_store_;
  VirtualEdgeStore edge_store_;
};

}  // namespace memgraph::query

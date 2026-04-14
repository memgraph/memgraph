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

#include "query/virtual_node.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/unordered_map.hpp"

namespace memgraph::query {

class VirtualNodeStore {
 public:
  using allocator_type = utils::Allocator<VirtualNodeStore>;

  explicit VirtualNodeStore(allocator_type alloc) : nodes_(alloc) {}

  VirtualNodeStore(const VirtualNodeStore &other, allocator_type alloc) : nodes_(other.nodes_, alloc) {}

  VirtualNodeStore(VirtualNodeStore &&other) noexcept = default;

  VirtualNodeStore(VirtualNodeStore &&other, allocator_type alloc) : nodes_(std::move(other.nodes_), alloc) {}

  VirtualNodeStore(const VirtualNodeStore &other) : VirtualNodeStore(other, other.nodes_.get_allocator()) {}

  VirtualNodeStore &operator=(const VirtualNodeStore &) = default;
  VirtualNodeStore &operator=(VirtualNodeStore &&) noexcept = default;
  ~VirtualNodeStore() = default;

  // Inserts if original Gid is new; returns reference to the stored node (stable synthetic Gid).
  const VirtualNode &InsertOrGet(VirtualNode node);

  // Unconditional insert-or-replace by original Gid.
  void InsertOrUpdate(VirtualNode node);

  const VirtualNode *Find(storage::Gid original_gid) const;
  bool Contains(storage::Gid original_gid) const;

  auto &nodes() const { return nodes_; }

  auto size() const { return nodes_.size(); }

  auto empty() const { return nodes_.empty(); }

 private:
  // keyed by original Gid for dedup
  utils::pmr::unordered_map<storage::Gid, VirtualNode> nodes_;
};

}  // namespace memgraph::query

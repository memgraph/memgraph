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

#include "query/virtual_node_store.hpp"

namespace memgraph::query {

const VirtualNode &VirtualNodeStore::InsertOrGet(VirtualNode node) {
  const auto original_gid = node.OriginalGid();
  const auto synthetic_gid = node.Gid();
  auto [it, inserted] = nodes_.try_emplace(original_gid, std::move(node));
  if (inserted) {
    synthetic_to_original_[synthetic_gid] = original_gid;
  }
  return it->second;
}

void VirtualNodeStore::MergeFrom(const VirtualNodeStore &other) {
  // try_emplace keeps this store's canonical node for any original_gid it already holds.
  // regardless, we register other's synthetic gid as an alias pointing to the canonical
  // original_gid so virtual edges constructed in other's synth space remain resolvable.
  for (const auto &[original_gid, node] : other.nodes_) {
    nodes_.try_emplace(original_gid, node);
    synthetic_to_original_[node.Gid()] = original_gid;
  }
}

void VirtualNodeStore::MergeFrom(VirtualNodeStore &&other) {
  for (const auto &[original_gid, node] : other.nodes_) {
    synthetic_to_original_[node.Gid()] = original_gid;
  }
  if (nodes_.get_allocator() == other.nodes_.get_allocator()) {
    // Matching allocators: unordered_map::merge transfers node handles for keys we don't
    // already hold; conflicting nodes are left in other (discarded when other goes away).
    nodes_.merge(other.nodes_);
    return;
  }
  // Allocator mismatch: fall back to per-element move-insert.
  for (auto &[original_gid, node] : other.nodes_) {
    nodes_.try_emplace(original_gid, std::move(node));
  }
}

const VirtualNode *VirtualNodeStore::Find(storage::Gid original_gid) const {
  if (const auto it = nodes_.find(original_gid); it != nodes_.end()) return &it->second;
  return nullptr;
}

const VirtualNode *VirtualNodeStore::FindBySyntheticGid(storage::Gid synthetic_gid) const {
  if (const auto it = synthetic_to_original_.find(synthetic_gid); it != synthetic_to_original_.end()) {
    return Find(it->second);
  }
  return nullptr;
}

bool VirtualNodeStore::Contains(storage::Gid original_gid) const { return nodes_.contains(original_gid); }

}  // namespace memgraph::query

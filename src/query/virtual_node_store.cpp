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
  auto [it, inserted] = nodes_.try_emplace(original_gid, std::move(node));
  return it->second;
}

void VirtualNodeStore::InsertOrUpdate(VirtualNode node) {
  const auto original_gid = node.OriginalGid();
  nodes_.insert_or_assign(original_gid, std::move(node));
}

const VirtualNode *VirtualNodeStore::Find(storage::Gid original_gid) const {
  if (auto it = nodes_.find(original_gid); it != nodes_.end()) return &it->second;
  return nullptr;
}

bool VirtualNodeStore::Contains(storage::Gid original_gid) const { return nodes_.contains(original_gid); }

}  // namespace memgraph::query

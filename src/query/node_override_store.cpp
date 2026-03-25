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

#include "query/node_override_store.hpp"

namespace memgraph::query {

void NodeOverrideStore::Set(storage::Gid gid, NodeOverride override) {
  overrides_.insert_or_assign(gid, std::move(override));
}

const NodeOverride *NodeOverrideStore::Find(storage::Gid gid) const {
  if (auto it = overrides_.find(gid); it != overrides_.end()) return &it->second;
  return nullptr;
}

bool NodeOverrideStore::Contains(storage::Gid gid) const { return overrides_.contains(gid); }

}  // namespace memgraph::query

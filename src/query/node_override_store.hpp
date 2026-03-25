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

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::query {

struct NodeOverride {
  std::vector<std::string> labels;
  std::map<storage::PropertyId, storage::PropertyValue> properties;
};

class NodeOverrideStore {
 public:
  void Set(storage::Gid gid, NodeOverride override);
  const NodeOverride *Find(storage::Gid gid) const;
  bool Contains(storage::Gid gid) const;

  auto size() const { return overrides_.size(); }

  auto empty() const { return overrides_.empty(); }

 private:
  std::unordered_map<storage::Gid, NodeOverride> overrides_;
};

}  // namespace memgraph::query

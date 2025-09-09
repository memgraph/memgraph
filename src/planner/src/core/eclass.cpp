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
#include "planner/core/eclass.hpp"

namespace memgraph::planner::core::detail {

void EClassBase::add_parent(ENodeId parent_enode_id, EClassId parent_class_id) {
  parents.emplace_back(parent_enode_id, parent_class_id);
}

auto EClassBase::representative_id() const -> ENodeId {
  // Every e-class has at least one e-node
  return *nodes.begin();
}

}  // namespace memgraph::planner::core::detail

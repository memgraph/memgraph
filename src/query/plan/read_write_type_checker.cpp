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

#include "query/plan/read_write_type_checker.hpp"

namespace memgraph::query::plan {

bool StorageAccessChecker::PreVisit(CallProcedure &op) {
  // A call that reaches no storage is still a read to RWType, which is what clients and the read
  // counters are told. Only the storage question is answered differently here.
  if (op.graph_access_ == GraphAccess::None) {
    UpdateType(RWType::NONE);
    return true;
  }
  return ReadWriteTypeChecker::PreVisit(op);
}

bool PlanRequiresStorageAccess(const LogicalOperator &plan) {
  StorageAccessChecker checker;
  // Const only because the visitor framework has no const traversal; the checker mutates nothing.
  checker.InferRWType(const_cast<LogicalOperator &>(plan));
  return checker.type != RWType::NONE;
}

bool ReadWriteTypeChecker::Visit(Once &) { return false; }  // NOLINT(hicpp-named-parameter)

void detail::RWTypeState::UpdateType(RWType op_type) {
  // Stop inference because RW is the most "dominant" type, i.e. it isn't
  // affected by the type of nodes in the plan appearing after the node for
  // which the type is set to RW.
  if (type == RWType::RW) {
    return;
  }

  // if op_type is NONE, type doesn't change.
  if (op_type == RWType::NONE) {
    return;
  }

  // Update type only if it's not the NONE type and the current operator's type
  // is different than the one that's currently inferred.
  if (type != RWType::NONE && type != op_type) {
    type = RWType::RW;
  }

  if (type == RWType::NONE) {
    type = op_type;
  }
}

void ReadWriteTypeChecker::InferRWType(LogicalOperator &root) { root.Accept(*this); }

std::string ReadWriteTypeChecker::TypeToString(const RWType type) {
  switch (type) {
    // Unfortunately, neo4j Java drivers do not allow query types that differ
    // from the ones defined by neo4j. We'll keep using the NONE type internally
    // but we'll convert it to "rw" to keep in line with the neo4j definition.
    // Oddly enough, but not surprisingly, Python drivers don't have any problems
    // with non-neo4j query types.
    case RWType::NONE:
      return "rw";
    case RWType::R:
      return "r";
    case RWType::W:
      return "w";
    case RWType::RW:
      return "rw";
  }
}

}  // namespace memgraph::query::plan

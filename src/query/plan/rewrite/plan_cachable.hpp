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

#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

namespace impl {

class IsPlanCachable final : public HierarchicalLogicalOperatorVisitor {
 public:
  IsPlanCachable() = default;
  ~IsPlanCachable() override = default;

  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool Visit(Once & /*op*/) override { return true; }

  bool PreVisit(ExpandVariable &op) override {
    // BFS expansion is not cachable because its cost estimation depends on runtime data
    if (op.type_ == EdgeAtom::Type::BREADTH_FIRST) {
      cachable_ = false;
      return false;  // No need to continue visiting children
    }
    return true;  // Continue visiting
  }

  bool Cachable() const { return cachable_; }

 private:
  bool cachable_{true};
};

}  // namespace impl

inline bool IsPlanCachable(LogicalOperator &root_op) {
  auto validator = impl::IsPlanCachable{};
  root_op.Accept(validator);
  return validator.Cachable();
}

}  // namespace memgraph::query::plan

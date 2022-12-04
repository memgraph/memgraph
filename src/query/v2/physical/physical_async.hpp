// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

///
/// Physical Execute/Next/Emit Async Architecture Implementation
///
/// The whole new set of possibilities!
///
/// Since most of the operators have complex internal state, each Execute
/// function should be implemented in a way so that single threaded execution
/// of the whole query is possible via SingleThreadedExecutor. With the right
/// implementation, it should also be possible to parallelize execution of
/// stateless operators and simpler statefull operators like ScanAll by using
/// the same Execute implementation and MultiThreadedExecutor.
///
/// Blocking, but time and space limited, implementations of Execute functions,
/// should be wrapped into ExecuteAsync to allow efficient multi-threaded
/// execution.
///

#pragma once

#include "query/plan/operator.hpp"
#include "utils/visitor.hpp"

namespace memgraph::query::v2::physical {

using memgraph::query::plan::HierarchicalLogicalOperatorVisitor;
using memgraph::query::plan::Once;
using memgraph::query::plan::Produce;
using memgraph::query::plan::ScanAll;

class PhysicalAsyncPlanGenerator final : public HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  bool PreVisit(Produce & /*unused*/) override { return true; }
  bool PostVisit(Produce & /*unused*/) override { return true; }

  bool PreVisit(ScanAll & /*unused*/) override { return true; }
  bool PostVisit(ScanAll & /*unused*/) override { return true; }

  bool Visit(Once & /*unused*/) override { return true; }
};

}  // namespace memgraph::query::v2::physical

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

#include <concepts>
#include <memory>
#include <utility>
#include <vector>

#include "query/plan/operator.hpp"

namespace memgraph::query::plan::impl {

/// Base for the plan rewriters that substitute real-graph scans - the index,
/// edge-index, and join rewriters. Their substitutions consult the real graph's
/// indexes and statistics, so they must never descend into a `CALL { USE ... }`
/// scope body (a `BindGraphView`), whose scan runs over a bound projection:
/// substituting an index scan there would silently read the real graph, which
/// ADR 0004 calls a correctness bug, not an optimization.
///
/// The guard is declared here, once, and `final`, so it cannot be copy-pasted
/// out of sync per rewriter and no derived rewriter can re-enable descent - not
/// even by accident. `prev_ops_` lives here too, since the guard maintains it.
class IndexSubstitutionRewriter : public HierarchicalLogicalOperatorVisitor {
 public:
  using HierarchicalLogicalOperatorVisitor::PostVisit;
  using HierarchicalLogicalOperatorVisitor::PreVisit;
  using HierarchicalLogicalOperatorVisitor::Visit;

  // Do not descend into a USE-scope body. The push/pop keeps prev_ops_ balanced
  // with the surrounding PreVisit/PostVisit pairs; PostVisit always runs, even
  // though PreVisit returns false to skip the body.
  bool PreVisit(BindGraphView &op) final {
    prev_ops_.push_back(&op);
    return false;
  }

  bool PostVisit(BindGraphView & /*op*/) final {
    prev_ops_.pop_back();
    return true;
  }

 protected:
  std::vector<LogicalOperator *> prev_ops_;
};

/// The single sanctioned way to run an index-substitution rewrite: construct the
/// rewriter, walk the plan, and hand back its replacement root (null when the
/// root was not replaced). The concept constraint means a rewriter that does not
/// derive from `IndexSubstitutionRewriter` - and so lacks the USE-body guard -
/// cannot be run through here: it is a compile error at the call site, not a
/// silent wrong-graph read. Callers apply their own replacement-root policy.
template <typename TRewriter, typename... Args>
  requires std::derived_from<TRewriter, IndexSubstitutionRewriter>
std::shared_ptr<LogicalOperator> RunIndexSubstitution(LogicalOperator &root, Args &&...args) {
  TRewriter rewriter(std::forward<Args>(args)...);
  root.Accept(rewriter);
  return rewriter.new_root_;
}

}  // namespace memgraph::query::plan::impl

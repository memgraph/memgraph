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

#include <memory>

namespace memgraph::query::plan::v2 {

/// Per-session planner state.  Today this owns the FrontierContext buffers
/// (frontier map, resolver scratch) so their allocated capacity is reused
/// across queries instead of being freed and re-grown each time; it will grow
/// to hold any other per-session planner state (caches, scratch arenas) as
/// the planner stabilises.  Hold one per Interpreter and pass it to
/// ConvertToLogicalOperator.  Pimpl so callers don't see CostFrontier /
/// Alternative - holding one needs only this header, not the egraph or the
/// cost-model types ConvertToLogicalOperator consumes.
class QueryPlannerContext {
 public:
  QueryPlannerContext();
  ~QueryPlannerContext();
  QueryPlannerContext(QueryPlannerContext &&) noexcept;
  QueryPlannerContext &operator=(QueryPlannerContext &&) noexcept;
  QueryPlannerContext(QueryPlannerContext const &) = delete;
  QueryPlannerContext &operator=(QueryPlannerContext const &) = delete;

  struct Impl;

  /// The opaque per-session buffers.  Public so ConvertToLogicalOperator can
  /// reach them without this header depending on the egraph / cost-model types;
  /// `Impl` is incomplete here, so no caller can do anything with it but pass
  /// the reference back to the converter.
  Impl &impl() { return *impl_; }

 private:
  std::unique_ptr<Impl> impl_;
};

}  // namespace memgraph::query::plan::v2

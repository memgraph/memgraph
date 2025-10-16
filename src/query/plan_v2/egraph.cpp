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

#include "query/plan_v2/egraph.hpp"

#include "planner/core/egraph.hpp"

/// SYMBOL
namespace {
struct symbol {
  friend bool operator==(const symbol &, const symbol &) = default;
};
}  // namespace
namespace std {
template <>
struct hash<symbol> {
  size_t operator()(symbol const &value) const noexcept { return 1; }
};
}  // namespace std

/// ANALYSIS
namespace {
struct analysis {};
}  // namespace

using namespace ::memgraph::planner::core;

namespace memgraph::query::plan::v2 {

/// EGRAPH
struct egraph::impl {
  EGraph<symbol, analysis> egraph_;
};

egraph::egraph() : pimpl_(std::make_unique<impl>()) {}
egraph::~egraph() = default;  // required because pimpl

}  // namespace memgraph::query::plan::v2

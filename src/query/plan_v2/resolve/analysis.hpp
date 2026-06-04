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

#include <cstddef>
#include <optional>
#include <variant>

#include "storage/v2/property_value.hpp"

namespace memgraph::query::plan::v2 {

/// Per-e-class analysis facts about an expression's output. Each fact is
/// `nullopt` when unknown. Merged field-wise on e-class union: agreeing facts
/// are kept, a one-sided fact is taken, and a genuine disagreement is a planner
/// bug (sound rewrites cannot equate two different constants).
struct ExpressionAnalysis {
  std::optional<storage::ExternalPropertyValue> known_constant_value;
  std::optional<storage::ExternalPropertyValue::Type> known_type;
  std::optional<std::size_t> known_list_length;

  void merge(ExpressionAnalysis const &other);
};

/// Analysis facts about an operator's output. Empty for now; future facts
/// (distinctness, ordering, cardinality bounds, ...) land here.
struct OperatorAnalysis {};

/// Symbol e-classes are singletons by invariant and carry no analysis facts.
struct SymbolAnalysis {};

using analysis_variant = std::variant<ExpressionAnalysis, OperatorAnalysis, SymbolAnalysis>;

/// The per-e-class analysis slot. The variant arm is the e-class kind
/// (Expression / Operator / Symbol); merging two e-classes of different kinds
/// is a planner bug.
struct analysis : analysis_variant {
  using analysis_variant::analysis_variant;

  void merge(analysis const &other);

  /// The ExpressionAnalysis arm, or nullptr if this e-class is not an
  /// Expression. The single place a fact-gated read recovers expression facts -
  /// callers ask for the arm rather than open-coding a `std::get_if` on the
  /// variant, so the kind check lives here once.
  [[nodiscard]] auto expression() const -> ExpressionAnalysis const * { return std::get_if<ExpressionAnalysis>(this); }
};

}  // namespace memgraph::query::plan::v2

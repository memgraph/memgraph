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
#include <cstdint>
#include <optional>
#include <variant>

#include "storage/v2/property_value.hpp"

namespace memgraph::query::plan::v2 {

/// What a proved length measures. Length arithmetic is not shared between
/// these: concatenating two lists adds their lengths, while `+` over a list and
/// a string appends the string as one element. A new enumerator therefore has
/// to be answered for at every switch over this.
enum class LengthOf : std::uint8_t {
  List,
};

/// A statically proved length, carrying what it is the length of.
struct KnownLength {
  LengthOf of;
  std::size_t value;

  auto operator==(KnownLength const &) const -> bool = default;
};

/// Per-e-class analysis facts about an expression's output. Each fact is
/// `nullopt` when unknown. Merged field-wise on e-class union: agreeing facts
/// are kept, a one-sided fact is taken, and a genuine disagreement is a planner
/// bug (sound rewrites cannot equate two different constants).
struct ExpressionAnalysis {
  std::optional<storage::ExternalPropertyValue> known_constant_value;
  std::optional<KnownLength> known_length;

  void merge(ExpressionAnalysis const &other);

  /// Equality is what lets the rewriter audit, in a debug build, that a fact
  /// never moves without the e-class being announced as touched. See EClass's
  /// note on a conforming analysis.
  auto operator==(ExpressionAnalysis const &) const -> bool = default;
};

/// Analysis facts about an operator's output. Carries none: distinctness,
/// ordering and cardinality bounds would belong here.
struct OperatorAnalysis {
  auto operator==(OperatorAnalysis const &) const -> bool = default;
};

/// Symbol e-classes are singletons by invariant and carry no analysis facts.
struct SymbolAnalysis {
  auto operator==(SymbolAnalysis const &) const -> bool = default;
};

using analysis_variant = std::variant<ExpressionAnalysis, OperatorAnalysis, SymbolAnalysis>;

/// The per-e-class analysis slot. The variant arm is the e-class kind
/// (Expression / Operator / Symbol); merging two e-classes of different kinds
/// is a planner bug.
struct analysis : analysis_variant {
  using analysis_variant::analysis_variant;

  void merge(analysis const &other);

  /// The ExpressionAnalysis arm, or nullptr if this e-class is not an
  /// Expression, so callers take the arm instead of open-coding `std::get_if`.
  /// Reading one fact goes through the readers below, which fold the arm check
  /// and the fact's own absence into a single answer.
  [[nodiscard]] auto expression() const -> ExpressionAnalysis const * { return std::get_if<ExpressionAnalysis>(this); }

  /// The ExpressionAnalysis arm for a symbol's `update` to widen, or nullptr if
  /// this e-class is not an Expression. The caller owes what merge guarantees: a
  /// fact may be added, never retracted.
  [[nodiscard]] auto expression_for_update() -> ExpressionAnalysis * { return std::get_if<ExpressionAnalysis>(this); }

  /// The constant value this e-class is statically known to hold, or nullptr
  /// when that is unknown or the e-class is not an Expression. Borrows from the
  /// analysis, so the pointer dangles once the e-class is merged away.
  [[nodiscard]] auto known_constant() const -> storage::ExternalPropertyValue const * {
    auto const *expr = expression();
    if (expr == nullptr || !expr->known_constant_value) return nullptr;
    return &*expr->known_constant_value;
  }

  /// The known constant as an integer, or nullopt when it is unknown or of
  /// another type. An integral double such as `5.0` reads as unknown, so a
  /// caller whose operation is integer-only cannot silently accept one.
  [[nodiscard]] auto known_int() const -> std::optional<std::int64_t> {
    auto const *value = known_constant();
    if (value == nullptr || !value->IsInt()) return std::nullopt;
    return value->ValueInt();
  }

  /// The provably exact length of the list this e-class holds, or nullopt when
  /// that is unknown or the length proved is not a list's. An answer here is
  /// therefore also a proof that the e-class holds a list, which is what lets a
  /// caller sum two of them or use one as a row count.
  [[nodiscard]] auto known_list_length() const -> std::optional<std::size_t> {
    auto const *expr = expression();
    if (expr == nullptr || !expr->known_length) return std::nullopt;
    switch (expr->known_length->of) {
      case LengthOf::List:
        return expr->known_length->value;
    }
    return std::nullopt;
  }
};

}  // namespace memgraph::query::plan::v2

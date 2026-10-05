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
#include <optional>
#include <variant>
#include <vector>

#include "query/frontend/ast/ast_storage.hpp"

namespace memgraph::query {

class Expression;

using QueryLabelType = std::variant<LabelIx, Expression *>;

/// A node label expression: `&`, `|`, `!`, `%` and parentheses over label leaves, or a plain conjunction
/// such as `:A:B`. Not a `Tree`; `LabelsTest::Make` holds it in a `LabelsTest`.
///
/// Copying duplicates the term for the same `AstStorage`: a `Label` keeps the index it was interned at, and
/// a `Dynamic` keeps pointing at the storage that owns its expression. Use `Clone` to duplicate into another
/// storage, which re-interns the one and rebuilds the other.
struct LabelTerm {
  struct Label {
    LabelIx label;
  };

  /// The `variable.prop` that names a label, which only CREATE reads.
  struct Dynamic {
    Expression *expression{nullptr};
  };

  /// `%`: the node carries any label.
  struct Wildcard {};

  struct And {
    std::vector<LabelTerm> operands;
  };

  struct Or {
    std::vector<LabelTerm> operands;
  };

  /// Holds its one operand on the heap and copies it when copied, as `LabelTerm` is copied by value. C++26
  /// `std::indirect<LabelTerm>` is this member; use it once the project builds as C++26.
  struct Not {
    explicit Not(LabelTerm operand) : operand(std::make_unique<LabelTerm>(std::move(operand))) {}

    Not(const Not &other) : operand(std::make_unique<LabelTerm>(*other.operand)) {}

    Not &operator=(const Not &other) {
      if (this != &other) operand = std::make_unique<LabelTerm>(*other.operand);
      return *this;
    }

    Not(Not &&) noexcept = default;
    Not &operator=(Not &&) noexcept = default;
    ~Not() = default;

    std::unique_ptr<LabelTerm> operand;
  };

  template <typename T>
  const T *As() const {
    return std::get_if<T>(&node);
  }

  template <typename T>
  T *As() {
    return std::get_if<T>(&node);
  }

  LabelTerm Clone(AstStorage *storage) const;

  /// The leaves of a plain conjunction -- one leaf, or an `And` of leaves -- or nullopt for any other shape.
  /// Only a conjunction holds a `Dynamic` leaf: the grammar keeps it away from the operators.
  std::optional<std::vector<QueryLabelType>> Conjunction() const;

  std::variant<Label, Dynamic, Wildcard, And, Or, Not> node;
};

/// What a node must carry for a test of plain labels: each of `labels`, and one of each group in `or_labels`.
struct LabelCnf {
  std::vector<LabelIx> labels;
  std::vector<std::vector<LabelIx>> or_labels;
};

}  // namespace memgraph::query

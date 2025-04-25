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

#pragma once

#include "query/frontend/ast/query/expression.hpp"

#include "query/frontend/ast/query/identifier.hpp"
#include "query/frontend/ast/query/pattern.hpp"
#include "query/frontend/ast/query/where.hpp"
#include "query/frontend/semantic/symbol.hpp"

namespace memgraph::query {
class PatternComprehension : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  PatternComprehension() = default;

  DECLARE_VISITABLE(ExpressionVisitor<TypedValue>);
  DECLARE_VISITABLE(ExpressionVisitor<TypedValue *>);
  DECLARE_VISITABLE(ExpressionVisitor<void>);
  DECLARE_VISITABLE(HierarchicalTreeVisitor);

  PatternComprehension *MapTo(const Symbol &symbol) {
    symbol_pos_ = symbol.position();
    return this;
  }

  // The variable name.
  Identifier *variable_{nullptr};
  // The pattern to match.
  Pattern *pattern_{nullptr};
  // Optional WHERE clause for filtering.
  Where *filter_{nullptr};
  // The projection expression.
  Expression *resultExpr_{nullptr};

  /// Symbol table position of the symbol this Aggregation is mapped to.
  int32_t symbol_pos_{-1};

  PatternComprehension *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<PatternComprehension>();
    object->variable_ = variable_ ? variable_->Clone(storage) : nullptr;
    object->pattern_ = pattern_ ? pattern_->Clone(storage) : nullptr;
    object->filter_ = filter_ ? filter_->Clone(storage) : nullptr;
    object->resultExpr_ = resultExpr_ ? resultExpr_->Clone(storage) : nullptr;

    object->symbol_pos_ = symbol_pos_;
    return object;
  }

 protected:
  PatternComprehension(Identifier *variable, Pattern *pattern, Where *filter, Expression *resultExpr)
      : variable_(variable), pattern_(pattern), filter_(filter), resultExpr_(resultExpr) {}

 private:
  friend class AstStorage;
};
}  // namespace memgraph::query

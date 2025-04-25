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
#include "query/frontend/ast/query/pattern.hpp"
#include "query/frontend/semantic/symbol.hpp"

namespace memgraph::query {
class Exists : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Exists() = default;

  DECLARE_VISITABLE(ExpressionVisitor<TypedValue>);
  DECLARE_VISITABLE(ExpressionVisitor<TypedValue *>);
  DECLARE_VISITABLE(ExpressionVisitor<void>);
  DECLARE_VISITABLE(HierarchicalTreeVisitor);

  Exists *MapTo(const Symbol &symbol) {
    symbol_pos_ = symbol.position();
    return this;
  }

  memgraph::query::Pattern *pattern_{nullptr};
  /// Symbol table position of the symbol this Aggregation is mapped to.
  int32_t symbol_pos_{-1};

  Exists *Clone(AstStorage *storage) const override {
    Exists *object = storage->Create<Exists>();
    object->pattern_ = pattern_ ? pattern_->Clone(storage) : nullptr;
    object->symbol_pos_ = symbol_pos_;
    return object;
  }

 protected:
  Exists(Pattern *pattern) : pattern_(pattern) {}

 private:
  friend class AstStorage;
};
}  // namespace memgraph::query

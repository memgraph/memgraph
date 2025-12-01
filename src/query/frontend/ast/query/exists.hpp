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

#include "query/frontend/ast/ast.hpp"
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
  DECLARE_VISITABLE(ExpressionVisitor<TypedValue const *>);
  DECLARE_VISITABLE(ExpressionVisitor<void>);
  DECLARE_VISITABLE(HierarchicalTreeVisitor);

  Exists *MapTo(const Symbol &symbol) {
    symbol_pos_ = symbol.position();
    return this;
  }

  std::variant<std::monostate, memgraph::query::Pattern *, memgraph::query::CypherQuery *> content_;
  /// Symbol table position of the symbol this Aggregation is mapped to.
  int32_t symbol_pos_{-1};

  Exists *Clone(AstStorage *storage) const override {
    Exists *object = storage->Create<Exists>();
    if (std::holds_alternative<Pattern *>(content_)) {
      object->content_ = std::get<Pattern *>(content_)->Clone(storage);
    } else if (std::holds_alternative<CypherQuery *>(content_)) {
      object->content_ = std::get<CypherQuery *>(content_)->Clone(storage);
    } else {
      object->content_ = std::monostate{};
    }
    object->symbol_pos_ = symbol_pos_;
    return object;
  }

  bool HasPattern() const { return std::holds_alternative<Pattern *>(content_); }
  bool HasSubquery() const { return std::holds_alternative<CypherQuery *>(content_); }

  Pattern *GetPattern() const { return HasPattern() ? std::get<Pattern *>(content_) : nullptr; }
  CypherQuery *GetSubquery() const { return HasSubquery() ? std::get<CypherQuery *>(content_) : nullptr; }

 protected:
  explicit Exists(Pattern *pattern) : content_(pattern) {}
  explicit Exists(CypherQuery *subquery) : content_(subquery) {}

 private:
  friend class AstStorage;
};
}  // namespace memgraph::query

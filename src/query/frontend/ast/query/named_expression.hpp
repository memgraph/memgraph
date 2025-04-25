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

#include "query/frontend/ast/ast_storage.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/ast/query/expression.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/typed_value.hpp"

namespace memgraph::query {
class NamedExpression : public memgraph::query::Tree,
                        public utils::Visitable<HierarchicalTreeVisitor>,
                        public utils::Visitable<ExpressionVisitor<TypedValue>>,
                        public utils::Visitable<ExpressionVisitor<TypedValue *>>,
                        public utils::Visitable<ExpressionVisitor<void>> {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  using utils::Visitable<ExpressionVisitor<TypedValue>>::Accept;
  using utils::Visitable<ExpressionVisitor<void>>::Accept;
  using utils::Visitable<HierarchicalTreeVisitor>::Accept;

  NamedExpression() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  NamedExpression *MapTo(const Symbol &symbol) {
    symbol_pos_ = symbol.position();
    return this;
  }

  std::string name_;
  memgraph::query::Expression *expression_{nullptr};
  /// This field contains token position of first token in named expression used to create name_. If NamedExpression
  /// object is not created from query or it is aliased leave this value at -1.
  int32_t token_position_{-1};
  /// Symbol table position of the symbol this NamedExpression is mapped to.
  int32_t symbol_pos_{-1};
  /// True if the variable is aliased
  bool is_aliased_{false};

  NamedExpression *Clone(AstStorage *storage) const override {
    NamedExpression *object = storage->Create<NamedExpression>();
    object->name_ = name_;
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    object->token_position_ = token_position_;
    object->symbol_pos_ = symbol_pos_;
    object->is_aliased_ = is_aliased_;
    return object;
  }

 protected:
  explicit NamedExpression(const std::string &name) : name_(name) {}
  NamedExpression(const std::string &name, Expression *expression) : name_(name), expression_(expression) {}
  NamedExpression(const std::string &name, Expression *expression, int token_position)
      : name_(name), expression_(expression), token_position_(token_position) {}

 private:
  friend class AstStorage;
};
}  // namespace memgraph::query

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
#include "query/frontend/semantic/symbol.hpp"

namespace memgraph::query {
class Identifier : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Identifier() = default;

  DECLARE_VISITABLE(ExpressionVisitor<TypedValue>);
  DECLARE_VISITABLE(ExpressionVisitor<TypedValue *>);
  DECLARE_VISITABLE(ExpressionVisitor<void>);
  DECLARE_VISITABLE(HierarchicalTreeVisitor);

  Identifier *MapTo(const Symbol &symbol) {
    symbol_pos_ = symbol.position();
    is_temporary_ = symbol.is_temporary();
    return this;
  }

  explicit Identifier(const std::string &name) : name_(name) {}
  Identifier(const std::string &name, bool user_declared) : name_(name), user_declared_(user_declared) {}

  std::string name_;
  bool user_declared_{true};
  bool is_temporary_{false};
  /// Symbol table position of the symbol this Identifier is mapped to.
  int32_t symbol_pos_{-1};

  Identifier *Clone(AstStorage *storage) const override {
    Identifier *object = storage->Create<Identifier>();
    object->name_ = name_;
    object->user_declared_ = user_declared_;
    object->is_temporary_ = is_temporary_;
    object->symbol_pos_ = symbol_pos_;
    return object;
  }

 private:
  friend class AstStorage;
};

}  // namespace memgraph::query

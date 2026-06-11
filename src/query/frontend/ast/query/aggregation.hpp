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

#include <array>
#include <string_view>

#include "query/frontend/ast/query/binary_operator.hpp"
#include "query/frontend/semantic/symbol.hpp"

namespace memgraph::query {
class Aggregation : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;

  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Op {
    COUNT,
    MIN,
    MAX,
    SUM,
    AVG,
    COLLECT_LIST,
    COLLECT_MAP,
    PROJECT_PATH,
    PROJECT_LISTS,
    DERIVE,
    DERIVE_LISTS
  };

  Aggregation() = default;

  static constexpr std::string_view kCount = "COUNT";
  static constexpr std::string_view kMin = "MIN";
  static constexpr std::string_view kMax = "MAX";
  static constexpr std::string_view kSum = "SUM";
  static constexpr std::string_view kAvg = "AVG";
  static constexpr std::string_view kCollect = "COLLECT";
  static constexpr std::string_view kProject = "PROJECT";
  static constexpr std::string_view kDerive = "DERIVE";

  static std::string OpToString(Op op) {
    static constexpr std::array op_strings = {
        kCount, kMin, kMax, kSum, kAvg, kCollect, kCollect, kProject, kProject, kDerive, kDerive};
    return std::string{op_strings[static_cast<int>(op)]};
  }

  DECLARE_VISITABLE(ExpressionVisitor<TypedValue>);
  DECLARE_VISITABLE(ExpressionVisitor<TypedValue *>);
  DECLARE_VISITABLE(ExpressionVisitor<TypedValue const *>);
  DECLARE_VISITABLE(ExpressionVisitor<void>);
  DECLARE_VISITABLE(HierarchicalTreeVisitor);

  Aggregation *MapTo(const Symbol &symbol) {
    symbol_pos_ = symbol.position();
    return this;
  }

  /// Aggregation's third expression. Only used by DERIVE_LISTS, where the call is derive(nodes, edges, options):
  /// expression1_ holds the node list, expression2_ the relationship list, and expression3_ the options map.
  /// Null for every other aggregation.
  memgraph::query::Expression *expression3_{nullptr};

  memgraph::query::Aggregation::Op op_;
  /// Symbol table position of the symbol this Aggregation is mapped to.
  int32_t symbol_pos_{-1};
  bool distinct_{false};

  Aggregation *Clone(AstStorage *storage) const override {
    Aggregation *object = storage->Create<Aggregation>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    object->expression3_ = expression3_ ? expression3_->Clone(storage) : nullptr;
    object->op_ = op_;
    object->symbol_pos_ = symbol_pos_;
    object->distinct_ = distinct_;
    return object;
  }

 protected:
  // Use only for serialization.
  explicit Aggregation(Op op) : op_(op) {}

  /// Aggregation's first expression is the value being aggregated. The second expression is used either as a key in
  /// COLLECT_MAP or for the relationships list in the two-argument overload of PROJECT_PATH; no other aggregate
  /// functions use this parameter.
  Aggregation(Expression *expression1, Expression *expression2, Op op, bool distinct);

  /// Three-expression overload used by DERIVE_LISTS: derive(nodes, edges, options).
  Aggregation(Expression *expression1, Expression *expression2, Expression *expression3, Op op, bool distinct);

 private:
  friend class AstStorage;
};
}  // namespace memgraph::query

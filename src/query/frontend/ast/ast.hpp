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

#include <memory>
#include <unordered_map>
#include <variant>
#include <vector>

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast_storage.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/ast/ordering.hpp"
#include "query/frontend/ast/query/binary_operator.hpp"
#include "query/frontend/ast/query/expression.hpp"
#include "query/frontend/ast/query/identifier.hpp"
#include "query/frontend/ast/query/named_expression.hpp"
#include "query/frontend/ast/query/pattern.hpp"
#include "query/frontend/ast/query/query.hpp"
#include "query/frontend/ast/query/where.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "query/procedure/module_fwd.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/constraints/type_constraints.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/exceptions.hpp"
#include "utils/string.hpp"
#include "utils/typeinfo.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::query {

inline constexpr std::string_view kBoltServer = "bolt_server";
inline constexpr std::string_view kReplicationServer = "replication_server";
inline constexpr std::string_view kCoordinatorServer = "coordinator_server";
inline constexpr std::string_view kManagementServer = "management_server";

inline constexpr std::string_view kLabel = "label";
inline constexpr std::string_view kProperty = "property";
inline constexpr std::string_view kMetric = "metric";
inline constexpr std::string_view kDimension = "dimension";
inline constexpr std::string_view kCapacity = "capacity";
inline constexpr std::string_view kResizeCoefficient = "resize_coefficient";
inline constexpr std::uint16_t kDefaultResizeCoefficient = 2;
inline constexpr std::string_view kDefaultMetric = "l2sq";

class UnaryOperator : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  UnaryOperator() = default;

  memgraph::query::Expression *expression_{nullptr};

  UnaryOperator *Clone(AstStorage *storage) const override = 0;

 protected:
  explicit UnaryOperator(Expression *expression) : expression_(expression) {}

 private:
  friend class AstStorage;
};

class OrOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  OrOperator *Clone(AstStorage *storage) const override {
    OrOperator *object = storage->Create<OrOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class XorOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  XorOperator *Clone(AstStorage *storage) const override {
    XorOperator *object = storage->Create<XorOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class AndOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  AndOperator *Clone(AstStorage *storage) const override {
    AndOperator *object = storage->Create<AndOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class AdditionOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  AdditionOperator *Clone(AstStorage *storage) const override {
    AdditionOperator *object = storage->Create<AdditionOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class SubtractionOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  SubtractionOperator *Clone(AstStorage *storage) const override {
    SubtractionOperator *object = storage->Create<SubtractionOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class MultiplicationOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  MultiplicationOperator *Clone(AstStorage *storage) const override {
    MultiplicationOperator *object = storage->Create<MultiplicationOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class DivisionOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  DivisionOperator *Clone(AstStorage *storage) const override {
    DivisionOperator *object = storage->Create<DivisionOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class ModOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  ModOperator *Clone(AstStorage *storage) const override {
    ModOperator *object = storage->Create<ModOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class ExponentiationOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  ExponentiationOperator *Clone(AstStorage *storage) const override {
    ExponentiationOperator *object = storage->Create<ExponentiationOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class NotEqualOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  NotEqualOperator *Clone(AstStorage *storage) const override {
    NotEqualOperator *object = storage->Create<NotEqualOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class EqualOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  EqualOperator *Clone(AstStorage *storage) const override {
    EqualOperator *object = storage->Create<EqualOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class LessOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  LessOperator *Clone(AstStorage *storage) const override {
    LessOperator *object = storage->Create<LessOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class GreaterOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  GreaterOperator *Clone(AstStorage *storage) const override {
    GreaterOperator *object = storage->Create<GreaterOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class LessEqualOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  LessEqualOperator *Clone(AstStorage *storage) const override {
    LessEqualOperator *object = storage->Create<LessEqualOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class GreaterEqualOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  GreaterEqualOperator *Clone(AstStorage *storage) const override {
    GreaterEqualOperator *object = storage->Create<GreaterEqualOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class RangeOperator : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Expression *expr1_{};
  Expression *expr2_{};

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expr1_->Accept(visitor) && expr2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  RangeOperator *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<RangeOperator>();
    object->expr1_ = expr1_ ? expr1_->Clone(storage) : nullptr;
    object->expr2_ = expr2_ ? expr2_->Clone(storage) : nullptr;
    return object;
  }

 private:
  friend class AstStorage;
};

class InListOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  InListOperator *Clone(AstStorage *storage) const override {
    InListOperator *object = storage->Create<InListOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class SubscriptOperator : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  SubscriptOperator *Clone(AstStorage *storage) const override {
    SubscriptOperator *object = storage->Create<SubscriptOperator>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class AstStorage;
};

class NotOperator : public memgraph::query::UnaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  NotOperator *Clone(AstStorage *storage) const override {
    NotOperator *object = storage->Create<NotOperator>();
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using UnaryOperator::UnaryOperator;

 private:
  friend class AstStorage;
};

class UnaryPlusOperator : public memgraph::query::UnaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  UnaryPlusOperator *Clone(AstStorage *storage) const override {
    UnaryPlusOperator *object = storage->Create<UnaryPlusOperator>();
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using UnaryOperator::UnaryOperator;

 private:
  friend class AstStorage;
};

class UnaryMinusOperator : public memgraph::query::UnaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  UnaryMinusOperator *Clone(AstStorage *storage) const override {
    UnaryMinusOperator *object = storage->Create<UnaryMinusOperator>();
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using UnaryOperator::UnaryOperator;

 private:
  friend class AstStorage;
};

class IsNullOperator : public memgraph::query::UnaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  IsNullOperator *Clone(AstStorage *storage) const override {
    IsNullOperator *object = storage->Create<IsNullOperator>();
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using UnaryOperator::UnaryOperator;

 private:
  friend class AstStorage;
};

class ListSlicingOperator : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  ListSlicingOperator() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = list_->Accept(visitor);
      if (cont && lower_bound_) {
        cont = lower_bound_->Accept(visitor);
      }
      if (cont && upper_bound_) {
        upper_bound_->Accept(visitor);
      }
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Expression *list_{nullptr};
  memgraph::query::Expression *lower_bound_{nullptr};
  memgraph::query::Expression *upper_bound_{nullptr};

  ListSlicingOperator *Clone(AstStorage *storage) const override {
    ListSlicingOperator *object = storage->Create<ListSlicingOperator>();
    object->list_ = list_ ? list_->Clone(storage) : nullptr;
    object->lower_bound_ = lower_bound_ ? lower_bound_->Clone(storage) : nullptr;
    object->upper_bound_ = upper_bound_ ? upper_bound_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  ListSlicingOperator(Expression *list, Expression *lower_bound, Expression *upper_bound)
      : list_(list), lower_bound_(lower_bound), upper_bound_(upper_bound) {}

 private:
  friend class AstStorage;
};

class IfOperator : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  IfOperator() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      condition_->Accept(visitor) && then_expression_->Accept(visitor) && else_expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  /// None of the expressions should be nullptr. If there is no else_expression, you should make it null
  /// PrimitiveLiteral.
  memgraph::query::Expression *condition_;
  memgraph::query::Expression *then_expression_;
  memgraph::query::Expression *else_expression_;

  IfOperator *Clone(AstStorage *storage) const override {
    IfOperator *object = storage->Create<IfOperator>();
    object->condition_ = condition_ ? condition_->Clone(storage) : nullptr;
    object->then_expression_ = then_expression_ ? then_expression_->Clone(storage) : nullptr;
    object->else_expression_ = else_expression_ ? else_expression_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  IfOperator(Expression *condition, Expression *then_expression, Expression *else_expression)
      : condition_(condition), then_expression_(then_expression), else_expression_(else_expression) {}

 private:
  friend class AstStorage;
};

class BaseLiteral : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  BaseLiteral() = default;

  BaseLiteral *Clone(AstStorage *storage) const override = 0;

 private:
  friend class AstStorage;
};

class PrimitiveLiteral : public memgraph::query::BaseLiteral {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  PrimitiveLiteral() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  storage::ExternalPropertyValue value_;
  /// This field contains token position of literal used to create PrimitiveLiteral object. If PrimitiveLiteral object
  /// is not created from query, leave its value at -1.
  int32_t token_position_{-1};

  PrimitiveLiteral *Clone(AstStorage *storage) const override {
    PrimitiveLiteral *object = storage->Create<PrimitiveLiteral>();
    object->value_ = value_;
    object->token_position_ = token_position_;
    return object;
  }

 protected:
  template <typename T>
  explicit PrimitiveLiteral(T value) : value_(value) {}
  template <typename T>
  PrimitiveLiteral(T value, int token_position) : value_(value), token_position_(token_position) {}

 private:
  friend class AstStorage;
};

class ListLiteral : public memgraph::query::BaseLiteral {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  ListLiteral() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto expr_ptr : elements_)
        if (!expr_ptr->Accept(visitor)) break;
    }
    return visitor.PostVisit(*this);
  }

  std::vector<memgraph::query::Expression *> elements_;

  ListLiteral *Clone(AstStorage *storage) const override {
    ListLiteral *object = storage->Create<ListLiteral>();
    object->elements_.resize(elements_.size());
    for (auto i0 = 0; i0 < elements_.size(); ++i0) {
      object->elements_[i0] = elements_[i0] ? elements_[i0]->Clone(storage) : nullptr;
    }
    return object;
  }

 protected:
  explicit ListLiteral(const std::vector<Expression *> &elements) : elements_(elements) {}

 private:
  friend class AstStorage;
};

class MapLiteral : public memgraph::query::BaseLiteral {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  MapLiteral() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto pair : elements_) {
        if (!pair.second->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  std::unordered_map<memgraph::query::PropertyIx, memgraph::query::Expression *> elements_;

  MapLiteral *Clone(AstStorage *storage) const override {
    MapLiteral *object = storage->Create<MapLiteral>();
    for (const auto &entry : elements_) {
      PropertyIx key = storage->GetPropertyIx(entry.first.name);
      object->elements_[key] = entry.second->Clone(storage);
    }
    return object;
  }

 protected:
  explicit MapLiteral(const std::unordered_map<PropertyIx, Expression *> &elements) : elements_(elements) {}

 private:
  friend class AstStorage;
};

struct MapProjectionData {
  Expression *map_variable;
  std::unordered_map<PropertyIx, Expression *> elements;
};

class MapProjectionLiteral : public memgraph::query::BaseLiteral {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  MapProjectionLiteral() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      map_variable_->Accept(visitor);

      for (auto pair : elements_) {
        if (!pair.second) continue;

        if (!pair.second->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  Expression *map_variable_;
  std::unordered_map<PropertyIx, Expression *> elements_;

  MapProjectionLiteral *Clone(AstStorage *storage) const override {
    MapProjectionLiteral *object = storage->Create<MapProjectionLiteral>();
    object->map_variable_ = map_variable_->Clone(storage);

    for (const auto &entry : elements_) {
      auto key = storage->GetPropertyIx(entry.first.name);

      if (!entry.second) {
        object->elements_[key] = nullptr;
        continue;
      }

      object->elements_[key] = entry.second->Clone(storage);
    }
    return object;
  }

 protected:
  explicit MapProjectionLiteral(Expression *map_variable, std::unordered_map<PropertyIx, Expression *> &&elements)
      : map_variable_(map_variable), elements_(std::move(elements)) {}

 private:
  friend class AstStorage;
};

class PropertyLookup : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class EvaluationMode { GET_OWN_PROPERTY, GET_ALL_PROPERTIES };

  PropertyLookup() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Expression *expression_{nullptr};
  memgraph::query::PropertyIx property_;
  memgraph::query::PropertyLookup::EvaluationMode evaluation_mode_{EvaluationMode::GET_OWN_PROPERTY};

  PropertyLookup *Clone(AstStorage *storage) const override {
    PropertyLookup *object = storage->Create<PropertyLookup>();
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    object->property_ = storage->GetPropertyIx(property_.name);
    object->evaluation_mode_ = evaluation_mode_;
    return object;
  }

 protected:
  PropertyLookup(Expression *expression, PropertyIx property)
      : expression_(expression), property_(std::move(property)) {}

 private:
  friend class AstStorage;
};

class AllPropertiesLookup : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  AllPropertiesLookup() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Expression *expression_{nullptr};

  AllPropertiesLookup *Clone(AstStorage *storage) const override {
    AllPropertiesLookup *object = storage->Create<AllPropertiesLookup>();
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  explicit AllPropertiesLookup(Expression *expression) : expression_(expression) {}

 private:
  friend class AstStorage;
};

using QueryLabelType = std::variant<LabelIx, Expression *>;

class LabelsTest : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  LabelsTest() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Expression *expression_{nullptr};
  std::vector<memgraph::query::LabelIx> labels_;  // TODO: Maybe we should unify this with or_labels_
  std::vector<std::vector<memgraph::query::LabelIx>>
      or_labels_;  // Because we need to support OR in labels -> node has to have at least one of the labels in "inner"
                   // vector

  LabelsTest *Clone(AstStorage *storage) const override {
    LabelsTest *object = storage->Create<LabelsTest>();
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    object->labels_.resize(labels_.size());
    for (auto i = 0; i < object->labels_.size(); ++i) {
      object->labels_[i] = storage->GetLabelIx(labels_[i].name);
    }
    object->or_labels_.resize(or_labels_.size());
    for (auto i = 0; i < object->or_labels_.size(); ++i) {
      object->or_labels_[i].resize(or_labels_[i].size());
      for (auto j = 0; j < object->or_labels_[i].size(); ++j) {
        object->or_labels_[i][j] = storage->GetLabelIx(or_labels_[i][j].name);
      }
    }
    return object;
  }

 protected:
  LabelsTest(Expression *expression, std::vector<LabelIx> labels, bool label_expression = false)
      : expression_(expression) {
    if (!label_expression) {
      labels_ = std::move(labels);
    } else {
      or_labels_.push_back(std::move(labels));
    }
  }
  LabelsTest(Expression *expression, const std::vector<QueryLabelType> &labels) : expression_(expression) {
    labels_.reserve(labels.size());
    for (const auto &label : labels) {
      if (const auto *label_ix = std::get_if<LabelIx>(&label)) {
        labels_.push_back(*label_ix);
      } else {
        throw SemanticException("You can't use labels in filter expressions.");
      }
    }
  }

 private:
  friend class AstStorage;
};

class Function : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Function() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto *argument : arguments_) {
        if (!argument->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  std::vector<memgraph::query::Expression *> arguments_;
  std::string function_name_;
  std::function<TypedValue(const TypedValue *, int64_t, const FunctionContext &)> function_;
  // This is needed to acquire the shared lock on the module so it doesn't get reloaded while the query is running
  std::shared_ptr<procedure::Module> module_;

  Function *Clone(AstStorage *storage) const override {
    Function *object = storage->Create<Function>();
    object->arguments_.resize(arguments_.size());
    for (auto i1 = 0; i1 < arguments_.size(); ++i1) {
      object->arguments_[i1] = arguments_[i1] ? arguments_[i1]->Clone(storage) : nullptr;
    }
    object->function_name_ = function_name_;
    object->function_ = function_;
    object->module_ = module_;
    return object;
  }

  bool IsBuiltin() const { return not bool(module_); }

  bool IsUserDefined() const { return bool(module_); }

 protected:
  Function(const std::string &function_name, const std::vector<Expression *> &arguments)
      : arguments_(arguments), function_name_(function_name) {
    auto func_result = NameToFunction(function_name_);

    std::visit(utils::Overloaded{
                   [this](func_impl function) {
                     function_ = function;
                     module_.reset();
                   },
                   [this](std::pair<func_impl, std::shared_ptr<procedure::Module>> &function) {
                     function_ = function.first;
                     module_ = std::move(function.second);
                   },
                   [&](std::monostate) { throw SemanticException("Function '{}' doesn't exist.", function_name); }},
               func_result);
  }

 private:
  friend class AstStorage;
};

class Reduce : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Reduce() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      accumulator_->Accept(visitor) && initializer_->Accept(visitor) && identifier_->Accept(visitor) &&
          list_->Accept(visitor) && expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  /// Identifier for the accumulating variable
  memgraph::query::Identifier *accumulator_{nullptr};
  /// Expression which produces the initial accumulator value.
  memgraph::query::Expression *initializer_{nullptr};
  /// Identifier for the list element.
  memgraph::query::Identifier *identifier_{nullptr};
  /// Expression which produces a list to be reduced.
  memgraph::query::Expression *list_{nullptr};
  /// Expression which does the reduction, i.e. produces the new accumulator value.
  memgraph::query::Expression *expression_{nullptr};

  Reduce *Clone(AstStorage *storage) const override {
    Reduce *object = storage->Create<Reduce>();
    object->accumulator_ = accumulator_ ? accumulator_->Clone(storage) : nullptr;
    object->initializer_ = initializer_ ? initializer_->Clone(storage) : nullptr;
    object->identifier_ = identifier_ ? identifier_->Clone(storage) : nullptr;
    object->list_ = list_ ? list_->Clone(storage) : nullptr;
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  Reduce(Identifier *accumulator, Expression *initializer, Identifier *identifier, Expression *list,
         Expression *expression)
      : accumulator_(accumulator),
        initializer_(initializer),
        identifier_(identifier),
        list_(list),
        expression_(expression) {}

 private:
  friend class AstStorage;
};

class Coalesce : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Coalesce() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto *expr : expressions_) {
        if (!expr->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  /// A list of expressions to evaluate. None of the expressions should be nullptr.
  std::vector<memgraph::query::Expression *> expressions_;

  Coalesce *Clone(AstStorage *storage) const override {
    Coalesce *object = storage->Create<Coalesce>();
    object->expressions_.resize(expressions_.size());
    for (auto i2 = 0; i2 < expressions_.size(); ++i2) {
      object->expressions_[i2] = expressions_[i2] ? expressions_[i2]->Clone(storage) : nullptr;
    }
    return object;
  }

 private:
  explicit Coalesce(const std::vector<Expression *> &expressions) : expressions_(expressions) {}

  friend class AstStorage;
};

class Extract : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Extract() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor) && list_->Accept(visitor) && expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  /// Identifier for the list element.
  memgraph::query::Identifier *identifier_{nullptr};
  /// Expression which produces a list which will be extracted.
  memgraph::query::Expression *list_{nullptr};
  /// Expression which produces the new value for list element.
  memgraph::query::Expression *expression_{nullptr};

  Extract *Clone(AstStorage *storage) const override {
    Extract *object = storage->Create<Extract>();
    object->identifier_ = identifier_ ? identifier_->Clone(storage) : nullptr;
    object->list_ = list_ ? list_->Clone(storage) : nullptr;
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  Extract(Identifier *identifier, Expression *list, Expression *expression)
      : identifier_(identifier), list_(list), expression_(expression) {}

 private:
  friend class AstStorage;
};

class All : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  All() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor) && list_expression_->Accept(visitor) && where_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Identifier *identifier_{nullptr};
  memgraph::query::Expression *list_expression_{nullptr};
  memgraph::query::Where *where_{nullptr};

  All *Clone(AstStorage *storage) const override {
    All *object = storage->Create<All>();
    object->identifier_ = identifier_ ? identifier_->Clone(storage) : nullptr;
    object->list_expression_ = list_expression_ ? list_expression_->Clone(storage) : nullptr;
    object->where_ = where_ ? where_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  All(Identifier *identifier, Expression *list_expression, Where *where)
      : identifier_(identifier), list_expression_(list_expression), where_(where) {}

 private:
  friend class AstStorage;
};

class Single : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Single() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor) && list_expression_->Accept(visitor) && where_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Identifier *identifier_{nullptr};
  memgraph::query::Expression *list_expression_{nullptr};
  memgraph::query::Where *where_{nullptr};

  Single *Clone(AstStorage *storage) const override {
    Single *object = storage->Create<Single>();
    object->identifier_ = identifier_ ? identifier_->Clone(storage) : nullptr;
    object->list_expression_ = list_expression_ ? list_expression_->Clone(storage) : nullptr;
    object->where_ = where_ ? where_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  Single(Identifier *identifier, Expression *list_expression, Where *where)
      : identifier_(identifier), list_expression_(list_expression), where_(where) {}

 private:
  friend class AstStorage;
};

class Any : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Any() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor) && list_expression_->Accept(visitor) && where_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Identifier *identifier_{nullptr};
  memgraph::query::Expression *list_expression_{nullptr};
  memgraph::query::Where *where_{nullptr};

  Any *Clone(AstStorage *storage) const override {
    Any *object = storage->Create<Any>();
    object->identifier_ = identifier_ ? identifier_->Clone(storage) : nullptr;
    object->list_expression_ = list_expression_ ? list_expression_->Clone(storage) : nullptr;
    object->where_ = where_ ? where_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  Any(Identifier *identifier, Expression *list_expression, Where *where)
      : identifier_(identifier), list_expression_(list_expression), where_(where) {}

 private:
  friend class AstStorage;
};

class None : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  None() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor) && list_expression_->Accept(visitor) && where_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Identifier *identifier_{nullptr};
  memgraph::query::Expression *list_expression_{nullptr};
  memgraph::query::Where *where_{nullptr};

  None *Clone(AstStorage *storage) const override {
    None *object = storage->Create<None>();
    object->identifier_ = identifier_ ? identifier_->Clone(storage) : nullptr;
    object->list_expression_ = list_expression_ ? list_expression_->Clone(storage) : nullptr;
    object->where_ = where_ ? where_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  None(Identifier *identifier, Expression *list_expression, Where *where)
      : identifier_(identifier), list_expression_(list_expression), where_(where) {}

 private:
  friend class AstStorage;
};

class ListComprehension : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  ListComprehension() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor);
      list_->Accept(visitor);
      if (where_) {
        where_->Accept(visitor);
      }
      if (expression_) {
        expression_->Accept(visitor);
      }
    }
    return visitor.PostVisit(*this);
  }

  /// Identifier for the list element.
  memgraph::query::Identifier *identifier_{nullptr};
  /// Expression which produces a list which will be extracted.
  memgraph::query::Expression *list_{nullptr};
  /// Expression which is the predicate for the list.
  memgraph::query::Where *where_{nullptr};
  /// Expression which produces the new value for list element.
  memgraph::query::Expression *expression_{nullptr};

  ListComprehension *Clone(AstStorage *storage) const override {
    ListComprehension *object = storage->Create<ListComprehension>();
    object->identifier_ = identifier_ ? identifier_->Clone(storage) : nullptr;
    object->list_ = list_ ? list_->Clone(storage) : nullptr;
    object->where_ = where_ ? where_->Clone(storage) : nullptr;
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  ListComprehension(Identifier *identifier, Expression *list, Where *where, Expression *expression)
      : identifier_(identifier), list_(list), where_(where), expression_(expression) {}

 private:
  friend class AstStorage;
};

class ParameterLookup : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  ParameterLookup() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  /// This field contains token position of *literal* used to create ParameterLookup object. If ParameterLookup object
  /// is not created from a literal leave this value at -1.
  int32_t token_position_{-1};

  ParameterLookup *Clone(AstStorage *storage) const override {
    ParameterLookup *object = storage->Create<ParameterLookup>();
    object->token_position_ = token_position_;
    return object;
  }

 protected:
  explicit ParameterLookup(int token_position) : token_position_(token_position) {}

 private:
  friend class AstStorage;
};

class RegexMatch : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  RegexMatch() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      string_expr_->Accept(visitor) && regex_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Expression *string_expr_;
  memgraph::query::Expression *regex_;

  RegexMatch *Clone(AstStorage *storage) const override {
    RegexMatch *object = storage->Create<RegexMatch>();
    object->string_expr_ = string_expr_ ? string_expr_->Clone(storage) : nullptr;
    object->regex_ = regex_ ? regex_->Clone(storage) : nullptr;
    return object;
  }

 private:
  friend class AstStorage;
  RegexMatch(Expression *string_expr, Expression *regex) : string_expr_(string_expr), regex_(regex) {}
};

class NodeAtom : public memgraph::query::PatternAtom {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      if (auto *properties = std::get_if<std::unordered_map<PropertyIx, Expression *>>(&properties_)) {
        bool cont = identifier_->Accept(visitor);
        for (auto &property : *properties) {
          if (cont) {
            cont = property.second->Accept(visitor);
          }
        }
      } else {
        std::get<ParameterLookup *>(properties_)->Accept(visitor);
      }
    }
    return visitor.PostVisit(*this);
  }

  std::vector<QueryLabelType> labels_;
  std::variant<std::unordered_map<memgraph::query::PropertyIx, memgraph::query::Expression *>,
               memgraph::query::ParameterLookup *>
      properties_;
  bool label_expression_{false};

  NodeAtom *Clone(AstStorage *storage) const override {
    NodeAtom *object = storage->Create<NodeAtom>();
    object->identifier_ = identifier_ ? identifier_->Clone(storage) : nullptr;
    object->labels_.resize(labels_.size());
    for (auto i = 0; i < object->labels_.size(); ++i) {
      if (const auto *label = std::get_if<LabelIx>(&labels_[i])) {
        object->labels_[i] = storage->GetLabelIx(label->name);
      } else {
        object->labels_[i] = std::get<Expression *>(labels_[i])->Clone(storage);
      }
    }
    if (const auto *properties = std::get_if<std::unordered_map<PropertyIx, Expression *>>(&properties_)) {
      auto &new_obj_properties = std::get<std::unordered_map<PropertyIx, Expression *>>(object->properties_);
      for (const auto &[property, value_expression] : *properties) {
        PropertyIx key = storage->GetPropertyIx(property.name);
        new_obj_properties[key] = value_expression->Clone(storage);
      }
    } else {
      object->properties_ = std::get<ParameterLookup *>(properties_)->Clone(storage);
    }
    object->label_expression_ = label_expression_;
    return object;
  }

 protected:
  using PatternAtom::PatternAtom;

 private:
  friend class AstStorage;
};

using QueryEdgeType = std::variant<EdgeTypeIx, Expression *>;

class EdgeAtom : public memgraph::query::PatternAtom {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Type : uint8_t { SINGLE, DEPTH_FIRST, BREADTH_FIRST, WEIGHTED_SHORTEST_PATH, ALL_SHORTEST_PATHS };

  enum class Direction : uint8_t { IN, OUT, BOTH };

  /// Lambda for use in filtering or weight calculation during variable expand.
  struct Lambda {
    static const utils::TypeInfo kType;
    const utils::TypeInfo &GetTypeInfo() const { return kType; }

    /// Argument identifier for the edge currently being traversed.
    memgraph::query::Identifier *inner_edge{nullptr};
    /// Argument identifier for the destination node of the edge.
    memgraph::query::Identifier *inner_node{nullptr};
    /// Argument identifier for the currently-accumulated path.
    memgraph::query::Identifier *accumulated_path{nullptr};
    /// Argument identifier for the weight of the currently-accumulated path.
    memgraph::query::Identifier *accumulated_weight{nullptr};
    /// Evaluates the result of the lambda.
    memgraph::query::Expression *expression{nullptr};

    Lambda Clone(AstStorage *storage) const {
      Lambda object;
      object.inner_edge = inner_edge ? inner_edge->Clone(storage) : nullptr;
      object.inner_node = inner_node ? inner_node->Clone(storage) : nullptr;
      object.accumulated_path = accumulated_path ? accumulated_path->Clone(storage) : nullptr;
      object.accumulated_weight = accumulated_weight ? accumulated_weight->Clone(storage) : nullptr;
      object.expression = expression ? expression->Clone(storage) : nullptr;
      return object;
    }
  };

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = identifier_->Accept(visitor);
      if (auto *properties = std::get_if<std::unordered_map<query::PropertyIx, query::Expression *>>(&properties_)) {
        for (auto &property : *properties) {
          if (cont) {
            cont = property.second->Accept(visitor);
          }
        }
      } else {
        std::get<ParameterLookup *>(properties_)->Accept(visitor);
      }
      if (cont && lower_bound_) {
        cont = lower_bound_->Accept(visitor);
      }
      if (cont && upper_bound_) {
        cont = upper_bound_->Accept(visitor);
      }
      if (cont && total_weight_) {
        total_weight_->Accept(visitor);
      }
    }
    return visitor.PostVisit(*this);
  }

  bool IsVariable() const {
    switch (type_) {
      case Type::DEPTH_FIRST:
      case Type::BREADTH_FIRST:
      case Type::WEIGHTED_SHORTEST_PATH:
      case Type::ALL_SHORTEST_PATHS:
        return true;
      case Type::SINGLE:
        return false;
    }
  }

  memgraph::query::EdgeAtom::Type type_{Type::SINGLE};
  memgraph::query::EdgeAtom::Direction direction_{Direction::BOTH};
  std::vector<QueryEdgeType> edge_types_;
  std::variant<std::unordered_map<memgraph::query::PropertyIx, memgraph::query::Expression *>,
               memgraph::query::ParameterLookup *>
      properties_;
  /// Evaluates to lower bound in variable length expands.
  memgraph::query::Expression *lower_bound_{nullptr};
  /// Evaluated to upper bound in variable length expands.
  memgraph::query::Expression *upper_bound_{nullptr};
  /// Filter lambda for variable length expands. Can have an empty expression, but identifiers must be valid, because an
  /// optimization pass may inline other expressions into this lambda.
  memgraph::query::EdgeAtom::Lambda filter_lambda_;
  /// Used in weighted shortest path. It must have valid expressions and identifiers. In all other expand types, it is
  /// empty.
  memgraph::query::EdgeAtom::Lambda weight_lambda_;
  /// Variable where the total weight for weighted shortest path will be stored.
  memgraph::query::Identifier *total_weight_{nullptr};

  EdgeAtom *Clone(AstStorage *storage) const override {
    EdgeAtom *object = storage->Create<EdgeAtom>();
    object->identifier_ = identifier_ ? identifier_->Clone(storage) : nullptr;
    object->type_ = type_;
    object->direction_ = direction_;
    object->edge_types_.resize(edge_types_.size());
    for (auto i = 0; i < object->edge_types_.size(); ++i) {
      auto const clone_edge_type = utils::Overloaded{
          [&](EdgeTypeIx const &edge_type) { object->edge_types_[i] = storage->GetEdgeTypeIx(edge_type.name); },
          [&](Expression const *edge_type) { object->edge_types_[i] = edge_type->Clone(storage); },
      };
      std::visit(clone_edge_type, edge_types_[i]);
    }
    if (const auto *properties = std::get_if<std::unordered_map<PropertyIx, Expression *>>(&properties_)) {
      auto &new_obj_properties = std::get<std::unordered_map<PropertyIx, Expression *>>(object->properties_);
      for (const auto &[property, value_expression] : *properties) {
        PropertyIx key = storage->GetPropertyIx(property.name);
        new_obj_properties[key] = value_expression->Clone(storage);
      }
    } else {
      object->properties_ = std::get<ParameterLookup *>(properties_)->Clone(storage);
    }
    object->lower_bound_ = lower_bound_ ? lower_bound_->Clone(storage) : nullptr;
    object->upper_bound_ = upper_bound_ ? upper_bound_->Clone(storage) : nullptr;
    object->filter_lambda_ = filter_lambda_.Clone(storage);
    object->weight_lambda_ = weight_lambda_.Clone(storage);
    object->total_weight_ = total_weight_ ? total_weight_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  using PatternAtom::PatternAtom;
  EdgeAtom(Identifier *identifier, Type type, Direction direction)
      : PatternAtom(identifier), type_(type), direction_(direction) {}

  // Creates an edge atom for a SINGLE expansion with the given .
  EdgeAtom(Identifier *identifier, Type type, Direction direction, const std::vector<QueryEdgeType> &edge_types)
      : PatternAtom(identifier), type_(type), direction_(direction), edge_types_(edge_types) {}

 private:
  friend class AstStorage;
};

class Clause : public memgraph::query::Tree, public utils::Visitable<HierarchicalTreeVisitor> {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  using utils::Visitable<HierarchicalTreeVisitor>::Accept;

  Clause() = default;

  Clause *Clone(AstStorage *storage) const override = 0;

 private:
  friend class AstStorage;
};

class SingleQuery : public memgraph::query::Tree, public utils::Visitable<HierarchicalTreeVisitor> {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  using utils::Visitable<HierarchicalTreeVisitor>::Accept;

  SingleQuery() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto &clause : clauses_) {
        if (!clause->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  std::vector<memgraph::query::Clause *> clauses_;
  bool has_update{};

  SingleQuery *Clone(AstStorage *storage) const override {
    SingleQuery *object = storage->Create<SingleQuery>();
    object->clauses_.resize(clauses_.size());
    for (auto i4 = 0; i4 < clauses_.size(); ++i4) {
      object->clauses_[i4] = clauses_[i4] ? clauses_[i4]->Clone(storage) : nullptr;
    }
    object->has_update = has_update;
    return object;
  }

 private:
  friend class AstStorage;
};

class CypherUnion : public memgraph::query::Tree, public utils::Visitable<HierarchicalTreeVisitor> {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  using utils::Visitable<HierarchicalTreeVisitor>::Accept;

  CypherUnion() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      single_query_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::SingleQuery *single_query_{nullptr};
  bool distinct_{false};
  /// Holds symbols that are created during symbol generation phase. These symbols are used when UNION/UNION ALL
  /// combines single query results.
  std::vector<Symbol> union_symbols_;

  CypherUnion *Clone(AstStorage *storage) const override {
    CypherUnion *object = storage->Create<CypherUnion>();
    object->single_query_ = single_query_ ? single_query_->Clone(storage) : nullptr;
    object->distinct_ = distinct_;
    object->union_symbols_ = union_symbols_;
    return object;
  }

 protected:
  explicit CypherUnion(bool distinct) : distinct_(distinct) {}
  CypherUnion(bool distinct, SingleQuery *single_query, std::vector<Symbol> union_symbols)
      : single_query_(single_query), distinct_(distinct), union_symbols_(union_symbols) {}

 private:
  friend class AstStorage;
};

using PropertyPath = std::vector<memgraph::query::PropertyIx>;

struct IndexHint {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }

  enum class IndexType { LABEL, LABEL_PROPERTIES, POINT };

  memgraph::query::IndexHint::IndexType index_type_;
  memgraph::query::LabelIx label_ix_;
  // This is not the exact properies of the index, it is the prefix (which might be exact)
  std::vector<PropertyPath> property_ixs_;

  IndexHint Clone(AstStorage *storage) const;
};

struct PreQueryDirectives {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }

  /// Index hints
  std::vector<memgraph::query::IndexHint> index_hints_;
  /// Hops limit
  memgraph::query::Expression *hops_limit_{nullptr};
  /// Commit frequency
  memgraph::query::Expression *commit_frequency_{nullptr};

  PreQueryDirectives Clone(AstStorage *storage) const {
    PreQueryDirectives object;
    object.index_hints_.resize(index_hints_.size());
    for (auto i = 0; i < index_hints_.size(); ++i) {
      object.index_hints_[i] = index_hints_[i].Clone(storage);
    }
    object.hops_limit_ = hops_limit_ ? hops_limit_->Clone(storage) : nullptr;
    object.commit_frequency_ = commit_frequency_ ? commit_frequency_->Clone(storage) : nullptr;
    return object;
  }
};

class CypherQuery : public memgraph::query::Query, public utils::Visitable<HierarchicalTreeVisitor> {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  CypherQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      single_query_->Accept(visitor);
      for (auto *cypher_union : cypher_unions_) {
        cypher_union->Accept(visitor);
      }
    }

    return visitor.PostVisit(*this);
  }

  /// First and potentially only query.
  memgraph::query::SingleQuery *single_query_{nullptr};
  /// Contains remaining queries that should form and union with `single_query_`.
  std::vector<memgraph::query::CypherUnion *> cypher_unions_;
  /// Memory limit
  memgraph::query::Expression *memory_limit_{nullptr};
  size_t memory_scale_{1024U};
  /// Using statement
  memgraph::query::PreQueryDirectives pre_query_directives_;

  CypherQuery *Clone(AstStorage *storage) const override {
    CypherQuery *object = storage->Create<CypherQuery>();
    object->single_query_ = single_query_ ? single_query_->Clone(storage) : nullptr;
    object->cypher_unions_.resize(cypher_unions_.size());
    for (auto i5 = 0; i5 < cypher_unions_.size(); ++i5) {
      object->cypher_unions_[i5] = cypher_unions_[i5] ? cypher_unions_[i5]->Clone(storage) : nullptr;
    }
    object->memory_limit_ = memory_limit_ ? memory_limit_->Clone(storage) : nullptr;
    object->memory_scale_ = memory_scale_;
    object->pre_query_directives_ = pre_query_directives_.Clone(storage);
    return object;
  }

 private:
  friend class AstStorage;
};

class ExplainQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  ExplainQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  /// The CypherQuery to explain.
  memgraph::query::CypherQuery *cypher_query_{nullptr};

  ExplainQuery *Clone(AstStorage *storage) const override {
    ExplainQuery *object = storage->Create<ExplainQuery>();
    object->cypher_query_ = cypher_query_ ? cypher_query_->Clone(storage) : nullptr;
    return object;
  }

 private:
  friend class AstStorage;
};

class ProfileQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  ProfileQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  /// The CypherQuery to profile.
  memgraph::query::CypherQuery *cypher_query_{nullptr};

  ProfileQuery *Clone(AstStorage *storage) const override {
    ProfileQuery *object = storage->Create<ProfileQuery>();
    object->cypher_query_ = cypher_query_ ? cypher_query_->Clone(storage) : nullptr;
    return object;
  }

 private:
  friend class AstStorage;
};

class IndexQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action { CREATE, DROP };

  IndexQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::IndexQuery::Action action_;
  memgraph::query::LabelIx label_;
  // The outer vector is the composite indices; the inner vector is the nested
  // indices. So `CREATE INDEX ON ":L1(a, b.c, d.e.f)" will give us:
  // [[a], [b, c], [d, e, f]]
  std::vector<std::vector<memgraph::query::PropertyIx>> properties_;

  IndexQuery *Clone(AstStorage *storage) const override {
    IndexQuery *object = storage->Create<IndexQuery>();
    object->action_ = action_;
    object->label_ = storage->GetLabelIx(label_.name);
    object->properties_.reserve(properties_.size());
    for (auto &&nested_properties : properties_) {
      auto cloned_nested_properties =
          nested_properties |
          ranges::views::transform([&](auto &&property) { return storage->GetPropertyIx(property.name); }) |
          ranges::to_vector;
      object->properties_.emplace_back(std::move(cloned_nested_properties));
    }
    return object;
  }

 protected:
  IndexQuery(Action action, LabelIx label, std::vector<std::vector<PropertyIx>> properties)
      : action_(action), label_(std::move(label)), properties_(std::move(properties)) {}

 private:
  friend class AstStorage;
};

class EdgeIndexQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action { CREATE, DROP };

  EdgeIndexQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::EdgeIndexQuery::Action action_;
  memgraph::query::EdgeTypeIx edge_type_;
  std::vector<memgraph::query::PropertyIx> properties_;
  bool global_{false};

  EdgeIndexQuery *Clone(AstStorage *storage) const override {
    EdgeIndexQuery *object = storage->Create<EdgeIndexQuery>();
    object->action_ = action_;
    object->edge_type_ = storage->GetEdgeTypeIx(edge_type_.name);
    object->properties_.resize(properties_.size());
    for (auto i = 0; i < object->properties_.size(); ++i) {
      object->properties_[i] = storage->GetPropertyIx(properties_[i].name);
    }
    object->global_ = global_;
    return object;
  }

 protected:
  EdgeIndexQuery(Action action, EdgeTypeIx edge_type, std::vector<PropertyIx> properties)
      : action_(action), edge_type_(edge_type), properties_(std::move(properties)) {}

 private:
  friend class AstStorage;
};

class PointIndexQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action { CREATE, DROP };

  PointIndexQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::PointIndexQuery::Action action_;
  memgraph::query::LabelIx label_;
  memgraph::query::PropertyIx property_;

  PointIndexQuery *Clone(AstStorage *storage) const override {
    PointIndexQuery *object = storage->Create<PointIndexQuery>();
    object->action_ = action_;
    object->label_ = storage->GetLabelIx(label_.name);
    object->property_ = storage->GetPropertyIx(property_.name);
    return object;
  }

 protected:
  PointIndexQuery(Action action, LabelIx label, PropertyIx property)
      : action_(action), label_(std::move(label)), property_(std::move(property)) {}

 private:
  friend class AstStorage;
};

class TextIndexQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action { CREATE, DROP };

  TextIndexQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::TextIndexQuery::Action action_;
  memgraph::query::LabelIx label_;
  std::string index_name_;

  TextIndexQuery *Clone(AstStorage *storage) const override {
    TextIndexQuery *object = storage->Create<TextIndexQuery>();
    object->action_ = action_;
    object->label_ = storage->GetLabelIx(label_.name);
    object->index_name_ = index_name_;
    return object;
  }

 protected:
  TextIndexQuery(Action action, LabelIx label, std::string index_name)
      : action_(action), label_(std::move(label)), index_name_(index_name) {}

 private:
  friend class AstStorage;
};

class VectorIndexQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action { CREATE, DROP };

  VectorIndexQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::VectorIndexQuery::Action action_;
  std::string index_name_;
  memgraph::query::LabelIx label_;
  memgraph::query::PropertyIx property_;
  std::unordered_map<memgraph::query::Expression *, memgraph::query::Expression *> configs_;

  VectorIndexQuery *Clone(AstStorage *storage) const override {
    VectorIndexQuery *object = storage->Create<VectorIndexQuery>();
    object->action_ = action_;
    object->index_name_ = index_name_;
    object->label_ = storage->GetLabelIx(label_.name);
    object->property_ = storage->GetPropertyIx(property_.name);
    for (const auto &[key, value] : configs_) {
      object->configs_[key->Clone(storage)] = value->Clone(storage);
    }
    return object;
  }

 protected:
  VectorIndexQuery(Action action, std::string index_name, LabelIx label, PropertyIx property,
                   std::unordered_map<Expression *, Expression *> configs)
      : action_(action),
        index_name_(std::move(index_name)),
        label_(std::move(label)),
        property_(std::move(property)),
        configs_(std::move(configs)) {}

 private:
  friend class AstStorage;
};

class Create : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Create() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto &pattern : patterns_) {
        if (!pattern->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  std::vector<memgraph::query::Pattern *> patterns_;

  Create *Clone(AstStorage *storage) const override {
    Create *object = storage->Create<Create>();
    object->patterns_.resize(patterns_.size());
    for (auto i6 = 0; i6 < patterns_.size(); ++i6) {
      object->patterns_[i6] = patterns_[i6] ? patterns_[i6]->Clone(storage) : nullptr;
    }
    return object;
  }

 protected:
  explicit Create(std::vector<Pattern *> patterns) : patterns_(patterns) {}

 private:
  friend class AstStorage;
};

class CallProcedure : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  CallProcedure() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = true;
      for (auto &arg : arguments_) {
        if (!arg->Accept(visitor)) {
          cont = false;
          break;
        }
      }
      if (cont) {
        for (auto &ident : result_identifiers_) {
          if (!ident->Accept(visitor)) {
            cont = false;
            break;
          }
        }
      }
    }
    return visitor.PostVisit(*this);
  }

  std::string procedure_name_;
  std::vector<memgraph::query::Expression *> arguments_;
  std::vector<std::string> result_fields_;
  std::vector<memgraph::query::Identifier *> result_identifiers_;
  memgraph::query::Expression *memory_limit_{nullptr};
  size_t memory_scale_{1024U};
  bool is_write_;
  bool void_procedure_{false};

  CallProcedure *Clone(AstStorage *storage) const override {
    CallProcedure *object = storage->Create<CallProcedure>();
    object->procedure_name_ = procedure_name_;
    object->arguments_.resize(arguments_.size());
    for (auto i7 = 0; i7 < arguments_.size(); ++i7) {
      object->arguments_[i7] = arguments_[i7] ? arguments_[i7]->Clone(storage) : nullptr;
    }
    object->result_fields_ = result_fields_;
    object->result_identifiers_.resize(result_identifiers_.size());
    for (auto i8 = 0; i8 < result_identifiers_.size(); ++i8) {
      object->result_identifiers_[i8] = result_identifiers_[i8] ? result_identifiers_[i8]->Clone(storage) : nullptr;
    }
    object->memory_limit_ = memory_limit_ ? memory_limit_->Clone(storage) : nullptr;
    object->memory_scale_ = memory_scale_;
    object->is_write_ = is_write_;
    object->void_procedure_ = void_procedure_;
    return object;
  }

 private:
  friend class AstStorage;
};

class Match : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Match() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = true;
      for (auto &pattern : patterns_) {
        if (!pattern->Accept(visitor)) {
          cont = false;
          break;
        }
      }
      if (cont && where_) {
        where_->Accept(visitor);
      }
    }
    return visitor.PostVisit(*this);
  }

  std::vector<memgraph::query::Pattern *> patterns_;
  memgraph::query::Where *where_{nullptr};
  bool optional_{false};

  Match *Clone(AstStorage *storage) const override {
    Match *object = storage->Create<Match>();
    object->patterns_.resize(patterns_.size());
    for (auto i9 = 0; i9 < patterns_.size(); ++i9) {
      object->patterns_[i9] = patterns_[i9] ? patterns_[i9]->Clone(storage) : nullptr;
    }
    object->where_ = where_ ? where_->Clone(storage) : nullptr;
    object->optional_ = optional_;
    return object;
  }

 protected:
  explicit Match(bool optional) : optional_(optional) {}
  Match(bool optional, Where *where, std::vector<Pattern *> patterns)
      : patterns_(patterns), where_(where), optional_(optional) {}

 private:
  friend class AstStorage;
};

struct SortItem {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }

  memgraph::query::Ordering ordering;
  memgraph::query::Expression *expression;

  SortItem Clone(AstStorage *storage) const {
    SortItem object;
    object.ordering = ordering;
    object.expression = expression ? expression->Clone(storage) : nullptr;
    return object;
  }
};

/// Contents common to @c Return and @c With clauses.
struct ReturnBody {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }

  /// True if distinct results should be produced.
  bool distinct{false};
  /// True if asterisk was found in the return body.
  bool all_identifiers{false};
  /// Expressions which are used to produce results.
  std::vector<memgraph::query::NamedExpression *> named_expressions;
  /// Expressions used for ordering the results.
  std::vector<memgraph::query::SortItem> order_by;
  /// Optional expression on how many results to skip.
  memgraph::query::Expression *skip{nullptr};
  /// Optional expression on how many results to produce.
  memgraph::query::Expression *limit{nullptr};

  ReturnBody Clone(AstStorage *storage) const {
    ReturnBody object;
    object.distinct = distinct;
    object.all_identifiers = all_identifiers;
    object.named_expressions.resize(named_expressions.size());
    for (auto i10 = 0; i10 < named_expressions.size(); ++i10) {
      object.named_expressions[i10] = named_expressions[i10] ? named_expressions[i10]->Clone(storage) : nullptr;
    }
    object.order_by.resize(order_by.size());
    for (auto i11 = 0; i11 < order_by.size(); ++i11) {
      object.order_by[i11] = order_by[i11].Clone(storage);
    }
    object.skip = skip ? skip->Clone(storage) : nullptr;
    object.limit = limit ? limit->Clone(storage) : nullptr;
    return object;
  }
};

class Return : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Return() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = true;
      for (auto &expr : body_.named_expressions) {
        if (!expr->Accept(visitor)) {
          cont = false;
          break;
        }
      }
      if (cont) {
        for (auto &order_by : body_.order_by) {
          if (!order_by.expression->Accept(visitor)) {
            cont = false;
            break;
          }
        }
      }
      if (cont && body_.skip) cont = body_.skip->Accept(visitor);
      if (cont && body_.limit) cont = body_.limit->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::ReturnBody body_;

  Return *Clone(AstStorage *storage) const override {
    Return *object = storage->Create<Return>();
    object->body_ = body_.Clone(storage);
    return object;
  }

 protected:
  explicit Return(ReturnBody &body) : body_(body) {}

 private:
  friend class AstStorage;
};

class With : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  With() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = true;
      for (auto &expr : body_.named_expressions) {
        if (!expr->Accept(visitor)) {
          cont = false;
          break;
        }
      }
      if (cont) {
        for (auto &order_by : body_.order_by) {
          if (!order_by.expression->Accept(visitor)) {
            cont = false;
            break;
          }
        }
      }
      if (cont && where_) cont = where_->Accept(visitor);
      if (cont && body_.skip) cont = body_.skip->Accept(visitor);
      if (cont && body_.limit) cont = body_.limit->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::ReturnBody body_;
  memgraph::query::Where *where_{nullptr};

  With *Clone(AstStorage *storage) const override {
    With *object = storage->Create<With>();
    object->body_ = body_.Clone(storage);
    object->where_ = where_ ? where_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  With(ReturnBody &body, Where *where) : body_(body), where_(where) {}

 private:
  friend class AstStorage;
};

class Delete : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Delete() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto &expr : expressions_) {
        if (!expr->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  std::vector<memgraph::query::Expression *> expressions_;
  bool detach_{false};

  Delete *Clone(AstStorage *storage) const override {
    Delete *object = storage->Create<Delete>();
    object->expressions_.resize(expressions_.size());
    for (auto i12 = 0; i12 < expressions_.size(); ++i12) {
      object->expressions_[i12] = expressions_[i12] ? expressions_[i12]->Clone(storage) : nullptr;
    }
    object->detach_ = detach_;
    return object;
  }

 protected:
  Delete(bool detach, std::vector<Expression *> expressions) : expressions_(expressions), detach_(detach) {}

 private:
  friend class AstStorage;
};

class SetProperty : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  SetProperty() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      property_lookup_->Accept(visitor) && expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::PropertyLookup *property_lookup_{nullptr};
  memgraph::query::Expression *expression_{nullptr};

  SetProperty *Clone(AstStorage *storage) const override {
    SetProperty *object = storage->Create<SetProperty>();
    object->property_lookup_ = property_lookup_ ? property_lookup_->Clone(storage) : nullptr;
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  SetProperty(PropertyLookup *property_lookup, Expression *expression)
      : property_lookup_(property_lookup), expression_(expression) {}

 private:
  friend class AstStorage;
};

class SetProperties : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  SetProperties() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor) && expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Identifier *identifier_{nullptr};
  memgraph::query::Expression *expression_{nullptr};
  bool update_{false};

  SetProperties *Clone(AstStorage *storage) const override {
    SetProperties *object = storage->Create<SetProperties>();
    object->identifier_ = identifier_ ? identifier_->Clone(storage) : nullptr;
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    object->update_ = update_;
    return object;
  }

 protected:
  SetProperties(Identifier *identifier, Expression *expression, bool update = false)
      : identifier_(identifier), expression_(expression), update_(update) {}

 private:
  friend class AstStorage;
};

class SetLabels : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  SetLabels() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Identifier *identifier_{nullptr};
  std::vector<QueryLabelType> labels_;

  SetLabels *Clone(AstStorage *storage) const override {
    SetLabels *object = storage->Create<SetLabels>();
    object->identifier_ = identifier_ ? identifier_->Clone(storage) : nullptr;
    object->labels_.resize(labels_.size());
    for (auto i = 0; i < object->labels_.size(); ++i) {
      if (const auto *label = std::get_if<LabelIx>(&labels_[i])) {
        object->labels_[i] = storage->GetLabelIx(label->name);
      } else {
        object->labels_[i] = std::get<Expression *>(labels_[i])->Clone(storage);
      }
    }
    return object;
  }

 protected:
  SetLabels(Identifier *identifier, std::vector<QueryLabelType> labels)
      : identifier_(identifier), labels_(std::move(labels)) {}

 private:
  friend class AstStorage;
};

class RemoveProperty : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  RemoveProperty() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      property_lookup_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::PropertyLookup *property_lookup_{nullptr};

  RemoveProperty *Clone(AstStorage *storage) const override {
    RemoveProperty *object = storage->Create<RemoveProperty>();
    object->property_lookup_ = property_lookup_ ? property_lookup_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  explicit RemoveProperty(PropertyLookup *property_lookup) : property_lookup_(property_lookup) {}

 private:
  friend class AstStorage;
};

class RemoveLabels : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  RemoveLabels() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Identifier *identifier_{nullptr};
  std::vector<QueryLabelType> labels_;

  RemoveLabels *Clone(AstStorage *storage) const override {
    RemoveLabels *object = storage->Create<RemoveLabels>();
    object->identifier_ = identifier_ ? identifier_->Clone(storage) : nullptr;
    object->labels_.resize(labels_.size());
    for (auto i = 0; i < object->labels_.size(); ++i) {
      if (const auto *label = std::get_if<LabelIx>(&labels_[i])) {
        object->labels_[i] = storage->GetLabelIx(label->name);
      } else {
        object->labels_[i] = std::get<Expression *>(labels_[i])->Clone(storage);
      }
    }
    return object;
  }

 protected:
  RemoveLabels(Identifier *identifier, std::vector<QueryLabelType> labels)
      : identifier_(identifier), labels_(std::move(labels)) {}

 private:
  friend class AstStorage;
};

class Merge : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Merge() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = pattern_->Accept(visitor);
      if (cont) {
        for (auto &set : on_match_) {
          if (!set->Accept(visitor)) {
            cont = false;
            break;
          }
        }
      }
      if (cont) {
        for (auto &set : on_create_) {
          if (!set->Accept(visitor)) {
            cont = false;
            break;
          }
        }
      }
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Pattern *pattern_{nullptr};
  std::vector<memgraph::query::Clause *> on_match_;
  std::vector<memgraph::query::Clause *> on_create_;

  Merge *Clone(AstStorage *storage) const override {
    Merge *object = storage->Create<Merge>();
    object->pattern_ = pattern_ ? pattern_->Clone(storage) : nullptr;
    object->on_match_.resize(on_match_.size());
    for (auto i13 = 0; i13 < on_match_.size(); ++i13) {
      object->on_match_[i13] = on_match_[i13] ? on_match_[i13]->Clone(storage) : nullptr;
    }
    object->on_create_.resize(on_create_.size());
    for (auto i14 = 0; i14 < on_create_.size(); ++i14) {
      object->on_create_[i14] = on_create_[i14] ? on_create_[i14]->Clone(storage) : nullptr;
    }
    return object;
  }

 protected:
  Merge(Pattern *pattern, std::vector<Clause *> on_match, std::vector<Clause *> on_create)
      : pattern_(pattern), on_match_(on_match), on_create_(on_create) {}

 private:
  friend class AstStorage;
};

class Unwind : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Unwind() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      named_expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::NamedExpression *named_expression_{nullptr};

  Unwind *Clone(AstStorage *storage) const override {
    Unwind *object = storage->Create<Unwind>();
    object->named_expression_ = named_expression_ ? named_expression_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  explicit Unwind(NamedExpression *named_expression) : named_expression_(named_expression) {
    DMG_ASSERT(named_expression, "Unwind cannot take nullptr for named_expression");
  }

 private:
  friend class AstStorage;
};

class DatabaseInfoQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class InfoType { INDEX, CONSTRAINT, EDGE_TYPES, NODE_LABELS, METRICS, VECTOR_INDEX };

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::DatabaseInfoQuery::InfoType info_type_;

  DatabaseInfoQuery *Clone(AstStorage *storage) const override {
    DatabaseInfoQuery *object = storage->Create<DatabaseInfoQuery>();
    object->info_type_ = info_type_;
    return object;
  }
};

class SystemInfoQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class InfoType { STORAGE, BUILD, ACTIVE_USERS, LICENSE };

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::SystemInfoQuery::InfoType info_type_;

  SystemInfoQuery *Clone(AstStorage *storage) const override {
    SystemInfoQuery *object = storage->Create<SystemInfoQuery>();
    object->info_type_ = info_type_;
    return object;
  }
};

struct Constraint {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }

  enum class Type { EXISTS, UNIQUE, NODE_KEY, TYPE };

  memgraph::query::Constraint::Type type;
  std::optional<storage::TypeConstraintKind> type_constraint;
  memgraph::query::LabelIx label;
  std::vector<memgraph::query::PropertyIx> properties;

  Constraint Clone(AstStorage *storage) const {
    Constraint object;
    object.type = type;
    object.type_constraint = type_constraint;
    object.label = storage->GetLabelIx(label.name);
    object.properties.resize(properties.size());
    for (auto i = 0; i < object.properties.size(); ++i) {
      object.properties[i] = storage->GetPropertyIx(properties[i].name);
    }
    return object;
  }
};

class ConstraintQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class ActionType { CREATE, DROP };

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::ConstraintQuery::ActionType action_type_;
  memgraph::query::Constraint constraint_;

  ConstraintQuery *Clone(AstStorage *storage) const override {
    ConstraintQuery *object = storage->Create<ConstraintQuery>();
    object->action_type_ = action_type_;
    object->constraint_ = constraint_.Clone(storage);
    return object;
  }
};

class DumpQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(QueryVisitor<void>);

  DumpQuery *Clone(AstStorage *storage) const override {
    DumpQuery *object = storage->Create<DumpQuery>();
    return object;
  }
};

class ReplicationQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action { SET_REPLICATION_ROLE, REGISTER_REPLICA, DROP_REPLICA };

  enum class ReplicationRole { MAIN, REPLICA };

  enum class SyncMode { SYNC, ASYNC };

  ReplicationQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::ReplicationQuery::Action action_;
  memgraph::query::ReplicationQuery::ReplicationRole role_;
  std::string instance_name_;
  memgraph::query::Expression *socket_address_{nullptr};
  memgraph::query::Expression *coordinator_socket_address_{nullptr};
  memgraph::query::Expression *port_{nullptr};
  memgraph::query::ReplicationQuery::SyncMode sync_mode_;

  ReplicationQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<ReplicationQuery>();
    object->action_ = action_;
    object->role_ = role_;
    object->instance_name_ = instance_name_;
    object->socket_address_ = socket_address_ ? socket_address_->Clone(storage) : nullptr;
    object->port_ = port_ ? port_->Clone(storage) : nullptr;
    object->sync_mode_ = sync_mode_;
    object->coordinator_socket_address_ =
        coordinator_socket_address_ ? coordinator_socket_address_->Clone(storage) : nullptr;

    return object;
  }

 private:
  friend class AstStorage;
};

class ReplicationInfoQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action { SHOW_REPLICATION_ROLE, SHOW_REPLICAS };

  ReplicationInfoQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::ReplicationInfoQuery::Action action_;

  ReplicationInfoQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<ReplicationInfoQuery>();
    object->action_ = action_;
    return object;
  }

 private:
  friend class AstStorage;
};

class CoordinatorQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action {
    REGISTER_INSTANCE,
    UNREGISTER_INSTANCE,
    SET_INSTANCE_TO_MAIN,
    SHOW_INSTANCE,
    SHOW_INSTANCES,
    ADD_COORDINATOR_INSTANCE,
    REMOVE_COORDINATOR_INSTANCE,
    DEMOTE_INSTANCE,
    FORCE_RESET_CLUSTER_STATE,
    YIELD_LEADERSHIP,
    SET_COORDINATOR_SETTING,
    SHOW_COORDINATOR_SETTINGS
  };

  enum class SyncMode { SYNC, ASYNC };

  CoordinatorQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::CoordinatorQuery::Action action_;
  std::string instance_name_{};
  std::unordered_map<memgraph::query::Expression *, memgraph::query::Expression *> configs_;
  memgraph::query::Expression *coordinator_id_{nullptr};
  memgraph::query::CoordinatorQuery::SyncMode sync_mode_;
  memgraph::query::Expression *setting_name_{nullptr};
  memgraph::query::Expression *setting_value_{nullptr};

  CoordinatorQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<CoordinatorQuery>();

    object->action_ = action_;
    object->instance_name_ = instance_name_;
    object->coordinator_id_ = coordinator_id_ ? coordinator_id_->Clone(storage) : nullptr;
    object->sync_mode_ = sync_mode_;
    for (const auto &[key, value] : configs_) {
      object->configs_[key->Clone(storage)] = value->Clone(storage);
    }
    object->setting_name_ = setting_name_ ? setting_name_->Clone(storage) : nullptr;
    object->setting_value_ = setting_value_ ? setting_value_->Clone(storage) : nullptr;

    return object;
  }

 private:
  friend class AstStorage;
};

class DropGraphQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DropGraphQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  DropGraphQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<DropGraphQuery>();
    return object;
  }

 private:
  friend class AstStorage;
};

class EdgeImportModeQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Status { ACTIVE, INACTIVE };

  EdgeImportModeQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::EdgeImportModeQuery::Status status_;

  EdgeImportModeQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<EdgeImportModeQuery>();
    object->status_ = status_;
    return object;
  }

 private:
  friend class AstStorage;
};

class LockPathQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action { LOCK_PATH, UNLOCK_PATH, STATUS };

  LockPathQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::LockPathQuery::Action action_;

  LockPathQuery *Clone(AstStorage *storage) const override {
    LockPathQuery *object = storage->Create<LockPathQuery>();
    object->action_ = action_;
    return object;
  }

 private:
  friend class AstStorage;
};

class LoadCsv : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  LoadCsv() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      row_var_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Expression *file_;
  bool with_header_;
  bool ignore_bad_;
  memgraph::query::Expression *delimiter_{nullptr};
  memgraph::query::Expression *quote_{nullptr};
  memgraph::query::Expression *nullif_{nullptr};
  memgraph::query::Identifier *row_var_{nullptr};

  LoadCsv *Clone(AstStorage *storage) const override {
    LoadCsv *object = storage->Create<LoadCsv>();
    object->file_ = file_ ? file_->Clone(storage) : nullptr;
    object->with_header_ = with_header_;
    object->ignore_bad_ = ignore_bad_;
    object->delimiter_ = delimiter_ ? delimiter_->Clone(storage) : nullptr;
    object->quote_ = quote_ ? quote_->Clone(storage) : nullptr;
    object->nullif_ = nullif_;
    object->row_var_ = row_var_ ? row_var_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  explicit LoadCsv(Expression *file, bool with_header, bool ignore_bad, Expression *delimiter, Expression *quote,
                   Expression *nullif, Identifier *row_var)
      : file_(file),
        with_header_(with_header),
        ignore_bad_(ignore_bad),
        delimiter_(delimiter),
        quote_(quote),
        nullif_(nullif),
        row_var_(row_var) {
    DMG_ASSERT(row_var, "LoadCsv cannot take nullptr for identifier");
  }

 private:
  friend class AstStorage;
};

class FreeMemoryQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(QueryVisitor<void>);

  FreeMemoryQuery *Clone(AstStorage *storage) const override {
    FreeMemoryQuery *object = storage->Create<FreeMemoryQuery>();
    return object;
  }
};

class TriggerQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action { CREATE_TRIGGER, DROP_TRIGGER, SHOW_TRIGGERS };

  enum class EventType {
    ANY,
    VERTEX_CREATE,
    EDGE_CREATE,
    CREATE,
    VERTEX_DELETE,
    EDGE_DELETE,
    DELETE,
    VERTEX_UPDATE,
    EDGE_UPDATE,
    UPDATE
  };

  TriggerQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::TriggerQuery::Action action_;
  memgraph::query::TriggerQuery::EventType event_type_;
  std::string trigger_name_;
  bool before_commit_;
  std::string statement_;

  TriggerQuery *Clone(AstStorage *storage) const override {
    TriggerQuery *object = storage->Create<TriggerQuery>();
    object->action_ = action_;
    object->event_type_ = event_type_;
    object->trigger_name_ = trigger_name_;
    object->before_commit_ = before_commit_;
    object->statement_ = statement_;
    return object;
  }

 private:
  friend class AstStorage;
};

class IsolationLevelQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class IsolationLevel { SNAPSHOT_ISOLATION, READ_COMMITTED, READ_UNCOMMITTED };

  enum class IsolationLevelScope { NEXT, SESSION, GLOBAL };

  IsolationLevelQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::IsolationLevelQuery::IsolationLevel isolation_level_;
  memgraph::query::IsolationLevelQuery::IsolationLevelScope isolation_level_scope_;

  IsolationLevelQuery *Clone(AstStorage *storage) const override {
    IsolationLevelQuery *object = storage->Create<IsolationLevelQuery>();
    object->isolation_level_ = isolation_level_;
    object->isolation_level_scope_ = isolation_level_scope_;
    return object;
  }

 private:
  friend class AstStorage;
};

class StorageModeQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class StorageMode { IN_MEMORY_TRANSACTIONAL, IN_MEMORY_ANALYTICAL, ON_DISK_TRANSACTIONAL };

  StorageModeQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::StorageModeQuery::StorageMode storage_mode_;

  StorageModeQuery *Clone(AstStorage *storage) const override {
    StorageModeQuery *object = storage->Create<StorageModeQuery>();
    object->storage_mode_ = storage_mode_;
    return object;
  }

 private:
  friend class AstStorage;
};

class CreateSnapshotQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(QueryVisitor<void>);

  CreateSnapshotQuery *Clone(AstStorage *storage) const override {
    CreateSnapshotQuery *object = storage->Create<CreateSnapshotQuery>();
    return object;
  }
};

class RecoverSnapshotQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(QueryVisitor<void>);

  RecoverSnapshotQuery *Clone(AstStorage *storage) const override {
    RecoverSnapshotQuery *object = storage->Create<RecoverSnapshotQuery>();
    object->snapshot_ = snapshot_ ? snapshot_->Clone(storage) : nullptr;
    object->force_ = force_;
    return object;
  }

  Expression *snapshot_;
  bool force_;
};

class ShowSnapshotsQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(QueryVisitor<void>);

  ShowSnapshotsQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<ShowSnapshotsQuery>();
    return object;
  }
};

class StreamQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action {
    CREATE_STREAM,
    DROP_STREAM,
    START_STREAM,
    STOP_STREAM,
    START_ALL_STREAMS,
    STOP_ALL_STREAMS,
    SHOW_STREAMS,
    CHECK_STREAM
  };

  enum class Type { KAFKA, PULSAR };

  StreamQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::StreamQuery::Action action_;
  memgraph::query::StreamQuery::Type type_;
  std::string stream_name_;
  memgraph::query::Expression *batch_limit_{nullptr};
  memgraph::query::Expression *timeout_{nullptr};
  std::string transform_name_;
  memgraph::query::Expression *batch_interval_{nullptr};
  memgraph::query::Expression *batch_size_{nullptr};
  std::variant<memgraph::query::Expression *, std::vector<std::string>> topic_names_{nullptr};
  std::string consumer_group_;
  memgraph::query::Expression *bootstrap_servers_{nullptr};
  memgraph::query::Expression *service_url_{nullptr};
  std::unordered_map<memgraph::query::Expression *, memgraph::query::Expression *> configs_;
  std::unordered_map<memgraph::query::Expression *, memgraph::query::Expression *> credentials_;

  StreamQuery *Clone(AstStorage *storage) const override {
    StreamQuery *object = storage->Create<StreamQuery>();
    object->action_ = action_;
    object->type_ = type_;
    object->stream_name_ = stream_name_;
    object->batch_limit_ = batch_limit_ ? batch_limit_->Clone(storage) : nullptr;
    object->timeout_ = timeout_ ? timeout_->Clone(storage) : nullptr;
    object->transform_name_ = transform_name_;
    object->batch_interval_ = batch_interval_ ? batch_interval_->Clone(storage) : nullptr;
    object->batch_size_ = batch_size_ ? batch_size_->Clone(storage) : nullptr;
    if (auto *topic_expression = std::get_if<Expression *>(&topic_names_)) {
      if (*topic_expression == nullptr) {
        object->topic_names_ = nullptr;
      } else {
        object->topic_names_ = (*topic_expression)->Clone(storage);
      }
    } else {
      object->topic_names_ = std::get<std::vector<std::string>>(topic_names_);
    }
    object->consumer_group_ = consumer_group_;
    object->bootstrap_servers_ = bootstrap_servers_ ? bootstrap_servers_->Clone(storage) : nullptr;
    object->service_url_ = service_url_ ? service_url_->Clone(storage) : nullptr;
    for (const auto &[key, value] : configs_) {
      object->configs_[key->Clone(storage)] = value->Clone(storage);
    }
    for (const auto &[key, value] : credentials_) {
      object->credentials_[key->Clone(storage)] = value->Clone(storage);
    }
    return object;
  }

 private:
  friend class AstStorage;
};

class SettingQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action { SHOW_SETTING, SHOW_ALL_SETTINGS, SET_SETTING };

  SettingQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::SettingQuery::Action action_;
  memgraph::query::Expression *setting_name_{nullptr};
  memgraph::query::Expression *setting_value_{nullptr};

  SettingQuery *Clone(AstStorage *storage) const override {
    SettingQuery *object = storage->Create<SettingQuery>();
    object->action_ = action_;
    object->setting_name_ = setting_name_ ? setting_name_->Clone(storage) : nullptr;
    object->setting_value_ = setting_value_ ? setting_value_->Clone(storage) : nullptr;
    return object;
  }

 private:
  friend class AstStorage;
};

class VersionQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(QueryVisitor<void>);

  VersionQuery *Clone(AstStorage *storage) const override {
    VersionQuery *object = storage->Create<VersionQuery>();
    return object;
  }
};

class Foreach : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Foreach() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      named_expression_->Accept(visitor);
      for (auto &clause : clauses_) {
        clause->Accept(visitor);
      }
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::NamedExpression *named_expression_{nullptr};
  std::vector<memgraph::query::Clause *> clauses_;

  Foreach *Clone(AstStorage *storage) const override {
    Foreach *object = storage->Create<Foreach>();
    object->named_expression_ = named_expression_ ? named_expression_->Clone(storage) : nullptr;
    object->clauses_.resize(clauses_.size());
    for (auto i15 = 0; i15 < clauses_.size(); ++i15) {
      object->clauses_[i15] = clauses_[i15] ? clauses_[i15]->Clone(storage) : nullptr;
    }
    return object;
  }

 protected:
  Foreach(NamedExpression *expression, std::vector<Clause *> clauses)
      : named_expression_(expression), clauses_(clauses) {}

 private:
  friend class AstStorage;
};

class ShowConfigQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(QueryVisitor<void>);

  ShowConfigQuery *Clone(AstStorage *storage) const override {
    ShowConfigQuery *object = storage->Create<ShowConfigQuery>();
    return object;
  }
};

class TransactionQueueQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action { SHOW_TRANSACTIONS, TERMINATE_TRANSACTIONS };

  TransactionQueueQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::TransactionQueueQuery::Action action_;
  std::vector<Expression *> transaction_id_list_;

  TransactionQueueQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<TransactionQueueQuery>();
    object->action_ = action_;
    object->transaction_id_list_ = transaction_id_list_;
    return object;
  }
};

class AnalyzeGraphQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(QueryVisitor<void>);

  enum class Action { ANALYZE, DELETE };

  memgraph::query::AnalyzeGraphQuery::Action action_;
  std::vector<std::string> labels_;

  AnalyzeGraphQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<AnalyzeGraphQuery>();
    object->action_ = action_;
    object->labels_ = labels_;
    return object;
  }
};

class CallSubquery : public memgraph::query::Clause {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  CallSubquery() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      cypher_query_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::CypherQuery *cypher_query_;

  CallSubquery *Clone(AstStorage *storage) const override {
    CallSubquery *object = storage->Create<CallSubquery>();
    object->cypher_query_ = cypher_query_ ? cypher_query_->Clone(storage) : nullptr;
    return object;
  }

 private:
  friend class AstStorage;
};

class MultiDatabaseQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(QueryVisitor<void>);

  enum class Action { CREATE, DROP };

  memgraph::query::MultiDatabaseQuery::Action action_;
  std::string db_name_;

  MultiDatabaseQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<MultiDatabaseQuery>();
    object->action_ = action_;
    object->db_name_ = db_name_;
    return object;
  }
};

class UseDatabaseQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(QueryVisitor<void>);

  std::string db_name_;

  UseDatabaseQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<UseDatabaseQuery>();
    object->db_name_ = db_name_;
    return object;
  }
};

class ShowDatabaseQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(QueryVisitor<void>);

  ShowDatabaseQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<ShowDatabaseQuery>();
    return object;
  }
};

class ShowDatabasesQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DEFVISITABLE(QueryVisitor<void>);

  ShowDatabasesQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<ShowDatabasesQuery>();
    return object;
  }
};

class CreateEnumQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  CreateEnumQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  std::string enum_name_;
  std::vector<std::string> enum_values_;

  CreateEnumQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<CreateEnumQuery>();
    object->enum_name_ = enum_name_;
    object->enum_values_ = enum_values_;
    return object;
  }

 private:
  friend class AstStorage;
};

class ShowEnumsQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  ShowEnumsQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  ShowEnumsQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<ShowEnumsQuery>();
    return object;
  }

 private:
  friend class AstStorage;
};

class EnumValueAccess : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  EnumValueAccess() = default;

  EnumValueAccess(std::string enum_name, std::string enum_value)
      : enum_name_(std::move(enum_name)), enum_value_(std::move(enum_value)) {}

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  std::string enum_name_;
  std::string enum_value_;
  int32_t symbol_pos_{-1};

  EnumValueAccess *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<EnumValueAccess>();
    object->enum_name_ = enum_name_;
    object->enum_value_ = enum_value_;
    return object;
  }

 private:
  friend class AstStorage;
};

class AlterEnumAddValueQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  AlterEnumAddValueQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  std::string enum_name_;
  std::string enum_value_;

  AlterEnumAddValueQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<AlterEnumAddValueQuery>();
    object->enum_name_ = enum_name_;
    object->enum_value_ = enum_value_;
    return object;
  }

 private:
  friend class AstStorage;
};

class AlterEnumUpdateValueQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  AlterEnumUpdateValueQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  std::string enum_name_;
  std::string old_enum_value_;
  std::string new_enum_value_;

  AlterEnumUpdateValueQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<AlterEnumUpdateValueQuery>();
    object->enum_name_ = enum_name_;
    object->old_enum_value_ = old_enum_value_;
    object->new_enum_value_ = new_enum_value_;
    return object;
  }

 private:
  friend class AstStorage;
};

class AlterEnumRemoveValueQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  AlterEnumRemoveValueQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  std::string enum_name_;
  std::string removed_value_;

  AlterEnumRemoveValueQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<AlterEnumRemoveValueQuery>();
    object->enum_name_ = enum_name_;
    object->removed_value_ = removed_value_;
    return object;
  }

 private:
  friend class AstStorage;
};

class DropEnumQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  DropEnumQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  std::string enum_name_;

  DropEnumQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<DropEnumQuery>();
    object->enum_name_ = enum_name_;
    return object;
  }

 private:
  friend class AstStorage;
};

class ShowSchemaInfoQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  ShowSchemaInfoQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  ShowSchemaInfoQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<ShowSchemaInfoQuery>();
    return object;
  }

 private:
  friend class AstStorage;
};

class TtlQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  TtlQuery() = default;

  enum class Type { UNKNOWN = 0, ENABLE, DISABLE, STOP } type_;
  Expression *period_{};
  Expression *specific_time_{};

  DEFVISITABLE(QueryVisitor<void>);

  TtlQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<TtlQuery>();
    object->type_ = type_;
    object->period_ = period_ ? period_->Clone(storage) : nullptr;
    object->specific_time_ = specific_time_ ? specific_time_->Clone(storage) : nullptr;
    return object;
  }

 private:
  friend class AstStorage;
};

class SessionTraceQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  SessionTraceQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  bool enabled_{false};

  SessionTraceQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<SessionTraceQuery>();
    object->enabled_ = enabled_;
    return object;
  }

 private:
  friend class AstStorage;
};

}  // namespace memgraph::query

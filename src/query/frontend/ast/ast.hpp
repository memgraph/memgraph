// Copyright 2024 Memgraph Ltd.
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

#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/exceptions.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::query {

constexpr std::string_view kBoltServer = "bolt_server";
constexpr std::string_view kReplicationServer = "replication_server";
constexpr std::string_view kCoordinatorServer = "coordinator_server";
constexpr std::string_view kManagementServer = "management_server";

struct LabelIx {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }

  std::string name;
  int64_t ix;
};

struct PropertyIx {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }

  std::string name;
  int64_t ix;
};

struct EdgeTypeIx {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }

  std::string name;
  int64_t ix;
};

inline bool operator==(const LabelIx &a, const LabelIx &b) { return a.ix == b.ix && a.name == b.name; }

inline bool operator!=(const LabelIx &a, const LabelIx &b) { return !(a == b); }

inline bool operator==(const PropertyIx &a, const PropertyIx &b) { return a.ix == b.ix && a.name == b.name; }

inline bool operator!=(const PropertyIx &a, const PropertyIx &b) { return !(a == b); }

inline bool operator==(const EdgeTypeIx &a, const EdgeTypeIx &b) { return a.ix == b.ix && a.name == b.name; }

inline bool operator!=(const EdgeTypeIx &a, const EdgeTypeIx &b) { return !(a == b); }
}  // namespace memgraph::query

namespace std {

template <>
struct hash<memgraph::query::LabelIx> {
  size_t operator()(const memgraph::query::LabelIx &label) const { return label.ix; }
};

template <>
struct hash<memgraph::query::PropertyIx> {
  size_t operator()(const memgraph::query::PropertyIx &prop) const { return prop.ix; }
};

template <>
struct hash<memgraph::query::EdgeTypeIx> {
  size_t operator()(const memgraph::query::EdgeTypeIx &edge_type) const { return edge_type.ix; }
};

}  // namespace std

namespace memgraph::query {

class Tree;

// It would be better to call this AstTree, but we already have a class Tree,
// which could be renamed to Node or AstTreeNode, but we also have a class
// called NodeAtom...
class AstStorage {
 public:
  AstStorage() = default;
  AstStorage(const AstStorage &) = delete;
  AstStorage &operator=(const AstStorage &) = delete;
  AstStorage(AstStorage &&) = default;
  AstStorage &operator=(AstStorage &&) = default;

  template <typename T, typename... Args>
  T *Create(Args &&...args) {
    T *ptr = new T(std::forward<Args>(args)...);
    std::unique_ptr<T> tmp(ptr);
    storage_.emplace_back(std::move(tmp));
    return ptr;
  }

  LabelIx GetLabelIx(const std::string &name) { return LabelIx{name, FindOrAddName(name, &labels_)}; }

  PropertyIx GetPropertyIx(const std::string &name) { return PropertyIx{name, FindOrAddName(name, &properties_)}; }

  EdgeTypeIx GetEdgeTypeIx(const std::string &name) { return EdgeTypeIx{name, FindOrAddName(name, &edge_types_)}; }

  std::vector<std::string> labels_;
  std::vector<std::string> edge_types_;
  std::vector<std::string> properties_;

  // Public only for serialization access
  std::vector<std::unique_ptr<Tree>> storage_;

 private:
  int64_t FindOrAddName(const std::string &name, std::vector<std::string> *names) {
    for (int64_t i = 0; i < names->size(); ++i) {
      if ((*names)[i] == name) {
        return i;
      }
    }
    names->push_back(name);
    return names->size() - 1;
  }
};

class Tree {
 public:
  static const utils::TypeInfo kType;
  virtual const utils::TypeInfo &GetTypeInfo() const { return kType; }

  Tree() = default;
  virtual ~Tree() {}

  virtual Tree *Clone(AstStorage *storage) const = 0;

 private:
  friend class AstStorage;
};

class Expression : public memgraph::query::Tree,
                   public utils::Visitable<HierarchicalTreeVisitor>,
                   public utils::Visitable<ExpressionVisitor<TypedValue>>,
                   public utils::Visitable<ExpressionVisitor<TypedValue *>>,
                   public utils::Visitable<ExpressionVisitor<void>> {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  using utils::Visitable<HierarchicalTreeVisitor>::Accept;
  using utils::Visitable<ExpressionVisitor<TypedValue>>::Accept;
  using utils::Visitable<ExpressionVisitor<TypedValue *>>::Accept;
  using utils::Visitable<ExpressionVisitor<void>>::Accept;

  Expression() = default;

  Expression *Clone(AstStorage *storage) const override = 0;

 private:
  friend class AstStorage;
};

class Where : public memgraph::query::Tree, public utils::Visitable<HierarchicalTreeVisitor> {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  using utils::Visitable<HierarchicalTreeVisitor>::Accept;

  Where() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Expression *expression_{nullptr};

  Where *Clone(AstStorage *storage) const override {
    Where *object = storage->Create<Where>();
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    return object;
  }

 protected:
  explicit Where(Expression *expression) : expression_(expression) {}

 private:
  friend class AstStorage;
};

class BinaryOperator : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  BinaryOperator() = default;

  memgraph::query::Expression *expression1_{nullptr};
  memgraph::query::Expression *expression2_{nullptr};

  BinaryOperator *Clone(AstStorage *storage) const override = 0;

 protected:
  BinaryOperator(Expression *expression1, Expression *expression2)
      : expression1_(expression1), expression2_(expression2) {}

 private:
  friend class AstStorage;
};

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

class Aggregation : public memgraph::query::BinaryOperator {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Op { COUNT, MIN, MAX, SUM, AVG, COLLECT_LIST, COLLECT_MAP, PROJECT };

  Aggregation() = default;

  static const constexpr char *const kCount = "COUNT";
  static const constexpr char *const kMin = "MIN";
  static const constexpr char *const kMax = "MAX";
  static const constexpr char *const kSum = "SUM";
  static const constexpr char *const kAvg = "AVG";
  static const constexpr char *const kCollect = "COLLECT";
  static const constexpr char *const kProject = "PROJECT";

  static std::string OpToString(Op op) {
    const char *op_strings[] = {kCount, kMin, kMax, kSum, kAvg, kCollect, kCollect, kProject};
    return op_strings[static_cast<int>(op)];
  }

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      if (expression1_) expression1_->Accept(visitor);
      if (expression2_) expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  Aggregation *MapTo(const Symbol &symbol) {
    symbol_pos_ = symbol.position();
    return this;
  }

  memgraph::query::Aggregation::Op op_;
  /// Symbol table position of the symbol this Aggregation is mapped to.
  int32_t symbol_pos_{-1};
  bool distinct_{false};

  Aggregation *Clone(AstStorage *storage) const override {
    Aggregation *object = storage->Create<Aggregation>();
    object->expression1_ = expression1_ ? expression1_->Clone(storage) : nullptr;
    object->expression2_ = expression2_ ? expression2_->Clone(storage) : nullptr;
    object->op_ = op_;
    object->symbol_pos_ = symbol_pos_;
    object->distinct_ = distinct_;
    return object;
  }

 protected:
  // Use only for serialization.
  explicit Aggregation(Op op) : op_(op) {}

  /// Aggregation's first expression is the value being aggregated. The second
  /// expression is the key used only in COLLECT_MAP.
  Aggregation(Expression *expression1, Expression *expression2, Op op, bool distinct)
      : BinaryOperator(expression1, expression2), op_(op), distinct_(distinct) {
    // COUNT without expression denotes COUNT(*) in cypher.
    DMG_ASSERT(expression1 || op == Aggregation::Op::COUNT, "All aggregations, except COUNT require expression");
    DMG_ASSERT((expression2 == nullptr) ^ (op == Aggregation::Op::COLLECT_MAP),
               "The second expression is obligatory in COLLECT_MAP and "
               "invalid otherwise");
  }

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

  storage::PropertyValue value_;
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
    object->map_variable_ = map_variable_;

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

class Identifier : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Identifier() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  Identifier *MapTo(const Symbol &symbol) {
    symbol_pos_ = symbol.position();
    return this;
  }

  explicit Identifier(const std::string &name) : name_(name) {}
  Identifier(const std::string &name, bool user_declared) : name_(name), user_declared_(user_declared) {}

  std::string name_;
  bool user_declared_{true};
  /// Symbol table position of the symbol this Identifier is mapped to.
  int32_t symbol_pos_{-1};

  Identifier *Clone(AstStorage *storage) const override {
    Identifier *object = storage->Create<Identifier>();
    object->name_ = name_;
    object->user_declared_ = user_declared_;
    object->symbol_pos_ = symbol_pos_;
    return object;
  }

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
  std::vector<memgraph::query::LabelIx> labels_;

  LabelsTest *Clone(AstStorage *storage) const override {
    LabelsTest *object = storage->Create<LabelsTest>();
    object->expression_ = expression_ ? expression_->Clone(storage) : nullptr;
    object->labels_.resize(labels_.size());
    for (auto i = 0; i < object->labels_.size(); ++i) {
      object->labels_[i] = storage->GetLabelIx(labels_[i].name);
    }
    return object;
  }

 protected:
  LabelsTest(Expression *expression, const std::vector<LabelIx> &labels) : expression_(expression), labels_(labels) {}
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

  Function *Clone(AstStorage *storage) const override {
    Function *object = storage->Create<Function>();
    object->arguments_.resize(arguments_.size());
    for (auto i1 = 0; i1 < arguments_.size(); ++i1) {
      object->arguments_[i1] = arguments_[i1] ? arguments_[i1]->Clone(storage) : nullptr;
    }
    object->function_name_ = function_name_;
    object->function_ = function_;
    return object;
  }

 protected:
  Function(const std::string &function_name, const std::vector<Expression *> &arguments)
      : arguments_(arguments), function_name_(function_name), function_(NameToFunction(function_name_)) {
    if (!function_) {
      throw SemanticException("Function '{}' doesn't exist.", function_name);
    }
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

class PatternAtom : public memgraph::query::Tree, public utils::Visitable<HierarchicalTreeVisitor> {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  using utils::Visitable<HierarchicalTreeVisitor>::Accept;

  PatternAtom() = default;

  memgraph::query::Identifier *identifier_{nullptr};

  PatternAtom *Clone(AstStorage *storage) const override = 0;

 protected:
  explicit PatternAtom(Identifier *identifier) : identifier_(identifier) {}

 private:
  friend class AstStorage;
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
    return object;
  }

 protected:
  using PatternAtom::PatternAtom;

 private:
  friend class AstStorage;
};

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
  std::vector<memgraph::query::EdgeTypeIx> edge_types_;
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
      object->edge_types_[i] = storage->GetEdgeTypeIx(edge_types_[i].name);
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
  EdgeAtom(Identifier *identifier, Type type, Direction direction, const std::vector<EdgeTypeIx> &edge_types)
      : PatternAtom(identifier), type_(type), direction_(direction), edge_types_(edge_types) {}

 private:
  friend class AstStorage;
};

class Pattern : public memgraph::query::Tree, public utils::Visitable<HierarchicalTreeVisitor> {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  using utils::Visitable<HierarchicalTreeVisitor>::Accept;

  Pattern() = default;

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = identifier_->Accept(visitor);
      for (auto &part : atoms_) {
        if (cont) {
          cont = part->Accept(visitor);
        }
      }
    }
    return visitor.PostVisit(*this);
  }

  memgraph::query::Identifier *identifier_{nullptr};
  std::vector<memgraph::query::PatternAtom *> atoms_;

  Pattern *Clone(AstStorage *storage) const override {
    Pattern *object = storage->Create<Pattern>();
    object->identifier_ = identifier_ ? identifier_->Clone(storage) : nullptr;
    object->atoms_.resize(atoms_.size());
    for (auto i3 = 0; i3 < atoms_.size(); ++i3) {
      object->atoms_[i3] = atoms_[i3] ? atoms_[i3]->Clone(storage) : nullptr;
    }
    return object;
  }

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

  SingleQuery *Clone(AstStorage *storage) const override {
    SingleQuery *object = storage->Create<SingleQuery>();
    object->clauses_.resize(clauses_.size());
    for (auto i4 = 0; i4 < clauses_.size(); ++i4) {
      object->clauses_[i4] = clauses_[i4] ? clauses_[i4]->Clone(storage) : nullptr;
    }
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

class Query : public memgraph::query::Tree, public utils::Visitable<QueryVisitor<void>> {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  using utils::Visitable<QueryVisitor<void>>::Accept;

  Query() = default;

  Query *Clone(AstStorage *storage) const override = 0;

 private:
  friend class AstStorage;
};

struct IndexHint {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }

  enum class IndexType { LABEL, LABEL_PROPERTY };

  memgraph::query::IndexHint::IndexType index_type_;
  memgraph::query::LabelIx label_;
  std::optional<memgraph::query::PropertyIx> property_{std::nullopt};

  IndexHint Clone(AstStorage *storage) const {
    IndexHint object;
    object.index_type_ = index_type_;
    object.label_ = storage->GetLabelIx(label_.name);
    if (property_) {
      object.property_ = storage->GetPropertyIx(property_->name);
    }
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
  /// Index hint
  /// Suggestion: If we’re going to have multiple pre-query directives (not only index_hints_), they need to be
  /// contained within a dedicated class/struct
  std::vector<memgraph::query::IndexHint> index_hints_;
  /// Memory limit
  memgraph::query::Expression *memory_limit_{nullptr};
  size_t memory_scale_{1024U};

  CypherQuery *Clone(AstStorage *storage) const override {
    CypherQuery *object = storage->Create<CypherQuery>();
    object->single_query_ = single_query_ ? single_query_->Clone(storage) : nullptr;
    object->cypher_unions_.resize(cypher_unions_.size());
    for (auto i5 = 0; i5 < cypher_unions_.size(); ++i5) {
      object->cypher_unions_[i5] = cypher_unions_[i5] ? cypher_unions_[i5]->Clone(storage) : nullptr;
    }
    object->index_hints_.resize(index_hints_.size());
    for (auto i6 = 0; i6 < index_hints_.size(); ++i6) {
      object->index_hints_[i6] = index_hints_[i6].Clone(storage);
    }
    object->memory_limit_ = memory_limit_ ? memory_limit_->Clone(storage) : nullptr;
    object->memory_scale_ = memory_scale_;
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
  std::vector<memgraph::query::PropertyIx> properties_;

  IndexQuery *Clone(AstStorage *storage) const override {
    IndexQuery *object = storage->Create<IndexQuery>();
    object->action_ = action_;
    object->label_ = storage->GetLabelIx(label_.name);
    object->properties_.resize(properties_.size());
    for (auto i = 0; i < object->properties_.size(); ++i) {
      object->properties_[i] = storage->GetPropertyIx(properties_[i].name);
    }
    return object;
  }

 protected:
  IndexQuery(Action action, LabelIx label, std::vector<PropertyIx> properties)
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

  EdgeIndexQuery *Clone(AstStorage *storage) const override {
    EdgeIndexQuery *object = storage->Create<EdgeIndexQuery>();
    object->action_ = action_;
    object->edge_type_ = storage->GetEdgeTypeIx(edge_type_.name);
    return object;
  }

 protected:
  EdgeIndexQuery(Action action, EdgeTypeIx edge_type) : action_(action), edge_type_(edge_type) {}

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

/// Defines the order for sorting values (ascending or descending).
enum class Ordering { ASC, DESC };

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

class AuthQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action {
    CREATE_ROLE,
    DROP_ROLE,
    SHOW_ROLES,
    CREATE_USER,
    SET_PASSWORD,
    DROP_USER,
    SHOW_USERS,
    SET_ROLE,
    CLEAR_ROLE,
    GRANT_PRIVILEGE,
    DENY_PRIVILEGE,
    REVOKE_PRIVILEGE,
    SHOW_PRIVILEGES,
    SHOW_ROLE_FOR_USER,
    SHOW_USERS_FOR_ROLE,
    GRANT_DATABASE_TO_USER,
    DENY_DATABASE_FROM_USER,
    REVOKE_DATABASE_FROM_USER,
    SHOW_DATABASE_PRIVILEGES,
    SET_MAIN_DATABASE,
  };

  enum class Privilege {
    CREATE,
    DELETE,
    MATCH,
    MERGE,
    SET,
    REMOVE,
    INDEX,
    STATS,
    AUTH,
    CONSTRAINT,
    DUMP,
    REPLICATION,
    DURABILITY,
    READ_FILE,
    FREE_MEMORY,
    TRIGGER,
    CONFIG,
    STREAM,
    MODULE_READ,
    MODULE_WRITE,
    WEBSOCKET,
    STORAGE_MODE,
    TRANSACTION_MANAGEMENT,
    MULTI_DATABASE_EDIT,
    MULTI_DATABASE_USE,
    COORDINATOR
  };

  enum class FineGrainedPrivilege { NOTHING, READ, UPDATE, CREATE_DELETE };

  AuthQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::AuthQuery::Action action_;
  std::string user_;
  std::string role_;
  std::string user_or_role_;
  memgraph::query::Expression *password_{nullptr};
  std::string database_;
  std::vector<memgraph::query::AuthQuery::Privilege> privileges_;
  std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
      label_privileges_;
  std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
      edge_type_privileges_;

  AuthQuery *Clone(AstStorage *storage) const override {
    AuthQuery *object = storage->Create<AuthQuery>();
    object->action_ = action_;
    object->user_ = user_;
    object->role_ = role_;
    object->user_or_role_ = user_or_role_;
    object->password_ = password_ ? password_->Clone(storage) : nullptr;
    object->database_ = database_;
    object->privileges_ = privileges_;
    object->label_privileges_ = label_privileges_;
    object->edge_type_privileges_ = edge_type_privileges_;
    return object;
  }

 protected:
  AuthQuery(Action action, std::string user, std::string role, std::string user_or_role, Expression *password,
            std::string database, std::vector<Privilege> privileges,
            std::vector<std::unordered_map<FineGrainedPrivilege, std::vector<std::string>>> label_privileges,
            std::vector<std::unordered_map<FineGrainedPrivilege, std::vector<std::string>>> edge_type_privileges)
      : action_(action),
        user_(user),
        role_(role),
        user_or_role_(user_or_role),
        password_(password),
        database_(database),
        privileges_(privileges),
        label_privileges_(label_privileges),
        edge_type_privileges_(edge_type_privileges) {}

 private:
  friend class AstStorage;
};

/// Constant that holds all available privileges.
const std::vector<AuthQuery::Privilege> kPrivilegesAll = {AuthQuery::Privilege::CREATE,
                                                          AuthQuery::Privilege::DELETE,
                                                          AuthQuery::Privilege::MATCH,
                                                          AuthQuery::Privilege::MERGE,
                                                          AuthQuery::Privilege::SET,
                                                          AuthQuery::Privilege::REMOVE,
                                                          AuthQuery::Privilege::INDEX,
                                                          AuthQuery::Privilege::STATS,
                                                          AuthQuery::Privilege::AUTH,
                                                          AuthQuery::Privilege::CONSTRAINT,
                                                          AuthQuery::Privilege::DUMP,
                                                          AuthQuery::Privilege::REPLICATION,
                                                          AuthQuery::Privilege::READ_FILE,
                                                          AuthQuery::Privilege::DURABILITY,
                                                          AuthQuery::Privilege::FREE_MEMORY,
                                                          AuthQuery::Privilege::TRIGGER,
                                                          AuthQuery::Privilege::CONFIG,
                                                          AuthQuery::Privilege::STREAM,
                                                          AuthQuery::Privilege::MODULE_READ,
                                                          AuthQuery::Privilege::MODULE_WRITE,
                                                          AuthQuery::Privilege::WEBSOCKET,
                                                          AuthQuery::Privilege::TRANSACTION_MANAGEMENT,
                                                          AuthQuery::Privilege::STORAGE_MODE,
                                                          AuthQuery::Privilege::MULTI_DATABASE_EDIT,
                                                          AuthQuery::Privilege::MULTI_DATABASE_USE,
                                                          AuthQuery::Privilege::COORDINATOR};

class DatabaseInfoQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class InfoType { INDEX, CONSTRAINT, EDGE_TYPES, NODE_LABELS };

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

  enum class InfoType { STORAGE, BUILD };

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

  enum class Type { EXISTS, UNIQUE, NODE_KEY };

  memgraph::query::Constraint::Type type;
  memgraph::query::LabelIx label;
  std::vector<memgraph::query::PropertyIx> properties;

  Constraint Clone(AstStorage *storage) const {
    Constraint object;
    object.type = type;
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

  enum class Action { SET_REPLICATION_ROLE, SHOW_REPLICATION_ROLE, REGISTER_REPLICA, DROP_REPLICA, SHOW_REPLICAS };

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
    ReplicationQuery *object = storage->Create<ReplicationQuery>();
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

class CoordinatorQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action {
    REGISTER_INSTANCE,
    UNREGISTER_INSTANCE,
    SET_INSTANCE_TO_MAIN,
    SHOW_INSTANCES,
    ADD_COORDINATOR_INSTANCE
  };

  enum class SyncMode { SYNC, ASYNC };

  CoordinatorQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::CoordinatorQuery::Action action_;
  std::string instance_name_{};
  std::unordered_map<memgraph::query::Expression *, memgraph::query::Expression *> configs_;
  memgraph::query::Expression *coordinator_server_id_{nullptr};
  memgraph::query::CoordinatorQuery::SyncMode sync_mode_;

  CoordinatorQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<CoordinatorQuery>();

    object->action_ = action_;
    object->instance_name_ = instance_name_;
    object->coordinator_server_id_ = coordinator_server_id_ ? coordinator_server_id_->Clone(storage) : nullptr;
    object->sync_mode_ = sync_mode_;
    for (const auto &[key, value] : configs_) {
      object->configs_[key->Clone(storage)] = value->Clone(storage);
    }

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

class Exists : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  Exists() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      pattern_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
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

class PatternComprehension : public memgraph::query::Expression {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  PatternComprehension() = default;

  DEFVISITABLE(ExpressionVisitor<TypedValue>);
  DEFVISITABLE(ExpressionVisitor<TypedValue *>);
  DEFVISITABLE(ExpressionVisitor<void>);

  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      if (variable_) {
        throw utils::NotYetImplemented("Variable in pattern comprehension.");
      }
      pattern_->Accept(visitor);
      if (filter_) {
        filter_->Accept(visitor);
      }
      resultExpr_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

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

  enum class Action { CREATE, USE, DROP, SHOW };

  memgraph::query::MultiDatabaseQuery::Action action_;
  std::string db_name_;

  MultiDatabaseQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<MultiDatabaseQuery>();
    object->action_ = action_;
    object->db_name_ = db_name_;
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

}  // namespace memgraph::query

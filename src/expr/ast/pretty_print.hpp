// Copyright 2022 Memgraph Ltd.
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

#include <iostream>
#include <type_traits>

#include "expr/ast.hpp"
#include "expr/typed_value.hpp"
#include "utils/algorithm.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"

namespace memgraph::expr {
namespace detail {
template <typename T>
void PrintObject(std::ostream *out, const T &arg) {
  static_assert(!std::is_convertible<T, Expression *>::value,
                "This overload shouldn't be called with pointers convertible "
                "to Expression *. This means your other PrintObject overloads aren't "
                "being called for certain AST nodes when they should (or perhaps such "
                "overloads don't exist yet).");
  *out << arg;
}

inline void PrintObject(std::ostream *out, const std::string &str) { *out << utils::Escape(str); }

inline void PrintObject(std::ostream *out, Aggregation::Op op) { *out << Aggregation::OpToString(op); }

inline void PrintObject(std::ostream *out, Expression *expr);

inline void PrintObject(std::ostream *out, Identifier *expr) { PrintObject(out, static_cast<Expression *>(expr)); }

template <typename T>
void PrintObject(std::ostream *out, const std::vector<T> &vec) {
  *out << "[";
  utils::PrintIterable(*out, vec, ", ", [](auto &stream, const auto &item) { PrintObject(&stream, item); });
  *out << "]";
}

template <typename T>
void PrintObject(std::ostream *out, const std::vector<T, utils::Allocator<T>> &vec) {
  *out << "[";
  utils::PrintIterable(*out, vec, ", ", [](auto &stream, const auto &item) { PrintObject(&stream, item); });
  *out << "]";
}

template <typename K, typename V>
void PrintObject(std::ostream *out, const std::map<K, V> &map) {
  *out << "{";
  utils::PrintIterable(*out, map, ", ", [](auto &stream, const auto &item) {
    PrintObject(&stream, item.first);
    stream << ": ";
    PrintObject(&stream, item.second);
  });
  *out << "}";
}

template <typename T>
void PrintObject(std::ostream *out, const utils::pmr::map<utils::pmr::string, T> &map) {
  *out << "{";
  utils::PrintIterable(*out, map, ", ", [](auto &stream, const auto &item) {
    PrintObject(&stream, item.first);
    stream << ": ";
    PrintObject(&stream, item.second);
  });
  *out << "}";
}

template <typename T1, typename T2, typename T3>
inline void PrintObject(std::ostream *out, const TypedValueT<T1, T2, T3> &value) {
  using TypedValue = TypedValueT<T1, T2, T3>;
  switch (value.type()) {
    case TypedValue::Type::Null:
      *out << "null";
      break;
    case TypedValue::Type::String:
      PrintObject(out, value.ValueString());
      break;
    case TypedValue::Type::Bool:
      *out << (value.ValueBool() ? "true" : "false");
      break;
    case TypedValue::Type::Int:
      PrintObject(out, value.ValueInt());
      break;
    case TypedValue::Type::Double:
      PrintObject(out, value.ValueDouble());
      break;
    case TypedValue::Type::List:
      PrintObject(out, value.ValueList());
      break;
    case TypedValue::Type::Map:
      PrintObject(out, value.ValueMap());
      break;
    case TypedValue::Type::Date:
      PrintObject(out, value.ValueDate());
      break;
    case TypedValue::Type::Duration:
      PrintObject(out, value.ValueDuration());
      break;
    case TypedValue::Type::LocalTime:
      PrintObject(out, value.ValueLocalTime());
      break;
    case TypedValue::Type::LocalDateTime:
      PrintObject(out, value.ValueLocalDateTime());
      break;
    default:
      MG_ASSERT(false, "PrintObject(std::ostream *out, const TypedValue &value) should not reach here");
  }
}

template <typename T>
void PrintOperatorArgs(std::ostream *out, const T &arg) {
  *out << " ";
  PrintObject(out, arg);
  *out << ")";
}

template <typename T, typename... Ts>
void PrintOperatorArgs(std::ostream *out, const T &arg, const Ts &...args) {
  *out << " ";
  PrintObject(out, arg);
  PrintOperatorArgs(out, args...);
}

template <typename... Ts>
void PrintOperator(std::ostream *out, const std::string &name, const Ts &...args) {
  *out << "(" << name;
  PrintOperatorArgs(out, args...);
}
}  // namespace detail

class ExpressionPrettyPrinter : public ExpressionVisitor<void> {
 public:
  explicit ExpressionPrettyPrinter(std::ostream *out) : out_(out) {}

  // Unary operators
  // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define UNARY_OPERATOR_VISIT(OP_NODE, OP_STR)      \
  /* NOLINTNEXTLINE(bugprone-macro-parentheses) */ \
  void Visit(OP_NODE &op) override { detail::PrintOperator(out_, OP_STR, op.expression_); }

  UNARY_OPERATOR_VISIT(NotOperator, "Not");
  UNARY_OPERATOR_VISIT(UnaryPlusOperator, "+");
  UNARY_OPERATOR_VISIT(UnaryMinusOperator, "-");
  UNARY_OPERATOR_VISIT(IsNullOperator, "IsNull");

#undef UNARY_OPERATOR_VISIT

  // Binary operators
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define BINARY_OPERATOR_VISIT(OP_NODE, OP_STR)     \
  /* NOLINTNEXTLINE(bugprone-macro-parentheses) */ \
  void Visit(OP_NODE &op) override { detail::PrintOperator(out_, OP_STR, op.expression1_, op.expression2_); }

  BINARY_OPERATOR_VISIT(OrOperator, "Or");
  BINARY_OPERATOR_VISIT(XorOperator, "Xor");
  BINARY_OPERATOR_VISIT(AndOperator, "And");
  BINARY_OPERATOR_VISIT(AdditionOperator, "+");
  BINARY_OPERATOR_VISIT(SubtractionOperator, "-");
  BINARY_OPERATOR_VISIT(MultiplicationOperator, "*");
  BINARY_OPERATOR_VISIT(DivisionOperator, "/");
  BINARY_OPERATOR_VISIT(ModOperator, "%");
  BINARY_OPERATOR_VISIT(NotEqualOperator, "!=");
  BINARY_OPERATOR_VISIT(EqualOperator, "==");
  BINARY_OPERATOR_VISIT(LessOperator, "<");
  BINARY_OPERATOR_VISIT(GreaterOperator, ">");
  BINARY_OPERATOR_VISIT(LessEqualOperator, "<=");
  BINARY_OPERATOR_VISIT(GreaterEqualOperator, ">=");
  BINARY_OPERATOR_VISIT(InListOperator, "In");
  BINARY_OPERATOR_VISIT(SubscriptOperator, "Subscript");

#undef BINARY_OPERATOR_VISIT

  // Other
  void Visit(ListSlicingOperator &op) override {
    detail::PrintOperator(out_, "ListSlicing", op.list_, op.lower_bound_, op.upper_bound_);
  }

  void Visit(IfOperator &op) override {
    detail::PrintOperator(out_, "If", op.condition_, op.then_expression_, op.else_expression_);
  }

  void Visit(ListLiteral &op) override { detail::PrintOperator(out_, "ListLiteral", op.elements_); }

  void Visit(MapLiteral &op) override {
    std::map<std::string, Expression *> map;
    for (const auto &kv : op.elements_) {
      map[kv.first.name] = kv.second;
    }
    detail::PrintObject(out_, map);
  }

  void Visit(LabelsTest &op) override { detail::PrintOperator(out_, "LabelsTest", op.expression_); }

  void Visit(Aggregation &op) override { detail::PrintOperator(out_, "Aggregation", op.op_); }

  void Visit(Function &op) override { detail::PrintOperator(out_, "Function", op.function_name_, op.arguments_); }

  void Visit(Reduce &op) override {
    detail::PrintOperator(out_, "Reduce", op.accumulator_, op.initializer_, op.identifier_, op.list_, op.expression_);
  }

  void Visit(Coalesce &op) override { detail::PrintOperator(out_, "Coalesce", op.expressions_); }

  void Visit(Extract &op) override { detail::PrintOperator(out_, "Extract", op.identifier_, op.list_, op.expression_); }

  void Visit(All &op) override {
    detail::PrintOperator(out_, "All", op.identifier_, op.list_expression_, op.where_->expression_);
  }

  void Visit(Single &op) override {
    detail::PrintOperator(out_, "Single", op.identifier_, op.list_expression_, op.where_->expression_);
  }

  void Visit(Any &op) override {
    detail::PrintOperator(out_, "Any", op.identifier_, op.list_expression_, op.where_->expression_);
  }

  void Visit(None &op) override {
    detail::PrintOperator(out_, "None", op.identifier_, op.list_expression_, op.where_->expression_);
  }

  void Visit(Identifier &op) override { detail::PrintOperator(out_, "Identifier", op.name_); }

  void Visit(PrimitiveLiteral &op) override { detail::PrintObject(out_, op.value_); }

  void Visit(PropertyLookup &op) override {
    detail::PrintOperator(out_, "PropertyLookup", op.expression_, op.property_.name);
  }

  void Visit(ParameterLookup &op) override { detail::PrintOperator(out_, "ParameterLookup", op.token_position_); }

  void Visit(NamedExpression &op) override { detail::PrintOperator(out_, "NamedExpression", op.name_, op.expression_); }

  void Visit(RegexMatch &op) override { detail::PrintOperator(out_, "=~", op.string_expr_, op.regex_); }

 private:
  std::ostream *out_;
};

namespace detail {
inline void PrintObject(std::ostream *out, Expression *expr) {
  if (expr) {
    ExpressionPrettyPrinter printer{out};
    expr->Accept(printer);
  } else {
    *out << "<null>";
  }
}
}  // namespace detail

inline void PrintExpression(Expression *expr, std::ostream *out) {
  ExpressionPrettyPrinter printer{out};
  expr->Accept(printer);
}

inline void PrintExpression(NamedExpression *expr, std::ostream *out) {
  ExpressionPrettyPrinter printer{out};
  expr->Accept(printer);
}
}  // namespace memgraph::expr

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
#include "utils/algorithm.hpp"
#include "utils/string.hpp"

namespace memgraph::expr {
namespace detail {
namespace {
template <typename T>
void PrintObject(std::ostream *out, const T &arg) {
  static_assert(!std::is_convertible<T, Expression *>::value,
                "This overload shouldn't be called with pointers convertible "
                "to Expression *. This means your other PrintObject overloads aren't "
                "being called for certain AST nodes when they should (or perhaps such "
                "overloads don't exist yet).");
  *out << arg;
}

void PrintObject(std::ostream *out, const std::string &str) { *out << utils::Escape(str); }

void PrintObject(std::ostream *out, Aggregation::Op op) { *out << Aggregation::OpToString(op); }

void PrintObject(std::ostream *out, Expression *expr);

void PrintObject(std::ostream *out, Identifier *expr) { PrintObject(out, static_cast<Expression *>(expr)); }

// void PrintObject(std::ostream *out, const storage::v3::PropertyValue &value) {
//   switch (value.type()) {
//     case storage::v3::PropertyValue::Type::Null:
//       *out << "null";
//       break;
//
//     case storage::v3::PropertyValue::Type::String:
//       PrintObject(out, value.ValueString());
//       break;
//
//     case storage::v3::PropertyValue::Type::Bool:
//       *out << (value.ValueBool() ? "true" : "false");
//       break;
//
//     case storage::v3::PropertyValue::Type::Int:
//       PrintObject(out, value.ValueInt());
//       break;
//
//     case storage::v3::PropertyValue::Type::Double:
//       PrintObject(out, value.ValueDouble());
//       break;
//
//     case storage::v3::PropertyValue::Type::List:
//       PrintObject(out, value.ValueList());
//       break;
//
//     case storage::v3::PropertyValue::Type::Map:
//       PrintObject(out, value.ValueMap());
//       break;
//     case storage::v3::PropertyValue::Type::TemporalData:
//       PrintObject(out, value.ValueTemporalData());
//       break;
//   }
// }

template <typename T>
void PrintObject(std::ostream *out, const std::vector<T> &vec) {
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
}  // namespace

}  // namespace detail

class ExpressionPrettyPrinter : public ExpressionVisitor<void> {
 public:
  explicit ExpressionPrettyPrinter(std::ostream *out) : out_(out) {}

  // Unary operators
#define UNARY_OPERATOR_VISIT(OP_NODE, OP_STR) \
  void Visit(OP_NODE &op) { detail::PrintOperator(out_, OP_STR, op.expression_); }

  UNARY_OPERATOR_VISIT(NotOperator, "Not");
  UNARY_OPERATOR_VISIT(UnaryPlusOperator, "+");
  UNARY_OPERATOR_VISIT(UnaryMinusOperator, "-");
  UNARY_OPERATOR_VISIT(IsNullOperator, "IsNull");

#undef UNARY_OPERATOR_VISIT

  // Binary operators
#define BINARY_OPERATOR_VISIT(OP_NODE, OP_STR) \
  void Visit(OP_NODE &op) { detail::PrintOperator(out_, OP_STR, op.expression1_, op.expression2_); }

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
  void Visit(ListSlicingOperator &op) {
    detail::PrintOperator(out_, "ListSlicing", op.list_, op.lower_bound_, op.upper_bound_);
  }

  void Visit(IfOperator &op) {
    detail::PrintOperator(out_, "If", op.condition_, op.then_expression_, op.else_expression_);
  }

  void Visit(ListLiteral &op) { detail::PrintOperator(out_, "ListLiteral", op.elements_); }

  void Visit(MapLiteral &op) {
    std::map<std::string, Expression *> map;
    for (const auto &kv : op.elements_) {
      map[kv.first.name] = kv.second;
    }
    detail::PrintObject(out_, map);
  }

  void Visit(LabelsTest &op) { detail::PrintOperator(out_, "LabelsTest", op.expression_); }

  void Visit(Aggregation &op) { detail::PrintOperator(out_, "Aggregation", op.op_); }

  void Visit(Function &op) { detail::PrintOperator(out_, "Function", op.function_name_, op.arguments_); }

  void Visit(Reduce &op) {
    detail::PrintOperator(out_, "Reduce", op.accumulator_, op.initializer_, op.identifier_, op.list_, op.expression_);
  }

  void Visit(Coalesce &op) { detail::PrintOperator(out_, "Coalesce", op.expressions_); }

  void Visit(Extract &op) { detail::PrintOperator(out_, "Extract", op.identifier_, op.list_, op.expression_); }

  void Visit(All &op) {
    detail::PrintOperator(out_, "All", op.identifier_, op.list_expression_, op.where_->expression_);
  }

  void Visit(Single &op) {
    detail::PrintOperator(out_, "Single", op.identifier_, op.list_expression_, op.where_->expression_);
  }

  void Visit(Any &op) {
    detail::PrintOperator(out_, "Any", op.identifier_, op.list_expression_, op.where_->expression_);
  }

  void Visit(None &op) {
    detail::PrintOperator(out_, "None", op.identifier_, op.list_expression_, op.where_->expression_);
  }

  void Visit(Identifier &op) { detail::PrintOperator(out_, "Identifier", op.name_); }

  void Visit(PrimitiveLiteral &op) {
    // PrintObject(out_, op.value_);
  }

  void Visit(PropertyLookup &op) { detail::PrintOperator(out_, "PropertyLookup", op.expression_, op.property_.name); }

  void Visit(ParameterLookup &op) { detail::PrintOperator(out_, "ParameterLookup", op.token_position_); }

  void Visit(NamedExpression &op) { detail::PrintOperator(out_, "NamedExpression", op.name_, op.expression_); }

  void Visit(RegexMatch &op) { detail::PrintOperator(out_, "=~", op.string_expr_, op.regex_); }

 private:
  std::ostream *out_;
};

namespace detail {
namespace {

void PrintObject(std::ostream *out, Expression *expr) {
  if (expr) {
    ExpressionPrettyPrinter printer{out};
    expr->Accept(printer);
  } else {
    *out << "<null>";
  }
}
}  // namespace
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

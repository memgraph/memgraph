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
static constexpr const char *identifier_symbol = "MG_SYMBOL";

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

inline void PrintObject(std::ostream *out, const std::string &str) { *out << str; }

inline void PrintObject(std::ostream * /*out*/, Aggregation::Op /*op*/) {
  throw utils::NotYetImplemented("PrintObject: Aggregation::Op");
}

inline void PrintObject(std::ostream * /*out*/, Expression * /*expr*/);

inline void PrintObject(std::ostream *out, Identifier *expr) { PrintObject(out, static_cast<Expression *>(expr)); }

template <typename T>
void PrintObject(std::ostream * /*out*/, const std::vector<T> & /*vec*/) {
  throw utils::NotYetImplemented("PrintObject: vector<T>");
}

template <typename T>
void PrintObject(std::ostream * /*out*/, const std::vector<T, utils::Allocator<T>> & /*vec*/) {
  throw utils::NotYetImplemented("PrintObject: vector<T, utils::Allocator<T>>");
}

template <typename K, typename V>
void PrintObject(std::ostream * /*out*/, const std::map<K, V> &map) {
  throw utils::NotYetImplemented("PrintObject: map<K, V>");
}

template <typename T>
void PrintObject(std::ostream * /*out*/, const utils::pmr::map<utils::pmr::string, T> & /*map*/) {
  throw utils::NotYetImplemented("PrintObject: map<utils::pmr::string, V>");
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
void PrintOperatorArgs(const std::string & /*name*/, std::ostream *out, bool with_parenthesis, const T &arg) {
  PrintObject(out, arg);
  if (with_parenthesis) {
    *out << ")";
  }
}

template <typename T, typename... Ts>
void PrintOperatorArgs(const std::string &name, std::ostream *out, bool with_parenthesis, const T &arg,
                       const Ts &...args) {
  PrintObject(out, arg);
  *out << " " << name << " ";
  PrintOperatorArgs(name, out, with_parenthesis, args...);
}

template <typename... Ts>
void PrintOperator(const std::string &name, std::ostream *out, bool with_parenthesis, const Ts &...args) {
  if (with_parenthesis) {
    *out << "(";
  }
  PrintOperatorArgs(name, out, with_parenthesis, args...);
}

// new
template <typename T>
void PrintOperatorArgs(std::ostream *out, const T &arg) {
  PrintObject(out, arg);
}

template <typename T, typename... Ts>
void PrintOperatorArgs(std::ostream *out, const T &arg, const Ts &...args) {
  PrintObject(out, arg);
  PrintOperatorArgs(out, args...);
}

template <typename... Ts>
void PrintOperator(std::ostream *out, const Ts &...args) {
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
  void Visit(OP_NODE &op) override { detail::PrintOperator(OP_STR, out_, false /*with_parenthesis*/, op.expression_); }

  UNARY_OPERATOR_VISIT(NotOperator, "Not");
  UNARY_OPERATOR_VISIT(UnaryPlusOperator, "+");
  UNARY_OPERATOR_VISIT(UnaryMinusOperator, "-");
  UNARY_OPERATOR_VISIT(IsNullOperator, "IsNull");

#undef UNARY_OPERATOR_VISIT

  // Binary operators
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define BINARY_OPERATOR_VISIT(OP_NODE, OP_STR)                                                        \
  /* NOLINTNEXTLINE(bugprone-macro-parentheses) */                                                    \
  void Visit(OP_NODE &op) override {                                                                  \
    detail::PrintOperator(OP_STR, out_, true /*with_parenthesis*/, op.expression1_, op.expression2_); \
  }
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define BINARY_OPERATOR_VISIT_NOT_IMPL(OP_NODE, OP_STR) \
  /* NOLINTNEXTLINE(bugprone-macro-parentheses) */      \
  void Visit(OP_NODE & /*op*/) override { throw utils::NotYetImplemented("OP_NODE"); }

  BINARY_OPERATOR_VISIT(OrOperator, "Or");
  BINARY_OPERATOR_VISIT(XorOperator, "Xor");
  BINARY_OPERATOR_VISIT(AndOperator, "And");
  BINARY_OPERATOR_VISIT(AdditionOperator, "+");
  BINARY_OPERATOR_VISIT(SubtractionOperator, "-");
  BINARY_OPERATOR_VISIT(MultiplicationOperator, "*");
  BINARY_OPERATOR_VISIT(DivisionOperator, "/");
  BINARY_OPERATOR_VISIT(ModOperator, "%");
  BINARY_OPERATOR_VISIT(NotEqualOperator, "!=");
  BINARY_OPERATOR_VISIT(EqualOperator, "=");
  BINARY_OPERATOR_VISIT(LessOperator, "<");
  BINARY_OPERATOR_VISIT(GreaterOperator, ">");
  BINARY_OPERATOR_VISIT(LessEqualOperator, "<=");
  BINARY_OPERATOR_VISIT(GreaterEqualOperator, ">=");
  BINARY_OPERATOR_VISIT_NOT_IMPL(InListOperator, "In");
  BINARY_OPERATOR_VISIT_NOT_IMPL(SubscriptOperator, "Subscript");

#undef BINARY_OPERATOR_VISIT
#undef BINARY_OPERATOR_VISIT_NOT_IMPL

  // Other
  void Visit(ListSlicingOperator & /*op*/) override { throw utils::NotYetImplemented("ListSlicingOperator"); }

  void Visit(IfOperator & /*op*/) override { throw utils::NotYetImplemented("IfOperator"); }

  void Visit(ListLiteral & /*op*/) override { throw utils::NotYetImplemented("ListLiteral"); }

  void Visit(MapLiteral & /*op*/) override { throw utils::NotYetImplemented("MapLiteral"); }

  void Visit(LabelsTest & /*op*/) override { throw utils::NotYetImplemented("LabelsTest"); }

  void Visit(Aggregation & /*op*/) override { throw utils::NotYetImplemented("Aggregation"); }

  void Visit(Function & /*op*/) override { throw utils::NotYetImplemented("Function"); }

  void Visit(Reduce & /*op*/) override { throw utils::NotYetImplemented("Reduce"); }

  void Visit(Coalesce & /*op*/) override { throw utils::NotYetImplemented("Coalesce"); }

  void Visit(Extract & /*op*/) override { throw utils::NotYetImplemented("Extract"); }

  void Visit(All & /*op*/) override { throw utils::NotYetImplemented("All"); }

  void Visit(Single & /*op*/) override { throw utils::NotYetImplemented("Single"); }

  void Visit(Any & /*op*/) override { throw utils::NotYetImplemented("Any"); }

  void Visit(None & /*op*/) override { throw utils::NotYetImplemented("None"); }

  void Visit(Identifier &op) override { detail::PrintOperator(out_, identifier_symbol); }

  void Visit(PrimitiveLiteral &op) override { detail::PrintObject(out_, op.value_); }

  void Visit(PropertyLookup &op) override { detail::PrintOperator(out_, op.expression_, ".", op.property_.name); }

  void Visit(ParameterLookup & /*op*/) override { throw utils::NotYetImplemented("ParameterLookup"); }

  void Visit(NamedExpression & /*op*/) override { throw utils::NotYetImplemented("NamedExpression"); }

  void Visit(RegexMatch & /*op*/) override { throw utils::NotYetImplemented("RegexMatch"); }

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

inline void PrintExpressionToOriginalAndReplaceNodeAndEdgeSymbols(Expression *expr, std::ostream *out) {
  ExpressionPrettyPrinter printer{out};
  expr->Accept(printer);
}

inline std::string ExpressiontoStringWhileReplacingNodeAndEdgeSymbols(Expression *expr) {
  std::ostringstream ss;
  expr::PrintExpressionToOriginalAndReplaceNodeAndEdgeSymbols(expr, &ss);
  return ss.str();
}
}  // namespace memgraph::expr

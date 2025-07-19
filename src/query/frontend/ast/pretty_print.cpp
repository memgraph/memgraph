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

#include "query/frontend/ast/pretty_print.hpp"

#include "query/frontend/ast/query/aggregation.hpp"
#include "query/frontend/ast/query/auth_query.hpp"
#include "query/frontend/ast/query/pattern_comprehension.hpp"

#include <type_traits>

#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/string_helpers.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/algorithm.hpp"
#include "utils/string.hpp"

namespace memgraph::query {

namespace {

class ExpressionPrettyPrinter : public ExpressionVisitor<void> {
 public:
  explicit ExpressionPrettyPrinter(std::ostream *out, const DbAccessor *dba);

  // Unary operators
  void Visit(NotOperator &op) override;
  void Visit(UnaryPlusOperator &op) override;
  void Visit(UnaryMinusOperator &op) override;
  void Visit(IsNullOperator &op) override;

  // Binary operators
  void Visit(OrOperator &op) override;
  void Visit(XorOperator &op) override;
  void Visit(AndOperator &op) override;
  void Visit(AdditionOperator &op) override;
  void Visit(SubtractionOperator &op) override;
  void Visit(MultiplicationOperator &op) override;
  void Visit(DivisionOperator &op) override;
  void Visit(ModOperator &op) override;
  void Visit(ExponentiationOperator &op) override;
  void Visit(NotEqualOperator &op) override;
  void Visit(EqualOperator &op) override;
  void Visit(LessOperator &op) override;
  void Visit(GreaterOperator &op) override;
  void Visit(LessEqualOperator &op) override;
  void Visit(GreaterEqualOperator &op) override;
  void Visit(RangeOperator &op) override;
  void Visit(InListOperator &op) override;
  void Visit(SubscriptOperator &op) override;

  // Other
  void Visit(ListSlicingOperator &op) override;
  void Visit(IfOperator &op) override;
  void Visit(ListLiteral &op) override;
  void Visit(MapLiteral &op) override;
  void Visit(MapProjectionLiteral &op) override;
  void Visit(LabelsTest &op) override;
  void Visit(Aggregation &op) override;
  void Visit(Function &op) override;
  void Visit(Reduce &op) override;
  void Visit(Coalesce &op) override;
  void Visit(Extract &op) override;
  void Visit(Exists &op) override;
  void Visit(All &op) override;
  void Visit(Single &op) override;
  void Visit(Any &op) override;
  void Visit(None &op) override;
  void Visit(ListComprehension &op) override;
  void Visit(Identifier &op) override;
  void Visit(PrimitiveLiteral &op) override;
  void Visit(PropertyLookup &op) override;
  void Visit(AllPropertiesLookup &op) override;
  void Visit(ParameterLookup &op) override;
  void Visit(NamedExpression &op) override;
  void Visit(RegexMatch &op) override;
  void Visit(PatternComprehension &op) override;
  void Visit(EnumValueAccess &op) override;

 private:
  std::ostream *out_;
  const DbAccessor *dba_;
};

// Declare all of the different `PrintObject` overloads upfront since they're
// mutually recursive. Without this, overload resolution depends on the ordering
// of the overloads within the source, which is quite fragile.

template <typename T>
void PrintObject(std::ostream *out, const DbAccessor *dba, const T &arg);

void PrintObject(std::ostream *out, const DbAccessor *dba, const std::string &str);

void PrintObject(std::ostream *out, const DbAccessor *dba, Aggregation::Op op);

void PrintObject(std::ostream *out, const DbAccessor *dba, Expression *expr);

void PrintObject(std::ostream *out, AllPropertiesLookup *apl);

void PrintObject(std::ostream *out, const DbAccessor *dba, Identifier *expr);

void PrintObject(std::ostream *out, const DbAccessor *dba, const storage::PropertyValue &value);

void PrintObject(std::ostream *out, const DbAccessor *dba, const storage::ExternalPropertyValue &value);

template <typename T>
void PrintObject(std::ostream *out, const DbAccessor *dba, const std::vector<T> &vec);

template <typename K, typename V>
void PrintObject(std::ostream *out, const DbAccessor *dba, const std::map<K, V> &map);

template <typename K, typename V, typename C, typename A>
void PrintObject(std::ostream *out, const DbAccessor *dba, const std::map<K, V, C, A> &map);

void PrintObject(std::ostream *out, EnumValueAccess op);

void PrintObject(std::ostream *out, const DbAccessor *dba, storage::Enum value);

void PrintObject(std::ostream *out, storage::Point2d const &value);

void PrintObject(std::ostream *out, storage::Point3d const &value);

void PrintObject(std::ostream *out, const DbAccessor *dba, PropertyIx const &property);

void PrintObject(std::ostream *out, PropertyIx const &property);

template <typename T>
void PrintObject(std::ostream *out, const DbAccessor * /*dba*/, const T &arg) {
  static_assert(!std::is_convertible_v<T, Expression *>,
                "This overload shouldn't be called with pointers convertible "
                "to Expression *. This means your other PrintObject overloads aren't "
                "being called for certain AST nodes when they should (or perhaps such "
                "overloads don't exist yet).");
  *out << arg;
}

void PrintObject(std::ostream *out, const DbAccessor * /*dba*/, const std::string &str) { *out << utils::Escape(str); }

void PrintObject(std::ostream *out, const DbAccessor * /*dba*/, Aggregation::Op op) {
  *out << Aggregation::OpToString(op);
}

void PrintObject(std::ostream *out, const DbAccessor *dba, Expression *expr) {
  if (expr) {
    ExpressionPrettyPrinter printer{out, dba};
    expr->Accept(printer);
  } else {
    *out << "<null>";
  }
}

void PrintObject(std::ostream *out, AllPropertiesLookup *apl) {
  if (apl) {
    *out << ".*";
  } else {
    *out << "<null>";
  }
}

void PrintObject(std::ostream *out, const DbAccessor *dba, Identifier *expr) {
  PrintObject(out, dba, static_cast<Expression *>(expr));
}

void PrintObject(std::ostream *out, EnumValueAccess op) { *out << op.enum_name_ << "::" << op.enum_value_; }

void PrintObject(std::ostream *out, const DbAccessor *dba, storage::Enum value) {
  auto enum_str_value = dba->EnumToName(value);
  if (enum_str_value.HasValue()) {
    *out << *enum_str_value;
  } else {
    *out << "<null>";
  }
}

void PrintObject(std::ostream *out, storage::Point2d const &value) { *out << query::CypherConstructionFor(value); }

void PrintObject(std::ostream *out, storage::Point3d const &value) { *out << query::CypherConstructionFor(value); }

void PrintObject(std::ostream *out, const DbAccessor *dba, const PropertyIx &property) { PrintObject(out, property); }

void PrintObject(std::ostream *out, PropertyIx const &property) { *out << property.name; }

void PrintObject(std::ostream *out, const DbAccessor *dba, const storage::PropertyValue &value) {
  switch (value.type()) {
    case storage::PropertyValue::Type::Null:
      *out << "null";
      break;

    case storage::PropertyValue::Type::String:
      PrintObject(out, dba, value.ValueString());
      break;

    case storage::PropertyValue::Type::Bool:
      *out << (value.ValueBool() ? "true" : "false");
      break;

    case storage::PropertyValue::Type::Int:
      PrintObject(out, dba, value.ValueInt());
      break;

    case storage::PropertyValue::Type::Double:
      PrintObject(out, dba, value.ValueDouble());
      break;

    case storage::PropertyValue::Type::List:
      PrintObject(out, dba, value.ValueList());
      break;

    case storage::PropertyValue::Type::Map:
      PrintObject(out, dba, value.ValueMap());
      break;

    case storage::PropertyValue::Type::TemporalData:
      PrintObject(out, dba, value.ValueTemporalData());
      break;

    case storage::PropertyValue::Type::ZonedTemporalData:
      PrintObject(out, dba, value.ValueZonedTemporalData());
      break;
    case storage::PropertyValue::Type::Enum:
      PrintObject(out, dba, value.ValueEnum());
      break;
    case storage::PropertyValue::Type::Point2d:
      PrintObject(out, value.ValuePoint2d());
      break;
    case storage::PropertyValue::Type::Point3d:
      PrintObject(out, value.ValuePoint3d());
  }
}

void PrintObject(std::ostream *out, const DbAccessor *dba, const storage::ExternalPropertyValue &value) {
  switch (value.type()) {
    case storage::ExternalPropertyValue::Type::Null:
      *out << "null";
      break;

    case storage::ExternalPropertyValue::Type::String:
      PrintObject(out, dba, value.ValueString());
      break;

    case storage::ExternalPropertyValue::Type::Bool:
      *out << (value.ValueBool() ? "true" : "false");
      break;

    case storage::ExternalPropertyValue::Type::Int:
      PrintObject(out, dba, value.ValueInt());
      break;

    case storage::ExternalPropertyValue::Type::Double:
      PrintObject(out, dba, value.ValueDouble());
      break;

    case storage::ExternalPropertyValue::Type::List:
      PrintObject(out, dba, value.ValueList());
      break;

    case storage::ExternalPropertyValue::Type::Map:
      PrintObject(out, dba, value.ValueMap());
      break;

    case storage::ExternalPropertyValue::Type::TemporalData:
      PrintObject(out, dba, value.ValueTemporalData());
      break;

    case storage::ExternalPropertyValue::Type::ZonedTemporalData:
      PrintObject(out, dba, value.ValueZonedTemporalData());
      break;
    case storage::ExternalPropertyValue::Type::Enum:
      PrintObject(out, dba, value.ValueEnum());
      break;
    case storage::ExternalPropertyValue::Type::Point2d:
      PrintObject(out, value.ValuePoint2d());
      break;
    case storage::ExternalPropertyValue::Type::Point3d:
      PrintObject(out, value.ValuePoint3d());
  }
}

template <typename T>
void PrintObject(std::ostream *out, const DbAccessor *dba, const std::vector<T> &vec) {
  *out << "[";
  utils::PrintIterable(*out, vec, ", ", [&dba](auto &stream, const auto &item) { PrintObject(&stream, dba, item); });
  *out << "]";
}

template <typename K, typename V>
void PrintObject(std::ostream *out, const DbAccessor *dba, const std::map<K, V> &map) {
  *out << "{";
  utils::PrintIterable(*out, map, ", ", [&dba](auto &stream, const auto &item) {
    PrintObject(&stream, dba, item.first);
    stream << ": ";
    PrintObject(&stream, dba, item.second);
  });
  *out << "}";
}

template <typename K, typename V, typename C, typename A>
void PrintObject(std::ostream *out, const DbAccessor *dba, const std::map<K, V, C, A> &map) {
  *out << "{";
  utils::PrintIterable(*out, map, ", ", [&dba](auto &stream, const auto &item) {
    PrintObject(&stream, dba, item.first);
    stream << ": ";
    PrintObject(&stream, dba, item.second);
  });
  *out << "}";
}

template <typename T>
void PrintOperatorArgs(std::ostream *out, const DbAccessor *dba, const T &arg) {
  *out << " ";
  PrintObject(out, dba, arg);
  *out << ")";
}

template <typename T, typename... Ts>
void PrintOperatorArgs(std::ostream *out, const DbAccessor *dba, const T &arg, const Ts &...args) {
  *out << " ";
  PrintObject(out, dba, arg);
  PrintOperatorArgs(out, dba, args...);
}

template <typename... Ts>
void PrintOperator(std::ostream *out, const DbAccessor *dba, const std::string &name, const Ts &...args) {
  *out << "(" << name;
  PrintOperatorArgs(out, dba, args...);
}

ExpressionPrettyPrinter::ExpressionPrettyPrinter(std::ostream *out, const DbAccessor *dba) : out_(out), dba_(dba) {}

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define UNARY_OPERATOR_VISIT(OP_NODE, OP_STR)      \
  /* NOLINTNEXTLINE(bugprone-macro-parentheses) */ \
  void ExpressionPrettyPrinter::Visit(OP_NODE &op) { PrintOperator(out_, dba_, OP_STR, op.expression_); }

UNARY_OPERATOR_VISIT(NotOperator, "Not");
UNARY_OPERATOR_VISIT(UnaryPlusOperator, "+");
UNARY_OPERATOR_VISIT(UnaryMinusOperator, "-");
UNARY_OPERATOR_VISIT(IsNullOperator, "IsNull");

#undef UNARY_OPERATOR_VISIT

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define BINARY_OPERATOR_VISIT(OP_NODE, OP_STR)                           \
  /* NOLINTNEXTLINE(bugprone-macro-parentheses) */                       \
  void ExpressionPrettyPrinter::Visit(OP_NODE &op) {                     \
    PrintOperator(out_, dba_, OP_STR, op.expression1_, op.expression2_); \
  }

BINARY_OPERATOR_VISIT(OrOperator, "Or");
BINARY_OPERATOR_VISIT(XorOperator, "Xor");
BINARY_OPERATOR_VISIT(AndOperator, "And");
BINARY_OPERATOR_VISIT(AdditionOperator, "+");
BINARY_OPERATOR_VISIT(SubtractionOperator, "-");
BINARY_OPERATOR_VISIT(MultiplicationOperator, "*");
BINARY_OPERATOR_VISIT(DivisionOperator, "/");
BINARY_OPERATOR_VISIT(ModOperator, "%");
BINARY_OPERATOR_VISIT(ExponentiationOperator, "^");
BINARY_OPERATOR_VISIT(NotEqualOperator, "!=");
BINARY_OPERATOR_VISIT(EqualOperator, "==");
BINARY_OPERATOR_VISIT(LessOperator, "<");
BINARY_OPERATOR_VISIT(GreaterOperator, ">");
BINARY_OPERATOR_VISIT(LessEqualOperator, "<=");
BINARY_OPERATOR_VISIT(GreaterEqualOperator, ">=");
BINARY_OPERATOR_VISIT(InListOperator, "In");
BINARY_OPERATOR_VISIT(SubscriptOperator, "Subscript");

#undef BINARY_OPERATOR_VISIT

void ExpressionPrettyPrinter::Visit(RangeOperator &op) {
  op.expr1_->Accept(*this);
  *out_ << " And ";
  op.expr2_->Accept(*this);
  *out_ << ")";
}

void ExpressionPrettyPrinter::Visit(ListSlicingOperator &op) {
  PrintOperator(out_, dba_, "ListSlicing", op.list_, op.lower_bound_, op.upper_bound_);
}

void ExpressionPrettyPrinter::Visit(IfOperator &op) {
  PrintOperator(out_, dba_, "If", op.condition_, op.then_expression_, op.else_expression_);
}

void ExpressionPrettyPrinter::Visit(ListLiteral &op) { PrintOperator(out_, dba_, "ListLiteral", op.elements_); }

void ExpressionPrettyPrinter::Visit(MapLiteral &op) {
  std::map<std::string, Expression *> map;
  for (const auto &kv : op.elements_) {
    map[kv.first.name] = kv.second;
  }
  PrintObject(out_, dba_, map);
}

void ExpressionPrettyPrinter::Visit(MapProjectionLiteral &op) {
  std::map<std::string, Expression *> map_projection_elements;
  for (const auto &kv : op.elements_) {
    map_projection_elements[kv.first.name] = kv.second;
  }
  PrintObject(out_, dba_, op.map_variable_);
  PrintObject(out_, dba_, map_projection_elements);
}

void ExpressionPrettyPrinter::Visit(AllPropertiesLookup &op) { PrintObject(out_, &op); }

void ExpressionPrettyPrinter::Visit(LabelsTest &op) { PrintOperator(out_, dba_, "LabelsTest", op.expression_); }

void ExpressionPrettyPrinter::Visit(Aggregation &op) { PrintOperator(out_, dba_, "Aggregation", op.op_); }

void ExpressionPrettyPrinter::Visit(Function &op) {
  PrintOperator(out_, dba_, "Function", op.function_name_, op.arguments_);
}

void ExpressionPrettyPrinter::Visit(Reduce &op) {
  PrintOperator(out_, dba_, "Reduce", op.accumulator_, op.initializer_, op.identifier_, op.list_, op.expression_);
}

void ExpressionPrettyPrinter::Visit(Coalesce &op) { PrintOperator(out_, dba_, "Coalesce", op.expressions_); }

void ExpressionPrettyPrinter::Visit(Extract &op) {
  PrintOperator(out_, dba_, "Extract", op.identifier_, op.list_, op.expression_);
}

void ExpressionPrettyPrinter::Visit(Exists & /*op*/) { PrintOperator(out_, dba_, "Exists", "expression"); }

void ExpressionPrettyPrinter::Visit(All &op) {
  PrintOperator(out_, dba_, "All", op.identifier_, op.list_expression_, op.where_->expression_);
}

void ExpressionPrettyPrinter::Visit(Single &op) {
  PrintOperator(out_, dba_, "Single", op.identifier_, op.list_expression_, op.where_->expression_);
}

void ExpressionPrettyPrinter::Visit(Any &op) {
  PrintOperator(out_, dba_, "Any", op.identifier_, op.list_expression_, op.where_->expression_);
}

void ExpressionPrettyPrinter::Visit(None &op) {
  PrintOperator(out_, dba_, "None", op.identifier_, op.list_expression_, op.where_->expression_);
}

void ExpressionPrettyPrinter::Visit(ListComprehension &op) {
  PrintOperator(out_, dba_, "ListComprehension", op.identifier_, op.list_, op.where_, op.expression_);
}

void ExpressionPrettyPrinter::Visit(Identifier &op) { PrintOperator(out_, dba_, "Identifier", op.name_); }

void ExpressionPrettyPrinter::Visit(PrimitiveLiteral &op) { PrintObject(out_, dba_, op.value_); }

void ExpressionPrettyPrinter::Visit(PropertyLookup &op) {
  PrintOperator(out_, dba_, "PropertyLookup", op.expression_, op.property_path_);
}

void ExpressionPrettyPrinter::Visit(ParameterLookup &op) {
  PrintOperator(out_, dba_, "ParameterLookup", op.token_position_);
}

void ExpressionPrettyPrinter::Visit(NamedExpression &op) {
  PrintOperator(out_, dba_, "NamedExpression", op.name_, op.expression_);
}

void ExpressionPrettyPrinter::Visit(RegexMatch &op) { PrintOperator(out_, dba_, "=~", op.string_expr_, op.regex_); }

void ExpressionPrettyPrinter::Visit(PatternComprehension &op) {
  PrintOperator(out_, dba_, "Pattern Comprehension", op.variable_, op.pattern_, op.filter_, op.resultExpr_);
}

void ExpressionPrettyPrinter::Visit(EnumValueAccess &op) { PrintObject(out_, op); }

}  // namespace

void PrintExpression(Expression *expr, std::ostream *out, const DbAccessor &dba) {
  ExpressionPrettyPrinter printer{out, &dba};
  expr->Accept(printer);
}

void PrintExpression(NamedExpression *expr, std::ostream *out, const DbAccessor &dba) {
  ExpressionPrettyPrinter printer{out, &dba};
  expr->Accept(printer);
}

}  // namespace memgraph::query

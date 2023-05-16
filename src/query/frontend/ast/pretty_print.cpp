// Copyright 2023 Memgraph Ltd.
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

#include <type_traits>

#include "query/frontend/ast/ast.hpp"
#include "utils/algorithm.hpp"
#include "utils/string.hpp"

namespace memgraph::query {

namespace {

class ExpressionPrettyPrinter : public ExpressionVisitor<void> {
 public:
  explicit ExpressionPrettyPrinter(std::ostream *out);

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
  void Visit(NotEqualOperator &op) override;
  void Visit(EqualOperator &op) override;
  void Visit(LessOperator &op) override;
  void Visit(GreaterOperator &op) override;
  void Visit(LessEqualOperator &op) override;
  void Visit(GreaterEqualOperator &op) override;
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
  void Visit(Identifier &op) override;
  void Visit(PrimitiveLiteral &op) override;
  void Visit(PropertyLookup &op) override;
  void Visit(AllPropertiesLookup &op) override;
  void Visit(ParameterLookup &op) override;
  void Visit(NamedExpression &op) override;
  void Visit(RegexMatch &op) override;

 private:
  std::ostream *out_;
};

// Declare all of the different `PrintObject` overloads upfront since they're
// mutually recursive. Without this, overload resolution depends on the ordering
// of the overloads within the source, which is quite fragile.

template <typename T>
void PrintObject(std::ostream *out, const T &arg);

void PrintObject(std::ostream *out, const std::string &str);

void PrintObject(std::ostream *out, Aggregation::Op op);

void PrintObject(std::ostream *out, Expression *expr);

void PrintObject(std::ostream *out, AllPropertiesLookup *apl);

void PrintObject(std::ostream *out, Identifier *expr);

void PrintObject(std::ostream *out, const storage::PropertyValue &value);

template <typename T>
void PrintObject(std::ostream *out, const std::vector<T> &vec);

template <typename K, typename V>
void PrintObject(std::ostream *out, const std::map<K, V> &map);

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

void PrintObject(std::ostream *out, Expression *expr) {
  if (expr) {
    ExpressionPrettyPrinter printer{out};
    expr->Accept(printer);
  } else {
    *out << "<null>";
  }
}

void PrintObject(std::ostream *out, AllPropertiesLookup *apl) {
  if (apl) {
    ExpressionPrettyPrinter printer{out};
    *out << ".*";
  } else {
    *out << "<null>";
  }
}

void PrintObject(std::ostream *out, Identifier *expr) { PrintObject(out, static_cast<Expression *>(expr)); }

void PrintObject(std::ostream *out, const storage::PropertyValue &value) {
  switch (value.type()) {
    case storage::PropertyValue::Type::Null:
      *out << "null";
      break;

    case storage::PropertyValue::Type::String:
      PrintObject(out, value.ValueString());
      break;

    case storage::PropertyValue::Type::Bool:
      *out << (value.ValueBool() ? "true" : "false");
      break;

    case storage::PropertyValue::Type::Int:
      PrintObject(out, value.ValueInt());
      break;

    case storage::PropertyValue::Type::Double:
      PrintObject(out, value.ValueDouble());
      break;

    case storage::PropertyValue::Type::List:
      PrintObject(out, value.ValueList());
      break;

    case storage::PropertyValue::Type::Map:
      PrintObject(out, value.ValueMap());
      break;
    case storage::PropertyValue::Type::TemporalData:
      PrintObject(out, value.ValueTemporalData());
      break;
  }
}

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

ExpressionPrettyPrinter::ExpressionPrettyPrinter(std::ostream *out) : out_(out) {}

#define UNARY_OPERATOR_VISIT(OP_NODE, OP_STR) \
  void ExpressionPrettyPrinter::Visit(OP_NODE &op) { PrintOperator(out_, OP_STR, op.expression_); }

UNARY_OPERATOR_VISIT(NotOperator, "Not");
UNARY_OPERATOR_VISIT(UnaryPlusOperator, "+");
UNARY_OPERATOR_VISIT(UnaryMinusOperator, "-");
UNARY_OPERATOR_VISIT(IsNullOperator, "IsNull");

#undef UNARY_OPERATOR_VISIT

#define BINARY_OPERATOR_VISIT(OP_NODE, OP_STR) \
  void ExpressionPrettyPrinter::Visit(OP_NODE &op) { PrintOperator(out_, OP_STR, op.expression1_, op.expression2_); }

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

void ExpressionPrettyPrinter::Visit(ListSlicingOperator &op) {
  PrintOperator(out_, "ListSlicing", op.list_, op.lower_bound_, op.upper_bound_);
}

void ExpressionPrettyPrinter::Visit(IfOperator &op) {
  PrintOperator(out_, "If", op.condition_, op.then_expression_, op.else_expression_);
}

void ExpressionPrettyPrinter::Visit(ListLiteral &op) { PrintOperator(out_, "ListLiteral", op.elements_); }

void ExpressionPrettyPrinter::Visit(MapLiteral &op) {
  std::map<std::string, Expression *> map;
  for (const auto &kv : op.elements_) {
    map[kv.first.name] = kv.second;
  }
  PrintObject(out_, map);
}

void ExpressionPrettyPrinter::Visit(MapProjectionLiteral &op) {
  std::map<std::string, Expression *> map_projection_elements;
  for (const auto &kv : op.elements_) {
    map_projection_elements[kv.first.name] = kv.second;
  }
  PrintObject(out_, op.map_variable_);
  PrintObject(out_, map_projection_elements);
}

void ExpressionPrettyPrinter::Visit(AllPropertiesLookup &op) { PrintObject(out_, &op); }

void ExpressionPrettyPrinter::Visit(LabelsTest &op) { PrintOperator(out_, "LabelsTest", op.expression_); }

void ExpressionPrettyPrinter::Visit(Aggregation &op) { PrintOperator(out_, "Aggregation", op.op_); }

void ExpressionPrettyPrinter::Visit(Function &op) { PrintOperator(out_, "Function", op.function_name_, op.arguments_); }

void ExpressionPrettyPrinter::Visit(Reduce &op) {
  PrintOperator(out_, "Reduce", op.accumulator_, op.initializer_, op.identifier_, op.list_, op.expression_);
}

void ExpressionPrettyPrinter::Visit(Coalesce &op) { PrintOperator(out_, "Coalesce", op.expressions_); }

void ExpressionPrettyPrinter::Visit(Extract &op) {
  PrintOperator(out_, "Extract", op.identifier_, op.list_, op.expression_);
}

void ExpressionPrettyPrinter::Visit(Exists & /*op*/) { PrintOperator(out_, "Exists", "expression"); }

void ExpressionPrettyPrinter::Visit(All &op) {
  PrintOperator(out_, "All", op.identifier_, op.list_expression_, op.where_->expression_);
}

void ExpressionPrettyPrinter::Visit(Single &op) {
  PrintOperator(out_, "Single", op.identifier_, op.list_expression_, op.where_->expression_);
}

void ExpressionPrettyPrinter::Visit(Any &op) {
  PrintOperator(out_, "Any", op.identifier_, op.list_expression_, op.where_->expression_);
}

void ExpressionPrettyPrinter::Visit(None &op) {
  PrintOperator(out_, "None", op.identifier_, op.list_expression_, op.where_->expression_);
}

void ExpressionPrettyPrinter::Visit(Identifier &op) { PrintOperator(out_, "Identifier", op.name_); }

void ExpressionPrettyPrinter::Visit(PrimitiveLiteral &op) { PrintObject(out_, op.value_); }

void ExpressionPrettyPrinter::Visit(PropertyLookup &op) {
  PrintOperator(out_, "PropertyLookup", op.expression_, op.property_.name);
}

void ExpressionPrettyPrinter::Visit(ParameterLookup &op) { PrintOperator(out_, "ParameterLookup", op.token_position_); }

void ExpressionPrettyPrinter::Visit(NamedExpression &op) {
  PrintOperator(out_, "NamedExpression", op.name_, op.expression_);
}

void ExpressionPrettyPrinter::Visit(RegexMatch &op) { PrintOperator(out_, "=~", op.string_expr_, op.regex_); }

}  // namespace

void PrintExpression(Expression *expr, std::ostream *out) {
  ExpressionPrettyPrinter printer{out};
  expr->Accept(printer);
}

void PrintExpression(NamedExpression *expr, std::ostream *out) {
  ExpressionPrettyPrinter printer{out};
  expr->Accept(printer);
}

}  // namespace memgraph::query

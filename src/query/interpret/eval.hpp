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

/// @file
#pragma once

#include <algorithm>
#include <limits>
#include <map>
#include <optional>
#include <regex>
#include <vector>

#include "query/common.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/frame.hpp"
#include "query/typed_value.hpp"
#include "utils/exceptions.hpp"

namespace memgraph::query {

class ExpressionEvaluator : public ExpressionVisitor<TypedValue> {
 public:
  ExpressionEvaluator(Frame *frame, const SymbolTable &symbol_table, const EvaluationContext &ctx, DbAccessor *dba,
                      storage::View view)
      : frame_(frame), symbol_table_(&symbol_table), ctx_(&ctx), dba_(dba), view_(view) {}

  using ExpressionVisitor<TypedValue>::Visit;

  utils::MemoryResource *GetMemoryResource() const { return ctx_->memory; }

  TypedValue Visit(NamedExpression &named_expression) override {
    const auto &symbol = symbol_table_->at(named_expression);
    auto value = named_expression.expression_->Accept(*this);
    frame_->at(symbol) = value;
    return value;
  }

  TypedValue Visit(Identifier &ident) override {
    return TypedValue(frame_->at(symbol_table_->at(ident)), ctx_->memory);
  }

#define BINARY_OPERATOR_VISITOR(OP_NODE, CPP_OP, CYPHER_OP)                                                    \
  TypedValue Visit(OP_NODE &op) override {                                                                     \
    auto val1 = op.expression1_->Accept(*this);                                                                \
    auto val2 = op.expression2_->Accept(*this);                                                                \
    try {                                                                                                      \
      return val1 CPP_OP val2;                                                                                 \
    } catch (const TypedValueException &) {                                                                    \
      throw QueryRuntimeException("Invalid types: {} and {} for '{}'.", val1.type(), val2.type(), #CYPHER_OP); \
    }                                                                                                          \
  }

#define UNARY_OPERATOR_VISITOR(OP_NODE, CPP_OP, CYPHER_OP)                              \
  TypedValue Visit(OP_NODE &op) override {                                              \
    auto val = op.expression_->Accept(*this);                                           \
    try {                                                                               \
      return CPP_OP val;                                                                \
    } catch (const TypedValueException &) {                                             \
      throw QueryRuntimeException("Invalid type {} for '{}'.", val.type(), #CYPHER_OP); \
    }                                                                                   \
  }

  BINARY_OPERATOR_VISITOR(OrOperator, ||, OR);
  BINARY_OPERATOR_VISITOR(XorOperator, ^, XOR);
  BINARY_OPERATOR_VISITOR(AdditionOperator, +, +);
  BINARY_OPERATOR_VISITOR(SubtractionOperator, -, -);
  BINARY_OPERATOR_VISITOR(MultiplicationOperator, *, *);
  BINARY_OPERATOR_VISITOR(DivisionOperator, /, /);
  BINARY_OPERATOR_VISITOR(ModOperator, %, %);
  BINARY_OPERATOR_VISITOR(NotEqualOperator, !=, <>);
  BINARY_OPERATOR_VISITOR(EqualOperator, ==, =);
  BINARY_OPERATOR_VISITOR(LessOperator, <, <);
  BINARY_OPERATOR_VISITOR(GreaterOperator, >, >);
  BINARY_OPERATOR_VISITOR(LessEqualOperator, <=, <=);
  BINARY_OPERATOR_VISITOR(GreaterEqualOperator, >=, >=);

  UNARY_OPERATOR_VISITOR(NotOperator, !, NOT);
  UNARY_OPERATOR_VISITOR(UnaryPlusOperator, +, +);
  UNARY_OPERATOR_VISITOR(UnaryMinusOperator, -, -);

#undef BINARY_OPERATOR_VISITOR
#undef UNARY_OPERATOR_VISITOR

  TypedValue Visit(AndOperator &op) override {
    auto value1 = op.expression1_->Accept(*this);
    if (value1.IsBool() && !value1.ValueBool()) {
      // If first expression is false, don't evaluate the second one.
      return value1;
    }
    auto value2 = op.expression2_->Accept(*this);
    try {
      return value1 && value2;
    } catch (const TypedValueException &) {
      throw QueryRuntimeException("Invalid types: {} and {} for AND.", value1.type(), value2.type());
    }
  }

  TypedValue Visit(IfOperator &if_operator) override {
    auto condition = if_operator.condition_->Accept(*this);
    if (condition.IsNull()) {
      return if_operator.then_expression_->Accept(*this);
    }
    if (condition.type() != TypedValue::Type::Bool) {
      // At the moment IfOperator is used only in CASE construct.
      throw QueryRuntimeException("CASE expected boolean expression, got {}.", condition.type());
    }
    if (condition.ValueBool()) {
      return if_operator.then_expression_->Accept(*this);
    }
    return if_operator.else_expression_->Accept(*this);
  }

  TypedValue Visit(InListOperator &in_list) override {
    auto literal = in_list.expression1_->Accept(*this);
    auto _list = in_list.expression2_->Accept(*this);
    if (_list.IsNull()) {
      return TypedValue(ctx_->memory);
    }
    // Exceptions have higher priority than returning nulls when list expression
    // is not null.
    if (_list.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("IN expected a list, got {}.", _list.type());
    }
    const auto &list = _list.ValueList();

    // If literal is NULL there is no need to try to compare it with every
    // element in the list since result of every comparison will be NULL. There
    // is one special case that we must test explicitly: if list is empty then
    // result is false since no comparison will be performed.
    if (list.empty()) return TypedValue(false, ctx_->memory);
    if (literal.IsNull()) return TypedValue(ctx_->memory);

    auto has_null = false;
    for (const auto &element : list) {
      auto result = literal == element;
      if (result.IsNull()) {
        has_null = true;
      } else if (result.ValueBool()) {
        return TypedValue(true, ctx_->memory);
      }
    }
    if (has_null) {
      return TypedValue(ctx_->memory);
    }
    return TypedValue(false, ctx_->memory);
  }

  TypedValue Visit(SubscriptOperator &list_indexing) override {
    auto lhs = list_indexing.expression1_->Accept(*this);
    auto index = list_indexing.expression2_->Accept(*this);
    if (!lhs.IsList() && !lhs.IsMap() && !lhs.IsVertex() && !lhs.IsEdge() && !lhs.IsNull())
      throw QueryRuntimeException(
          "Expected a list, a map, a node or an edge to index with '[]', got "
          "{}.",
          lhs.type());
    if (lhs.IsNull() || index.IsNull()) return TypedValue(ctx_->memory);
    if (lhs.IsList()) {
      if (!index.IsInt()) throw QueryRuntimeException("Expected an integer as a list index, got {}.", index.type());
      auto index_int = index.ValueInt();
      // NOTE: Take non-const reference to list, so that we can move out the
      // indexed element as the result.
      auto &list = lhs.ValueList();
      if (index_int < 0) {
        index_int += static_cast<int64_t>(list.size());
      }
      if (index_int >= static_cast<int64_t>(list.size()) || index_int < 0) return TypedValue(ctx_->memory);
      // NOTE: Explicit move is needed, so that we return the move constructed
      // value and preserve the correct MemoryResource.
      return std::move(list[index_int]);
    }

    if (lhs.IsMap()) {
      if (!index.IsString()) throw QueryRuntimeException("Expected a string as a map index, got {}.", index.type());
      // NOTE: Take non-const reference to map, so that we can move out the
      // looked-up element as the result.
      auto &map = lhs.ValueMap();
      auto found = map.find(index.ValueString());
      if (found == map.end()) return TypedValue(ctx_->memory);
      // NOTE: Explicit move is needed, so that we return the move constructed
      // value and preserve the correct MemoryResource.
      return std::move(found->second);
    }

    if (lhs.IsVertex()) {
      if (!index.IsString()) throw QueryRuntimeException("Expected a string as a property name, got {}.", index.type());
      return TypedValue(GetProperty(lhs.ValueVertex(), index.ValueString()), ctx_->memory);
    }

    if (lhs.IsEdge()) {
      if (!index.IsString()) throw QueryRuntimeException("Expected a string as a property name, got {}.", index.type());
      return TypedValue(GetProperty(lhs.ValueEdge(), index.ValueString()), ctx_->memory);
    }

    // lhs is Null
    return TypedValue(ctx_->memory);
  }

  TypedValue Visit(ListSlicingOperator &op) override {
    // If some type is null we can't return null, because throwing exception
    // on illegal type has higher priority.
    auto is_null = false;
    auto get_bound = [&](Expression *bound_expr, int64_t default_value) {
      if (bound_expr) {
        auto bound = bound_expr->Accept(*this);
        if (bound.type() == TypedValue::Type::Null) {
          is_null = true;
        } else if (bound.type() != TypedValue::Type::Int) {
          throw QueryRuntimeException("Expected an integer for a bound in list slicing, got {}.", bound.type());
        }
        return bound;
      }
      return TypedValue(default_value, ctx_->memory);
    };
    auto _upper_bound = get_bound(op.upper_bound_, std::numeric_limits<int64_t>::max());
    auto _lower_bound = get_bound(op.lower_bound_, 0);

    auto _list = op.list_->Accept(*this);
    if (_list.type() == TypedValue::Type::Null) {
      is_null = true;
    } else if (_list.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("Expected a list to slice, got {}.", _list.type());
    }

    if (is_null) {
      return TypedValue(ctx_->memory);
    }
    const auto &list = _list.ValueList();
    auto normalise_bound = [&](int64_t bound) {
      if (bound < 0) {
        bound = static_cast<int64_t>(list.size()) + bound;
      }
      return std::max(static_cast<int64_t>(0), std::min(bound, static_cast<int64_t>(list.size())));
    };
    auto lower_bound = normalise_bound(_lower_bound.ValueInt());
    auto upper_bound = normalise_bound(_upper_bound.ValueInt());
    if (upper_bound <= lower_bound) {
      return TypedValue(TypedValue::TVector(ctx_->memory), ctx_->memory);
    }
    return TypedValue(TypedValue::TVector(list.begin() + lower_bound, list.begin() + upper_bound, ctx_->memory));
  }

  TypedValue Visit(IsNullOperator &is_null) override {
    auto value = is_null.expression_->Accept(*this);
    return TypedValue(value.IsNull(), ctx_->memory);
  }

  TypedValue Visit(PropertyLookup &property_lookup) override {
    auto expression_result = property_lookup.expression_->Accept(*this);
    auto maybe_date = [this](const auto &date, const auto &prop_name) -> std::optional<TypedValue> {
      if (prop_name == "year") {
        return TypedValue(date.year, ctx_->memory);
      }
      if (prop_name == "month") {
        return TypedValue(date.month, ctx_->memory);
      }
      if (prop_name == "day") {
        return TypedValue(date.day, ctx_->memory);
      }
      return std::nullopt;
    };
    auto maybe_local_time = [this](const auto &lt, const auto &prop_name) -> std::optional<TypedValue> {
      if (prop_name == "hour") {
        return TypedValue(lt.hour, ctx_->memory);
      }
      if (prop_name == "minute") {
        return TypedValue(lt.minute, ctx_->memory);
      }
      if (prop_name == "second") {
        return TypedValue(lt.second, ctx_->memory);
      }
      if (prop_name == "millisecond") {
        return TypedValue(lt.millisecond, ctx_->memory);
      }
      if (prop_name == "microsecond") {
        return TypedValue(lt.microsecond, ctx_->memory);
      }
      return std::nullopt;
    };
    auto maybe_duration = [this](const auto &dur, const auto &prop_name) -> std::optional<TypedValue> {
      if (prop_name == "day") {
        return TypedValue(dur.Days(), ctx_->memory);
      }
      if (prop_name == "hour") {
        return TypedValue(dur.SubDaysAsHours(), ctx_->memory);
      }
      if (prop_name == "minute") {
        return TypedValue(dur.SubDaysAsMinutes(), ctx_->memory);
      }
      if (prop_name == "second") {
        return TypedValue(dur.SubDaysAsSeconds(), ctx_->memory);
      }
      if (prop_name == "millisecond") {
        return TypedValue(dur.SubDaysAsMilliseconds(), ctx_->memory);
      }
      if (prop_name == "microsecond") {
        return TypedValue(dur.SubDaysAsMicroseconds(), ctx_->memory);
      }
      if (prop_name == "nanosecond") {
        return TypedValue(dur.SubDaysAsNanoseconds(), ctx_->memory);
      }
      return std::nullopt;
    };
    switch (expression_result.type()) {
      case TypedValue::Type::Null:
        return TypedValue(ctx_->memory);
      case TypedValue::Type::Vertex:
        return TypedValue(GetProperty(expression_result.ValueVertex(), property_lookup.property_), ctx_->memory);
      case TypedValue::Type::Edge:
        return TypedValue(GetProperty(expression_result.ValueEdge(), property_lookup.property_), ctx_->memory);
      case TypedValue::Type::Map: {
        // NOTE: Take non-const reference to map, so that we can move out the
        // looked-up element as the result.
        auto &map = expression_result.ValueMap();
        auto found = map.find(property_lookup.property_.name.c_str());
        if (found == map.end()) return TypedValue(ctx_->memory);
        // NOTE: Explicit move is needed, so that we return the move constructed
        // value and preserve the correct MemoryResource.
        return std::move(found->second);
      }
      case TypedValue::Type::Duration: {
        const auto &prop_name = property_lookup.property_.name;
        const auto &dur = expression_result.ValueDuration();
        if (auto dur_field = maybe_duration(dur, prop_name); dur_field) {
          return std::move(*dur_field);
        }
        throw QueryRuntimeException("Invalid property name {} for Duration", prop_name);
      }
      case TypedValue::Type::Date: {
        const auto &prop_name = property_lookup.property_.name;
        const auto &date = expression_result.ValueDate();
        if (auto date_field = maybe_date(date, prop_name); date_field) {
          return std::move(*date_field);
        }
        throw QueryRuntimeException("Invalid property name {} for Date", prop_name);
      }
      case TypedValue::Type::LocalTime: {
        const auto &prop_name = property_lookup.property_.name;
        const auto &lt = expression_result.ValueLocalTime();
        if (auto lt_field = maybe_local_time(lt, prop_name); lt_field) {
          return std::move(*lt_field);
        }
        throw QueryRuntimeException("Invalid property name {} for LocalTime", prop_name);
      }
      case TypedValue::Type::LocalDateTime: {
        const auto &prop_name = property_lookup.property_.name;
        const auto &ldt = expression_result.ValueLocalDateTime();
        if (auto date_field = maybe_date(ldt.date, prop_name); date_field) {
          return std::move(*date_field);
        }
        if (auto lt_field = maybe_local_time(ldt.local_time, prop_name); lt_field) {
          return std::move(*lt_field);
        }
        throw QueryRuntimeException("Invalid property name {} for LocalDateTime", prop_name);
      }
      default:
        throw QueryRuntimeException("Only nodes, edges, maps and temporal types have properties to be looked-up.");
    }
  }

  TypedValue Visit(LabelsTest &labels_test) override {
    auto expression_result = labels_test.expression_->Accept(*this);
    switch (expression_result.type()) {
      case TypedValue::Type::Null:
        return TypedValue(ctx_->memory);
      case TypedValue::Type::Vertex: {
        const auto &vertex = expression_result.ValueVertex();
        for (const auto &label : labels_test.labels_) {
          auto has_label = vertex.HasLabel(view_, GetLabel(label));
          if (has_label.HasError() && has_label.GetError() == storage::Error::NONEXISTENT_OBJECT) {
            // This is a very nasty and temporary hack in order to make MERGE
            // work. The old storage had the following logic when returning an
            // `OLD` view: `return old ? old : new`. That means that if the
            // `OLD` view didn't exist, it returned the NEW view. With this hack
            // we simulate that behavior.
            // TODO (mferencevic, teon.banek): Remove once MERGE is
            // reimplemented.
            has_label = vertex.HasLabel(storage::View::NEW, GetLabel(label));
          }
          if (has_label.HasError()) {
            switch (has_label.GetError()) {
              case storage::Error::DELETED_OBJECT:
                throw QueryRuntimeException("Trying to access labels on a deleted node.");
              case storage::Error::NONEXISTENT_OBJECT:
                throw query::QueryRuntimeException("Trying to access labels from a node that doesn't exist.");
              case storage::Error::SERIALIZATION_ERROR:
              case storage::Error::VERTEX_HAS_EDGES:
              case storage::Error::PROPERTIES_DISABLED:
                throw QueryRuntimeException("Unexpected error when accessing labels.");
            }
          }
          if (!*has_label) {
            return TypedValue(false, ctx_->memory);
          }
        }
        return TypedValue(true, ctx_->memory);
      }
      default:
        throw QueryRuntimeException("Only nodes have labels.");
    }
  }

  TypedValue Visit(PrimitiveLiteral &literal) override {
    // TODO: no need to evaluate constants, we can write it to frame in one
    // of the previous phases.
    return TypedValue(literal.value_, ctx_->memory);
  }

  TypedValue Visit(ListLiteral &literal) override {
    TypedValue::TVector result(ctx_->memory);
    result.reserve(literal.elements_.size());
    for (const auto &expression : literal.elements_) result.emplace_back(expression->Accept(*this));
    return TypedValue(result, ctx_->memory);
  }

  TypedValue Visit(MapLiteral &literal) override {
    TypedValue::TMap result(ctx_->memory);
    for (const auto &pair : literal.elements_) result.emplace(pair.first.name, pair.second->Accept(*this));
    return TypedValue(result, ctx_->memory);
  }

  TypedValue Visit(Aggregation &aggregation) override {
    return TypedValue(frame_->at(symbol_table_->at(aggregation)), ctx_->memory);
  }

  TypedValue Visit(Coalesce &coalesce) override {
    auto &exprs = coalesce.expressions_;

    if (exprs.size() == 0) {
      throw QueryRuntimeException("'coalesce' requires at least one argument.");
    }

    for (int64_t i = 0; i < exprs.size(); ++i) {
      TypedValue val(exprs[i]->Accept(*this), ctx_->memory);
      if (!val.IsNull()) {
        return val;
      }
    }

    return TypedValue(ctx_->memory);
  }

  TypedValue Visit(Function &function) override {
    FunctionContext function_ctx{dba_, ctx_->memory, ctx_->timestamp, &ctx_->counters, view_};
    // Stack allocate evaluated arguments when there's a small number of them.
    if (function.arguments_.size() <= 8) {
      TypedValue arguments[8] = {TypedValue(ctx_->memory), TypedValue(ctx_->memory), TypedValue(ctx_->memory),
                                 TypedValue(ctx_->memory), TypedValue(ctx_->memory), TypedValue(ctx_->memory),
                                 TypedValue(ctx_->memory), TypedValue(ctx_->memory)};
      for (size_t i = 0; i < function.arguments_.size(); ++i) {
        arguments[i] = function.arguments_[i]->Accept(*this);
      }
      auto res = function.function_(arguments, function.arguments_.size(), function_ctx);
      MG_ASSERT(res.GetMemoryResource() == ctx_->memory);
      return res;
    } else {
      TypedValue::TVector arguments(ctx_->memory);
      arguments.reserve(function.arguments_.size());
      for (const auto &argument : function.arguments_) {
        arguments.emplace_back(argument->Accept(*this));
      }
      auto res = function.function_(arguments.data(), arguments.size(), function_ctx);
      MG_ASSERT(res.GetMemoryResource() == ctx_->memory);
      return res;
    }
  }

  TypedValue Visit(Reduce &reduce) override {
    auto list_value = reduce.list_->Accept(*this);
    if (list_value.IsNull()) {
      return TypedValue(ctx_->memory);
    }
    if (list_value.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("REDUCE expected a list, got {}.", list_value.type());
    }
    const auto &list = list_value.ValueList();
    const auto &element_symbol = symbol_table_->at(*reduce.identifier_);
    const auto &accumulator_symbol = symbol_table_->at(*reduce.accumulator_);
    auto accumulator = reduce.initializer_->Accept(*this);
    for (const auto &element : list) {
      frame_->at(accumulator_symbol) = accumulator;
      frame_->at(element_symbol) = element;
      accumulator = reduce.expression_->Accept(*this);
    }
    return accumulator;
  }

  TypedValue Visit(Extract &extract) override {
    auto list_value = extract.list_->Accept(*this);
    if (list_value.IsNull()) {
      return TypedValue(ctx_->memory);
    }
    if (list_value.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("EXTRACT expected a list, got {}.", list_value.type());
    }
    const auto &list = list_value.ValueList();
    const auto &element_symbol = symbol_table_->at(*extract.identifier_);
    TypedValue::TVector result(ctx_->memory);
    result.reserve(list.size());
    for (const auto &element : list) {
      if (element.IsNull()) {
        result.emplace_back();
      } else {
        frame_->at(element_symbol) = element;
        result.emplace_back(extract.expression_->Accept(*this));
      }
    }
    return TypedValue(result, ctx_->memory);
  }

  TypedValue Visit(All &all) override {
    auto list_value = all.list_expression_->Accept(*this);
    if (list_value.IsNull()) {
      return TypedValue(ctx_->memory);
    }
    if (list_value.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("ALL expected a list, got {}.", list_value.type());
    }
    const auto &list = list_value.ValueList();
    const auto &symbol = symbol_table_->at(*all.identifier_);
    bool has_null_elements = false;
    bool has_value = false;
    for (const auto &element : list) {
      frame_->at(symbol) = element;
      auto result = all.where_->expression_->Accept(*this);
      if (!result.IsNull() && result.type() != TypedValue::Type::Bool) {
        throw QueryRuntimeException("Predicate of ALL must evaluate to boolean, got {}.", result.type());
      }
      if (!result.IsNull()) {
        has_value = true;
        if (!result.ValueBool()) {
          return TypedValue(false, ctx_->memory);
        }
      } else {
        has_null_elements = true;
      }
    }
    if (!has_value) {
      return TypedValue(ctx_->memory);
    }
    if (has_null_elements) {
      return TypedValue(false, ctx_->memory);
    } else {
      return TypedValue(true, ctx_->memory);
    }
  }

  TypedValue Visit(Single &single) override {
    auto list_value = single.list_expression_->Accept(*this);
    if (list_value.IsNull()) {
      return TypedValue(ctx_->memory);
    }
    if (list_value.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("SINGLE expected a list, got {}.", list_value.type());
    }
    const auto &list = list_value.ValueList();
    const auto &symbol = symbol_table_->at(*single.identifier_);
    bool has_value = false;
    bool predicate_satisfied = false;
    for (const auto &element : list) {
      frame_->at(symbol) = element;
      auto result = single.where_->expression_->Accept(*this);
      if (!result.IsNull() && result.type() != TypedValue::Type::Bool) {
        throw QueryRuntimeException("Predicate of SINGLE must evaluate to boolean, got {}.", result.type());
      }
      if (result.type() == TypedValue::Type::Bool) {
        has_value = true;
      }
      if (result.IsNull() || !result.ValueBool()) {
        continue;
      }
      // Return false if more than one element satisfies the predicate.
      if (predicate_satisfied) {
        return TypedValue(false, ctx_->memory);
      } else {
        predicate_satisfied = true;
      }
    }
    if (!has_value) {
      return TypedValue(ctx_->memory);
    } else {
      return TypedValue(predicate_satisfied, ctx_->memory);
    }
  }

  TypedValue Visit(Any &any) override {
    auto list_value = any.list_expression_->Accept(*this);
    if (list_value.IsNull()) {
      return TypedValue(ctx_->memory);
    }
    if (list_value.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("ANY expected a list, got {}.", list_value.type());
    }
    const auto &list = list_value.ValueList();
    const auto &symbol = symbol_table_->at(*any.identifier_);
    bool has_value = false;
    for (const auto &element : list) {
      frame_->at(symbol) = element;
      auto result = any.where_->expression_->Accept(*this);
      if (!result.IsNull() && result.type() != TypedValue::Type::Bool) {
        throw QueryRuntimeException("Predicate of ANY must evaluate to boolean, got {}.", result.type());
      }
      if (!result.IsNull()) {
        has_value = true;
        if (result.ValueBool()) {
          return TypedValue(true, ctx_->memory);
        }
      }
    }
    // Return Null if all elements are Null
    if (!has_value) {
      return TypedValue(ctx_->memory);
    } else {
      return TypedValue(false, ctx_->memory);
    }
  }

  TypedValue Visit(None &none) override {
    auto list_value = none.list_expression_->Accept(*this);
    if (list_value.IsNull()) {
      return TypedValue(ctx_->memory);
    }
    if (list_value.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("NONE expected a list, got {}.", list_value.type());
    }
    const auto &list = list_value.ValueList();
    const auto &symbol = symbol_table_->at(*none.identifier_);
    bool has_value = false;
    for (const auto &element : list) {
      frame_->at(symbol) = element;
      auto result = none.where_->expression_->Accept(*this);
      if (!result.IsNull() && result.type() != TypedValue::Type::Bool) {
        throw QueryRuntimeException("Predicate of NONE must evaluate to boolean, got {}.", result.type());
      }
      if (!result.IsNull()) {
        has_value = true;
        if (result.ValueBool()) {
          return TypedValue(false, ctx_->memory);
        }
      }
    }
    // Return Null if all elements are Null
    if (!has_value) {
      return TypedValue(ctx_->memory);
    } else {
      return TypedValue(true, ctx_->memory);
    }
  }

  TypedValue Visit(ParameterLookup &param_lookup) override {
    return TypedValue(ctx_->parameters.AtTokenPosition(param_lookup.token_position_), ctx_->memory);
  }

  TypedValue Visit(RegexMatch &regex_match) override {
    auto target_string_value = regex_match.string_expr_->Accept(*this);
    auto regex_value = regex_match.regex_->Accept(*this);
    if (target_string_value.IsNull() || regex_value.IsNull()) {
      return TypedValue(ctx_->memory);
    }
    if (regex_value.type() != TypedValue::Type::String) {
      throw QueryRuntimeException("Regular expression must evaluate to a string, got {}.", regex_value.type());
    }
    if (target_string_value.type() != TypedValue::Type::String) {
      // Instead of error, we return Null which makes it compatible in case we
      // use indexed lookup which filters out any non-string properties.
      // Assuming a property lookup is the target_string_value.
      return TypedValue(ctx_->memory);
    }
    const auto &target_string = target_string_value.ValueString();
    try {
      std::regex regex(regex_value.ValueString());
      return TypedValue(std::regex_match(target_string, regex), ctx_->memory);
    } catch (const std::regex_error &e) {
      throw QueryRuntimeException("Regex error in '{}': {}", regex_value.ValueString(), e.what());
    }
  }

 private:
  template <class TRecordAccessor>
  storage::PropertyValue GetProperty(const TRecordAccessor &record_accessor, PropertyIx prop) {
    auto maybe_prop = record_accessor.GetProperty(view_, ctx_->properties[prop.ix]);
    if (maybe_prop.HasError() && maybe_prop.GetError() == storage::Error::NONEXISTENT_OBJECT) {
      // This is a very nasty and temporary hack in order to make MERGE work.
      // The old storage had the following logic when returning an `OLD` view:
      // `return old ? old : new`. That means that if the `OLD` view didn't
      // exist, it returned the NEW view. With this hack we simulate that
      // behavior.
      // TODO (mferencevic, teon.banek): Remove once MERGE is reimplemented.
      maybe_prop = record_accessor.GetProperty(storage::View::NEW, ctx_->properties[prop.ix]);
    }
    if (maybe_prop.HasError()) {
      switch (maybe_prop.GetError()) {
        case storage::Error::DELETED_OBJECT:
          throw QueryRuntimeException("Trying to get a property from a deleted object.");
        case storage::Error::NONEXISTENT_OBJECT:
          throw query::QueryRuntimeException("Trying to get a property from an object that doesn't exist.");
        case storage::Error::SERIALIZATION_ERROR:
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::PROPERTIES_DISABLED:
          throw QueryRuntimeException("Unexpected error when getting a property.");
      }
    }
    return *maybe_prop;
  }

  template <class TRecordAccessor>
  storage::PropertyValue GetProperty(const TRecordAccessor &record_accessor, const std::string_view name) {
    auto maybe_prop = record_accessor.GetProperty(view_, dba_->NameToProperty(name));
    if (maybe_prop.HasError() && maybe_prop.GetError() == storage::Error::NONEXISTENT_OBJECT) {
      // This is a very nasty and temporary hack in order to make MERGE work.
      // The old storage had the following logic when returning an `OLD` view:
      // `return old ? old : new`. That means that if the `OLD` view didn't
      // exist, it returned the NEW view. With this hack we simulate that
      // behavior.
      // TODO (mferencevic, teon.banek): Remove once MERGE is reimplemented.
      maybe_prop = record_accessor.GetProperty(view_, dba_->NameToProperty(name));
    }
    if (maybe_prop.HasError()) {
      switch (maybe_prop.GetError()) {
        case storage::Error::DELETED_OBJECT:
          throw QueryRuntimeException("Trying to get a property from a deleted object.");
        case storage::Error::NONEXISTENT_OBJECT:
          throw query::QueryRuntimeException("Trying to get a property from an object that doesn't exist.");
        case storage::Error::SERIALIZATION_ERROR:
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::PROPERTIES_DISABLED:
          throw QueryRuntimeException("Unexpected error when getting a property.");
      }
    }
    return *maybe_prop;
  }

  storage::LabelId GetLabel(LabelIx label) { return ctx_->labels[label.ix]; }

  Frame *frame_;
  const SymbolTable *symbol_table_;
  const EvaluationContext *ctx_;
  DbAccessor *dba_;
  // which switching approach should be used when evaluating
  storage::View view_;
};

/// A helper function for evaluating an expression that's an int.
///
/// @param what - Name of what's getting evaluated. Used for user feedback (via
///               exception) when the evaluated value is not an int.
/// @throw QueryRuntimeException if expression doesn't evaluate to an int.
int64_t EvaluateInt(ExpressionEvaluator *evaluator, Expression *expr, const std::string &what);

std::optional<size_t> EvaluateMemoryLimit(ExpressionEvaluator *eval, Expression *memory_limit, size_t memory_scale);

}  // namespace memgraph::query

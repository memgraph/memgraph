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

/// @file
#pragma once

#include <algorithm>
#include <cstddef>
#include <limits>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "query/common.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/frame.hpp"
#include "query/typed_value.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/storage_mode.hpp"
#include "utils/cast.hpp"
#include "utils/exceptions.hpp"
#include "utils/frame_change_id.hpp"
#include "utils/logging.hpp"
#include "utils/pmr/unordered_map.hpp"

namespace r = ranges;
namespace rv = r::views;
namespace memgraph::query {

class ReferenceExpressionEvaluator : public ExpressionVisitor<TypedValue *> {
 public:
  ReferenceExpressionEvaluator(Frame *frame, const SymbolTable *symbol_table, const EvaluationContext *ctx)
      : frame_(frame), symbol_table_(symbol_table), ctx_(ctx) {}

  using ExpressionVisitor<TypedValue *>::Visit;

  utils::MemoryResource *GetMemoryResource() const { return ctx_->memory; }

#define UNSUCCESSFUL_VISIT(expr_name) \
  TypedValue *Visit(expr_name &expr) override { return nullptr; }

  TypedValue *Visit(Identifier &ident) override { return &frame_->at(symbol_table_->at(ident)); }

  UNSUCCESSFUL_VISIT(NamedExpression);
  UNSUCCESSFUL_VISIT(OrOperator);
  UNSUCCESSFUL_VISIT(XorOperator);
  UNSUCCESSFUL_VISIT(AdditionOperator);
  UNSUCCESSFUL_VISIT(SubtractionOperator);
  UNSUCCESSFUL_VISIT(MultiplicationOperator);
  UNSUCCESSFUL_VISIT(DivisionOperator);
  UNSUCCESSFUL_VISIT(ModOperator);
  UNSUCCESSFUL_VISIT(ExponentiationOperator);
  UNSUCCESSFUL_VISIT(NotEqualOperator);
  UNSUCCESSFUL_VISIT(EqualOperator);
  UNSUCCESSFUL_VISIT(LessOperator);
  UNSUCCESSFUL_VISIT(GreaterOperator);
  UNSUCCESSFUL_VISIT(LessEqualOperator);
  UNSUCCESSFUL_VISIT(GreaterEqualOperator);
  UNSUCCESSFUL_VISIT(RangeOperator);

  UNSUCCESSFUL_VISIT(NotOperator);
  UNSUCCESSFUL_VISIT(UnaryPlusOperator);
  UNSUCCESSFUL_VISIT(UnaryMinusOperator);

  UNSUCCESSFUL_VISIT(AndOperator);
  UNSUCCESSFUL_VISIT(IfOperator);
  UNSUCCESSFUL_VISIT(InListOperator);

  UNSUCCESSFUL_VISIT(SubscriptOperator);

  UNSUCCESSFUL_VISIT(ListSlicingOperator);
  UNSUCCESSFUL_VISIT(IsNullOperator);
  UNSUCCESSFUL_VISIT(PropertyLookup);
  UNSUCCESSFUL_VISIT(AllPropertiesLookup);
  UNSUCCESSFUL_VISIT(LabelsTest);

  UNSUCCESSFUL_VISIT(PrimitiveLiteral);
  UNSUCCESSFUL_VISIT(ListLiteral);
  UNSUCCESSFUL_VISIT(MapLiteral);
  UNSUCCESSFUL_VISIT(MapProjectionLiteral);
  UNSUCCESSFUL_VISIT(Aggregation);
  UNSUCCESSFUL_VISIT(Coalesce);
  UNSUCCESSFUL_VISIT(Function);
  UNSUCCESSFUL_VISIT(Reduce);
  UNSUCCESSFUL_VISIT(Extract);
  UNSUCCESSFUL_VISIT(All);
  UNSUCCESSFUL_VISIT(Single);
  UNSUCCESSFUL_VISIT(Any);
  UNSUCCESSFUL_VISIT(None);
  UNSUCCESSFUL_VISIT(ListComprehension);
  UNSUCCESSFUL_VISIT(ParameterLookup);
  UNSUCCESSFUL_VISIT(RegexMatch);
  UNSUCCESSFUL_VISIT(Exists);
  UNSUCCESSFUL_VISIT(PatternComprehension);
  UNSUCCESSFUL_VISIT(EnumValueAccess);

#undef UNSUCCESSFUL_VISIT

 private:
  Frame *frame_;
  const SymbolTable *symbol_table_;
  const EvaluationContext *ctx_;
};

class PrimitiveLiteralExpressionEvaluator : public ExpressionVisitor<TypedValue> {
 public:
  explicit PrimitiveLiteralExpressionEvaluator(EvaluationContext const &ctx) : ctx_(&ctx) {}
  using ExpressionVisitor<TypedValue>::Visit;
  TypedValue Visit(PrimitiveLiteral &literal) override {
    // TODO: no need to evaluate constants, we can write it to frame in one
    // of the previous phases.
    return TypedValue(literal.value_, ctx_->memory);
  }
  TypedValue Visit(ParameterLookup &param_lookup) override {
    return TypedValue(ctx_->parameters.AtTokenPosition(param_lookup.token_position_), ctx_->memory);
  }

#define INVALID_VISIT(expr_name)                                                             \
  TypedValue Visit(expr_name & /*expr*/) override {                                          \
    DLOG_FATAL("Invalid expression type visited with PrimitiveLiteralExpressionEvaluator."); \
    return {};                                                                               \
  }

  INVALID_VISIT(NamedExpression)
  INVALID_VISIT(OrOperator)
  INVALID_VISIT(XorOperator)
  INVALID_VISIT(AndOperator)
  INVALID_VISIT(NotOperator)
  INVALID_VISIT(AdditionOperator)
  INVALID_VISIT(SubtractionOperator)
  INVALID_VISIT(MultiplicationOperator)
  INVALID_VISIT(DivisionOperator)
  INVALID_VISIT(ModOperator)
  INVALID_VISIT(ExponentiationOperator)
  INVALID_VISIT(NotEqualOperator)
  INVALID_VISIT(EqualOperator)
  INVALID_VISIT(LessOperator)
  INVALID_VISIT(GreaterOperator)
  INVALID_VISIT(LessEqualOperator)
  INVALID_VISIT(GreaterEqualOperator)
  INVALID_VISIT(RangeOperator)
  INVALID_VISIT(InListOperator)
  INVALID_VISIT(SubscriptOperator)
  INVALID_VISIT(ListSlicingOperator)
  INVALID_VISIT(IfOperator)
  INVALID_VISIT(UnaryPlusOperator)
  INVALID_VISIT(UnaryMinusOperator)
  INVALID_VISIT(IsNullOperator)
  INVALID_VISIT(ListLiteral)
  INVALID_VISIT(MapLiteral)
  INVALID_VISIT(MapProjectionLiteral)
  INVALID_VISIT(PropertyLookup)
  INVALID_VISIT(AllPropertiesLookup)
  INVALID_VISIT(LabelsTest)
  INVALID_VISIT(Aggregation)
  INVALID_VISIT(Function)
  INVALID_VISIT(Reduce)
  INVALID_VISIT(Coalesce)
  INVALID_VISIT(Extract)
  INVALID_VISIT(All)
  INVALID_VISIT(Single)
  INVALID_VISIT(Any)
  INVALID_VISIT(None)
  INVALID_VISIT(ListComprehension)
  INVALID_VISIT(Identifier)
  INVALID_VISIT(RegexMatch)
  INVALID_VISIT(Exists)
  INVALID_VISIT(PatternComprehension)
  INVALID_VISIT(EnumValueAccess)

#undef INVALID_VISIT
 private:
  EvaluationContext const *ctx_;
};
class ExpressionEvaluator : public ExpressionVisitor<TypedValue> {
 public:
  ExpressionEvaluator(Frame *frame, const SymbolTable &symbol_table, const EvaluationContext &ctx, DbAccessor *dba,
                      storage::View view, FrameChangeCollector *frame_change_collector = nullptr,
                      const int64_t *hops_counter = nullptr)
      : frame_(frame),
        symbol_table_(&symbol_table),
        ctx_(&ctx),
        dba_(dba),
        view_(view),
        frame_change_collector_(frame_change_collector),
        hops_counter_(hops_counter) {}

  using ExpressionVisitor<TypedValue>::Visit;

  utils::MemoryResource *GetMemoryResource() const { return ctx_->memory; }

  storage::NameIdMapper *GetNameIdMapper() const { return dba_->GetStorageAccessor()->GetNameIdMapper(); }

  void ResetPropertyLookupCache() { property_lookup_cache_.clear(); }

  int64_t GetHopsCounter() { return hops_counter_ != nullptr ? *hops_counter_ : 0; }

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

  TypedValue Visit(RangeOperator &op) override { return op.expr1_->Accept(*this) && op.expr2_->Accept(*this); }

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

  TypedValue Visit(OrOperator &op) override {
    auto value1 = op.expression1_->Accept(*this);
    if (value1.IsBool() && value1.ValueBool()) {
      // If first expression is true, don't evaluate the second one.
      return value1;
    }
    auto value2 = op.expression2_->Accept(*this);
    try {
      return value1 || value2;
    } catch (const TypedValueException &) {
      throw QueryRuntimeException("Invalid types: {} and {} for OR.", value1.type(), value2.type());
    }
  }

  TypedValue Visit(ExponentiationOperator &op) override {
    auto value1 = op.expression1_->Accept(*this);
    auto value2 = op.expression2_->Accept(*this);
    try {
      return pow(value1, value2);
    } catch (const TypedValueException &) {
      throw QueryRuntimeException("Invalid types: {} and {} for ^.", value1.type(), value2.type());
    }
  }

  TypedValue Visit(IfOperator &if_operator) override {
    auto condition = if_operator.condition_->Accept(*this);
    if (condition.IsNull()) {
      return if_operator.else_expression_->Accept(*this);
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

    auto get_list_literal = [this, &in_list]() -> TypedValue {
      ReferenceExpressionEvaluator reference_expression_evaluator{frame_, symbol_table_, ctx_};
      auto *list_ptr = in_list.expression2_->Accept(reference_expression_evaluator);
      if (nullptr == list_ptr) {
        return in_list.expression2_->Accept(*this);
      }
      return *list_ptr;
    };

    auto do_list_literal_checks = [this, &literal](const TypedValue &list) -> std::optional<TypedValue> {
      if (list.IsNull()) {
        return TypedValue(ctx_->memory);
      }
      // Exceptions have higher priority than returning nulls when list expression
      // is not null.
      if (list.type() != TypedValue::Type::List) {
        throw QueryRuntimeException("IN expected a list, got {}.", list.type());
      }
      const auto &list_value = list.ValueList();

      // If literal is NULL there is no need to try to compare it with every
      // element in the list since result of every comparison will be NULL. There
      // is one special case that we must test explicitly: if list is empty then
      // result is false since no comparison will be performed.
      if (list_value.empty()) return TypedValue(false, ctx_->memory);
      if (literal.IsNull()) return TypedValue(ctx_->memory);
      return {};
    };

    const auto cached_id = memgraph::utils::GetFrameChangeId(in_list);

    const auto do_cache{frame_change_collector_ != nullptr && cached_id &&
                        frame_change_collector_->IsKeyTracked(*cached_id)};
    if (do_cache) {
      if (!frame_change_collector_->IsKeyValueCached(*cached_id)) {
        // Check only first time if everything is okay, later when we use
        // cache there is no need to check again as we did check first time
        auto list = get_list_literal();
        auto preoperational_checks = do_list_literal_checks(list);
        if (preoperational_checks) {
          return std::move(*preoperational_checks);
        }
        auto &cached_value = frame_change_collector_->GetCachedValue(*cached_id);
        // Don't move here because we don't want to remove the element from the frame
        cached_value.CacheValue(list);
      }
      const auto &cached_value = frame_change_collector_->GetCachedValue(*cached_id);

      if (cached_value.ContainsValue(literal)) {
        return TypedValue(true, ctx_->memory);
      }
      // has null
      if (cached_value.ContainsValue(TypedValue(ctx_->memory))) {
        return TypedValue(ctx_->memory);
      }
      return TypedValue(false, ctx_->memory);
    }
    // When caching is not an option, we need to evaluate list literal every time
    // and do the checks
    const auto list = get_list_literal();
    auto preoperational_checks = do_list_literal_checks(list);
    if (preoperational_checks) {
      return std::move(*preoperational_checks);
    }

    const auto &list_value = list.ValueList();
    auto has_null = false;
    for (const auto &element : list_value) {
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
    ReferenceExpressionEvaluator referenceExpressionEvaluator(frame_, symbol_table_, ctx_);

    TypedValue *lhs_ptr = list_indexing.expression1_->Accept(referenceExpressionEvaluator);
    TypedValue lhs;
    const auto referenced = nullptr != lhs_ptr;
    if (!referenced) {
      lhs = list_indexing.expression1_->Accept(*this);
      lhs_ptr = &lhs;
    }
    auto index = list_indexing.expression2_->Accept(*this);
    if (!lhs_ptr->IsList() && !lhs_ptr->IsMap() && !lhs_ptr->IsVertex() && !lhs_ptr->IsEdge() && !lhs_ptr->IsNull())
      throw QueryRuntimeException(
          "Expected a list, a map, a node or an edge to index with '[]', got "
          "{}.",
          lhs_ptr->type());
    if (lhs_ptr->IsNull() || index.IsNull()) return TypedValue(ctx_->memory);
    if (lhs_ptr->IsList()) {
      if (!index.IsInt()) throw QueryRuntimeException("Expected an integer as a list index, got {}.", index.type());
      auto index_int = index.ValueInt();
      auto &list = lhs_ptr->ValueList();
      if (index_int < 0) {
        index_int += static_cast<int64_t>(list.size());
      }
      if (index_int >= static_cast<int64_t>(list.size()) || index_int < 0) return TypedValue(ctx_->memory);
      return referenced ? TypedValue(list[index_int], ctx_->memory)
                        : TypedValue(std::move(list[index_int]), ctx_->memory);
    }

    if (lhs_ptr->IsMap()) {
      if (!index.IsString()) throw QueryRuntimeException("Expected a string as a map index, got {}.", index.type());
      // NOTE: Take non-const reference to map, so that we can move out the
      // looked-up element as the result.
      auto &map = lhs_ptr->ValueMap();
      auto found = map.find(index.ValueString());
      if (found == map.end()) return TypedValue(ctx_->memory);
      return referenced ? TypedValue(found->second, ctx_->memory) : TypedValue(std::move(found->second), ctx_->memory);
    }

    if (lhs_ptr->IsVertex()) {
      if (!index.IsString()) throw QueryRuntimeException("Expected a string as a property name, got {}.", index.type());
      return {GetProperty(lhs_ptr->ValueVertex(), index.ValueString()), GetNameIdMapper(), ctx_->memory};
    }

    if (lhs_ptr->IsEdge()) {
      if (!index.IsString()) throw QueryRuntimeException("Expected a string as a property name, got {}.", index.type());
      return {GetProperty(lhs_ptr->ValueEdge(), index.ValueString()), GetNameIdMapper(), ctx_->memory};
    };

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

  TypedValue Visit(PropertyLookup &property_lookup) override;

  TypedValue Visit(AllPropertiesLookup &all_properties_lookup) override;

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
            // TODO: Remove once MERGE is
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
        for (const auto &or_labels_pattern : labels_test.or_labels_) {
          bool has_at_least_one_label = false;
          for (const auto &label : or_labels_pattern) {
            auto has_label = vertex.HasLabel(view_, GetLabel(label));
            if (has_label.HasError() && has_label.GetError() == storage::Error::NONEXISTENT_OBJECT) {
              // This is a very nasty and temporary hack in order to make MERGE
              // work. The old storage had the following logic when returning an
              // `OLD` view: `return old ? old : new`. That means that if the
              // `OLD` view didn't exist, it returned the NEW view. With this hack
              // we simulate that behavior.
              // TODO: Remove once MERGE is
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
            if (*has_label) {
              has_at_least_one_label = true;
              break;
            }
          }
          if (!has_at_least_one_label) {
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
    for (const auto &pair : literal.elements_) {
      result.emplace(TypedValue::TString(pair.first.name, ctx_->memory), pair.second->Accept(*this));
    }

    return TypedValue(result, ctx_->memory);
  }

  TypedValue Visit(MapProjectionLiteral &literal) override {
    constexpr std::string_view kAllPropertiesSelector{"*"};

    TypedValue::TMap result(ctx_->memory);
    TypedValue::TMap all_properties_lookup(ctx_->memory);

    auto map_variable = literal.map_variable_->Accept(*this);
    if (map_variable.IsNull()) {
      return TypedValue(ctx_->memory);
    }

    for (const auto &[property_key, property_value] : literal.elements_) {
      if (property_key.name == kAllPropertiesSelector.data()) {
        auto maybe_all_properties_lookup = property_value->Accept(*this);

        if (maybe_all_properties_lookup.type() != TypedValue::Type::Map) {
          LOG_FATAL("Expected a map from AllPropertiesLookup, got {}.", maybe_all_properties_lookup.type());
        }

        all_properties_lookup = std::move(maybe_all_properties_lookup.ValueMap());
        continue;
      }

      result.emplace(TypedValue::TString(property_key.name, ctx_->memory), property_value->Accept(*this));
    }

    if (!all_properties_lookup.empty()) result.merge(all_properties_lookup);

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

    for (auto &expr : exprs) {
      TypedValue val(expr->Accept(*this), ctx_->memory);
      if (!val.IsNull()) {
        return val;
      }
    }

    return TypedValue(ctx_->memory);
  }

  TypedValue Visit(Function &function) override {
    FunctionContext function_ctx{dba_, ctx_->memory, ctx_->timestamp, &ctx_->counters, view_, GetHopsCounter()};
    bool is_transactional = storage::IsTransactional(dba_->GetStorageMode());

    // Check if we can cache and try cache lookup
    auto can_cache =
        !kUncacheableFunctions.contains(function.function_name_) && r::all_of(function.arguments_, [](auto *arg) {
          if (auto *param_lookup = utils::Downcast<ParameterLookup>(arg)) {
            return param_lookup->token_position_ != -1;
          }
          return utils::Downcast<PrimitiveLiteral>(arg) != nullptr;
        });
    if (can_cache) {
      auto cached_function = ctx_->function_cache.find(function);
      if (cached_function != ctx_->function_cache.end()) {
        return cached_function->second;
      }
    }

    // Evaluate function and cache result if applicable
    auto result = [&]() -> TypedValue {
      // Stack allocate evaluated arguments when there's a small number of them.
      if (function.arguments_.size() <= 8) {
        utils::uninitialised_storage<std::array<TypedValue, 8>> arguments;
        auto constructed_count = 0;
        auto destroy_arguments = utils::OnScopeExit{[&] {
          for (size_t i = 0; i != constructed_count; ++i) {
            std::destroy_at(&(*arguments.as())[i]);
          }
        }};
        for (auto i = 0; i < function.arguments_.size(); ++i) {
          std::construct_at(&(*arguments.as())[i], function.arguments_[i]->Accept(*this));
          ++constructed_count;
        }
        return function.function_(arguments.as()->data(), function.arguments_.size(), function_ctx);
      }
      TypedValue::TVector arguments(ctx_->memory);
      arguments.reserve(function.arguments_.size());
      for (auto *argument : function.arguments_) {
        arguments.emplace_back(argument->Accept(*this));
      }
      return function.function_(arguments.data(), arguments.size(), function_ctx);
    }();
    if (can_cache) {
      ctx_->function_cache.emplace(function, result);
    }

    MG_ASSERT(result.get_allocator().resource() == ctx_->memory);
    if (!is_transactional && result.ContainsDeleted()) [[unlikely]] {
      return TypedValue(ctx_->memory);
    }
    return result;
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
    for (auto &element : list) {
      if (element.IsNull()) {
        result.emplace_back();
      } else {
        frame_->at(element_symbol) = std::move(element);
        result.emplace_back(extract.expression_->Accept(*this));
      }
    }
    return TypedValue(std::move(result), ctx_->memory);
  }

  TypedValue Visit(ListComprehension &list_comprehension) override {
    auto list_value = list_comprehension.list_->Accept(*this);
    if (list_value.IsNull()) {
      return TypedValue(ctx_->memory);
    }

    if (list_value.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("List comprehension expected a list, got {}.", list_value.type());
    }

    const auto &list = list_value.ValueList();
    const auto &element_symbol = symbol_table_->at(*list_comprehension.identifier_);
    const bool needs_predicate = !!list_comprehension.where_;
    const bool has_transformation = !!list_comprehension.expression_;
    TypedValue::TVector result(ctx_->memory);
    result.reserve(list.size());

    for (const auto &element : list) {
      frame_->at(element_symbol) = element;
      if (!needs_predicate) {
        if (has_transformation) {
          result.emplace_back(list_comprehension.expression_->Accept(*this));
        } else {
          result.emplace_back(element);
        }
        continue;
      }

      auto predicate_result = list_comprehension.where_->expression_->Accept(*this);
      if (!predicate_result.IsBool()) {
        return TypedValue(ctx_->memory);
      }
      if (predicate_result.ValueBool()) {
        if (has_transformation) {
          result.emplace_back(list_comprehension.expression_->Accept(*this));
        } else {
          result.emplace_back(element);
        }
      }
    }

    return TypedValue(std::move(result), ctx_->memory);
  }

  TypedValue Visit(Exists &exists) override {
    TypedValue &frame_exists_value = frame_->at(symbol_table_->at(exists));
    if (!frame_exists_value.IsFunction()) [[unlikely]] {
      throw QueryRuntimeException(
          "Unexpected behavior: Exists expected a function, got {}. Please report the problem on GitHub issues",
          frame_exists_value.type());
    }
    TypedValue result(ctx_->memory);
    frame_exists_value.ValueFunction()(&result);
    return result;
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
        if (!result.ValueBool()) {
          return TypedValue(false, ctx_->memory);
        }
        has_value = true;
      } else {
        has_null_elements = true;
      }
    }
    if (!list.empty() && !has_value) {
      return TypedValue(ctx_->memory);
    } else if (has_null_elements) {
      return TypedValue(ctx_->memory);
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
    bool has_null_elements = false;
    for (const auto &element : list) {
      frame_->at(symbol) = element;
      auto result = single.where_->expression_->Accept(*this);
      if (!result.IsNull() && result.type() != TypedValue::Type::Bool) {
        throw QueryRuntimeException("Predicate of SINGLE must evaluate to boolean, got {}.", result.type());
      }
      if (result.type() == TypedValue::Type::Bool) {
        has_value = true;
      }
      if (result.IsNull()) {
        has_null_elements = true;
        continue;
      }
      if (!result.ValueBool()) {
        continue;
      }
      // Return false if more than one element satisfies the predicate.
      if (predicate_satisfied) {
        return TypedValue(false, ctx_->memory);
      } else {
        predicate_satisfied = true;
      }
    }
    if (!list.empty() && !has_value) {
      return TypedValue(ctx_->memory);
    } else if (has_null_elements && !predicate_satisfied) {
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
    bool has_null_elements = false;
    bool has_value = false;
    for (const auto &element : list) {
      frame_->at(symbol) = element;
      auto result = any.where_->expression_->Accept(*this);
      if (!result.IsNull() && result.type() != TypedValue::Type::Bool) {
        throw QueryRuntimeException("Predicate of ANY must evaluate to boolean, got {}.", result.type());
      }
      if (!result.IsNull()) {
        if (result.ValueBool()) {
          return TypedValue(true, ctx_->memory);
        }
        has_value = true;
      } else {
        has_null_elements = true;
      }
    }
    // Return Null if all elements are Null
    if (!list.empty() && !has_value) {
      return TypedValue(ctx_->memory);
    } else if (has_null_elements) {
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
    bool has_null_elements = false;
    bool has_value = false;
    for (const auto &element : list) {
      frame_->at(symbol) = element;
      auto result = none.where_->expression_->Accept(*this);
      if (!result.IsNull() && result.type() != TypedValue::Type::Bool) {
        throw QueryRuntimeException("Predicate of NONE must evaluate to boolean, got {}.", result.type());
      }
      if (!result.IsNull()) {
        if (result.ValueBool()) {
          return TypedValue(false, ctx_->memory);
        }
        has_value = true;
      } else {
        has_null_elements = true;
      }
    }
    // Return Null if all elements are Null
    if (!list.empty() && !has_value) {
      return TypedValue(ctx_->memory);
    } else if (has_null_elements) {
      return TypedValue(ctx_->memory);
    } else {
      return TypedValue(true, ctx_->memory);
    }
  }

  TypedValue Visit(ParameterLookup &param_lookup) override {
    return TypedValue(ctx_->parameters.AtTokenPosition(param_lookup.token_position_), ctx_->memory);
  }

  TypedValue Visit(RegexMatch &regex_match) override;

  TypedValue Visit(PatternComprehension &pattern_comprehension) override {
    const TypedValue &frame_pattern_comprehension_value = frame_->at(symbol_table_->at(pattern_comprehension));
    if (!frame_pattern_comprehension_value.IsList()) [[unlikely]] {
      throw QueryRuntimeException(
          "Unexpected behavior: Pattern Comprehension expected a list, got {}. Please report the problem on GitHub "
          "issues",
          frame_pattern_comprehension_value.type());
    }
    return frame_pattern_comprehension_value;
  }

  TypedValue Visit(EnumValueAccess &enum_value_access) override {
    auto maybe_enum = dba_->GetEnumValue(enum_value_access.enum_name_, enum_value_access.enum_value_);
    if (maybe_enum.HasError()) [[unlikely]] {
      throw QueryRuntimeException("Enum value '{}' in enum '{}' not found.", enum_value_access.enum_value_,
                                  enum_value_access.enum_name_);
    }
    return TypedValue(*maybe_enum, ctx_->memory);
  }

  template <class TRecordAccessor>
  storage::PropertyValue GetProperty(const TRecordAccessor &record_accessor, const PropertyIx &prop) {
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
    return *std::move(maybe_prop);
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

 private:
  template <class TRecordAccessor>
  std::map<storage::PropertyId, storage::PropertyValue> GetAllProperties(const TRecordAccessor &record_accessor) {
    auto maybe_props = record_accessor.Properties(view_);
    if (maybe_props.HasError() && maybe_props.GetError() == storage::Error::NONEXISTENT_OBJECT) {
      // This is a very nasty and temporary hack in order to make MERGE work.
      // The old storage had the following logic when returning an `OLD` view:
      // `return old ? old : new`. That means that if the `OLD` view didn't
      // exist, it returned the NEW view. With this hack we simulate that
      // behavior.
      // TODO (mferencevic, teon.banek): Remove once MERGE is reimplemented.
      maybe_props = record_accessor.Properties(storage::View::NEW);
    }
    if (maybe_props.HasError()) {
      switch (maybe_props.GetError()) {
        case storage::Error::DELETED_OBJECT:
          throw QueryRuntimeException("Trying to get properties from a deleted object.");
        case storage::Error::NONEXISTENT_OBJECT:
          throw query::QueryRuntimeException("Trying to get properties from an object that doesn't exist.");
        case storage::Error::SERIALIZATION_ERROR:
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::PROPERTIES_DISABLED:
          throw QueryRuntimeException("Unexpected error when getting properties.");
      }
    }
    return *std::move(maybe_props);
  }

  storage::LabelId GetLabel(const LabelIx &label) { return ctx_->labels[label.ix]; }

  Frame *frame_;
  const SymbolTable *symbol_table_;
  const EvaluationContext *ctx_;
  DbAccessor *dba_;
  // which switching approach should be used when evaluating
  storage::View view_;
  FrameChangeCollector *frame_change_collector_;
  /// Property lookup cache ({symbol: {property_id: property_value, ...}, ...})
  mutable std::unordered_map<int32_t, std::map<storage::PropertyId, storage::PropertyValue>> property_lookup_cache_{};
  // use the getter function GetHopsCounter() to handle possible error for segfault
  const int64_t *hops_counter_;
};  // namespace memgraph::query

/// A helper function for evaluating an expression that's an int.
///
/// @param what - Name of what's getting evaluated. Used for user feedback (via
///               exception) when the evaluated value is not an int.
/// @throw QueryRuntimeException if expression doesn't evaluate to an int.
int64_t EvaluateInt(ExpressionVisitor<TypedValue> &eval, Expression *expr, std::string_view what);

/// A helper function for evaluating an expression that's an uint.
///
/// @param what - Name of what's getting evaluated. Used for user feedback (via
///               exception) when the evaluated value is not an uint.
/// @throw QueryRuntimeException if expression doesn't evaluate to an uint.
std::optional<int64_t> EvaluateUint(ExpressionVisitor<TypedValue> &eval, Expression *expr, std::string_view what);

std::optional<int64_t> EvaluateHopsLimit(ExpressionVisitor<TypedValue> &eval, Expression *expr);
std::optional<int64_t> EvaluateCommitFrequency(ExpressionVisitor<TypedValue> &eval, Expression *expr);
std::optional<int64_t> EvaluateDeleteBufferSize(ExpressionVisitor<TypedValue> &eval, Expression *expr);

std::optional<size_t> EvaluateMemoryLimit(ExpressionVisitor<TypedValue> &eval, Expression *memory_limit,
                                          size_t memory_scale);
}  // namespace memgraph::query

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

#include "query/plan/rewrite/range.hpp"

#include "query/frontend/ast/ast.hpp"
#include "query/plan/preprocess.hpp"
#include "query/plan/rewrite/general.hpp"

namespace memgraph::query::plan {

namespace {
struct ComparisonFilterInfo {
  enum class Type { UNKNOWN, LESS, GREATER } type{ComparisonFilterInfo::Type::UNKNOWN};
  PropertyLookup *prop_lookup{};
  Identifier *ident{};
  Expression *rhs{};

  bool Compatible(const ComparisonFilterInfo &rhs) const {
    if (*this && rhs && type != rhs.type) {
      return prop_lookup->GetBaseProperty() == rhs.prop_lookup->GetBaseProperty() &&
             ident->symbol_pos_ == rhs.ident->symbol_pos_;
    }
    return false;
  }

  explicit operator bool() const { return type != Type::UNKNOWN; }
};

/**
 * @brief Helper function that takes in a filter expression and normalizes it and generates comparison components.
 *
 * @param filter BinaryOperator that is used to filter
 * @param prop_lookup out
 * @param ident out
 * @param rhs out
 * @return utils::TypeInfo normalized operator type
 */
utils::TypeInfo PropertyComparisonType(Expression *filter, PropertyLookup *&prop_lookup, Identifier *&ident,
                                       Expression *&rhs) {
  auto get_prop_filter = [](BinaryOperator *op, PropertyLookup *&prop_lookup, Identifier *&ident, Expression *&rhs) {
    auto get_property_lookup = [](auto *maybe_lookup, PropertyLookup *&prop_lookup, Identifier *&ident) {
      return (prop_lookup = utils::Downcast<PropertyLookup>(maybe_lookup)) &&
             (ident = utils::Downcast<Identifier>(prop_lookup->expression_));
    };

    /* n.prop > value */
    if (get_property_lookup(op->expression1_, prop_lookup, ident)) {
      rhs = op->expression2_;
      return 1;
    }
    /* value > n.prop */
    if (get_property_lookup(op->expression2_, prop_lookup, ident)) {
      rhs = op->expression1_;
      return -1;  // invert operator
    }
    return 0;  // not filtering a property
  };

  if (auto *gt = utils::Downcast<GreaterOperator>(filter)) {
    if (const auto order = get_prop_filter(gt, prop_lookup, ident, rhs)) {
      return order == 1 ? GreaterOperator::kType : LessOperator::kType;
    }
  } else if (auto *ge = utils::Downcast<GreaterEqualOperator>(filter)) {
    if (const auto order = get_prop_filter(ge, prop_lookup, ident, rhs)) {
      return order == 1 ? GreaterEqualOperator::kType : LessEqualOperator::kType;
    }
  } else if (auto *lt = utils::Downcast<LessOperator>(filter)) {
    if (const auto order = get_prop_filter(lt, prop_lookup, ident, rhs)) {
      return order == 1 ? LessOperator::kType : GreaterOperator::kType;
    }
  } else if (auto *le = utils::Downcast<LessEqualOperator>(filter)) {
    if (const auto order = get_prop_filter(le, prop_lookup, ident, rhs)) {
      return order == 1 ? LessEqualOperator::kType : GreaterEqualOperator::kType;
    }
  }

  return Expression::kType;
}
}  // namespace

Expression *CompactFilters(Expression *filter_expr, AstStorage &storage) {
  auto can_range = [&](const auto &filter) -> ComparisonFilterInfo {
    ComparisonFilterInfo info;

    switch (PropertyComparisonType(filter, info.prop_lookup, info.ident, info.rhs).id) {
      case utils::TypeId::AST_GREATER_OPERATOR:
      case utils::TypeId::AST_GREATER_EQUAL_OPERATOR:
        info.type = ComparisonFilterInfo::Type::GREATER;
        break;
      case utils::TypeId::AST_LESS_OPERATOR:
      case utils::TypeId::AST_LESS_EQUAL_OPERATOR:
        info.type = ComparisonFilterInfo::Type::LESS;
        break;
      default:
        info.type = ComparisonFilterInfo::Type::UNKNOWN;
        break;
    }

    return info;
  };

  auto filters = SplitExpression(filter_expr);

  std::vector<ComparisonFilterInfo> infos;
  infos.reserve(filters.size());
  std::for_each(filters.begin(), filters.end(), [&](const auto &in) { infos.push_back(can_range(in)); });

  for (size_t i = 1; i < filters.size(); ++i) {
    const auto &filter = filters[i];
    auto &info = infos[i];

    // Filter not supported
    if (!info) continue;

    // Find if an already present filter can be combined with the incoming one
    for (size_t j = 0; j < i; ++j) {
      const auto &filter_old = filters[j];
      auto &info_old = infos[j];

      if (info.Compatible(info_old)) {
        // Combine and switch to range
        auto *range = storage.Create<RangeOperator>();
        range->expr1_ = filter;
        range->expr2_ = filter_old;
        // Substitute the current filter
        filter_expr = SubstituteExpression(filter_expr, filter_old, range);
        // Remove the other filter
        filter_expr = RemoveExpressions(filter_expr, {filter}).trimmed_expression;
        // Remove these filters from any further consideration
        info.type = ComparisonFilterInfo::Type::UNKNOWN;
        info_old.type = ComparisonFilterInfo::Type::UNKNOWN;
        break;
      }
    }
  }

  return filter_expr;
}

FilterInfo RangeOpToFilter(RangeOperator *range, const SymbolTable &symbol_table) {
  PropertyLookup *prop_lookup{};
  Identifier *ident{};

  auto update_bounds = [&](Expression *comparison, utils::Bound<Expression *> &lower_bound,
                           utils::Bound<Expression *> &upper_bound) {
    Expression *rhs{};

    switch (PropertyComparisonType(comparison, prop_lookup, ident, rhs).id) {
      // n.prop > val
      case utils::TypeId::AST_GREATER_OPERATOR:
        lower_bound = utils::Bound<Expression *>{rhs, utils::BoundType::EXCLUSIVE};
        break;
      // n.prop >= val
      case utils::TypeId::AST_GREATER_EQUAL_OPERATOR:
        lower_bound = utils::Bound<Expression *>{rhs, utils::BoundType::INCLUSIVE};
        break;
      // n.prop < val
      case utils::TypeId::AST_LESS_OPERATOR:
        upper_bound = utils::Bound<Expression *>{rhs, utils::BoundType::EXCLUSIVE};
        break;
      // n.prop <= val
      case utils::TypeId::AST_LESS_EQUAL_OPERATOR:
        upper_bound = utils::Bound<Expression *>{rhs, utils::BoundType::INCLUSIVE};
        break;
      default:
        // Unsupported type
        throw SemanticException("Unsupported range operator.");
    }
  };

  utils::Bound<Expression *> lower_bound{nullptr, utils::BoundType::INCLUSIVE};
  utils::Bound<Expression *> upper_bound{nullptr, utils::BoundType::INCLUSIVE};

  update_bounds(range->expr1_, lower_bound, upper_bound);
  update_bounds(range->expr2_, lower_bound, upper_bound);

  DMG_ASSERT(lower_bound.value() != nullptr && upper_bound.value() != nullptr,
             "Range requires both a lower and upper bound");

  UsedSymbolsCollector collector(symbol_table);
  range->Accept(collector);
  auto filter = FilterInfo{FilterInfo::Type::Property, range, collector.symbols_};
  filter.property_filter.emplace(symbol_table, symbol_table.at(*ident), prop_lookup->GetBaseProperty(), lower_bound,
                                 upper_bound);
  return filter;
}

}  // namespace memgraph::query::plan

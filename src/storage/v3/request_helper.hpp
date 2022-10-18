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

#include <type_traits>
#include <vector>

#include "ast/ast.hpp"
#include "pretty_print_ast_to_original_expression.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/bindings/typed_value.hpp"
#include "storage/v3/edge_accessor.hpp"
#include "storage/v3/expr.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/vertex_accessor.hpp"

namespace memgraph::storage::v3 {

template <typename T>
concept ObjectAccessor = std::is_same_v<T, VertexAccessor> || std::is_same_v<T, EdgeAccessor>;

inline bool TypedValueCompare(const TypedValue &a, const TypedValue &b) {
  // in ordering null comes after everything else
  // at the same time Null is not less that null
  // first deal with Null < Whatever case
  if (a.IsNull()) return false;
  // now deal with NotNull < Null case
  if (b.IsNull()) return true;

  // comparisons are from this point legal only between values of
  // the  same type, or int+float combinations
  if ((a.type() != b.type() && !(a.IsNumeric() && b.IsNumeric())))
    throw utils::BasicException("Can't compare value of type {} to value of type {}.", a.type(), b.type());

  switch (a.type()) {
    case TypedValue::Type::Bool:
      return !a.ValueBool() && b.ValueBool();
    case TypedValue::Type::Int:
      if (b.type() == TypedValue::Type::Double)
        // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
        return a.ValueInt() < b.ValueDouble();
      else
        return a.ValueInt() < b.ValueInt();
    case TypedValue::Type::Double:
      if (b.type() == TypedValue::Type::Int)
        // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
        return a.ValueDouble() < b.ValueInt();
      else
        return a.ValueDouble() < b.ValueDouble();
    case TypedValue::Type::String:
      // NOLINTNEXTLINE(modernize-use-nullptr)
      return a.ValueString() < b.ValueString();
    case TypedValue::Type::Date:
      // NOLINTNEXTLINE(modernize-use-nullptr)
      return a.ValueDate() < b.ValueDate();
    case TypedValue::Type::LocalTime:
      // NOLINTNEXTLINE(modernize-use-nullptr)
      return a.ValueLocalTime() < b.ValueLocalTime();
    case TypedValue::Type::LocalDateTime:
      // NOLINTNEXTLINE(modernize-use-nullptr)
      return a.ValueLocalDateTime() < b.ValueLocalDateTime();
    case TypedValue::Type::Duration:
      // NOLINTNEXTLINE(modernize-use-nullptr)
      return a.ValueDuration() < b.ValueDuration();
    case TypedValue::Type::List:
    case TypedValue::Type::Map:
    case TypedValue::Type::Vertex:
    case TypedValue::Type::Edge:
    case TypedValue::Type::Path:
      throw utils::BasicException("Comparison is not defined for values of type {}.", a.type());
    case TypedValue::Type::Null:
      LOG_FATAL("Invalid type");
  }
}

class TypedValueVectorCompare final {
 public:
  explicit TypedValueVectorCompare(const std::vector<Ordering> &ordering) : ordering_(ordering) {}

  bool operator()(const std::vector<TypedValue> &c1, const std::vector<TypedValue> &c2) const {
    // ordering is invalid if there are more elements in the collections
    // then there are in the ordering_ vector
    MG_ASSERT(c1.size() <= ordering_.size() && c2.size() <= ordering_.size(),
              "Collections contain more elements then there are orderings");

    auto c1_it = c1.begin();
    auto c2_it = c2.begin();
    auto ordering_it = ordering_.begin();
    for (; c1_it != c1.end() && c2_it != c2.end(); c1_it++, c2_it++, ordering_it++) {
      if (TypedValueCompare(*c1_it, *c2_it)) return *ordering_it == Ordering::ASC;
      if (TypedValueCompare(*c2_it, *c1_it)) return *ordering_it == Ordering::DESC;
    }

    // at least one collection is exhausted
    // c1 is less then c2 iff c1 reached the end but c2 didn't
    return (c1_it == c1.end()) && (c2_it != c2.end());
  }

 private:
  std::vector<Ordering> ordering_;
};

template <ObjectAccessor TObjectAccessor>
struct Element {
  std::vector<TypedValue> properties_order_by;
  TObjectAccessor object_acc;
};

template <ObjectAccessor TObjectAccessor, typename TIterable>
std::vector<Element<TObjectAccessor>> OrderByElements(Shard::Accessor &acc, DbAccessor &dba, TIterable &iterable,
                                                      std::vector<msgs::OrderBy> &order_bys) {
  std::vector<Element<TObjectAccessor>> ordered;
  ordered.reserve(acc.ApproximateVertexCount());
  std::vector<Ordering> ordering;
  ordering.reserve(order_bys.size());
  for (const auto &order : order_bys) {
    switch (order.direction) {
      case memgraph::msgs::OrderingDirection::ASCENDING: {
        ordering.push_back(Ordering::ASC);
        break;
      }
      case memgraph::msgs::OrderingDirection::DESCENDING: {
        ordering.push_back(Ordering::DESC);
        break;
      }
    }
  }
  auto compare_typed_values = TypedValueVectorCompare(ordering);
  auto it = iterable.begin();
  for (; it != iterable.end(); ++it) {
    std::vector<TypedValue> properties_order_by;
    properties_order_by.reserve(order_bys.size());

    for (const auto &order_by : order_bys) {
      if constexpr (std::is_same_v<TIterable, VerticesIterable>) {
        properties_order_by.push_back(ComputeExpression(dba, *it, std::nullopt, order_by.expression.expression,
                                                        expr::identifier_node_symbol, expr::identifier_edge_symbol));
      } else {
        properties_order_by.push_back(ComputeExpression(dba, std::nullopt, *it, order_by.expression.expression,
                                                        expr::identifier_node_symbol, expr::identifier_edge_symbol));
      }
    }
    ordered.push_back({std::move(properties_order_by), *it});
  }

  std::sort(ordered.begin(), ordered.end(), [compare_typed_values](const auto &pair1, const auto &pair2) {
    return compare_typed_values(pair1.properties_order_by, pair2.properties_order_by);
  });
  return ordered;
}

VerticesIterable::Iterator GetStartVertexIterator(VerticesIterable &vertex_iterable,
                                                  const std::vector<PropertyValue> &start_ids, View view);

std::vector<Element<VertexAccessor>>::const_iterator GetStartOrderedElementsIterator(
    const std::vector<Element<VertexAccessor>> &ordered_elements, const std::vector<PropertyValue> &start_ids,
    View view);

std::vector<EdgeAccessor> GetEdgesFromVertex(const VertexAccessor &vertex_accessor, msgs::EdgeDirection direction);

}  // namespace memgraph::storage::v3

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

#pragma once

#include <vector>

#include "query/v2/requests.hpp"
#include "storage/v3/bindings/ast/ast.hpp"
#include "storage/v3/bindings/pretty_print_ast_to_original_expression.hpp"
#include "storage/v3/bindings/typed_value.hpp"
#include "storage/v3/edge_accessor.hpp"
#include "storage/v3/expr.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/value_conversions.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "utils/template_utils.hpp"

namespace memgraph::storage::v3 {
using EdgeAccessors = std::vector<storage::v3::EdgeAccessor>;
using EdgeUniquenessFunction = std::function<EdgeAccessors(EdgeAccessors &&, msgs::EdgeDirection)>;
using EdgeFiller =
    std::function<ShardResult<void>(const EdgeAccessor &edge, bool is_in_edge, msgs::ExpandOneResultRow &result_row)>;
using msgs::Value;

template <typename T>
concept OrderableObject = utils::SameAsAnyOf<T, VertexAccessor, EdgeAccessor, std::pair<VertexAccessor, EdgeAccessor>>;

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

inline Ordering ConvertMsgsOrderByToOrdering(msgs::OrderingDirection ordering) {
  switch (ordering) {
    case memgraph::msgs::OrderingDirection::ASCENDING:
      return memgraph::storage::v3::Ordering::ASC;
    case memgraph::msgs::OrderingDirection::DESCENDING:
      return memgraph::storage::v3::Ordering::DESC;
    default:
      LOG_FATAL("Unknown ordering direction");
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

template <OrderableObject TObjectAccessor>
struct Element {
  std::vector<TypedValue> properties_order_by;
  TObjectAccessor object_acc;
};

template <typename T>
concept VerticesIt = utils::SameAsAnyOf<T, VerticesIterable, std::vector<VertexAccessor>>;

template <VerticesIt TIterable>
std::vector<Element<VertexAccessor>> OrderByVertices(DbAccessor &dba, TIterable &iterable,
                                                     std::vector<msgs::OrderBy> &order_by_vertices) {
  std::vector<Ordering> ordering;
  ordering.reserve(order_by_vertices.size());
  std::transform(order_by_vertices.begin(), order_by_vertices.end(), std::back_inserter(ordering),
                 [](const auto &order_by) { return ConvertMsgsOrderByToOrdering(order_by.direction); });

  std::vector<Element<VertexAccessor>> ordered;
  for (auto it = iterable.begin(); it != iterable.end(); ++it) {
    std::vector<TypedValue> properties_order_by;
    properties_order_by.reserve(order_by_vertices.size());

    std::transform(order_by_vertices.begin(), order_by_vertices.end(), std::back_inserter(properties_order_by),
                   [&dba, &it](const auto &order_by) {
                     return ComputeExpression(dba, *it, std::nullopt /*e_acc*/, order_by.expression.expression,
                                              expr::identifier_node_symbol, expr::identifier_edge_symbol);
                   });

    ordered.push_back({std::move(properties_order_by), *it});
  }

  auto compare_typed_values = TypedValueVectorCompare(ordering);
  std::sort(ordered.begin(), ordered.end(), [compare_typed_values](const auto &pair1, const auto &pair2) {
    return compare_typed_values(pair1.properties_order_by, pair2.properties_order_by);
  });
  return ordered;
}

std::vector<Element<EdgeAccessor>> OrderByEdges(DbAccessor &dba, std::vector<EdgeAccessor> &iterable,
                                                std::vector<msgs::OrderBy> &order_by_edges,
                                                const VertexAccessor &vertex_acc);

std::vector<Element<std::pair<VertexAccessor, EdgeAccessor>>> OrderByEdges(
    DbAccessor &dba, std::vector<EdgeAccessor> &iterable, std::vector<msgs::OrderBy> &order_by_edges,
    const std::vector<VertexAccessor> &vertex_acc);

VerticesIterable::Iterator GetStartVertexIterator(VerticesIterable &vertex_iterable,
                                                  const std::vector<PropertyValue> &primary_key, View view);

std::vector<Element<VertexAccessor>>::const_iterator GetStartOrderedElementsIterator(
    const std::vector<Element<VertexAccessor>> &ordered_elements, const std::vector<PropertyValue> &primary_key,
    View view);

std::array<std::vector<EdgeAccessor>, 2> GetEdgesFromVertex(const VertexAccessor &vertex_accessor,
                                                            msgs::EdgeDirection direction);

bool FilterOnVertex(DbAccessor &dba, const storage::v3::VertexAccessor &v_acc, const std::vector<std::string> &filters);

bool FilterOnEdge(DbAccessor &dba, const storage::v3::VertexAccessor &v_acc, const EdgeAccessor &e_acc,
                  const std::vector<std::string> &filters);

std::vector<TypedValue> EvaluateVertexExpressions(DbAccessor &dba, const VertexAccessor &v_acc,
                                                  const std::vector<std::string> &expressions,
                                                  std::string_view node_name);

std::vector<TypedValue> EvaluateEdgeExpressions(DbAccessor &dba, const VertexAccessor &v_acc, const EdgeAccessor &e_acc,
                                                const std::vector<std::string> &expressions);

template <typename T>
concept PropertiesAccessor = utils::SameAsAnyOf<T, VertexAccessor, EdgeAccessor>;

template <PropertiesAccessor TAccessor>
ShardResult<std::map<PropertyId, Value>> CollectSpecificPropertiesFromAccessor(const TAccessor &acc,
                                                                               const std::vector<PropertyId> &props,
                                                                               View view) {
  std::map<PropertyId, Value> ret;

  for (const auto &prop : props) {
    auto result = acc.GetProperty(prop, view);
    if (result.HasError()) {
      spdlog::debug("Encountered an Error while trying to get a vertex property.");
      return result.GetError();
    }
    auto &value = result.GetValue();
    ret.emplace(std::make_pair(prop, FromPropertyValueToValue(std::move(value))));
  }

  return ret;
}

ShardResult<std::map<PropertyId, Value>> CollectAllPropertiesFromAccessor(const VertexAccessor &acc, View view,
                                                                          const Schema &schema);
namespace impl {
template <PropertiesAccessor TAccessor>
ShardResult<std::map<PropertyId, Value>> CollectAllPropertiesImpl(const TAccessor &acc, View view) {
  std::map<PropertyId, Value> ret;
  auto props = acc.Properties(view);
  if (props.HasError()) {
    spdlog::debug("Encountered an error while trying to get vertex properties.");
    return props.GetError();
  }

  auto &properties = props.GetValue();
  std::transform(properties.begin(), properties.end(), std::inserter(ret, ret.begin()),
                 [](std::pair<const PropertyId, PropertyValue> &pair) {
                   return std::make_pair(pair.first, conversions::FromPropertyValueToValue(std::move(pair.second)));
                 });
  return ret;
}
}  // namespace impl

template <PropertiesAccessor TAccessor>
ShardResult<std::map<PropertyId, Value>> CollectAllPropertiesFromAccessor(const TAccessor &acc, View view) {
  return impl::CollectAllPropertiesImpl<TAccessor>(acc, view);
}

EdgeUniquenessFunction InitializeEdgeUniquenessFunction(bool only_unique_neighbor_rows);

EdgeFiller InitializeEdgeFillerFunction(const msgs::ExpandOneRequest &req);

ShardResult<msgs::ExpandOneResultRow> GetExpandOneResult(
    Shard::Accessor &acc, msgs::VertexId src_vertex, const msgs::ExpandOneRequest &req,
    const EdgeUniquenessFunction &maybe_filter_based_on_edge_uniqueness, const EdgeFiller &edge_filler,
    const Schema &schema);

ShardResult<msgs::ExpandOneResultRow> GetExpandOneResult(
    VertexAccessor v_acc, msgs::VertexId src_vertex, const msgs::ExpandOneRequest &req,
    std::vector<EdgeAccessor> in_edge_accessors, std::vector<EdgeAccessor> out_edge_accessors,
    const EdgeUniquenessFunction &maybe_filter_based_on_edge_uniqueness, const EdgeFiller &edge_filler,
    const Schema &schema);
}  // namespace memgraph::storage::v3

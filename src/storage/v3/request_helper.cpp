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

#include "storage/v3/request_helper.hpp"

#include <iterator>
#include <vector>

#include "pretty_print_ast_to_original_expression.hpp"
#include "storage/v3/bindings/db_accessor.hpp"
#include "storage/v3/bindings/pretty_print_ast_to_original_expression.hpp"
#include "storage/v3/expr.hpp"
#include "storage/v3/result.hpp"
#include "storage/v3/value_conversions.hpp"

namespace memgraph::storage::v3 {
using msgs::Label;
using msgs::PropertyId;

using conversions::ConvertPropertyVector;
using conversions::FromPropertyValueToValue;
using conversions::ToMsgsVertexId;

namespace {

using AllEdgePropertyDataStucture = std::map<PropertyId, msgs::Value>;
using SpecificEdgePropertyDataStucture = std::vector<msgs::Value>;

using AllEdgeProperties = std::tuple<msgs::VertexId, msgs::Gid, AllEdgePropertyDataStucture>;
using SpecificEdgeProperties = std::tuple<msgs::VertexId, msgs::Gid, SpecificEdgePropertyDataStucture>;

using SpecificEdgePropertiesVector = std::vector<SpecificEdgeProperties>;
using AllEdgePropertiesVector = std::vector<AllEdgeProperties>;

struct VertexIdCmpr {
  bool operator()(const storage::v3::VertexId *lhs, const storage::v3::VertexId *rhs) const { return *lhs < *rhs; }
};

std::optional<std::map<PropertyId, Value>> PrimaryKeysFromAccessor(const VertexAccessor &acc, View view,
                                                                   const Schemas::Schema &schema) {
  std::map<PropertyId, Value> ret;
  auto props = acc.Properties(view);
  auto maybe_pk = acc.PrimaryKey(view);
  if (maybe_pk.HasError()) {
    spdlog::debug("Encountered an error while trying to get vertex primary key.");
    return std::nullopt;
  }
  auto &pk = maybe_pk.GetValue();
  MG_ASSERT(schema.second.size() == pk.size(), "PrimaryKey size does not match schema!");
  for (size_t i{0}; i < schema.second.size(); ++i) {
    ret.emplace(schema.second[i].property_id, FromPropertyValueToValue(std::move(pk[i])));
  }

  return ret;
}

ShardResult<std::vector<msgs::Label>> FillUpSourceVertexSecondaryLabels(const std::optional<VertexAccessor> &v_acc,
                                                                        const msgs::ExpandOneRequest &req) {
  auto secondary_labels = v_acc->Labels(View::NEW);
  if (secondary_labels.HasError()) {
    spdlog::debug("Encountered an error while trying to get the secondary labels of a vertex. Transaction id: {}",
                  req.transaction_id.logical_id);
    return secondary_labels.GetError();
  }

  auto &sec_labels = secondary_labels.GetValue();
  std::vector<msgs::Label> msgs_secondary_labels;
  msgs_secondary_labels.reserve(sec_labels.size());

  std::transform(sec_labels.begin(), sec_labels.end(), std::back_inserter(msgs_secondary_labels),
                 [](auto label_id) { return msgs::Label{.id = label_id}; });

  return msgs_secondary_labels;
}

ShardResult<std::map<PropertyId, Value>> FillUpSourceVertexProperties(const std::optional<VertexAccessor> &v_acc,
                                                                      const msgs::ExpandOneRequest &req,
                                                                      storage::v3::View view,
                                                                      const Schemas::Schema &schema) {
  std::map<PropertyId, Value> src_vertex_properties;

  if (!req.src_vertex_properties) {
    auto props = v_acc->Properties(View::NEW);
    if (props.HasError()) {
      spdlog::debug("Encountered an error while trying to access vertex properties. Transaction id: {}",
                    req.transaction_id.logical_id);
      return props.GetError();
    }

    for (auto &[key, val] : props.GetValue()) {
      src_vertex_properties.insert(std::make_pair(key, FromPropertyValueToValue(std::move(val))));
    }
    auto pks = PrimaryKeysFromAccessor(*v_acc, view, schema);
    if (pks) {
      src_vertex_properties.merge(*pks);
    }

  } else if (req.src_vertex_properties.value().empty()) {
    // NOOP
  } else {
    for (const auto &prop : req.src_vertex_properties.value()) {
      auto prop_val = v_acc->GetProperty(prop, View::OLD);
      if (prop_val.HasError()) {
        spdlog::debug("Encountered an error while trying to access vertex properties. Transaction id: {}",
                      req.transaction_id.logical_id);
        return prop_val.GetError();
      }
      src_vertex_properties.insert(std::make_pair(prop, FromPropertyValueToValue(std::move(prop_val.GetValue()))));
    }
  }

  return src_vertex_properties;
}

ShardResult<std::array<std::vector<EdgeAccessor>, 2>> FillUpConnectingEdges(
    const std::optional<VertexAccessor> &v_acc, const msgs::ExpandOneRequest &req,
    const EdgeUniquenessFunction &maybe_filter_based_on_edge_uniqueness) {
  std::vector<EdgeTypeId> edge_types{};
  edge_types.reserve(req.edge_types.size());
  std::transform(req.edge_types.begin(), req.edge_types.end(), std::back_inserter(edge_types),
                 [](const msgs::EdgeType &edge_type) { return edge_type.id; });

  std::vector<EdgeAccessor> in_edges;
  std::vector<EdgeAccessor> out_edges;

  switch (req.direction) {
    case msgs::EdgeDirection::OUT: {
      auto out_edges_result = v_acc->OutEdges(View::NEW, edge_types);
      if (out_edges_result.HasError()) {
        spdlog::debug("Encountered an error while trying to get out-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return out_edges_result.GetError();
      }
      out_edges =
          maybe_filter_based_on_edge_uniqueness(std::move(out_edges_result.GetValue()), msgs::EdgeDirection::OUT);
      break;
    }
    case msgs::EdgeDirection::IN: {
      auto in_edges_result = v_acc->InEdges(View::NEW, edge_types);
      if (in_edges_result.HasError()) {
        spdlog::debug(
            "Encountered an error while trying to get in-going EdgeAccessors. Transaction id: {}"[req.transaction_id
                                                                                                      .logical_id]);
        return in_edges_result.GetError();
      }
      in_edges = maybe_filter_based_on_edge_uniqueness(std::move(in_edges_result.GetValue()), msgs::EdgeDirection::IN);
      break;
    }
    case msgs::EdgeDirection::BOTH: {
      auto in_edges_result = v_acc->InEdges(View::NEW, edge_types);
      if (in_edges_result.HasError()) {
        spdlog::debug("Encountered an error while trying to get in-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return in_edges_result.GetError();
      }
      in_edges = maybe_filter_based_on_edge_uniqueness(std::move(in_edges_result.GetValue()), msgs::EdgeDirection::IN);
      auto out_edges_result = v_acc->OutEdges(View::NEW, edge_types);
      if (out_edges_result.HasError()) {
        spdlog::debug("Encountered an error while trying to get out-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return out_edges_result.GetError();
      }
      out_edges =
          maybe_filter_based_on_edge_uniqueness(std::move(out_edges_result.GetValue()), msgs::EdgeDirection::OUT);
      break;
    }
  }
  return std::array<std::vector<EdgeAccessor>, 2>{std::move(in_edges), std::move(out_edges)};
}

template <bool are_in_edges>
ShardResult<void> FillEdges(const std::vector<EdgeAccessor> &edges, msgs::ExpandOneResultRow &row,
                            const EdgeFiller &edge_filler) {
  for (const auto &edge : edges) {
    if (const auto res = edge_filler(edge, are_in_edges, row); res.HasError()) {
      return res.GetError();
    }
  }

  return {};
}

};  // namespace

ShardResult<std::map<PropertyId, Value>> CollectSpecificPropertiesFromAccessor(const VertexAccessor &acc,
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

std::vector<TypedValue> EvaluateVertexExpressions(DbAccessor &dba, const VertexAccessor &v_acc,
                                                  const std::vector<std::string> &expressions,
                                                  std::string_view node_name) {
  std::vector<TypedValue> evaluated_expressions;
  evaluated_expressions.reserve(expressions.size());

  std::transform(expressions.begin(), expressions.end(), std::back_inserter(evaluated_expressions),
                 [&dba, &v_acc, &node_name](const auto &expression) {
                   return ComputeExpression(dba, v_acc, std::nullopt, expression, node_name, "");
                 });

  return evaluated_expressions;
}

std::vector<TypedValue> EvaluateEdgeExpressions(DbAccessor &dba, const VertexAccessor &v_acc, const EdgeAccessor &e_acc,
                                                const std::vector<std::string> &expressions) {
  std::vector<TypedValue> evaluated_expressions;
  evaluated_expressions.reserve(expressions.size());

  std::transform(expressions.begin(), expressions.end(), std::back_inserter(evaluated_expressions),
                 [&dba, &v_acc, &e_acc](const auto &expression) {
                   return ComputeExpression(dba, v_acc, e_acc, expression, expr::identifier_node_symbol,
                                            expr::identifier_edge_symbol);
                 });

  return evaluated_expressions;
}

ShardResult<std::map<PropertyId, Value>> CollectAllPropertiesFromAccessor(const VertexAccessor &acc, View view,
                                                                          const Schemas::Schema &schema) {
  auto ret = impl::CollectAllPropertiesImpl<VertexAccessor>(acc, view);
  if (ret.HasError()) {
    return ret.GetError();
  }

  auto pks = PrimaryKeysFromAccessor(acc, view, schema);
  if (pks) {
    ret.GetValue().merge(*pks);
  }

  return ret;
}

ShardResult<std::map<PropertyId, Value>> CollectAllPropertiesFromAccessor(const VertexAccessor &acc, View view) {
  return impl::CollectAllPropertiesImpl(acc, view);
}

EdgeUniquenessFunction InitializeEdgeUniquenessFunction(bool only_unique_neighbor_rows) {
  // Functions to select connecting edges based on uniquness
  EdgeUniquenessFunction maybe_filter_based_on_edge_uniquness;

  if (only_unique_neighbor_rows) {
    maybe_filter_based_on_edge_uniquness = [](EdgeAccessors &&edges,
                                              msgs::EdgeDirection edge_direction) -> EdgeAccessors {
      std::function<bool(std::set<const storage::v3::VertexId *, VertexIdCmpr> &, const storage::v3::EdgeAccessor &)>
          is_edge_unique;
      switch (edge_direction) {
        case msgs::EdgeDirection::OUT: {
          is_edge_unique = [](std::set<const storage::v3::VertexId *, VertexIdCmpr> &other_vertex_set,
                              const storage::v3::EdgeAccessor &edge_acc) {
            auto [it, insertion_happened] = other_vertex_set.insert(&edge_acc.ToVertex());
            return insertion_happened;
          };
          break;
        }
        case msgs::EdgeDirection::IN: {
          is_edge_unique = [](std::set<const storage::v3::VertexId *, VertexIdCmpr> &other_vertex_set,
                              const storage::v3::EdgeAccessor &edge_acc) {
            auto [it, insertion_happened] = other_vertex_set.insert(&edge_acc.FromVertex());
            return insertion_happened;
          };
          break;
        }
        case msgs::EdgeDirection::BOTH:
          MG_ASSERT(false, "This is should never happen, msgs::EdgeDirection::BOTH should not be passed here.");
      }

      EdgeAccessors ret;
      std::set<const storage::v3::VertexId *, VertexIdCmpr> other_vertex_set;

      for (const auto &edge : edges) {
        if (is_edge_unique(other_vertex_set, edge)) {
          ret.emplace_back(edge);
        }
      }

      return ret;
    };
  } else {
    maybe_filter_based_on_edge_uniquness =
        [](EdgeAccessors &&edges, msgs::EdgeDirection /*edge_direction*/) -> EdgeAccessors { return std::move(edges); };
  }

  return maybe_filter_based_on_edge_uniquness;
}

EdgeFiller InitializeEdgeFillerFunction(const msgs::ExpandOneRequest &req) {
  EdgeFiller edge_filler;

  if (!req.edge_properties) {
    edge_filler = [transaction_id = req.transaction_id.logical_id](
                      const EdgeAccessor &edge, const bool is_in_edge,
                      msgs::ExpandOneResultRow &result_row) -> ShardResult<void> {
      auto properties_results = edge.Properties(View::NEW);
      if (properties_results.HasError()) {
        spdlog::debug("Encountered an error while trying to get edge properties. Transaction id: {}", transaction_id);
        return properties_results.GetError();
      }

      std::map<PropertyId, msgs::Value> value_properties;
      for (auto &[prop_key, prop_val] : properties_results.GetValue()) {
        value_properties.insert(std::make_pair(prop_key, FromPropertyValueToValue(std::move(prop_val))));
      }
      using EdgeWithAllProperties = msgs::ExpandOneResultRow::EdgeWithAllProperties;
      EdgeWithAllProperties edges{ToMsgsVertexId(edge.FromVertex()), msgs::EdgeType{edge.EdgeType()},
                                  edge.Gid().AsUint(), std::move(value_properties)};
      if (is_in_edge) {
        result_row.in_edges_with_all_properties.push_back(std::move(edges));
      } else {
        result_row.out_edges_with_all_properties.push_back(std::move(edges));
      }
      return {};
    };
  } else {
    // TODO(gvolfing) - do we want to set the action_successful here?
    edge_filler = [&req](const EdgeAccessor &edge, const bool is_in_edge,
                         msgs::ExpandOneResultRow &result_row) -> ShardResult<void> {
      std::vector<msgs::Value> value_properties;
      value_properties.reserve(req.edge_properties.value().size());
      for (const auto &edge_prop : req.edge_properties.value()) {
        auto property_result = edge.GetProperty(edge_prop, View::NEW);
        if (property_result.HasError()) {
          spdlog::debug("Encountered an error while trying to get edge properties. Transaction id: {}",
                        req.transaction_id.logical_id);
          return property_result.GetError();
        }
        value_properties.emplace_back(FromPropertyValueToValue(std::move(property_result.GetValue())));
      }
      using EdgeWithSpecificProperties = msgs::ExpandOneResultRow::EdgeWithSpecificProperties;
      EdgeWithSpecificProperties edges{ToMsgsVertexId(edge.FromVertex()), msgs::EdgeType{edge.EdgeType()},
                                       edge.Gid().AsUint(), std::move(value_properties)};
      if (is_in_edge) {
        result_row.in_edges_with_specific_properties.push_back(std::move(edges));
      } else {
        result_row.out_edges_with_specific_properties.push_back(std::move(edges));
      }
      return {};
    };
  }

  return edge_filler;
}

bool FilterOnVertex(DbAccessor &dba, const storage::v3::VertexAccessor &v_acc,
                    const std::vector<std::string> &filters) {
  return std::ranges::all_of(filters, [&dba, &v_acc](const auto &filter_expr) {
    const auto result = ComputeExpression(dba, v_acc, std::nullopt, filter_expr, expr::identifier_node_symbol, "");
    return result.IsBool() && result.ValueBool();
  });
}

bool FilterOnEdge(DbAccessor &dba, const storage::v3::VertexAccessor &v_acc, const EdgeAccessor &e_acc,
                  const std::vector<std::string> &filters) {
  return std::ranges::all_of(filters, [&dba, &v_acc, &e_acc](const auto &filter_expr) {
    const auto result =
        ComputeExpression(dba, v_acc, e_acc, filter_expr, expr::identifier_node_symbol, expr::identifier_edge_symbol);
    return result.IsBool() && result.ValueBool();
  });
}

ShardResult<msgs::ExpandOneResultRow> GetExpandOneResult(
    Shard::Accessor &acc, msgs::VertexId src_vertex, const msgs::ExpandOneRequest &req,
    const EdgeUniquenessFunction &maybe_filter_based_on_edge_uniqueness, const EdgeFiller &edge_filler,
    const Schemas::Schema &schema) {
  /// Fill up source vertex
  const auto primary_key = ConvertPropertyVector(src_vertex.second);
  auto v_acc = acc.FindVertex(primary_key, View::NEW);

  msgs::Vertex source_vertex = {.id = src_vertex};
  auto maybe_secondary_labels = FillUpSourceVertexSecondaryLabels(v_acc, req);
  if (maybe_secondary_labels.HasError()) {
    return maybe_secondary_labels.GetError();
  }
  source_vertex.labels = std::move(*maybe_secondary_labels);

  auto src_vertex_properties = FillUpSourceVertexProperties(v_acc, req, storage::v3::View::NEW, schema);
  if (src_vertex_properties.HasError()) {
    return src_vertex_properties.GetError();
  }

  /// Fill up connecting edges
  auto fill_up_connecting_edges = FillUpConnectingEdges(v_acc, req, maybe_filter_based_on_edge_uniqueness);
  if (fill_up_connecting_edges.HasError()) {
    return fill_up_connecting_edges.GetError();
  }
  auto [in_edges, out_edges] = fill_up_connecting_edges.GetValue();

  msgs::ExpandOneResultRow result_row;
  result_row.src_vertex = std::move(source_vertex);
  result_row.src_vertex_properties = std::move(*src_vertex_properties);
  static constexpr bool kInEdges = true;
  static constexpr bool kOutEdges = false;
  if (const auto fill_edges_res = FillEdges<kInEdges>(in_edges, result_row, edge_filler); fill_edges_res.HasError()) {
    return fill_edges_res.GetError();
  }
  if (const auto fill_edges_res = FillEdges<kOutEdges>(out_edges, result_row, edge_filler); fill_edges_res.HasError()) {
    return fill_edges_res.GetError();
  }

  return result_row;
}

ShardResult<msgs::ExpandOneResultRow> GetExpandOneResult(
    VertexAccessor v_acc, msgs::VertexId src_vertex, const msgs::ExpandOneRequest &req,
    std::vector<EdgeAccessor> in_edge_accessors, std::vector<EdgeAccessor> out_edge_accessors,
    const EdgeUniquenessFunction &maybe_filter_based_on_edge_uniqueness, const EdgeFiller &edge_filler,
    const Schemas::Schema &schema) {
  /// Fill up source vertex
  msgs::Vertex source_vertex = {.id = src_vertex};
  auto maybe_secondary_labels = FillUpSourceVertexSecondaryLabels(v_acc, req);
  if (maybe_secondary_labels.HasError()) {
    return maybe_secondary_labels.GetError();
  }
  source_vertex.labels = std::move(*maybe_secondary_labels);

  /// Fill up source vertex properties
  auto src_vertex_properties = FillUpSourceVertexProperties(v_acc, req, storage::v3::View::NEW, schema);
  if (src_vertex_properties.HasError()) {
    return src_vertex_properties.GetError();
  }

  /// Fill up connecting edges
  auto in_edges = maybe_filter_based_on_edge_uniqueness(std::move(in_edge_accessors), msgs::EdgeDirection::IN);
  auto out_edges = maybe_filter_based_on_edge_uniqueness(std::move(out_edge_accessors), msgs::EdgeDirection::OUT);

  msgs::ExpandOneResultRow result_row;
  result_row.src_vertex = std::move(source_vertex);
  result_row.src_vertex_properties = std::move(*src_vertex_properties);
  static constexpr bool kInEdges = true;
  static constexpr bool kOutEdges = false;
  if (const auto fill_edges_res = FillEdges<kInEdges>(in_edges, result_row, edge_filler); fill_edges_res.HasError()) {
    return fill_edges_res.GetError();
  }
  if (const auto fill_edges_res = FillEdges<kOutEdges>(out_edges, result_row, edge_filler); fill_edges_res.HasError()) {
    return fill_edges_res.GetError();
  }

  return result_row;
}

VerticesIterable::Iterator GetStartVertexIterator(VerticesIterable &vertex_iterable,
                                                  const std::vector<PropertyValue> &primary_key, const View view) {
  auto it = vertex_iterable.begin();
  while (it != vertex_iterable.end()) {
    if (const auto &vertex = *it; primary_key <= vertex.PrimaryKey(view).GetValue()) {
      break;
    }
    ++it;
  }
  return it;
}

std::vector<Element<VertexAccessor>>::const_iterator GetStartOrderedElementsIterator(
    const std::vector<Element<VertexAccessor>> &ordered_elements, const std::vector<PropertyValue> &primary_key,
    const View view) {
  for (auto it = ordered_elements.begin(); it != ordered_elements.end(); ++it) {
    if (const auto &vertex = it->object_acc; primary_key <= vertex.PrimaryKey(view).GetValue()) {
      return it;
    }
  }
  return ordered_elements.end();
}

std::array<std::vector<EdgeAccessor>, 2> GetEdgesFromVertex(const VertexAccessor &vertex_accessor,
                                                            const msgs::EdgeDirection direction) {
  std::vector<EdgeAccessor> in_edges;
  std::vector<EdgeAccessor> out_edges;

  switch (direction) {
    case memgraph::msgs::EdgeDirection::IN: {
      auto edges = vertex_accessor.InEdges(View::OLD);
      if (edges.HasValue()) {
        in_edges = edges.GetValue();
      }
    }
    case memgraph::msgs::EdgeDirection::OUT: {
      auto edges = vertex_accessor.OutEdges(View::OLD);
      if (edges.HasValue()) {
        out_edges = edges.GetValue();
      }
    }
    case memgraph::msgs::EdgeDirection::BOTH: {
      auto maybe_in_edges = vertex_accessor.InEdges(View::OLD);
      auto maybe_out_edges = vertex_accessor.OutEdges(View::OLD);
      std::vector<EdgeAccessor> edges;
      if (maybe_in_edges.HasValue()) {
        in_edges = maybe_in_edges.GetValue();
      }
      if (maybe_out_edges.HasValue()) {
        out_edges = maybe_out_edges.GetValue();
      }
    }
  }

  return std::array<std::vector<EdgeAccessor>, 2>{std::move(in_edges), std::move(out_edges)};
}

std::vector<Element<EdgeAccessor>> OrderByEdges(DbAccessor &dba, std::vector<EdgeAccessor> &iterable,
                                                std::vector<msgs::OrderBy> &order_by_edges,
                                                const VertexAccessor &vertex_acc) {
  std::vector<Ordering> ordering;
  ordering.reserve(order_by_edges.size());
  std::transform(order_by_edges.begin(), order_by_edges.end(), std::back_inserter(ordering),
                 [](const auto &order_by) { return ConvertMsgsOrderByToOrdering(order_by.direction); });

  std::vector<Element<EdgeAccessor>> ordered;
  for (auto it = iterable.begin(); it != iterable.end(); ++it) {
    std::vector<TypedValue> properties_order_by;
    properties_order_by.reserve(order_by_edges.size());
    std::transform(order_by_edges.begin(), order_by_edges.end(), std::back_inserter(properties_order_by),
                   [&dba, &vertex_acc, &it](const auto &order_by) {
                     return ComputeExpression(dba, vertex_acc, *it, order_by.expression.expression,
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

std::vector<Element<std::pair<VertexAccessor, EdgeAccessor>>> OrderByEdges(
    DbAccessor &dba, std::vector<EdgeAccessor> &iterable, std::vector<msgs::OrderBy> &order_by_edges,
    const std::vector<VertexAccessor> &vertex_acc) {
  MG_ASSERT(vertex_acc.size() == iterable.size());
  std::vector<Ordering> ordering;
  ordering.reserve(order_by_edges.size());
  std::transform(order_by_edges.begin(), order_by_edges.end(), std::back_inserter(ordering),
                 [](const auto &order_by) { return ConvertMsgsOrderByToOrdering(order_by.direction); });

  std::vector<Element<std::pair<VertexAccessor, EdgeAccessor>>> ordered;
  VertexAccessor current = vertex_acc.front();
  size_t id = 0;
  for (auto it = iterable.begin(); it != iterable.end(); it++, id++) {
    current = vertex_acc[id];
    std::vector<TypedValue> properties_order_by;
    properties_order_by.reserve(order_by_edges.size());
    std::transform(order_by_edges.begin(), order_by_edges.end(), std::back_inserter(properties_order_by),
                   [&dba, it, current](const auto &order_by) {
                     return ComputeExpression(dba, current, *it, order_by.expression.expression,
                                              expr::identifier_node_symbol, expr::identifier_edge_symbol);
                   });

    ordered.push_back({std::move(properties_order_by), {current, *it}});
  }

  auto compare_typed_values = TypedValueVectorCompare(ordering);
  std::sort(ordered.begin(), ordered.end(), [compare_typed_values](const auto &pair1, const auto &pair2) {
    return compare_typed_values(pair1.properties_order_by, pair2.properties_order_by);
  });
  return ordered;
}

}  // namespace memgraph::storage::v3

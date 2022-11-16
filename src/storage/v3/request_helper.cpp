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

#include <vector>

#include "pretty_print_ast_to_original_expression.hpp"
#include "storage/v3/bindings/db_accessor.hpp"
#include "storage/v3/expr.hpp"
#include "storage/v3/value_conversions.hpp"

namespace memgraph::storage::v3 {
using msgs::Label;
using msgs::PropertyId;

using conversions::ConvertPropertyMap;
using conversions::ConvertPropertyVector;
using conversions::ConvertValueVector;
using conversions::FromPropertyValueToValue;
using conversions::ToMsgsVertexId;
using conversions::ToPropertyValue;

namespace {
namespace msgs = msgs;

using AllEdgePropertyDataSructure = std::map<PropertyId, msgs::Value>;
using SpecificEdgePropertyDataSructure = std::vector<msgs::Value>;

using AllEdgeProperties = std::tuple<msgs::VertexId, msgs::Gid, AllEdgePropertyDataSructure>;
using SpecificEdgeProperties = std::tuple<msgs::VertexId, msgs::Gid, SpecificEdgePropertyDataSructure>;

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

struct LocalError {};

std::optional<std::vector<msgs::Label>> FillUpSourceVertexSecondaryLabels(const std::optional<VertexAccessor> &v_acc,
                                                                          const msgs::ExpandOneRequest &req) {
  auto secondary_labels = v_acc->Labels(View::NEW);
  if (secondary_labels.HasError()) {
    spdlog::debug("Encountered an error while trying to get the secondary labels of a vertex. Transaction id: {}",
                  req.transaction_id.logical_id);
    return std::nullopt;
  }

  auto &sec_labels = secondary_labels.GetValue();
  std::vector<msgs::Label> msgs_secondary_labels;
  msgs_secondary_labels.reserve(sec_labels.size());

  std::transform(sec_labels.begin(), sec_labels.end(), std::back_inserter(msgs_secondary_labels),
                 [](auto label_id) { return msgs::Label{.id = label_id}; });

  return msgs_secondary_labels;
}

std::optional<std::map<PropertyId, Value>> FillUpSourceVertexProperties(const std::optional<VertexAccessor> &v_acc,
                                                                        const msgs::ExpandOneRequest &req,
                                                                        storage::v3::View view,
                                                                        const Schemas::Schema &schema) {
  std::map<PropertyId, Value> src_vertex_properties;

  if (!req.src_vertex_properties) {
    auto props = v_acc->Properties(View::NEW);
    if (props.HasError()) {
      spdlog::debug("Encountered an error while trying to access vertex properties. Transaction id: {}",
                    req.transaction_id.logical_id);
      return std::nullopt;
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
        return std::nullopt;
      }
      src_vertex_properties.insert(std::make_pair(prop, FromPropertyValueToValue(std::move(prop_val.GetValue()))));
    }
  }

  return src_vertex_properties;
}

std::optional<std::array<std::vector<EdgeAccessor>, 2>> FillUpConnectingEdges(
    const std::optional<VertexAccessor> &v_acc, const msgs::ExpandOneRequest &req,
    const EdgeUniqunessFunction &maybe_filter_based_on_edge_uniquness) {
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
        return std::nullopt;
      }
      out_edges =
          maybe_filter_based_on_edge_uniquness(std::move(out_edges_result.GetValue()), msgs::EdgeDirection::OUT);
      break;
    }
    case msgs::EdgeDirection::IN: {
      auto in_edges_result = v_acc->InEdges(View::NEW, edge_types);
      if (in_edges_result.HasError()) {
        spdlog::debug(
            "Encountered an error while trying to get in-going EdgeAccessors. Transaction id: {}"[req.transaction_id
                                                                                                      .logical_id]);
        return std::nullopt;
      }
      in_edges = maybe_filter_based_on_edge_uniquness(std::move(in_edges_result.GetValue()), msgs::EdgeDirection::IN);
      break;
    }
    case msgs::EdgeDirection::BOTH: {
      auto in_edges_result = v_acc->InEdges(View::NEW, edge_types);
      if (in_edges_result.HasError()) {
        spdlog::debug("Encountered an error while trying to get in-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return std::nullopt;
      }
      in_edges = maybe_filter_based_on_edge_uniquness(std::move(in_edges_result.GetValue()), msgs::EdgeDirection::IN);
      auto out_edges_result = v_acc->OutEdges(View::NEW, edge_types);
      if (out_edges_result.HasError()) {
        spdlog::debug("Encountered an error while trying to get out-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return std::nullopt;
      }
      out_edges =
          maybe_filter_based_on_edge_uniquness(std::move(out_edges_result.GetValue()), msgs::EdgeDirection::OUT);
      break;
    }
  }
  return std::array<std::vector<EdgeAccessor>, 2>{std::move(in_edges), std::move(out_edges)};
}

using AllEdgePropertyDataSructure = std::map<PropertyId, msgs::Value>;
using SpecificEdgePropertyDataSructure = std::vector<msgs::Value>;

using AllEdgeProperties = std::tuple<msgs::VertexId, msgs::Gid, AllEdgePropertyDataSructure>;
using SpecificEdgeProperties = std::tuple<msgs::VertexId, msgs::Gid, SpecificEdgePropertyDataSructure>;

using SpecificEdgePropertiesVector = std::vector<SpecificEdgeProperties>;
using AllEdgePropertiesVector = std::vector<AllEdgeProperties>;

using EdgeFiller = std::function<bool(const EdgeAccessor &edge, bool is_in_edge, msgs::ExpandOneResultRow &result_row)>;

template <bool are_in_edges>
bool FillEdges(const std::vector<EdgeAccessor> &edges, msgs::ExpandOneResultRow &row, const EdgeFiller &edge_filler) {
  for (const auto &edge : edges) {
    if (!edge_filler(edge, are_in_edges, row)) {
      return false;
    }
  }

  return true;
}

};  // namespace

std::optional<std::map<PropertyId, Value>> CollectSpecificPropertiesFromAccessor(const VertexAccessor &acc,
                                                                                 const std::vector<PropertyId> &props,
                                                                                 View view) {
  std::map<PropertyId, Value> ret;

  for (const auto &prop : props) {
    auto result = acc.GetProperty(prop, view);
    if (result.HasError()) {
      spdlog::debug("Encountered an Error while trying to get a vertex property.");
      return std::nullopt;
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

std::optional<std::map<PropertyId, Value>> CollectAllPropertiesFromAccessor(const VertexAccessor &acc, View view,
                                                                            const Schemas::Schema &schema) {
  std::map<PropertyId, Value> ret;
  auto props = acc.Properties(view);
  if (props.HasError()) {
    spdlog::debug("Encountered an error while trying to get vertex properties.");
    return std::nullopt;
  }

  auto &properties = props.GetValue();
  std::transform(properties.begin(), properties.end(), std::inserter(ret, ret.begin()),
                 [](std::pair<const PropertyId, PropertyValue> &pair) {
                   return std::make_pair(pair.first, FromPropertyValueToValue(std::move(pair.second)));
                 });
  properties.clear();

  auto pks = PrimaryKeysFromAccessor(acc, view, schema);
  if (pks) {
    ret.merge(*pks);
  }

  return ret;
}

EdgeUniqunessFunction InitializeEdgeUniqunessFunction(bool only_unique_neighbor_rows) {
  // Functions to select connecting edges based on uniquness
  EdgeUniqunessFunction maybe_filter_based_on_edge_uniquness;

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
    edge_filler = [transaction_id = req.transaction_id.logical_id](const EdgeAccessor &edge, const bool is_in_edge,
                                                                   msgs::ExpandOneResultRow &result_row) -> bool {
      auto properties_results = edge.Properties(View::NEW);
      if (properties_results.HasError()) {
        spdlog::debug("Encountered an error while trying to get edge properties. Transaction id: {}", transaction_id);
        return false;
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
      return true;
    };
  } else {
    // TODO(gvolfing) - do we want to set the action_successful here?
    edge_filler = [&req](const EdgeAccessor &edge, const bool is_in_edge,
                         msgs::ExpandOneResultRow &result_row) -> bool {
      std::vector<msgs::Value> value_properties;
      value_properties.reserve(req.edge_properties.value().size());
      for (const auto &edge_prop : req.edge_properties.value()) {
        auto property_result = edge.GetProperty(edge_prop, View::NEW);
        if (property_result.HasError()) {
          spdlog::debug("Encountered an error while trying to get edge properties. Transaction id: {}",
                        req.transaction_id.logical_id);
          return false;
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
      return true;
    };
  }

  return edge_filler;
}

bool FilterOnVertex(DbAccessor &dba, const storage::v3::VertexAccessor &v_acc, const std::vector<std::string> &filters,
                    const std::string_view node_name) {
  return std::ranges::all_of(filters, [&node_name, &dba, &v_acc](const auto &filter_expr) {
    auto res = ComputeExpression(dba, v_acc, std::nullopt, filter_expr, node_name, "");
    return res.IsBool() && res.ValueBool();
  });
}

std::optional<msgs::ExpandOneResultRow> GetExpandOneResult(
    Shard::Accessor &acc, msgs::VertexId src_vertex, const msgs::ExpandOneRequest &req,
    const EdgeUniqunessFunction &maybe_filter_based_on_edge_uniquness, const EdgeFiller &edge_filler,
    const Schemas::Schema &schema) {
  /// Fill up source vertex
  const auto primary_key = ConvertPropertyVector(src_vertex.second);
  auto v_acc = acc.FindVertex(primary_key, View::NEW);

  msgs::Vertex source_vertex = {.id = src_vertex};
  if (const auto maybe_secondary_labels = FillUpSourceVertexSecondaryLabels(v_acc, req); maybe_secondary_labels) {
    source_vertex.labels = *maybe_secondary_labels;
  } else {
    return std::nullopt;
  }

  std::optional<std::map<PropertyId, Value>> src_vertex_properties;
  src_vertex_properties = FillUpSourceVertexProperties(v_acc, req, storage::v3::View::NEW, schema);

  if (!src_vertex_properties) {
    return std::nullopt;
  }

  /// Fill up connecting edges
  auto fill_up_connecting_edges = FillUpConnectingEdges(v_acc, req, maybe_filter_based_on_edge_uniquness);
  if (!fill_up_connecting_edges) {
    return std::nullopt;
  }

  auto [in_edges, out_edges] = fill_up_connecting_edges.value();

  msgs::ExpandOneResultRow result_row;
  result_row.src_vertex = std::move(source_vertex);
  result_row.src_vertex_properties = std::move(*src_vertex_properties);
  static constexpr bool kInEdges = true;
  static constexpr bool kOutEdges = false;
  if (!in_edges.empty() && !FillEdges<kInEdges>(in_edges, result_row, edge_filler)) {
    return std::nullopt;
  }
  if (!out_edges.empty() && !FillEdges<kOutEdges>(out_edges, result_row, edge_filler)) {
    return std::nullopt;
  }

  return result_row;
}

std::optional<msgs::ExpandOneResultRow> GetExpandOneResult(
    VertexAccessor v_acc, msgs::VertexId src_vertex, const msgs::ExpandOneRequest &req,
    std::vector<EdgeAccessor> in_edge_accessors, std::vector<EdgeAccessor> out_edge_accessors,
    const EdgeUniqunessFunction &maybe_filter_based_on_edge_uniquness, const EdgeFiller &edge_filler,
    const Schemas::Schema &schema) {
  /// Fill up source vertex
  msgs::Vertex source_vertex = {.id = src_vertex};
  if (const auto maybe_secondary_labels = FillUpSourceVertexSecondaryLabels(v_acc, req); maybe_secondary_labels) {
    source_vertex.labels = *maybe_secondary_labels;
  } else {
    return std::nullopt;
  }

  /// Fill up source vertex properties
  auto src_vertex_properties = FillUpSourceVertexProperties(v_acc, req, storage::v3::View::NEW, schema);
  if (!src_vertex_properties) {
    return std::nullopt;
  }

  /// Fill up connecting edges
  auto in_edges = maybe_filter_based_on_edge_uniquness(std::move(in_edge_accessors), msgs::EdgeDirection::IN);
  auto out_edges = maybe_filter_based_on_edge_uniquness(std::move(out_edge_accessors), msgs::EdgeDirection::OUT);

  msgs::ExpandOneResultRow result_row;
  result_row.src_vertex = std::move(source_vertex);
  result_row.src_vertex_properties = std::move(*src_vertex_properties);
  static constexpr bool kInEdges = true;
  static constexpr bool kOutEdges = false;
  if (!in_edges.empty() && !FillEdges<kInEdges>(in_edges, result_row, edge_filler)) {
    return std::nullopt;
  }
  if (!out_edges.empty() && !FillEdges<kOutEdges>(out_edges, result_row, edge_filler)) {
    return std::nullopt;
  }

  return result_row;
}

VerticesIterable::Iterator GetStartVertexIterator(VerticesIterable &vertex_iterable,
                                                  const std::vector<PropertyValue> &start_ids, const View view) {
  auto it = vertex_iterable.begin();
  while (it != vertex_iterable.end()) {
    if (const auto &vertex = *it; start_ids <= vertex.PrimaryKey(view).GetValue()) {
      break;
    }
    ++it;
  }
  return it;
}

std::vector<Element<VertexAccessor>>::const_iterator GetStartOrderedElementsIterator(
    const std::vector<Element<VertexAccessor>> &ordered_elements, const std::vector<PropertyValue> &start_ids,
    const View view) {
  for (auto it = ordered_elements.begin(); it != ordered_elements.end(); ++it) {
    if (const auto &vertex = it->object_acc; start_ids <= vertex.PrimaryKey(view).GetValue()) {
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

}  // namespace memgraph::storage::v3

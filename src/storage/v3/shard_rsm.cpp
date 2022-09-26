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

#include <functional>
#include <iterator>
#include <utility>

#include "parser/opencypher/parser.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/shard_rsm.hpp"
#include "storage/v3/value_conversions.hpp"
#include "storage/v3/vertex_accessor.hpp"

using memgraph::msgs::Label;
using memgraph::msgs::PropertyId;
using memgraph::msgs::Value;
using memgraph::msgs::Vertex;
using memgraph::msgs::VertexId;

using memgraph::storage::conversions::ConvertPropertyVector;
using memgraph::storage::conversions::ConvertValueVector;
using memgraph::storage::conversions::ToPropertyValue;
using memgraph::storage::conversions::ToValue;

namespace {
std::vector<std::pair<memgraph::storage::v3::PropertyId, memgraph::storage::v3::PropertyValue>> ConvertPropertyMap(
    std::vector<std::pair<PropertyId, Value>> &&properties) {
  std::vector<std::pair<memgraph::storage::v3::PropertyId, memgraph::storage::v3::PropertyValue>> ret;
  ret.reserve(properties.size());

  for (auto &[key, value] : properties) {
    ret.emplace_back(std::make_pair(key, ToPropertyValue(std::move(value))));
  }

  return ret;
}

std::vector<std::pair<memgraph::storage::v3::PropertyId, Value>> FromMap(
    const std::map<PropertyId, Value> &properties) {
  std::vector<std::pair<memgraph::storage::v3::PropertyId, Value>> ret;
  ret.reserve(properties.size());

  for (const auto &[key, value] : properties) {
    ret.emplace_back(std::make_pair(key, value));
  }

  return ret;
}

std::optional<std::map<PropertyId, Value>> CollectPropertiesFromAccessor(
    const memgraph::storage::v3::VertexAccessor &acc, const std::vector<memgraph::storage::v3::PropertyId> &props,
    memgraph::storage::v3::View view) {
  std::map<PropertyId, Value> ret;

  for (const auto &prop : props) {
    auto result = acc.GetProperty(prop, view);
    if (result.HasError()) {
      spdlog::debug("Encountered an Error while trying to get a vertex property.");
      continue;
    }
    auto &value = result.GetValue();
    if (value.IsNull()) {
      spdlog::debug("The specified property does not exist but it should");
      continue;
    }
    ret.emplace(std::make_pair(prop, ToValue(value)));
  }

  return ret;
}

std::optional<std::map<PropertyId, Value>> CollectAllPropertiesFromAccessor(
    const memgraph::storage::v3::VertexAccessor &acc, memgraph::storage::v3::View view) {
  std::map<PropertyId, Value> ret;
  auto iter = acc.Properties(view);
  if (iter.HasError()) {
    spdlog::debug("Encountered an error while trying to get vertex properties.");
  }

  for (const auto &[prop_key, prop_val] : iter.GetValue()) {
    ret.emplace(prop_key, ToValue(prop_val));
  }

  return ret;
}

Value ConstructValueVertex(const memgraph::storage::v3::VertexAccessor &acc, memgraph::storage::v3::View view) {
  // Get the vertex id
  auto prim_label = acc.PrimaryLabel(view).GetValue();
  Label value_label{.id = prim_label};

  auto prim_key = ConvertValueVector(acc.PrimaryKey(view).GetValue());
  VertexId vertex_id = std::make_pair(value_label, prim_key);

  // Get the labels
  auto vertex_labels = acc.Labels(view).GetValue();
  std::vector<Label> value_labels;
  for (const auto &label : vertex_labels) {
    Label l = {.id = label};
    value_labels.push_back(l);
  }

  return Value({.id = vertex_id, .labels = value_labels});
}

bool DoesEdgeTypeMatch(const memgraph::msgs::ExpandOneRequest &req, const memgraph::storage::v3::EdgeAccessor &edge) {
  for (const auto &edge_type : req.edge_types) {
    if (memgraph::storage::v3::EdgeTypeId::FromUint(edge_type.id) == edge.EdgeType()) {
      return true;
    }
  }
  return false;
}

struct LocalError {};

std::optional<memgraph::msgs::Vertex> FillUpSourceVertex(
    const std::optional<memgraph::storage::v3::VertexAccessor> &v_acc, memgraph::msgs::ExpandOneRequest &req,
    memgraph::msgs::VertexId src_vertex) {
  auto secondary_labels = v_acc->Labels(memgraph::storage::v3::View::OLD);
  if (secondary_labels.HasError()) {
    spdlog::debug("Encountered an error while trying to get the secondary labels of a vertex. Transaction id: {}",
                  req.transaction_id.logical_id);
    return std::nullopt;
  }

  memgraph::msgs::Vertex source_vertex;
  source_vertex.id = src_vertex;
  source_vertex.labels.reserve(secondary_labels.GetValue().size());
  for (auto label_id : secondary_labels.GetValue()) {
    source_vertex.labels.push_back({.id = label_id});
  }
  return source_vertex;
}

std::optional<std::map<PropertyId, Value>> FillUpSourceVertexProperties(
    const std::optional<memgraph::storage::v3::VertexAccessor> &v_acc, memgraph::msgs::ExpandOneRequest &req) {
  std::map<PropertyId, Value> src_vertex_properties;

  if (!req.src_vertex_properties) {
    auto props = v_acc->Properties(memgraph::storage::v3::View::OLD);
    if (props.HasError()) {
      spdlog::debug("Encountered an error while trying to access vertex properties. Transaction id: {}",
                    req.transaction_id.logical_id);
      return std::nullopt;
    }

    for (auto &[key, val] : props.GetValue()) {
      src_vertex_properties.insert(std::make_pair(key, ToValue(val)));
    }

  } else if (req.src_vertex_properties.value().empty()) {
    // NOOP
  } else {
    for (const auto &prop : req.src_vertex_properties.value()) {
      const auto &prop_val = v_acc->GetProperty(prop, memgraph::storage::v3::View::OLD);
      src_vertex_properties.insert(std::make_pair(prop, ToValue(prop_val.GetValue())));
    }
  }

  return src_vertex_properties;
}

std::optional<std::array<std::vector<memgraph::storage::v3::EdgeAccessor>, 2>> FillUpConnectingEdges(
    const std::optional<memgraph::storage::v3::VertexAccessor> &v_acc, memgraph::msgs::ExpandOneRequest &req) {
  std::vector<memgraph::storage::v3::EdgeAccessor> in_edges;
  std::vector<memgraph::storage::v3::EdgeAccessor> out_edges;

  switch (req.direction) {
    case memgraph::msgs::EdgeDirection::OUT: {
      auto out_edges_result = v_acc->OutEdges(memgraph::storage::v3::View::OLD);
      if (out_edges_result.HasError()) {
        spdlog::debug("Encountered an error while trying to get out-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return std::nullopt;
      }
      out_edges = out_edges_result.GetValue();
      break;
    }
    case memgraph::msgs::EdgeDirection::IN: {
      auto in_edges_result = v_acc->InEdges(memgraph::storage::v3::View::OLD);
      if (in_edges_result.HasError()) {
        spdlog::debug(
            "Encountered an error while trying to get in-going EdgeAccessors. Transaction id: {}"[req.transaction_id
                                                                                                      .logical_id]);
        return std::nullopt;
      }
      in_edges = in_edges_result.GetValue();
      break;
    }
    case memgraph::msgs::EdgeDirection::BOTH: {
      auto in_edges_result = v_acc->InEdges(memgraph::storage::v3::View::OLD);
      if (in_edges_result.HasError()) {
        spdlog::debug("Encountered an error while trying to get in-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return std::nullopt;
      }
      in_edges = in_edges_result.GetValue();

      auto out_edges_result = v_acc->OutEdges(memgraph::storage::v3::View::OLD);
      if (out_edges_result.HasError()) {
        spdlog::debug("Encountered an error while trying to get out-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return std::nullopt;
      }
      out_edges = out_edges_result.GetValue();
      break;
    }
  }
  return std::array<std::vector<memgraph::storage::v3::EdgeAccessor>, 2>{in_edges, out_edges};
}

std::optional<memgraph::msgs::ExpandOneResultRow> GetExpandOneResult(memgraph::storage::v3::Shard::Accessor &acc,
                                                                     memgraph::msgs::VertexId src_vertex,
                                                                     memgraph::msgs::ExpandOneRequest req) {
  using EdgeProperties =
      std::variant<LocalError, std::map<PropertyId, memgraph::msgs::Value>, std::vector<memgraph::msgs::Value>>;
  std::function<EdgeProperties(const memgraph::storage::v3::EdgeAccessor &)> get_edge_properties;

  if (!req.edge_properties) {
    get_edge_properties = [&req](const memgraph::storage::v3::EdgeAccessor &edge) -> EdgeProperties {
      std::map<PropertyId, memgraph::msgs::Value> ret;
      auto property_results = edge.Properties(memgraph::storage::v3::View::OLD);
      if (property_results.HasError()) {
        spdlog::debug("Encountered an error while trying to get out-going EdgeAccessors. Transaction id: {}",
                      req.transaction_id.logical_id);
        return LocalError{};
      }

      for (const auto &[prop_key, prop_val] : property_results.GetValue()) {
        ret.insert(std::make_pair(prop_key, ToValue(prop_val)));
      }
      return ret;
    };
  } else {
    // TODO(gvolfing) - do we want to set the action_successful here?
    get_edge_properties = [&req](const memgraph::storage::v3::EdgeAccessor &edge) {
      std::vector<memgraph::msgs::Value> ret;
      ret.reserve(req.edge_properties.value().size());
      for (const auto &edge_prop : req.edge_properties.value()) {
        // TODO(gvolfing) maybe check for the absence of certain properties
        ret.push_back(ToValue(edge.GetProperty(edge_prop, memgraph::storage::v3::View::OLD).GetValue()));
      }
      return ret;
    };
  }

  memgraph::msgs::ExpandOneResultRow current_row;
  /// The empty optional means return all of the properties, while an empty
  /// list means do not return any properties.

  /// Fill up source vertex
  auto v_acc = acc.FindVertex(ConvertPropertyVector(std::move(src_vertex.second)), memgraph::storage::v3::View::OLD);

  auto source_vertex = FillUpSourceVertex(v_acc, req, src_vertex);
  if (!source_vertex) {
    return std::nullopt;
  }

  /// Fill up source vertex properties
  auto src_vertex_properties = FillUpSourceVertexProperties(v_acc, req);
  if (!src_vertex_properties) {
    return std::nullopt;
  }

  /// Fill up connecting edges
  auto fill_up_connecting_edges = FillUpConnectingEdges(v_acc, req);
  if (!fill_up_connecting_edges) {
    return std::nullopt;
  }

  auto [in_edges, out_edges] = fill_up_connecting_edges.value();

  /// Assemble the edge properties
  std::optional<std::vector<
      std::tuple<memgraph::msgs::VertexId, memgraph::msgs::Gid, std::map<PropertyId, memgraph::msgs::Value>>>>
      edges_with_all_properties;
  std::optional<
      std::vector<std::tuple<memgraph::msgs::VertexId, memgraph::msgs::Gid, std::vector<memgraph::msgs::Value>>>>
      edges_with_specific_properties;

  if (!req.edge_properties) {
    std::vector<std::tuple<memgraph::msgs::VertexId, memgraph::msgs::Gid, std::map<PropertyId, memgraph::msgs::Value>>>
        ret_in;
    ret_in.reserve(in_edges.size());
    std::vector<std::tuple<memgraph::msgs::VertexId, memgraph::msgs::Gid, std::map<PropertyId, memgraph::msgs::Value>>>
        ret_out;
    ret_out.reserve(out_edges.size());

    for (const auto &edge : in_edges) {
      if (!DoesEdgeTypeMatch(req, edge)) {
        continue;
      }

      std::tuple<memgraph::msgs::VertexId, memgraph::msgs::Gid, std::map<PropertyId, memgraph::msgs::Value>> ret_tuple;

      memgraph::msgs::Label label;
      label.id = edge.FromVertex().primary_label;
      memgraph::msgs::VertexId other_vertex = std::make_pair(label, ConvertValueVector(edge.FromVertex().primary_key));

      const auto edge_props_var = get_edge_properties(edge);

      if (std::get_if<LocalError>(&edge_props_var) != nullptr) {
        return std::nullopt;
      }

      auto edge_props = std::get<std::map<PropertyId, memgraph::msgs::Value>>(edge_props_var);
      memgraph::msgs::Gid gid = edge.Gid().AsUint();

      ret_tuple = {other_vertex, gid, edge_props};
      ret_in.push_back(ret_tuple);
    }

    for (const auto &edge : out_edges) {
      if (!DoesEdgeTypeMatch(req, edge)) {
        continue;
      }

      std::tuple<memgraph::msgs::VertexId, memgraph::msgs::Gid, std::map<PropertyId, memgraph::msgs::Value>> ret_tuple;

      memgraph::msgs::Label label;
      label.id = edge.FromVertex().primary_label;
      memgraph::msgs::VertexId other_vertex = std::make_pair(label, ConvertValueVector(edge.ToVertex().primary_key));

      const auto edge_props_var = get_edge_properties(edge);

      if (std::get_if<LocalError>(&edge_props_var) != nullptr) {
        return std::nullopt;
      }

      auto edge_props = std::get<std::map<PropertyId, memgraph::msgs::Value>>(edge_props_var);
      memgraph::msgs::Gid gid = edge.Gid().AsUint();

      ret_tuple = {other_vertex, gid, edge_props};
      ret_out.push_back(ret_tuple);
    }

    // Set one of the options to the actual datastructure and the otherone to nullopt
    switch (req.direction) {
      case memgraph::msgs::EdgeDirection::OUT: {
        edges_with_all_properties = ret_out;
        break;
      }
      case memgraph::msgs::EdgeDirection::IN: {
        edges_with_all_properties = ret_in;
        break;
      }
      case memgraph::msgs::EdgeDirection::BOTH: {
        std::vector<
            std::tuple<memgraph::msgs::VertexId, memgraph::msgs::Gid, std::map<PropertyId, memgraph::msgs::Value>>>
            ret;
        ret.resize(ret_out.size() + ret_in.size());
        ret.insert(ret.end(), ret_in.begin(), ret_in.end());
        ret.insert(ret.end(), ret_out.begin(), ret_out.end());

        edges_with_all_properties = ret;
        break;
      }
    }
    edges_with_specific_properties = {};

  } else {
    // when user specifies specific properties, it's enough to return just a vector
    std::vector<std::tuple<memgraph::msgs::VertexId, memgraph::msgs::Gid, std::vector<memgraph::msgs::Value>>> ret_in;
    ret_in.reserve(in_edges.size());
    std::vector<std::tuple<memgraph::msgs::VertexId, memgraph::msgs::Gid, std::vector<memgraph::msgs::Value>>> ret_out;
    ret_out.reserve(out_edges.size());

    for (const auto &edge : in_edges) {
      if (!DoesEdgeTypeMatch(req, edge)) {
        continue;
      }

      std::tuple<memgraph::msgs::VertexId, memgraph::msgs::Gid, std::vector<memgraph::msgs::Value>> ret_tuple;

      memgraph::msgs::Label label;
      label.id = edge.FromVertex().primary_label;
      memgraph::msgs::VertexId other_vertex = std::make_pair(label, ConvertValueVector(edge.FromVertex().primary_key));

      const auto edge_props_var = get_edge_properties(edge);

      if (std::get_if<LocalError>(&edge_props_var) != nullptr) {
        return std::nullopt;
      }

      auto edge_props = std::get<std::vector<memgraph::msgs::Value>>(edge_props_var);
      memgraph::msgs::Gid gid = edge.Gid().AsUint();

      ret_tuple = {other_vertex, gid, edge_props};
      ret_in.push_back(ret_tuple);
    }

    for (const auto &edge : out_edges) {
      if (!DoesEdgeTypeMatch(req, edge)) {
        continue;
      }

      std::tuple<memgraph::msgs::VertexId, memgraph::msgs::Gid, std::vector<memgraph::msgs::Value>> ret_tuple;

      memgraph::msgs::Label label;
      label.id = edge.FromVertex().primary_label;
      memgraph::msgs::VertexId other_vertex = std::make_pair(label, ConvertValueVector(edge.ToVertex().primary_key));

      const auto edge_props_var = get_edge_properties(edge);

      if (std::get_if<LocalError>(&edge_props_var) != nullptr) {
        return std::nullopt;
      }

      auto edge_props = std::get<std::vector<memgraph::msgs::Value>>(edge_props_var);
      memgraph::msgs::Gid gid = edge.Gid().AsUint();

      ret_tuple = {other_vertex, gid, edge_props};
      ret_out.push_back(ret_tuple);
    }

    // Set one of the options to the actual datastructure and the otherone to nullopt
    switch (req.direction) {
      case memgraph::msgs::EdgeDirection::OUT: {
        edges_with_specific_properties = ret_out;
        break;
      }
      case memgraph::msgs::EdgeDirection::IN: {
        edges_with_specific_properties = ret_in;
        break;
      }
      case memgraph::msgs::EdgeDirection::BOTH: {
        std::vector<std::tuple<memgraph::msgs::VertexId, memgraph::msgs::Gid, std::vector<memgraph::msgs::Value>>> ret;
        ret.resize(ret_out.size() + ret_in.size());
        ret.insert(ret.end(), ret_in.begin(), ret_in.end());
        ret.insert(ret.end(), ret_out.begin(), ret_out.end());

        edges_with_specific_properties = ret;
        break;
      }
    }
    edges_with_all_properties = {};
  }

  return memgraph::msgs::ExpandOneResultRow{.src_vertex = *source_vertex,
                                            .src_vertex_properties = src_vertex_properties,
                                            .edges_with_all_properties = edges_with_all_properties,
                                            .edges_with_specific_properties = edges_with_specific_properties};
}

}  // namespace

namespace memgraph::storage::v3 {

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::CreateVerticesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);

  // Workaround untill we have access to CreateVertexAndValidate()
  // with the new signature that does not require the primary label.
  const auto prim_label = acc.GetPrimaryLabel();

  bool action_successful = true;

  for (auto &new_vertex : req.new_vertices) {
    /// TODO(gvolfing) Remove this. In the new implementation each shard
    /// should have a predetermined primary label, so there is no point in
    /// specifying it in the accessor functions. Their signature will
    /// change.
    /// TODO(gvolfing) Consider other methods than converting. Change either
    /// the way that the property map is stored in the messages, or the
    /// signature of CreateVertexAndValidate.
    auto converted_property_map = ConvertPropertyMap(std::move(new_vertex.properties));

    // TODO(gvolfing) make sure if this conversion is actually needed.
    std::vector<memgraph::storage::v3::LabelId> converted_label_ids;
    converted_label_ids.reserve(new_vertex.label_ids.size());
    for (const auto &label_id : new_vertex.label_ids) {
      converted_label_ids.emplace_back(label_id.id);
    }

    auto result_schema =
        acc.CreateVertexAndValidate(prim_label, converted_label_ids,
                                    ConvertPropertyVector(std::move(new_vertex.primary_key)), converted_property_map);

    if (result_schema.HasError()) {
      auto &error = result_schema.GetError();

      std::visit(
          []<typename T>(T &&) {
            using ErrorType = std::remove_cvref_t<T>;
            if constexpr (std::is_same_v<ErrorType, SchemaViolation>) {
              spdlog::debug("Creating vertex failed with error: SchemaViolation");
            } else if constexpr (std::is_same_v<ErrorType, Error>) {
              spdlog::debug("Creating vertex failed with error: Error");
            } else {
              static_assert(kAlwaysFalse<T>, "Missing type from variant visitor");
            }
          },
          error);

      action_successful = false;
      break;
    }
  }

  return memgraph::msgs::CreateVerticesResponse{.success = action_successful};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::UpdateVerticesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);

  bool action_successful = true;

  for (auto &vertex : req.new_properties) {
    if (!action_successful) {
      break;
    }

    auto vertex_to_update = acc.FindVertex(ConvertPropertyVector(std::move(vertex.primary_key)), View::OLD);
    if (!vertex_to_update) {
      action_successful = false;
      spdlog::debug("Vertex could not be found while trying to update its properties. Transaction id: {}",
                    req.transaction_id.logical_id);
      continue;
    }

    for (auto &update_prop : vertex.property_updates) {
      // TODO(gvolfing) Maybe check if the setting is valid if SetPropertyAndValidate()
      // does not do that alreaedy.
      auto result_schema =
          vertex_to_update->SetPropertyAndValidate(update_prop.first, ToPropertyValue(std::move(update_prop.second)));
      if (result_schema.HasError()) {
        auto &error = result_schema.GetError();

        std::visit(
            [&action_successful]<typename T>(T &&) {
              using ErrorType = std::remove_cvref_t<T>;
              if constexpr (std::is_same_v<ErrorType, SchemaViolation>) {
                action_successful = false;
                spdlog::debug("Updating vertex failed with error: SchemaViolation");
              } else if constexpr (std::is_same_v<ErrorType, Error>) {
                action_successful = false;
                spdlog::debug("Updating vertex failed with error: Error");
              } else {
                static_assert(kAlwaysFalse<T>, "Missing type from variant visitor");
              }
            },
            error);

        break;
      }
    }
  }

  return memgraph::msgs::UpdateVerticesResponse{.success = action_successful};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::DeleteVerticesRequest &&req) {
  bool action_successful = true;
  auto acc = shard_->Access(req.transaction_id);

  for (auto &propval : req.primary_keys) {
    if (!action_successful) {
      break;
    }

    auto vertex_acc = acc.FindVertex(ConvertPropertyVector(std::move(propval)), View::OLD);

    if (!vertex_acc) {
      spdlog::debug("Error while trying to delete vertex. Vertex to delete does not exist. Transaction id: {}",
                    req.transaction_id.logical_id);
      action_successful = false;
    } else {
      // TODO(gvolfing)
      // Since we will not have different kinds of deletion types in one transaction,
      // we dont have to enter the switch statement on every iteration. Optimize this.
      switch (req.deletion_type) {
        case memgraph::msgs::DeleteVerticesRequest::DeletionType::DELETE: {
          auto result = acc.DeleteVertex(&vertex_acc.value());
          if (result.HasError() || !(result.GetValue().has_value())) {
            action_successful = false;
            spdlog::debug("Error while trying to delete vertex. Transaction id: {}", req.transaction_id.logical_id);
          }

          break;
        }
        case memgraph::msgs::DeleteVerticesRequest::DeletionType::DETACH_DELETE: {
          auto result = acc.DetachDeleteVertex(&vertex_acc.value());
          if (result.HasError() || !(result.GetValue().has_value())) {
            action_successful = false;
            spdlog::debug("Error while trying to detach and delete vertex. Transaction id: {}",
                          req.transaction_id.logical_id);
          }

          break;
        }
      }
    }
  }

  return memgraph::msgs::DeleteVerticesResponse{.success = action_successful};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::CreateEdgesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);
  bool action_successful = true;

  for (auto &edge : req.edges) {
    auto vertex_acc_from_primary_key = edge.src.second;
    auto vertex_from_acc = acc.FindVertex(ConvertPropertyVector(std::move(vertex_acc_from_primary_key)), View::OLD);

    auto vertex_acc_to_primary_key = edge.dst.second;
    auto vertex_to_acc = acc.FindVertex(ConvertPropertyVector(std::move(vertex_acc_to_primary_key)), View::OLD);

    if (!vertex_from_acc || !vertex_to_acc) {
      action_successful = false;
      spdlog::debug("Error while trying to insert edge, vertex does not exist. Transaction id: {}",
                    req.transaction_id.logical_id);
      break;
    }

    auto from_vertex_id = VertexId(edge.src.first.id, ConvertPropertyVector(std::move(edge.src.second)));
    auto to_vertex_id = VertexId(edge.dst.first.id, ConvertPropertyVector(std::move(edge.dst.second)));
    auto edge_acc =
        acc.CreateEdge(from_vertex_id, to_vertex_id, EdgeTypeId::FromUint(edge.type.id), Gid::FromUint(edge.id.gid));

    if (edge_acc.HasError()) {
      action_successful = false;
      spdlog::debug("Creating edge was not successful. Transaction id: {}", req.transaction_id.logical_id);
      break;
    }

    // Add properties to the edge if there is any
    if (edge.properties) {
      for (auto &[edge_prop_key, edge_prop_val] : edge.properties.value()) {
        auto set_result = edge_acc->SetProperty(edge_prop_key, ToPropertyValue(std::move(edge_prop_val)));
        if (set_result.HasError()) {
          action_successful = false;
          spdlog::debug("Adding property to edge was not successful. Transaction id: {}",
                        req.transaction_id.logical_id);
          break;
        }
      }
    }
  }

  return memgraph::msgs::CreateEdgesResponse{.success = action_successful};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::DeleteEdgesRequest &&req) {
  bool action_successful = true;
  auto acc = shard_->Access(req.transaction_id);

  for (auto &edge : req.edges) {
    if (!action_successful) {
      break;
    }

    auto edge_acc = acc.DeleteEdge(VertexId(edge.src.first.id, ConvertPropertyVector(std::move(edge.src.second))),
                                   VertexId(edge.dst.first.id, ConvertPropertyVector(std::move(edge.dst.second))),
                                   Gid::FromUint(edge.id.gid));
    if (edge_acc.HasError() || !edge_acc.HasValue()) {
      spdlog::debug("Error while trying to delete edge. Transaction id: {}", req.transaction_id.logical_id);
      action_successful = false;
      continue;
    }
  }

  return memgraph::msgs::DeleteEdgesResponse{.success = action_successful};
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::UpdateEdgesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);

  bool action_successful = true;

  for (auto &edge : req.new_properties) {
    if (!action_successful) {
      break;
    }

    auto vertex_acc = acc.FindVertex(ConvertPropertyVector(std::move(edge.src.second)), View::OLD);
    if (!vertex_acc) {
      action_successful = false;
      spdlog::debug("Encountered an error while trying to acquire VertexAccessor with transaction id: {}",
                    req.transaction_id.logical_id);
      continue;
    }

    // Since we are using the source vertex of the edge we are only intrested
    // in the vertex's out-going edges
    auto edges_res = vertex_acc->OutEdges(View::OLD);
    if (edges_res.HasError()) {
      action_successful = false;
      spdlog::debug("Encountered an error while trying to acquire EdgeAccessor with transaction id: {}",
                    req.transaction_id.logical_id);
      continue;
    }

    auto &edge_accessors = edges_res.GetValue();

    // Look for the appropriate edge accessor
    bool edge_accessor_did_match = false;
    for (auto &edge_accessor : edge_accessors) {
      if (edge_accessor.Gid().AsUint() == edge.edge_id.gid) {  // Found the appropriate accessor
        edge_accessor_did_match = true;
        for (auto &[key, value] : edge.property_updates) {
          // TODO(gvolfing)
          // Check if the property was set if SetProperty does not do that itself.
          auto res = edge_accessor.SetProperty(key, ToPropertyValue(std::move(value)));
          if (res.HasError()) {
            spdlog::debug("Encountered an error while trying to set the property of an Edge with transaction id: {}",
                          req.transaction_id.logical_id);
          }
        }
      }
    }

    if (!edge_accessor_did_match) {
      action_successful = false;
      spdlog::debug("Could not find the Edge with the specified Gid. Transaction id: {}",
                    req.transaction_id.logical_id);
      continue;
    }
  }

  return memgraph::msgs::UpdateEdgesResponse{.success = action_successful};
}

msgs::ReadResponses ShardRsm::HandleRead(msgs::ScanVerticesRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);
  bool action_successful = true;

  std::vector<memgraph::msgs::ScanResultRow> results;
  std::optional<memgraph::msgs::VertexId> next_start_id;

  const auto view = View(req.storage_view);
  auto vertex_iterable = acc.Vertices(view);
  bool did_reach_starting_point = false;
  uint64_t sample_counter = 0;

  const auto start_ids = ConvertPropertyVector(std::move(req.start_id.second));

  for (auto it = vertex_iterable.begin(); it != vertex_iterable.end(); ++it) {
    const auto &vertex = *it;

    if (start_ids == vertex.PrimaryKey(View(req.storage_view)).GetValue()) {
      did_reach_starting_point = true;
    }

    if (did_reach_starting_point) {
      std::optional<std::map<PropertyId, Value>> found_props;

      if (req.props_to_return) {
        found_props = CollectPropertiesFromAccessor(vertex, req.props_to_return.value(), view);
      } else {
        found_props = CollectAllPropertiesFromAccessor(vertex, view);
      }

      // TODO(gvolfing) -VERIFY- Is this actually correct here?
      // Vertex is seperated from the properties in the response.
      // Is it useful to return just a vertex without the properties?
      // If so, this logic here is incorrect.

      // I don't think this will ever be std::nullopt ...
      if (!found_props) {
        continue;
      }

      results.emplace_back(msgs::ScanResultRow{.vertex = ConstructValueVertex(vertex, view).vertex_v,
                                               .props = FromMap(found_props.value())});

      ++sample_counter;
      // TODO(gvolfing) -VERIFY- is it safe to check against an optional
      // like that? if it is std::nullopt the ScanVertices should return
      // all of the vertices in the shard.
      if (sample_counter == req.batch_limit) {
        // Reached the maximum specified batch size.
        // Get the next element before exiting.
        const auto &next_vertex = *(++it);
        next_start_id = ConstructValueVertex(next_vertex, view).vertex_v.id;

        break;
      }
    }
  }

  memgraph::msgs::ScanVerticesResponse resp{};
  resp.success = action_successful;

  if (action_successful) {
    resp.next_start_id = next_start_id;
    resp.results = std::move(results);
  }

  return resp;
}

msgs::ReadResponses ShardRsm::HandleRead(msgs::ExpandOneRequest &&req) {
  auto acc = shard_->Access(req.transaction_id);
  bool action_successful = true;

  std::vector<memgraph::msgs::ExpandOneResultRow> results;

  for (auto &src_vertex : req.src_vertices) {
    auto result = GetExpandOneResult(acc, src_vertex, req);

    if (!result) {
      action_successful = false;
      break;
    }

    results.emplace_back(result.value());
  }

  memgraph::msgs::ExpandOneResponse resp{};
  if (action_successful) {
    resp.result = std::move(results);
  }

  return resp;
}

msgs::WriteResponses ShardRsm::ApplyWrite(msgs::CommitRequest &&req) {
  shard_->Access(req.transaction_id).Commit(req.commit_timestamp);
  return memgraph::msgs::CommitResponse{true};
};

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
msgs::ReadResponses ShardRsm::HandleRead(msgs::GetPropertiesRequest && /*req*/) {
  return memgraph::msgs::GetPropertiesResponse{};
}

}  //    namespace memgraph::storage::v3

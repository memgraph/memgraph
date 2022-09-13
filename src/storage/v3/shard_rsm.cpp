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

#include <iterator>
#include <utility>

#include "query/v2/plan/operator.hpp"
#include "storage/v3/shard_rsm.hpp"
#include "storage/v3/vertex_accessor.hpp"

namespace {
// TODO(gvolfing use come algorithm instead of explicit for loops)
memgraph::storage::v3::PropertyValue ToPropertyValue(const Value &value) {
  using PV = memgraph::storage::v3::PropertyValue;
  PV ret;
  switch (value.type) {
    case Value::Type::NILL:
      return PV();
    case Value::Type::BOOL:
      return PV(value.bool_v);
    case Value::Type::INT64:
      return PV(static_cast<int>(value.int_v));
    case Value::Type::DOUBLE:
      return PV(value.double_v);
    case Value::Type::STRING:
      return PV(value.string_v);
    case Value::Type::LIST: {
      std::vector<PV> list;
      for (auto &elem : value.list_v) {
        list.emplace_back(ToPropertyValue(elem));
      }
      return PV(list);
    }
    case Value::Type::MAP: {
      std::map<std::string, PV> map;
      for (auto &[key, value] : value.map_v) {
        map.emplace(std::make_pair(key, ToPropertyValue(value)));
      }
      return PV(map);
    }
    // These are not PropertyValues
    case Value::Type::VERTEX:
    case Value::Type::EDGE:
    case Value::Type::PATH:
    default:
      MG_ASSERT(false, "Missing or type from Value");
  }
  return ret;
}

Value ToValue(const memgraph::storage::v3::PropertyValue &pv) {
  // There should be a better solution.
  if (pv.IsBool()) {
    return Value(pv.ValueBool());
  }
  if (pv.IsDouble()) {
    return Value(pv.ValueDouble());
  }
  if (pv.IsInt()) {
    return Value(pv.ValueInt());
  }
  if (pv.IsList()) {
    std::vector<Value> list(pv.ValueList().size());
    for (const auto &elem : pv.ValueList()) {
      list.emplace_back(ToValue(elem));
    }

    return Value(list);
  }
  if (pv.IsMap()) {
    std::map<std::string, Value> map;
    for (const auto &[key, val] : pv.ValueMap()) {
      // maybe use std::make_pair once the && issue is resolved.
      map.emplace(key, ToValue(val));
    }

    return Value(map);
  }
  if (pv.IsNull()) {
    // NOOP -> default ctor
  }
  if (pv.IsString()) {
    return Value(pv.ValueString());
  }
  if (pv.IsTemporalData()) {
    // TBD -> we need to specify this in the messages, not a priority.
    MG_ASSERT(false, "Temporal datatypes are not yet implemented on Value!");
  }

  MG_ASSERT(false, "Typematching Value and PropertyValue encounterd unspecified type!");
  return Value{};
}

std::vector<std::pair<memgraph::storage::v3::PropertyId, memgraph::storage::v3::PropertyValue>> ConvertPropertyMap(
    std::vector<std::pair<PropertyId, Value>> &properties) {
  std::vector<std::pair<memgraph::storage::v3::PropertyId, memgraph::storage::v3::PropertyValue>> ret;
  ret.reserve(properties.size());

  for (auto &[key, value] : properties) {
    memgraph::storage::v3::PropertyValue converted_value(ToPropertyValue(value));
    ret.push_back(std::make_pair(key, converted_value));
  }

  return ret;
}

std::vector<memgraph::storage::v3::PropertyValue> ConvertPropertyVector(std::vector<Value> &vec) {
  std::vector<memgraph::storage::v3::PropertyValue> ret;
  ret.reserve(vec.size());

  for (auto &elem : vec) {
    memgraph::storage::v3::PropertyValue converted_value(ToPropertyValue(elem));
    ret.push_back(converted_value);
  }

  return ret;
}

std::vector<Value> ConvertValueVector(const std::vector<memgraph::storage::v3::PropertyValue> &vec) {
  std::vector<Value> ret;
  ret.reserve(vec.size());

  for (auto &elem : vec) {
    Value converted_value(ToValue(elem));
    ret.push_back(converted_value);
  }

  return ret;
}

std::optional<std::map<PropertyId, Value>> CollectPropertiesFromAccessor(
    const memgraph::storage::v3::VertexAccessor &acc,
    const std::optional<std::vector<memgraph::storage::v3::PropertyId>> &props, memgraph::storage::v3::View view) {
  std::map<PropertyId, Value> ret;

  for (const auto &prop : props.value()) {
    auto result = acc.GetProperty(prop, view);
    if (result.HasError()) {
      spdlog::debug("Encountered an Error while trying to get a vertex property.");
      continue;
    }
    if (result.HasValue()) {
      if (result.GetValue().IsNull()) {
        spdlog::debug("The specified property does not exist but it should");
        continue;
      }

      ret.emplace(prop, ToValue(result.GetValue()));
    }
  }

  return ret;
}

std::optional<std::map<PropertyId, Value>> CollectAllPropertiesFromAccessor(
    const memgraph::storage::v3::VertexAccessor &acc, memgraph::storage::v3::View view) {
  std::map<PropertyId, Value> ret;
  auto iter = acc.Properties(view);
  if (iter.HasError()) {
    spdlog::debug("Encountered an Error while trying to get vertex properties.");
  }

  for (const auto &[prop_key, prop_val] : iter.GetValue()) {
    ret.emplace(prop_key, ToValue(prop_val));
  }

  return ret;
}

Value ConstructValueVertex(const memgraph::storage::v3::VertexAccessor &acc, memgraph::storage::v3::View view) {
  // Get the vertex id
  auto prim_label = acc.PrimaryLabel(view).GetValue();
  Label value_label = {.id = prim_label};

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

}  // namespace

namespace memgraph::storage::v3 {

WriteResponses ShardRsm::ApplyWrite(CreateVerticesRequest &&req) {
  auto acc = shard_->Access();

  // Workaround untill we have access to CreateVertexAndValidate()
  // with the new signature that does not require the primary label.
  auto prim_label = acc.GetPrimaryLabel();

  bool action_successful = true;

  for (auto &new_vertex : req.new_vertices) {
    if (!action_successful) {
      break;
    }
    /// TODO(gvolfing) Remove this. In the new implementation each shard
    /// should have a predetermined primary label, so there is no point in
    /// specifying it in the accessor functions. Their signature will
    /// change.
    /// TODO(gvolfing) Consider other methods than converting. Change either
    /// the way that the property map is stored in the messages, or the
    /// signature of CreateVertexAndValidate.
    auto converted_property_map = ConvertPropertyMap(new_vertex.properties);

    // TODO(gvolfing) make sure you don't create vectors bigger than they actually should be, e.g.
    // std::vector<memgraph::storage::v3::LabelId> converted_label_ids(new_vertex.label_ids.size());
    // TODO(gvolfing) make sure if this conversion is actually needed.
    std::vector<memgraph::storage::v3::LabelId> converted_label_ids;
    converted_label_ids.reserve(new_vertex.label_ids.size());
    for (const auto &label_id : new_vertex.label_ids) {
      converted_label_ids.emplace_back(label_id.id);
    }

    auto result_schema = acc.CreateVertexAndValidate(
        prim_label, converted_label_ids, ConvertPropertyVector(new_vertex.primary_key), converted_property_map);

    if (result_schema.HasError()) {
      auto &error = result_schema.GetError();

      std::visit(
          [&action_successful]<typename T>(T &&) {
            using ErrorType = std::remove_cvref_t<T>;
            if constexpr (std::is_same_v<ErrorType, SchemaViolation>) {
              action_successful = false;
              spdlog::debug("Creating vertex failed with error: SchemaViolation");
            } else if constexpr (std::is_same_v<ErrorType, Error>) {
              action_successful = false;
              spdlog::debug("Creating vertex failed with error: Error");
            } else {
              static_assert(kAlwaysFalse<T>, "Missing type from variant visitor");
            }
          },
          error);

      action_successful = false;
    }
  }

  CreateVerticesResponse resp{};
  resp.success = action_successful;

  if (action_successful) {
    auto result = acc.Commit(req.transaction_id.logical_id);
    if (result.HasError()) {
      resp.success = false;
      spdlog::debug(&"ConstraintViolation, commiting vertices was unsuccesfull with transaction id: "[req.transaction_id
                                                                                                          .logical_id]);
    }
  }
  return resp;
}

WriteResponses ShardRsm::ApplyWrite(DeleteVerticesRequest &&req) {
  bool action_successful = true;
  auto acc = shard_->Access();

  for (auto &propval : req.primary_keys) {
    if (!action_successful) {
      break;
    }

    auto vertex_acc = acc.FindVertex(ConvertPropertyVector(propval), View::OLD);

    if (!vertex_acc) {
      spdlog::debug(
          &"Error while trying to delete vertex. Vertex to delete does not exist. Transaction id: "[req.transaction_id
                                                                                                        .logical_id]);
      action_successful = false;
    } else {
      // TODO(gvolfing)
      // Since we will not have different kinds of deletion types in one transaction,
      // we dont have to enter the switch statement on every iteration. Optimize this.
      switch (req.deletion_type) {
        case DeleteVerticesRequest::DeletionType::DELETE: {
          auto result = acc.DeleteVertex(&vertex_acc.value());
          if (result.HasError() || !(result.GetValue().has_value())) {
            action_successful = false;
            spdlog::debug(&"Error while trying to delete vertex. Transaction id: "[req.transaction_id.logical_id]);
          }

          break;
        }
        case DeleteVerticesRequest::DeletionType::DETACH_DELETE: {
          auto result = acc.DetachDeleteVertex(&vertex_acc.value());
          if (result.HasError() || !(result.GetValue().has_value())) {
            action_successful = false;
            spdlog::debug(
                &"Error while trying to detach and delete vertex. Transaction id: "[req.transaction_id.logical_id]);
          }

          break;
        }
        default:
          MG_ASSERT(false, "Non-existent deletion type.");
      }
    }
  }

  DeleteVerticesResponse resp{};
  resp.success = action_successful;

  if (action_successful) {
    auto result = acc.Commit(req.transaction_id.logical_id);
    if (result.HasError()) {
      resp.success = false;
      spdlog::debug(&"ConstraintViolation, commiting vertices was unsuccesfull with transaction id: "[req.transaction_id
                                                                                                          .logical_id]);
    }
  }

  return resp;
}

WriteResponses ShardRsm::ApplyWrite(UpdateVerticesRequest &&req) {
  /*
  auto acc = shard_->Access();

  bool action_successful = true;

  for (auto &vertex : req.new_properties) {
    if (!action_successful) {
      break;
    }

    auto vertex_to_update = acc.FindVertex(ConvertPropertyVector(vertex.vertex), View::OLD);
    if (!vertex_to_update) {
      action_successful = false;
      spdlog::debug(
          &"Vertex could not be found while trying to update its properties. Transaction id: "[req.transaction_id
                                                                                                   .logical_id]);
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
  */
  UpdateVerticesResponse resp{};

  // resp.success = action_successful;

  // if (action_successful) {
  //   auto result = acc.Commit(req.transaction_id.logical_id);
  //   if (result.HasError()) {
  //     resp.success = false;
  //     spdlog::debug(&"ConstraintViolation, commiting vertices was unsuccesfull with transaction id:
  //     "[req.transaction_id
  //                                                                                                         .logical_id]);
  //   }
  // }

  return resp;
}

WriteResponses ShardRsm::ApplyWrite(CreateEdgesRequest &&req) {
  auto acc = shard_->Access();
  bool action_successful = true;

  for (const auto &edge : req.edges) {
    if (!action_successful) {
      break;
    }

    auto vertex_acc_from_primary_key = edge.id.src.second;
    auto vertex_from_acc = acc.FindVertex(ConvertPropertyVector(vertex_acc_from_primary_key), View::OLD);

    auto vertex_acc_to_primary_key = edge.id.dst.second;
    auto vertex_to_acc = acc.FindVertex(ConvertPropertyVector(vertex_acc_to_primary_key), View::OLD);

    if (!vertex_from_acc || !vertex_to_acc) {
      action_successful = false;
      spdlog::debug(
          &"Error while trying to insert edge, vertex does not exist. Transaction id: "[req.transaction_id.logical_id]);
      continue;
    }

    auto edge_type_id = EdgeTypeId::FromUint(edge.type.id);
    auto edge_acc = acc.CreateEdge(&vertex_from_acc.value(), &vertex_to_acc.value(), edge_type_id);

    if (edge_acc.HasError()) {
      action_successful = false;
      spdlog::debug(&"Creating edge was not successful. Transaction id: "[req.transaction_id.logical_id]);
      continue;
    }
  }

  CreateEdgesResponse resp{};

  resp.success = action_successful;

  if (action_successful) {
    auto result = acc.Commit(req.transaction_id.logical_id);
    if (result.HasError()) {
      resp.success = false;
      spdlog::debug(
          &"ConstraintViolation, commiting edge creation was unsuccesfull with transaction id: "[req.transaction_id
                                                                                                     .logical_id]);
    }
  }

  return resp;
}

// TODO(gvolfing)
// Uncomment this once the new overload for DeleteEdges() is in place.
// DeleteEdges Will get a new signature -> DeleteEdges(FromVertex, ToVertex, Gid)
WriteResponses ShardRsm::ApplyWrite(DeleteEdgesRequest &&req) {
  // bool action_successful = true;
  // auto acc = shard_->Access();

  // for(const auto& edge : req.edges)
  // {
  //   if (!action_successful) {
  //     break;
  //   }

  //   auto edge_acc = acc.DeleteEdge(edge.id.src, edge.id.dst, edge.id.gid);
  //   if(!edge_acc.HasError() || !edge_acc.HasValue())
  //   {
  //     spdlog::debug(&"Error while trying to delete edge. Transaction id: "[req.transaction_id.logical_id]);
  //     action_successful = false;
  //     continue;
  //   }
  // }

  DeleteEdgesResponse resp{};

  // resp.success = action_successful;

  // if (action_successful) {
  //   auto result = acc.Commit(req.transaction_id.logical_id);
  //   if (result.HasError()) {
  //     resp.success = false;
  //     spdlog::debug(
  //         &"ConstraintViolation, commiting edge creation was unsuccesfull with transaction id: "[req.transaction_id
  //                                                                                                    .logical_id]);
  //   }

  return resp;
}

// TODO(gvolfing) refactor this abomination
WriteResponses ShardRsm::ApplyWrite(UpdateEdgesRequest &&req) {
  /*
  auto acc = shard_->Access();

  bool action_successful = true;

  for (auto &edge : req.new_properties) {
    if (!action_successful) {
      break;
    }

    auto vertex_acc = acc.FindVertex(ConvertPropertyVector(edge.edge.id.src), View::OLD);
    if (!vertex_acc) {
      action_successful = false;
      // TODO(gvolfing) add debug error msg
      continue;
    }

    // Since we are using the source vertex of the edge we are only intrested
    // in the vertex's outgoind edges
    auto edges_res = vertex_acc->OutEdges(View::OLD);
    if (edges_res.HasError()) {
      action_successful = false;
      // TODO(gvolfing) add debug error msg
      continue;
    }

    auto &edge_accessors = edges_res.GetValue();

    // Look for the appropriate edge accessor
    bool edge_accesspr_did_match = false;
    for (auto &edge_accessor : edge_accessors) {
      if (edge_accessor.Gid().AsUint() == edge.edge.id.gid) {  // Found the appropriate accessor
        edge_accesspr_did_match = true;
        for (auto &[key, value] : edge.property_updates) {
          // TODO(gvolfing)
          // Check if the property was set if SetProperty does not do that itself.
          auto res = edge_accessor.SetProperty(key, ToPropertyValue(std::move(value)));
          if (res.HasError()) {
            //....
          }
        }
      }
    }

    if (!edge_accesspr_did_match) {
      action_successful = false;
      // TODO(gvolfing) add debug error msg
      continue;
    }
  }
  */
  UpdateEdgesResponse resp{};

  // resp.success = action_successful;

  // if (action_successful) {
  //   auto result = acc.Commit(req.transaction_id.logical_id);
  //   if (result.HasError()) {
  //     resp.success = false;
  //     spdlog::debug(
  //         &"ConstraintViolation, commiting edge update was unsuccesfull with transaction id: "[req.transaction_id
  //                                                                                                  .logical_id]);
  //   }
  // }

  return resp;
}

ReadResponses ShardRsm::HandleRead(ScanVerticesRequest &&req) {
  auto acc = shard_->Access();
  bool action_successful = true;

  std::vector<ScanResultRow> results;
  std::optional<VertexId> next_start_id;

  const auto view = View(req.storage_view);
  auto vertex_iterable = acc.Vertices(view);
  bool did_reach_starting_point = false;
  uint64_t sample_counter = 0;

  for (auto it = vertex_iterable.begin(); it != vertex_iterable.end(); ++it) {
    const auto &vertex = *it;

    if (ConvertPropertyVector(req.start_id.second) == vertex.PrimaryKey(View(req.storage_view)).GetValue()) {
      did_reach_starting_point = true;
    }

    if (did_reach_starting_point) {
      std::optional<std::map<PropertyId, Value>> found_props;

      if (req.props_to_return) {
        found_props = CollectPropertiesFromAccessor(vertex, req.props_to_return, view);
      } else {
        found_props = CollectAllPropertiesFromAccessor(vertex, view);
      }

      if (!found_props) {
        continue;
      }

      results.emplace_back(ScanResultRow{.vertex = ConstructValueVertex(vertex, view), .props = found_props.value()});

      ++sample_counter;
      if (sample_counter == req.batch_limit) {
        // Reached the maximum specified batch size.
        // Get the next element before exiting.
        const auto &next_vertex = *(++it);
        next_start_id = ConstructValueVertex(next_vertex, view).vertex_v.id;

        break;
      }
    }
  }

  ScanVerticesResponse resp{};
  resp.success = action_successful;

  if (action_successful) {
    resp.next_start_id = next_start_id;
    resp.results = std::move(results);
  }

  return resp;
}

ReadResponses ShardRsm::HandleRead(ExpandOneRequest &&req) { return ExpandOneResponse{}; }
ReadResponses ShardRsm::HandleRead(GetPropertiesRequest &&req) { return GetPropertiesResponse{}; }

}  //    namespace memgraph::storage::v3

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

#include "storage/v3/shard_rsm.hpp"
#include "storage/v3/vertex_accessor.hpp"

using memgraph::messages::Label;
using memgraph::messages::PropertyId;
using memgraph::messages::Value;
using memgraph::messages::VertexId;

namespace {
// TODO(gvolfing use come algorithm instead of explicit for loops)
memgraph::storage::v3::PropertyValue ToPropertyValue(Value &&value) {
  using PV = memgraph::storage::v3::PropertyValue;
  PV ret;
  switch (value.type) {
    case Value::Type::Null:
      return PV();
    case Value::Type::Bool:
      return PV(value.bool_v);
    case Value::Type::Int64:
      return PV(static_cast<int64_t>(value.int_v));
    case Value::Type::Double:
      return PV(value.double_v);
    case Value::Type::String:
      return PV(value.string_v);
    case Value::Type::List: {
      std::vector<PV> list;
      for (auto &elem : value.list_v) {
        list.emplace_back(ToPropertyValue(std::move(elem)));
      }
      return PV(list);
    }
    case Value::Type::Map: {
      std::map<std::string, PV> map;
      for (auto &[key, value] : value.map_v) {
        map.emplace(std::make_pair(key, ToPropertyValue(std::move(value))));
      }
      return PV(map);
    }
    // These are not PropertyValues
    case Value::Type::Vertex:
    case Value::Type::Edge:
    case Value::Type::Path:
      MG_ASSERT(false, "Not PropertyValue");
  }
  return ret;
}

Value ToValue(const memgraph::storage::v3::PropertyValue &pv) {
  using memgraph::storage::v3::PropertyValue;

  switch (pv.type()) {
    case PropertyValue::Type::Bool:
      return Value(pv.ValueBool());
    case PropertyValue::Type::Double:
      return Value(pv.ValueDouble());
    case PropertyValue::Type::Int:
      return Value(pv.ValueInt());
    case PropertyValue::Type::List: {
      std::vector<Value> list(pv.ValueList().size());
      for (const auto &elem : pv.ValueList()) {
        list.emplace_back(ToValue(elem));
      }

      return Value(list);
    }
    case PropertyValue::Type::Map: {
      std::map<std::string, Value> map;
      for (const auto &[key, val] : pv.ValueMap()) {
        // maybe use std::make_pair once the && issue is resolved.
        map.emplace(key, ToValue(val));
      }

      return Value(map);
    }
    case PropertyValue::Type::Null:
      return Value{};
    case PropertyValue::Type::String:
      return Value(pv.ValueString());
    case PropertyValue::Type::TemporalData: {
      // TBD -> we need to specify this in the messages, not a priority.
      MG_ASSERT(false, "Temporal datatypes are not yet implemented on Value!");
      return Value{};
    }
  }
}

std::vector<std::pair<memgraph::storage::v3::PropertyId, memgraph::storage::v3::PropertyValue>> ConvertPropertyMap(
    std::vector<std::pair<PropertyId, Value>> &properties) {
  std::vector<std::pair<memgraph::storage::v3::PropertyId, memgraph::storage::v3::PropertyValue>> ret;
  ret.reserve(properties.size());

  for (auto &[key, value] : properties) {
    ret.push_back(std::make_pair(key, ToPropertyValue(std::move(value))));
  }

  return ret;
}

std::vector<memgraph::storage::v3::PropertyValue> ConvertPropertyVector(std::vector<Value> &&vec) {
  std::vector<memgraph::storage::v3::PropertyValue> ret;
  ret.reserve(vec.size());

  for (auto &elem : vec) {
    ret.push_back(ToPropertyValue(std::move(elem)));
  }

  return ret;
}

std::vector<Value> ConvertValueVector(const std::vector<memgraph::storage::v3::PropertyValue> &vec) {
  std::vector<Value> ret;
  ret.reserve(vec.size());

  for (auto &elem : vec) {
    ret.push_back(ToValue(elem));
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
    ret.emplace(prop, ToValue(value));
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

}  // namespace

namespace memgraph::storage::v3 {

WriteResponses ShardRsm::ApplyWrite(CreateVerticesRequest &&req) {
  auto acc = shard_->Access();

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
    auto converted_property_map = ConvertPropertyMap(new_vertex.properties);

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

    auto vertex_acc = acc.FindVertex(ConvertPropertyVector(std::move(propval)), View::OLD);

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

WriteResponses ShardRsm::ApplyWrite(CreateEdgesRequest &&req) {
  auto acc = shard_->Access();
  bool action_successful = true;

  for (auto &edge : req.edges) {
    auto vertex_acc_from_primary_key = edge.src.second;
    auto vertex_from_acc = acc.FindVertex(ConvertPropertyVector(std::move(vertex_acc_from_primary_key)), View::OLD);

    auto vertex_acc_to_primary_key = edge.dst.second;
    auto vertex_to_acc = acc.FindVertex(ConvertPropertyVector(std::move(vertex_acc_to_primary_key)), View::OLD);

    if (!vertex_from_acc || !vertex_to_acc) {
      action_successful = false;
      spdlog::debug(
          &"Error while trying to insert edge, vertex does not exist. Transaction id: "[req.transaction_id.logical_id]);
      break;
    }

    auto from_vertex_id = VertexId(edge.src.first.id, ConvertPropertyVector(std::move(edge.src.second)));
    auto to_vertex_id = VertexId(edge.dst.first.id, ConvertPropertyVector(std::move(edge.dst.second)));
    auto edge_acc =
        acc.CreateEdge(from_vertex_id, to_vertex_id, EdgeTypeId::FromUint(edge.type.id), Gid::FromUint(edge.id.gid));

    if (edge_acc.HasError()) {
      action_successful = false;
      spdlog::debug(&"Creating edge was not successful. Transaction id: "[req.transaction_id.logical_id]);
      break;
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

ReadResponses ShardRsm::HandleRead(ScanVerticesRequest &&req) {
  auto acc = shard_->Access();
  bool action_successful = true;

  std::vector<ScanResultRow> results;
  std::optional<messages::VertexId> next_start_id;

  const auto view = View(req.storage_view);
  auto vertex_iterable = acc.Vertices(view);
  bool did_reach_starting_point = false;
  uint64_t sample_counter = 0;

  for (auto it = vertex_iterable.begin(); it != vertex_iterable.end(); ++it) {
    const auto &vertex = *it;

    if (ConvertPropertyVector(std::move(req.start_id.second)) == vertex.PrimaryKey(View(req.storage_view)).GetValue()) {
      did_reach_starting_point = true;
    }

    if (did_reach_starting_point) {
      std::optional<std::map<PropertyId, Value>> found_props;

      if (req.props_to_return) {
        found_props = CollectPropertiesFromAccessor(vertex, req.props_to_return.value(), view);
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

WriteResponses ShardRsm::ApplyWrite(UpdateVerticesRequest &&req) { return UpdateVerticesResponse{}; }
WriteResponses ShardRsm::ApplyWrite(DeleteEdgesRequest &&req) { return DeleteEdgesResponse{}; }
WriteResponses ShardRsm::ApplyWrite(UpdateEdgesRequest &&req) { return UpdateEdgesResponse{}; }
ReadResponses ShardRsm::HandleRead(ExpandOneRequest &&req) { return ExpandOneResponse{}; }
ReadResponses ShardRsm::HandleRead(GetPropertiesRequest &&req) { return GetPropertiesResponse{}; }

}  //    namespace memgraph::storage::v3

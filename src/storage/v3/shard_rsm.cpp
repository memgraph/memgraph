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

#include <utility>

#include "query/v2/plan/operator.hpp"
#include "storage/v3/shard_rsm.hpp"

namespace {
// TODO(gvolfing use come algorithm instead of explicit for loops)
memgraph::storage::v3::PropertyValue to_property_value(const Value &value) {
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
      for (const auto &elem : value.list_v) {
        list.push_back(to_property_value(elem));
      }
      return PV(list);
    }
    case Value::Type::MAP: {
      std::map<std::string, PV> map;
      for (const auto &[key, value] : value.map_v) {
        map.emplace(std::make_pair(key, to_property_value(value)));
      }
      return PV(map);
    }
    /// These are not PropertyValues
    case Value::Type::VERTEX:
    case Value::Type::EDGE:
    case Value::Type::PATH:
    default:
      MG_ASSERT(false, "Missing or type from Value");
  }
  return ret;
}

std::vector<std::pair<memgraph::storage::v3::PropertyId, memgraph::storage::v3::PropertyValue>> ConvertPropertyMap(
    const std::vector<std::pair<PropertyId, Value>> &properties) {
  std::vector<std::pair<memgraph::storage::v3::PropertyId, memgraph::storage::v3::PropertyValue>> ret(
      properties.size());

  for (const auto &[key, value] : properties) {
    memgraph::storage::v3::PropertyValue converted_value(to_property_value(value));

    ret.emplace_back(std::make_pair(key, converted_value));
  }

  return ret;
}

std::vector<memgraph::storage::v3::PropertyValue> ConvertPropertyVector(const std::vector<Value> &vec) {
  std::vector<memgraph::storage::v3::PropertyValue> ret(vec.size());
  for (const auto &elem : vec) {
    memgraph::storage::v3::PropertyValue converted_value(to_property_value(elem));
    ret.emplace_back(converted_value);
  }

  return ret;
}

}  // namespace

namespace memgraph::storage::v3 {

WriteResponses ShardRsm::ApplyWrite(CreateVerticesRequest &&req) {
  auto acc = shard_.Access();

  bool action_successful = true;

  for (auto &new_vertex : req.new_vertices) {
    if (!action_successful) {
      break;
    }

    /// TODO(gvolfing) Remove this. In the new implementation each shard
    /// will have a predetermined primary label, so there is no point in
    /// specifying it in the accessor functions. Their signature will
    /// change.
    LabelId primary_label;

    /// TODO(gvolfing) Consider other methods than converting. Change either
    /// the way that the property map is stored in the messages, or the
    /// signature of CreateVertexAndValidate.
    auto converted_property_map = ConvertPropertyMap(new_vertex.properties);

    auto result_schema = acc.CreateVertexAndValidate(primary_label, new_vertex.label_ids, converted_property_map);

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
  auto acc = shard_.Access();

  for (auto &propval : req.primary_keys) {
    if (!action_successful) {
      break;
    }

    auto vertex_acc = acc.FindVertex(ConvertPropertyVector(propval), View::OLD);

    if (!vertex_acc) {
      // Vertex does not exist.
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
          // Result<std::optional<VertexAccessor>> DeleteVertex(VertexAccessor *vertex);
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
  auto acc = shard_.Access();

  bool action_successful = true;

  for (const auto &vertex : req.new_properties) {
    if (!action_successful) {
      break;
    }

    auto vertex_to_update = acc.FindVertex(vertex.vertex.second, View::OLD);
    if (!vertex_to_update) {
      action_successful = false;
      spdlog::debug(
          &"Vertex could not be found while trying to update its properties. Transaction id: "[req.transaction_id
                                                                                                   .logical_id]);
      continue;
    }

    for (const auto &update_prop : vertex.property_updates) {
      // TODO(gvolfing) Maybe check if the setting is valid if SetPropertyAndValidate()
      // does not do that alreaedy.
      auto result_schema =
          vertex_to_update->SetPropertyAndValidate(update_prop.first, to_property_value(update_prop.second));
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

  UpdateVerticesResponse resp{};

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
  auto acc = shard_.Access();
  bool action_successful = true;

  for (const auto &edge : req.edges) {
    if (!action_successful) {
      break;
    }

    // auto vertex_acc_from_primary_key = edge.src.second;
    auto vertex_acc_from_primary_key = edge.id.src.second;
    auto vertex_from_acc = acc.FindVertex(vertex_acc_from_primary_key, View::OLD);

    auto vertex_acc_to_primary_key = edge.id.dst.second;
    auto vertex_to_acc = acc.FindVertex(vertex_acc_to_primary_key, View::OLD);

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
// Uncomment this once the new overload for DeleteEdges is in place.
// DeleteEdges Will get a new signature -> DeleteEdges(FromVertex, ToVertex, Gid)
WriteResponses ShardRsm::ApplyWrite(DeleteEdgesRequest &&req) {
  // bool action_successful = true;
  // auto acc = shard_.Access();

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
  auto acc = shard_.Access();

  bool action_successful = true;

  for (const auto &edge : req.new_properties) {
    if (!action_successful) {
      break;
    }

    auto vertex_acc = acc.FindVertex(edge.edge.id.src.second, View::OLD);
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
        for (const auto &[key, value] : edge.property_updates) {
          // TODO(gvolfing)
          // Check if the property was set if SetProperty does not do that itself.
          edge_accessor.SetProperty(key, to_property_value(value));
        }
      }
    }

    if (!edge_accesspr_did_match) {
      action_successful = false;
      // TODO(gvolfing) add debug error msg
      continue;
    }
  }

  UpdateEdgesResponse resp{};

  resp.success = action_successful;

  if (action_successful) {
    auto result = acc.Commit(req.transaction_id.logical_id);
    if (result.HasError()) {
      resp.success = false;
      spdlog::debug(
          &"ConstraintViolation, commiting edge update was unsuccesfull with transaction id: "[req.transaction_id
                                                                                                   .logical_id]);
    }
  }

  return resp;
}

ReadResponses HandleRead(ScanVerticesRequest &&req) {
  ScanVerticesResponse resp{};

  return resp;
}

}  //    namespace memgraph::storage::v3

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

}  // namespace

namespace memgraph::storage::v3 {

WriteResponses ShardRsm::ApplyWrite(CreateVerticesRequest &&req) {
  auto acc = shard_.Access();

  bool action_successful = true;

  for (auto &new_vertex : req.new_vertices) {
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
      spdlog::debug(&"Commiting vertices was unsuccesfull with transaction id: "[req.transaction_id.logical_id]);
    } else {
      spdlog::debug(&"Vertices commited succesfully with transaction id: "[req.transaction_id.logical_id]);
    }
  }
  return resp;
}

WriteResponses ShardRsm::ApplyWrite(DeleteVerticesRequest &&req) {
  DeleteVerticesResponse resp{};

  auto acc = shard_.Access();
  // std::optional<VertexAccessor> FindVertex(std::vector<PropertyValue> primary_key, View view);
  // Find the vertex

  // QUESTION what should be the view?
  // acc.FindVertex(std::vector<PropertyValue> primary_key, View::OLD);

  // Result<std::optional<VertexAccessor>> DeleteVertex(VertexAccessor *vertex);

  return resp;
}

}  //    namespace memgraph::storage::v3

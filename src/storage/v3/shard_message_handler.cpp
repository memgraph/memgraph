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

#include "storage/v3/shard_message_handler.hpp"
#include "query/v2/plan/operator.hpp"

namespace {
std::vector<memgraph::storage::v3::LabelId> ConvertLabelId(std::vector<Label> label_ids) {
  return std::vector<memgraph::storage::v3::LabelId>{};
}

std::vector<std::pair<memgraph::storage::v3::PropertyId, memgraph::storage::v3::PropertyValue>> ConvertPropertyMap(
    const std::map<PropertyId, Value> &properties) {
  return std::vector<std::pair<memgraph::storage::v3::PropertyId, memgraph::storage::v3::PropertyValue>>{};
}

}  // namespace

namespace memgraph::storage::v3 {

WriteResponses ShardMessageHandler::ApplyWrite(CreateVerticesRequest &&req) {
  auto acc = shard_.Access();

  bool action_successful = true;

  for (auto &new_vertex : req.new_vertices) {
    // Primary label missing?
    LabelId primary_label;

    // We need to convert these into a different type. why?
    auto converted_label_ids = ConvertLabelId(new_vertex.label_ids);

    auto converted_property_map = ConvertPropertyMap(new_vertex.properties);

    auto result_schema = acc.CreateVertexAndValidate(primary_label, converted_label_ids, converted_property_map);

    /*
     *   Verify creation
     */
    // Do we have to call Commit() on success/failure ?
    if (!result_schema.HasError()) {
      // Do we have to do something with the vertex accessor in case of succes?
    } else {
      auto &error = result_schema.GetError();

      std::visit(
          [&action_successful]<typename T>(T &&arg) {
            // Do we want to log?
            using ErrorType = std::remove_cvref_t<T>;
            if constexpr (std::is_same_v<ErrorType, SchemaViolation>) {
              action_successful = false;
            } else if constexpr (std::is_same_v<ErrorType, Error>) {
              action_successful = false;
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

  // Do we want to call Commit()?
  // only if every single insertion succeeds?
  // or even on partial success?
  if (action_successful) {
    // Do we want to log something?
    auto result = acc.Commit(req.transaction_id.logical_id);
    if (result.HasError()) {
      resp.success = false;
    }
  }
  return resp;
}

}  //    namespace memgraph::storage::v3

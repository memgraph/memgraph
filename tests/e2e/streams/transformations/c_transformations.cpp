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

#include <exception>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "mg_procedure.h"

const std::string query_part_1{"CREATE (n:MESSAGE {timestamp: '"};
const std::string query_part_2{"', payload: '"};
const std::string query_part_3{"', topic: '"};
const std::string query_part_4{"'})"};
const std::string emptyQuery{""};

std::optional<std::string> create_query(mgp_message &message, struct mgp_result *result) {
  int64_t timestamp{0};
  auto mgp_result = mgp_message_timestamp(&message, &timestamp);
  if (MGP_ERROR_NO_ERROR != mgp_message_timestamp(&message, &timestamp)) {
    static_cast<void>(mgp_result_set_error_msg(result, "Error when mgp_message_timestamp"));
    return std::nullopt;
  }

  const char *payload;
  mgp_result = mgp_message_payload(&message, &payload);
  if (MGP_ERROR_NO_ERROR != mgp_result) {
    static_cast<void>(mgp_result_set_error_msg(result, "Error when mgp_message_payload"));
    return std::nullopt;
  }

  size_t payload_size;
  mgp_result = mgp_message_payload_size(&message, &payload_size);
  if (MGP_ERROR_NO_ERROR != mgp_result) {
    static_cast<void>(mgp_result_set_error_msg(result, "Error when mgp_message_payload_size"));
    return std::nullopt;
  }

  const char *topic_name;
  mgp_result = mgp_message_topic_name(&message, &topic_name);
  if (MGP_ERROR_NO_ERROR != mgp_result) {
    static_cast<void>(mgp_result_set_error_msg(result, "Error when mgp_message_topic_name"));
    return std::nullopt;
  }

  return query_part_1 + std::to_string(timestamp) + query_part_2 + std::string{payload, payload_size} + query_part_3 +
         topic_name + query_part_4;
}

void c_transformation(struct mgp_messages *messages, mgp_graph *, mgp_result *result, mgp_memory *memory) {
  mgp_value *null_value = NULL;
  auto mgp_result = mgp_value_make_null(memory, &null_value);
  if (MGP_ERROR_NO_ERROR != mgp_result) {
    static_cast<void>(mgp_result_set_error_msg(result, "Error when mgp_value_make_null"));
    return;
  }

  try {
    size_t messages_size;
    mgp_result = mgp_messages_size(messages, &messages_size);
    if (MGP_ERROR_NO_ERROR != mgp_result) {
      static_cast<void>(mgp_result_set_error_msg(result, "Error when mgp_messages_size"));
      return;
    }

    for (auto i = 0; i < messages_size; ++i) {
      mgp_message *message{nullptr};
      mgp_result = mgp_messages_at(messages, i, &message);
      if (MGP_ERROR_NO_ERROR != mgp_result) {
        static_cast<void>(mgp_result_set_error_msg(result, "Error when mgp_messages_at"));
        break;
      }

      auto query = create_query(*message, result);
      if (!query.has_value()) {
        static_cast<void>(mgp_result_set_error_msg(result, "Error when converting query"));
        break;
      }

      mgp_result_record *record{nullptr};
      mgp_result = mgp_result_new_record(result, &record);
      if (MGP_ERROR_NO_ERROR != mgp_result) {
        static_cast<void>(mgp_result_set_error_msg(result, "Error when mgp_result_new_record"));
        break;
      }

      mgp_value *query_value = NULL;
      mgp_result = mgp_value_make_string(query.value().c_str(), memory, &query_value);
      if (MGP_ERROR_NO_ERROR != mgp_result) {
        static_cast<void>(mgp_result_set_error_msg(result, "Error when mgp_value_make_string"));
        break;
      }

      mgp_result = mgp_result_record_insert(record, "query", query_value);
      if (MGP_ERROR_NO_ERROR != mgp_result) {
        mgp_value_destroy(query_value);
        static_cast<void>(mgp_result_set_error_msg(result, "Error when mgp_result_record_insert"));
        break;
      }

      mgp_value_destroy(query_value);

      if (MGP_ERROR_NO_ERROR != mgp_result) {
        static_cast<void>(mgp_result_set_error_msg(result, "Couldn't insert field"));
        break;
      }

      mgp_result = mgp_result_record_insert(record, "parameters", null_value);
      if (MGP_ERROR_NO_ERROR != mgp_result) {
        static_cast<void>(mgp_result_set_error_msg(result, "Couldn't insert field"));
        break;
      }
    }
    mgp_value_destroy(null_value);
  } catch (const std::exception &e) {
    mgp_value_destroy(null_value);
    static_cast<void>(mgp_result_set_error_msg(result, e.what()));
    return;
  }
}

extern "C" int mgp_init_module(mgp_module *module, mgp_memory *memory) {
  static const auto no_op_cb = [](mgp_messages *msg, mgp_graph *graph, mgp_result *result, mgp_memory *memory) {};

  if (MGP_ERROR_NO_ERROR != mgp_module_add_transformation(module, "empty_transformation", no_op_cb)) {
    return 1;
  }

  if (MGP_ERROR_NO_ERROR != mgp_module_add_transformation(module, "c_transformation", c_transformation)) {
    return 1;
  }
  return 0;
}
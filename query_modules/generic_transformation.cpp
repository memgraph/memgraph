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

#include <fmt/format.h>
#include <exception>
#include <json/json.hpp>
#include <mgp.hpp>

static constexpr std::string_view kQuery = "query";
static constexpr std::string_view kParameters = "parameters";

template <typename T>
void ReplaceQueryParametersArray(T array, std::string key, std::string &query, mgp::Map &params) {
  auto list = mgp::List();
  for (size_t i = 0; i < array.size(); i++) {
    auto array_element = array[i];
    for (auto &element_prop : array_element) {
      if (element_prop.is_object()) {
        continue;
      }

      if (element_prop.is_string()) {
        list.AppendExtend(mgp::Value(element_prop.template get<std::string>()));
      }
    }
  }

  std::string param_key = key;
  param_key.replace(0, 1, "");
  params.Insert(param_key, mgp::Value(std::move(list)));
}

template <typename T>
void ReplaceQueryParameters(T object, std::string key, std::string &query, mgp::Map &params) {
  for (auto &el : object.items()) {
    auto new_key = fmt::format("{}__{}", key, el.key());

    if (el.value().type() == nlohmann::json::value_t::object) {
      ReplaceQueryParameters(el.value(), new_key, query, params);
      continue;
    }

    if (el.value().type() == nlohmann::json::value_t::array) {
      ReplaceQueryParametersArray(el.value(), new_key, query, params);
      continue;
    }

    size_t pos;
    while ((pos = query.find(new_key)) != std::string::npos) {
      query.replace(pos, new_key.size(), el.value());
    }
  }
}

void ReplaceQueryParameters(nlohmann::json &json, std::string &query, mgp::Map &params) {
  for (auto &el : json.items()) {
    auto key = fmt::format("${}", el.key());

    if (el.value().type() == nlohmann::json::value_t::object) {
      ReplaceQueryParameters(el.value(), key, query, params);
      continue;
    }

    if (el.value().type() == nlohmann::json::value_t::array) {
      continue;
    }

    size_t pos;
    while ((pos = query.find(key)) != std::string::npos) {
      query.replace(pos, key.size(), el.value());
    }
  }
}

void Transformation(struct mgp_messages *messages, mgp_trans_context *ctx, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  auto record_factory = mgp::RecordFactory(result);
  auto transformation_context = mgp::TransformationContext(ctx);
  try {
    auto stream_messages = mgp::Messages(messages);
    for (const mgp::Message &message : stream_messages) {
      auto record = record_factory.NewRecord();

      auto payload = nlohmann::json::parse(message.Payload());

      std::string generic_query = transformation_context.Query();
      auto params = mgp::Map();
      ReplaceQueryParameters(payload, generic_query, params);
      auto query_value = mgp::Value(generic_query.data());

      record.Insert(kQuery.data(), query_value);
      record.Insert(kParameters.data(), mgp::Value(params));
    }
  } catch (std::exception &ex) {
    record_factory.SetErrorMessage(ex.what());
    return;
  }
}

extern "C" int mgp_init_module(mgp_module *module, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);
    mgp::AddTransformation(Transformation, "transform", module);
  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }

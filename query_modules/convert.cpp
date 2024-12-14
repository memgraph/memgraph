// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <mgp.hpp>
#include "json/json.hpp"

mgp::Value ParseJsonToMgpValue(const nlohmann::json &json_obj, mgp_memory *memory);

mgp::Value ParseJsonToMgpMap(const nlohmann::json &json_obj, mgp_memory *memory) {
  auto map = mgp::Map();

  for (auto &[key, value] : json_obj.items()) {
    auto sub_value = ParseJsonToMgpValue(value, memory);
    map.Insert(key, std::move(sub_value));
  }

  return mgp::Value(std::move(map));
}

mgp::Value ParseJsonToMgpList(const nlohmann::json &json_array, mgp_memory *memory) {
  auto list = mgp::List();
  for (const auto &element : json_array) {
    auto value = ParseJsonToMgpValue(element, memory);
    list.AppendExtend(std::move(value));
  }

  return mgp::Value(std::move(list));
}

mgp::Value ParseJsonToMgpValue(const nlohmann::json &json_obj, mgp_memory *memory) {
  if (json_obj.is_object()) {
    return ParseJsonToMgpMap(json_obj, memory);
  } else if (json_obj.is_array()) {
    return ParseJsonToMgpList(json_obj, memory);
  } else if (json_obj.is_string()) {
    return mgp::Value(mgp::value_make_string(json_obj.get<std::string>().c_str(), memory));
  } else if (json_obj.is_number_integer()) {
    return mgp::Value(mgp::value_make_int(json_obj.get<int64_t>(), memory));
  } else if (json_obj.is_number_float()) {
    return mgp::Value(mgp::value_make_double(json_obj.get<double>(), memory));
  } else if (json_obj.is_boolean()) {
    return mgp::Value(mgp::value_make_bool(json_obj.get<bool>(), memory));
  } else {
    return mgp::Value();
  }
}

void str2object(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);

    // Retrieve the string argument
    auto str = std::string(mgp::Value(mgp::list_at(args, 0)).ValueString());
    const auto json_object = nlohmann::json::parse(str);

    const auto result = ParseJsonToMgpValue(json_object, memory);

    const auto func_result = mgp::Result(res);

    if (result.IsMap()) {
      func_result.SetValue(result.ValueMap());
    } else if (result.IsList()) {
      func_result.SetValue(result.ValueList());
    } else {
      mgp::func_result_set_error_msg(res, "The end result is not map or list!", memory);
    }
  } catch (const std::exception &e) {
    mgp::func_result_set_error_msg(res, e.what(), memory);
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);

    mgp::AddFunction(str2object, "str2object", {mgp::Parameter("string", mgp::Type::String)}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }

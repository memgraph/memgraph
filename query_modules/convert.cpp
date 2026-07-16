// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <iostream>
#include <mgp.hpp>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

constexpr const std::string_view kFunctionStr2Object = "str2object";
constexpr const std::string_view kParameterString = "string";

constexpr const std::string_view kFunctionFromJsonMap = "from_json_map";
constexpr const std::string_view kFunctionToMap = "to_map";
constexpr const std::string_view kParameterMap = "map";

// Forward declarations
std::optional<mgp::Value> ParseJsonToMgpValue(const nlohmann::json &json_obj, mgp_memory *memory);

mgp::Value ParseJsonToMgpMap(const nlohmann::json &json_obj, mgp_memory *memory) {
  auto map = mgp::Map();

  for (auto &[key, value] : json_obj.items()) {
    auto sub_value = ParseJsonToMgpValue(value, memory).value();
    map.Insert(key, sub_value);
  }

  return mgp::Value(std::move(map));
}

mgp::Value ParseJsonToMgpList(const nlohmann::json &json_array, mgp_memory *memory) {
  auto list = mgp::List();
  list.Reserve(json_array.size());
  for (const auto &element : json_array) {
    auto value = ParseJsonToMgpValue(element, memory).value();
    list.AppendExtend(value);
  }

  return mgp::Value(std::move(list));
}

std::optional<mgp::Value> ParseJsonToMgpValue(const nlohmann::json &json_obj, mgp_memory *memory) {
  if (json_obj.is_object()) {
    return ParseJsonToMgpMap(json_obj, memory);
  } else if (json_obj.is_array()) {
    return ParseJsonToMgpList(json_obj, memory);
  } else if (json_obj.is_string()) {
    return mgp::Value(mgp::steal_type, mgp::value_make_string(json_obj.get<std::string>().c_str(), memory));
  } else if (json_obj.is_number_integer()) {
    return mgp::Value(mgp::steal_type, mgp::value_make_int(json_obj.get<int64_t>(), memory));
  } else if (json_obj.is_number_float()) {
    return mgp::Value(mgp::steal_type, mgp::value_make_double(json_obj.get<double>(), memory));
  } else if (json_obj.is_boolean()) {
    return mgp::Value(mgp::steal_type, mgp::value_make_bool(json_obj.get<bool>(), memory));
  } else if (json_obj.is_null()) {
    return mgp::Value();
  } else {
    return std::nullopt;
  }
}

void str2object(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);

    // Retrieve the string argument
    const auto string_arg = mgp::Value(mgp::ref_type, mgp::list_at(args, 0));
    const auto json_object = nlohmann::json::parse(string_arg.ValueString());
    std::optional<mgp::Value> maybe_result = ParseJsonToMgpValue(json_object, memory);

    if (!maybe_result.has_value()) {
      mgp::func_result_set_error_msg(
          res,
          "The end result is not one of the following JSON-language  data types: object, array, "
          "number, string, boolean, or null.",
          memory);
      return;
    }

    auto func_result = mgp::Result(res);

    auto const &result = *maybe_result;
    if (result.IsMap()) {
      func_result.SetValue(result.ValueMap());
    } else if (result.IsList()) {
      func_result.SetValue(result.ValueList());
    } else if (result.IsString()) {
      func_result.SetValue(result.ValueString());
    } else if (result.IsInt()) {
      func_result.SetValue(result.ValueInt());
    } else if (result.IsDouble()) {
      func_result.SetValue(result.ValueDouble());
    } else if (result.IsBool()) {
      func_result.SetValue(result.ValueBool());
    } else if (result.IsNull()) {
      func_result.SetValue();
    }
  } catch (const std::exception &e) {
    mgp::func_result_set_error_msg(res, e.what(), memory);
  }
}

// Parses a JSON object string into a Cypher map. A null argument yields null; a
// JSON value that is not an object is rejected.
void from_json_map(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  auto func_result = mgp::Result(res);
  try {
    const auto arguments = mgp::List(args);
    const auto &argument = arguments[0];
    if (argument.IsNull()) {
      func_result.SetValue();
      return;
    }

    const auto json_object = nlohmann::json::parse(argument.ValueString());
    if (!json_object.is_object()) {
      func_result.SetErrorMessage("The provided string does not represent a JSON object.");
      return;
    }

    const auto map_value = ParseJsonToMgpMap(json_object, memory);
    func_result.SetValue(map_value.ValueMap());
  } catch (const std::exception &e) {
    func_result.SetErrorMessage(e.what());
  }
}

// Converts a value into a map: a map is returned unchanged, a node or relationship
// yields its properties, null yields null, and anything else yields null.
void to_map(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  auto func_result = mgp::Result(res);
  try {
    const auto arguments = mgp::List(args);
    const auto &argument = arguments[0];

    if (argument.IsMap()) {
      func_result.SetValue(argument.ValueMap());
      return;
    }

    if (argument.IsNode() || argument.IsRelationship()) {
      const auto properties =
          argument.IsNode() ? argument.ValueNode().Properties() : argument.ValueRelationship().Properties();
      auto map = mgp::Map();
      for (const auto &[key, value] : properties) {
        map.Insert(key, value);
      }
      func_result.SetValue(std::move(map));
      return;
    }

    func_result.SetValue();
  } catch (const std::exception &e) {
    func_result.SetErrorMessage(e.what());
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);

    mgp::AddFunction(
        str2object, kFunctionStr2Object, {mgp::Parameter(kParameterString, mgp::Type::String)}, module, memory);

    // These accept a null argument, which mgp::AddFunction cannot express, so register them directly.
    auto *from_json_map_func =
        mgp::module_add_function(module, std::string(kFunctionFromJsonMap).c_str(), from_json_map);
    mgp::func_add_arg(from_json_map_func, std::string(kParameterMap).c_str(), mgp::type_nullable(mgp::type_string()));

    auto *to_map_func = mgp::module_add_function(module, std::string(kFunctionToMap).c_str(), to_map);
    mgp::func_add_arg(to_map_func, std::string(kParameterMap).c_str(), mgp::type_nullable(mgp::type_any()));
  } catch (const std::exception &e) {
    std::cerr << "Error while initializing query module: " << e.what() << '\n';
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }

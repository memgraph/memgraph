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
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>

constexpr const std::string_view kFunctionStr2Object = "str2object";
constexpr const std::string_view kParameterString = "string";

constexpr const std::string_view kFunctionFromJsonMap = "from_json_map";
constexpr const std::string_view kFunctionFromJsonList = "from_json_list";
constexpr const std::string_view kFunctionToMap = "to_map";
constexpr const std::string_view kFunctionToJson = "to_json";
constexpr const std::string_view kParameterMap = "map";
constexpr const std::string_view kParameterList = "list";
constexpr const std::string_view kParameterValue = "value";
constexpr const std::string_view kParameterPath = "path";

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

// Translates a subset of path syntax into a JSON pointer. Supported: an optional
// leading '$', '.key' dot steps, "['key']"/'["key"]' quoted steps, and '[index]'
// numeric steps. An empty path (or a lone '$') yields an empty pointer, which
// selects the whole document. Unsupported constructs (wildcards, filters,
// recursive descent, slices) throw. Reference-token escaping is handled by
// json_pointer itself.
nlohmann::json::json_pointer JsonPathToPointer(std::string_view path) {
  nlohmann::json::json_pointer pointer;
  const auto append_key = [&pointer](std::string_view key) { pointer.push_back(std::string(key)); };

  size_t i = 0;
  const size_t n = path.size();
  if (i < n && path[i] == '$') {
    ++i;
  }
  while (i < n) {
    if (path[i] == '.') {
      ++i;
      if (i < n && path[i] == '.') {
        throw std::invalid_argument("Recursive descent ('..') is not supported in a path expression.");
      }
      const size_t start = i;
      while (i < n && path[i] != '.' && path[i] != '[') {
        ++i;
      }
      const auto key = path.substr(start, i - start);
      if (key.empty() || key == "*") {
        throw std::invalid_argument("Wildcards and empty steps are not supported in a path expression.");
      }
      append_key(key);
    } else if (path[i] == '[') {
      ++i;
      if (i < n && (path[i] == '\'' || path[i] == '"')) {
        const char quote = path[i++];
        const size_t start = i;
        while (i < n && path[i] != quote) {
          ++i;
        }
        if (i >= n) {
          throw std::invalid_argument("Unterminated quoted step in a path expression.");
        }
        const auto key = path.substr(start, i - start);
        ++i;  // closing quote
        if (i >= n || path[i] != ']') {
          throw std::invalid_argument("Malformed bracket step in a path expression.");
        }
        ++i;  // ']'
        append_key(key);
      } else {
        const size_t start = i;
        while (i < n && path[i] != ']') {
          ++i;
        }
        if (i >= n) {
          throw std::invalid_argument("Malformed bracket step in a path expression.");
        }
        const auto index = path.substr(start, i - start);
        ++i;  // ']'
        if (index.empty() || index.find_first_not_of("0123456789") != std::string_view::npos) {
          throw std::invalid_argument("Only numeric array indices are supported in a path expression.");
        }
        append_key(index);
      }
    } else {  // leading bare key, e.g. "a.b" written without a '$' prefix
      const size_t start = i;
      while (i < n && path[i] != '.' && path[i] != '[') {
        ++i;
      }
      append_key(path.substr(start, i - start));
    }
  }
  return pointer;
}

// Parses a JSON object string into a Cypher map. A null string yields null. An
// optional path selects a nested part of the document first: an unresolved path
// yields null, a JSON null yields null, and a selection that is not an object is
// rejected.
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

    const auto root = nlohmann::json::parse(argument.ValueString());

    const auto &path_arg = arguments[1];
    const auto pointer = path_arg.IsNull() ? nlohmann::json::json_pointer{} : JsonPathToPointer(path_arg.ValueString());

    const nlohmann::json *selected = &root;
    if (!pointer.empty()) {
      if (!root.contains(pointer)) {
        func_result.SetValue();  // unresolved path -> null
        return;
      }
      selected = &root.at(pointer);
    }

    if (selected->is_null()) {
      func_result.SetValue();
      return;
    }
    if (!selected->is_object()) {
      func_result.SetErrorMessage("The selected value does not represent a JSON object.");
      return;
    }

    const auto map_value = ParseJsonToMgpMap(*selected, memory);
    func_result.SetValue(map_value.ValueMap());
  } catch (const std::exception &e) {
    func_result.SetErrorMessage(e.what());
  }
}

// Parses a JSON array string into a Cypher list. A null string yields null. An
// optional path selects a nested part of the document first: an unresolved path
// yields null, a JSON null yields null, and a selection that is not an array is
// rejected.
void from_json_list(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  auto func_result = mgp::Result(res);
  try {
    const auto arguments = mgp::List(args);
    const auto &argument = arguments[0];
    if (argument.IsNull()) {
      func_result.SetValue();
      return;
    }

    const auto root = nlohmann::json::parse(argument.ValueString());

    const auto &path_arg = arguments[1];
    const auto pointer = path_arg.IsNull() ? nlohmann::json::json_pointer{} : JsonPathToPointer(path_arg.ValueString());

    const nlohmann::json *selected = &root;
    if (!pointer.empty()) {
      if (!root.contains(pointer)) {
        func_result.SetValue();  // unresolved path -> null
        return;
      }
      selected = &root.at(pointer);
    }

    if (selected->is_null()) {
      func_result.SetValue();
      return;
    }
    if (!selected->is_array()) {
      func_result.SetErrorMessage("The selected value does not represent a JSON array.");
      return;
    }

    const auto list_value = ParseJsonToMgpList(*selected, memory);
    func_result.SetValue(list_value.ValueList());
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

// Forward declaration for the recursive value serializer below.
nlohmann::ordered_json MgpValueToJson(const mgp::Value &value);

// Serializes a node as {id, type, labels, properties}. The id is stringified;
// labels and properties are omitted when empty.
nlohmann::ordered_json MgpNodeToJson(const mgp::Node &node) {
  auto obj = nlohmann::ordered_json::object();
  obj["id"] = std::to_string(node.Id().AsInt());
  obj["type"] = "node";
  const auto labels = node.Labels();
  if (labels.Size() > 0) {
    auto label_array = nlohmann::ordered_json::array();
    for (size_t i = 0; i < labels.Size(); ++i) {
      label_array.push_back(std::string(labels[i]));
    }
    obj["labels"] = std::move(label_array);
  }
  const auto properties = node.Properties();
  if (!properties.empty()) {
    auto props = nlohmann::ordered_json::object();
    for (const auto &[key, value] : properties) {
      props[key] = MgpValueToJson(value);
    }
    obj["properties"] = std::move(props);
  }
  return obj;
}

// Serializes a relationship as {id, type, label, start, end, properties}. The
// endpoints are full node objects; properties are omitted when empty.
nlohmann::ordered_json MgpRelationshipToJson(const mgp::Relationship &relationship) {
  auto obj = nlohmann::ordered_json::object();
  obj["id"] = std::to_string(relationship.Id().AsInt());
  obj["type"] = "relationship";
  obj["label"] = std::string(relationship.Type());
  obj["start"] = MgpNodeToJson(relationship.From());
  obj["end"] = MgpNodeToJson(relationship.To());
  const auto properties = relationship.Properties();
  if (!properties.empty()) {
    auto props = nlohmann::ordered_json::object();
    for (const auto &[key, value] : properties) {
      props[key] = MgpValueToJson(value);
    }
    obj["properties"] = std::move(props);
  }
  return obj;
}

// Serializes a path as a flat array of interleaved nodes and relationships:
// [node, relationship, node, ...].
nlohmann::ordered_json MgpPathToJson(const mgp::Path &path) {
  auto array = nlohmann::ordered_json::array();
  const size_t length = path.Length();
  for (size_t i = 0; i < length; ++i) {
    array.push_back(MgpNodeToJson(path.GetNodeAt(i)));
    array.push_back(MgpRelationshipToJson(path.GetRelationshipAt(i)));
  }
  array.push_back(MgpNodeToJson(path.GetNodeAt(length)));
  return array;
}

nlohmann::ordered_json MgpValueToJson(const mgp::Value &value) {
  if (value.IsNull()) return nullptr;
  if (value.IsBool()) return value.ValueBool();
  if (value.IsInt()) return value.ValueInt();
  if (value.IsDouble()) return value.ValueDouble();
  if (value.IsString()) return std::string(value.ValueString());
  if (value.IsList()) {
    auto array = nlohmann::ordered_json::array();
    for (const auto item : value.ValueList()) {
      array.push_back(MgpValueToJson(item));
    }
    return array;
  }
  if (value.IsMap()) {
    auto obj = nlohmann::ordered_json::object();
    for (const auto &item : value.ValueMap()) {
      obj[std::string(item.key)] = MgpValueToJson(item.value);
    }
    return obj;
  }
  if (value.IsNode()) return MgpNodeToJson(value.ValueNode());
  if (value.IsRelationship()) return MgpRelationshipToJson(value.ValueRelationship());
  if (value.IsPath()) return MgpPathToJson(value.ValuePath());
  if (value.IsDate()) return value.ValueDate().ToString();
  if (value.IsLocalTime()) return value.ValueLocalTime().ToString();
  if (value.IsLocalDateTime()) return value.ValueLocalDateTime().ToString();
  if (value.IsDuration()) return value.ValueDuration().ToString();
  if (value.IsZonedDateTime()) return value.ValueZonedDateTime().ToString();
  throw std::invalid_argument("Unsupported value type for JSON conversion.");
}

// Serializes any value to a JSON string. Null yields the string "null".
void to_json(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  auto func_result = mgp::Result(res);
  try {
    const auto arguments = mgp::List(args);
    const auto serialized = MgpValueToJson(arguments[0]).dump();
    func_result.SetValue(serialized);
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
    auto *default_path = mgp::value_make_string("", memory);
    mgp::func_add_opt_arg(
        from_json_map_func, std::string(kParameterPath).c_str(), mgp::type_nullable(mgp::type_string()), default_path);
    mgp::value_destroy(default_path);

    auto *from_json_list_func =
        mgp::module_add_function(module, std::string(kFunctionFromJsonList).c_str(), from_json_list);
    mgp::func_add_arg(from_json_list_func, std::string(kParameterList).c_str(), mgp::type_nullable(mgp::type_string()));
    auto *default_list_path = mgp::value_make_string("", memory);
    mgp::func_add_opt_arg(from_json_list_func,
                          std::string(kParameterPath).c_str(),
                          mgp::type_nullable(mgp::type_string()),
                          default_list_path);
    mgp::value_destroy(default_list_path);

    auto *to_map_func = mgp::module_add_function(module, std::string(kFunctionToMap).c_str(), to_map);
    mgp::func_add_arg(to_map_func, std::string(kParameterMap).c_str(), mgp::type_nullable(mgp::type_any()));

    auto *to_json_func = mgp::module_add_function(module, std::string(kFunctionToJson).c_str(), to_json);
    mgp::func_add_arg(to_json_func, std::string(kParameterValue).c_str(), mgp::type_nullable(mgp::type_any()));
  } catch (const std::exception &e) {
    std::cerr << "Error while initializing query module: " << e.what() << '\n';
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }

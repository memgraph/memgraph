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

#include <format>
#include <iostream>
#include <mgp.hpp>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

constexpr const char *kFunctionStr2Object = "str2object";
constexpr const char *kParameterString = "string";

constexpr const char *kFunctionFromJsonMap = "from_json_map";
constexpr const char *kFunctionFromJsonList = "from_json_list";
constexpr const char *kFunctionToMap = "to_map";
constexpr const char *kFunctionToJson = "to_json";
constexpr const char *kParameterMap = "map";
constexpr const char *kParameterList = "list";
constexpr const char *kParameterValue = "value";
constexpr const char *kParameterPath = "path";

namespace {

// Forward declarations
std::optional<mgp::Value> ParseJsonToMgpValue(const nlohmann::json &json_obj, mgp_memory *memory);

mgp::Value ParseJsonToMgpMap(const nlohmann::json &json_obj, mgp_memory *memory) {
  auto map = mgp::Map();

  for (const auto &[key, value] : json_obj.items()) {
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

void str2object(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard(memory);

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
  // Advances past a dot/bare key, stopping at the next step delimiter.
  const auto scan_key = [&path, n](size_t from) {
    while (from < n && path[from] != '.' && path[from] != '[') {
      ++from;
    }
    return from;
  };
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
      i = scan_key(i);
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
      i = scan_key(i);
      append_key(path.substr(start, i - start));
    }
  }
  return pointer;
}

// Parses a nullable JSON string and applies the optional path selector, returning
// the selected sub-document. Returns nullopt when the result should be null: a null
// input string, an unresolved path, or a JSON null leaf. Throws on parse or
// path-syntax errors.
std::optional<nlohmann::json> ResolveJsonPath(const mgp::Value &json_arg, const mgp::Value &path_arg) {
  if (json_arg.IsNull()) {
    return std::nullopt;
  }
  auto root = nlohmann::json::parse(json_arg.ValueString());
  const auto pointer = path_arg.IsNull() ? nlohmann::json::json_pointer{} : JsonPathToPointer(path_arg.ValueString());
  if (!pointer.empty() && !root.contains(pointer)) {
    return std::nullopt;  // unresolved path -> null
  }
  auto &selected = pointer.empty() ? root : root.at(pointer);
  if (selected.is_null()) {
    return std::nullopt;
  }
  return std::move(selected);
}

// Shared body for from_json_map / from_json_list: resolves the optional path,
// propagates null, then hands the selected JSON leaf to `emit`, which validates
// its shape and assigns the result. Parse and path-syntax errors surface as a
// function error.
template <typename Emit>
void FromJson(mgp_list *args, mgp_func_result *res, mgp_memory *memory, Emit emit) {
  const mgp::MemoryDispatcherGuard guard(memory);
  auto func_result = mgp::Result(res);
  try {
    const auto arguments = mgp::List(args);
    const auto selected = ResolveJsonPath(arguments[0], arguments[1]);
    if (!selected) {
      func_result.SetValue();
      return;
    }
    emit(*selected, func_result);
  } catch (const std::exception &e) {
    func_result.SetErrorMessage(e.what());
  }
}

// Parses a JSON object string into a Cypher map. A null string yields null. An
// optional path selects a nested part of the document first: an unresolved path
// yields null, a JSON null yields null, and a selection that is not an object is
// rejected.
void from_json_map(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  FromJson(args, res, memory, [memory](const nlohmann::json &selected, mgp::Result &func_result) {
    if (!selected.is_object()) {
      func_result.SetErrorMessage("The selected value does not represent a JSON object.");
      return;
    }
    func_result.SetValue(ParseJsonToMgpMap(selected, memory).ValueMap());
  });
}

// Parses a JSON array string into a Cypher list. A null string yields null. An
// optional path selects a nested part of the document first: an unresolved path
// yields null, a JSON null yields null, and a selection that is not an array is
// rejected.
void from_json_list(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  FromJson(args, res, memory, [memory](const nlohmann::json &selected, mgp::Result &func_result) {
    if (!selected.is_array()) {
      func_result.SetErrorMessage("The selected value does not represent a JSON array.");
      return;
    }
    func_result.SetValue(ParseJsonToMgpList(selected, memory).ValueList());
  });
}

// Converts a value into a map: a map is returned unchanged, a node or relationship
// yields its properties, null yields null, and anything else yields null.
void to_map(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard(memory);
  auto func_result = mgp::Result(res);
  try {
    const auto arguments = mgp::List(args);
    const auto argument = arguments[0];

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
nlohmann::json MgpValueToJson(const mgp::Value &value);

// Serializes a property map into a JSON object.
nlohmann::json PropertiesToJson(const std::unordered_map<std::string, mgp::Value> &properties) {
  auto props = nlohmann::json::object();
  for (const auto &[key, value] : properties) {
    props[key] = MgpValueToJson(value);
  }
  return props;
}

// Serializes a node as {id, type, labels, properties}. The id is stringified;
// labels and properties are omitted when empty.
nlohmann::json MgpNodeToJson(const mgp::Node &node) {
  auto obj = nlohmann::json::object();
  obj["id"] = std::to_string(node.Id().AsInt());
  obj["type"] = "node";
  const auto labels = node.Labels();
  if (labels.Size() > 0) {
    auto label_array = nlohmann::json::array();
    for (size_t i = 0; i < labels.Size(); ++i) {
      label_array.push_back(std::string(labels[i]));
    }
    obj["labels"] = std::move(label_array);
  }
  const auto properties = node.Properties();
  if (!properties.empty()) {
    obj["properties"] = PropertiesToJson(properties);
  }
  return obj;
}

// Serializes a relationship as {id, type, label, start, end, properties}. The
// endpoints are full node objects; properties are omitted when empty.
nlohmann::json MgpRelationshipToJson(const mgp::Relationship &relationship) {
  auto obj = nlohmann::json::object();
  obj["id"] = std::to_string(relationship.Id().AsInt());
  obj["type"] = "relationship";
  obj["label"] = std::string(relationship.Type());
  obj["start"] = MgpNodeToJson(relationship.From());
  obj["end"] = MgpNodeToJson(relationship.To());
  const auto properties = relationship.Properties();
  if (!properties.empty()) {
    obj["properties"] = PropertiesToJson(properties);
  }
  return obj;
}

// Serializes a path as a flat array of interleaved nodes and relationships:
// [node, relationship, node, ...].
nlohmann::json MgpPathToJson(const mgp::Path &path) {
  auto array = nlohmann::json::array();
  const size_t length = path.Length();
  for (size_t i = 0; i < length; ++i) {
    array.push_back(MgpNodeToJson(path.GetNodeAt(i)));
    array.push_back(MgpRelationshipToJson(path.GetRelationshipAt(i)));
  }
  array.push_back(MgpNodeToJson(path.GetNodeAt(length)));
  return array;
}

// Neo4j/OGC spatial reference ids used to pick the coordinate names and CRS label.
constexpr uint16_t kSridWgs84_2d = 4326;
constexpr uint16_t kSridWgs84_3d = 4979;

// Serializes a 2D point as {crs, x/y (or longitude/latitude), z/height=null}.
nlohmann::json MgpPoint2dToJson(const mgp::Point2d &point) {
  auto obj = nlohmann::json::object();
  if (point.Srid() == kSridWgs84_2d) {
    obj["crs"] = "wgs-84";
    obj["latitude"] = point.Y();
    obj["longitude"] = point.X();
    obj["height"] = nullptr;
  } else {
    obj["crs"] = "cartesian";
    obj["x"] = point.X();
    obj["y"] = point.Y();
    obj["z"] = nullptr;
  }
  return obj;
}

// Serializes a 3D point as {crs, x/y/z (or longitude/latitude/height)}.
nlohmann::json MgpPoint3dToJson(const mgp::Point3d &point) {
  auto obj = nlohmann::json::object();
  if (point.Srid() == kSridWgs84_3d) {
    obj["crs"] = "wgs-84-3d";
    obj["latitude"] = point.Y();
    obj["longitude"] = point.X();
    obj["height"] = point.Z();
  } else {
    obj["crs"] = "cartesian-3d";
    obj["x"] = point.X();
    obj["y"] = point.Y();
    obj["z"] = point.Z();
  }
  return obj;
}

// Formats a duration given in microseconds as P<d>DT<h>H<m>M<s>.<micros>S, matching
// the canonical string form the engine uses for durations.
std::string DurationToString(int64_t total_microseconds) {
  constexpr int64_t kMicrosPerSecond = 1000000;
  constexpr int64_t kMicrosPerMinute = 60 * kMicrosPerSecond;
  constexpr int64_t kMicrosPerHour = 60 * kMicrosPerMinute;
  constexpr int64_t kMicrosPerDay = 24 * kMicrosPerHour;

  int64_t micros = total_microseconds;
  const int64_t days = micros / kMicrosPerDay;
  micros %= kMicrosPerDay;
  const int64_t hours = micros / kMicrosPerHour;
  micros %= kMicrosPerHour;
  const int64_t minutes = micros / kMicrosPerMinute;
  micros %= kMicrosPerMinute;
  const int64_t seconds = micros / kMicrosPerSecond;
  micros %= kMicrosPerSecond;

  const int64_t fraction = micros < 0 ? -micros : micros;
  auto result = std::format("P{}DT{}H{}M", days, hours, minutes);
  if (seconds == 0 && micros < 0) {
    result += '-';
  }
  result += std::format("{}.{:0>6}S", seconds, fraction);
  return result;
}

nlohmann::json MgpValueToJson(const mgp::Value &value) {
  switch (value.Type()) {
    case mgp::Type::Null:
      return nullptr;
    case mgp::Type::Bool:
      return value.ValueBool();
    case mgp::Type::Int:
      return value.ValueInt();
    case mgp::Type::Double:
      return value.ValueDouble();
    case mgp::Type::String:
      return std::string(value.ValueString());
    case mgp::Type::List: {
      auto array = nlohmann::json::array();
      for (const auto item : value.ValueList()) {
        array.push_back(MgpValueToJson(item));
      }
      return array;
    }
    case mgp::Type::Map: {
      auto obj = nlohmann::json::object();
      for (const auto &item : value.ValueMap()) {
        obj[std::string(item.key)] = MgpValueToJson(item.value);
      }
      return obj;
    }
    case mgp::Type::Node:
      return MgpNodeToJson(value.ValueNode());
    case mgp::Type::Relationship:
      return MgpRelationshipToJson(value.ValueRelationship());
    case mgp::Type::Path:
      return MgpPathToJson(value.ValuePath());
    case mgp::Type::Point2d:
      return MgpPoint2dToJson(value.ValuePoint2d());
    case mgp::Type::Point3d:
      return MgpPoint3dToJson(value.ValuePoint3d());
    case mgp::Type::Date:
      return value.ValueDate().ToString();
    case mgp::Type::LocalTime:
      return value.ValueLocalTime().ToString();
    case mgp::Type::LocalDateTime:
      return value.ValueLocalDateTime().ToString();
    case mgp::Type::Duration:
      return DurationToString(value.ValueDuration().Microseconds());
    case mgp::Type::ZonedDateTime:
      return value.ValueZonedDateTime().ToString();
    case mgp::Type::Any:
    case mgp::Type::Enum:
      break;
  }
  throw std::invalid_argument("Unsupported value type for JSON conversion.");
}

// Serializes any value to a JSON string. Null yields the string "null".
void to_json(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard(memory);
  auto func_result = mgp::Result(res);
  try {
    const auto arguments = mgp::List(args);
    const auto serialized = MgpValueToJson(arguments[0]).dump();
    func_result.SetValue(serialized);
  } catch (const std::exception &e) {
    func_result.SetErrorMessage(e.what());
  }
}

}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard(memory);

    mgp::AddFunction(
        str2object, kFunctionStr2Object, {mgp::Parameter(kParameterString, mgp::Type::String)}, module, memory);

    // These accept a null argument, which mgp::AddFunction cannot express, so register them directly.
    // func_add_opt_arg copies the default value, so a single RAII-owned instance serves both.
    const auto default_path = mgp::Value("");

    auto *from_json_map_func = mgp::module_add_function(module, kFunctionFromJsonMap, from_json_map);
    mgp::func_add_arg(from_json_map_func, kParameterMap, mgp::type_nullable(mgp::type_string()));
    mgp::func_add_opt_arg(
        from_json_map_func, kParameterPath, mgp::type_nullable(mgp::type_string()), default_path.ptr());

    auto *from_json_list_func = mgp::module_add_function(module, kFunctionFromJsonList, from_json_list);
    mgp::func_add_arg(from_json_list_func, kParameterList, mgp::type_nullable(mgp::type_string()));
    mgp::func_add_opt_arg(
        from_json_list_func, kParameterPath, mgp::type_nullable(mgp::type_string()), default_path.ptr());

    auto *to_map_func = mgp::module_add_function(module, kFunctionToMap, to_map);
    mgp::func_add_arg(to_map_func, kParameterMap, mgp::type_nullable(mgp::type_any()));

    auto *to_json_func = mgp::module_add_function(module, kFunctionToJson, to_json);
    mgp::func_add_arg(to_json_func, kParameterValue, mgp::type_nullable(mgp::type_any()));
  } catch (const std::exception &e) {
    std::cerr << "Error while initializing query module: " << e.what() << '\n';
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }

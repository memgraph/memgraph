// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <algorithm>
#include <iostream>
#include <mgp.hpp>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include <nlohmann/json.hpp>

// Forward declarations
mgp::Value ConvertToTreeImpl(const mgp::Value &value, const mgp::Map &config, mgp_memory *memory);
std::optional<mgp::Value> ParseJsonToMgpValue(const nlohmann::json &json_obj, mgp_memory *memory);

// Helper function to check if a property should be included
bool ShouldIncludeProperty(const std::string_view &prop_name, const mgp::Map &config, const mgp::Labels &labels) {
  if (!config.KeyExists("nodes")) {
    return true;
  }

  auto nodes = config.At("nodes").ValueMap();
  bool any_exclude_mode = false;
  std::vector<std::string_view> excluded_props;

  for (size_t l = 0; l < labels.Size(); ++l) {
    std::string_view label = labels[l];
    if (!nodes.KeyExists(label)) continue;
    auto props = nodes.At(label).ValueList();
    if (props.Size() == 0) continue;
    auto first_prop = props[0].ValueString();
    if (first_prop.size() > 0 && first_prop[0] == '-') {
      any_exclude_mode = true;
      if (first_prop.size() > 1) {
        excluded_props.push_back(first_prop.substr(1));
      }
      for (size_t i = 1; i < props.Size(); ++i) {
        auto excluded = props[i].ValueString();
        if (excluded.size() > 0 && excluded[0] == '-') {
          excluded = excluded.substr(1);
        }
        excluded_props.push_back(excluded);
      }
    }
  }

  if (!any_exclude_mode) {
    for (size_t l = 0; l < labels.Size(); ++l) {
      std::string_view label = labels[l];
      if (!nodes.KeyExists(label)) continue;
      auto props = nodes.At(label).ValueList();
      if (props.Size() == 0) continue;
      if (props[0].ValueString() == "*") return true;
      for (size_t i = 0; i < props.Size(); ++i) {
        if (props[i].ValueString() == prop_name) return true;
      }
    }
    return false;
  }

  for (const auto &excluded : excluded_props) {
    if (prop_name == excluded) {
      return false;
    }
  }
  return true;
}

// Helper function to check if a relationship property should be included
bool ShouldIncludeRelProperty(const std::string_view &prop_name, const mgp::Map &config,
                              const std::string_view &rel_type) {
  if (!config.KeyExists("rels")) {
    return true;
  }

  auto rels = config.At("rels").ValueMap();
  if (!rels.KeyExists(rel_type)) {
    return true;
  }

  auto props = rels.At(rel_type).ValueList();
  if (props.Size() == 0) {
    return true;
  }

  auto first_prop = props[0].ValueString();
  if (first_prop.size() > 0 && first_prop[0] == '-') {
    std::vector<std::string_view> excluded_props;
    if (first_prop.size() > 1) {
      excluded_props.push_back(first_prop.substr(1));
    }
    for (size_t i = 1; i < props.Size(); ++i) {
      auto excluded = props[i].ValueString();
      if (excluded.size() > 0 && excluded[0] == '-') {
        excluded = excluded.substr(1);
      }
      excluded_props.push_back(excluded);
    }
    for (const auto &excluded : excluded_props) {
      if (prop_name == excluded) {
        return false;
      }
    }
    return true;
  }

  if (first_prop == "*") return true;
  for (size_t i = 0; i < props.Size(); ++i) {
    if (props[i].ValueString() == prop_name) return true;
  }
  return false;
}

// Main implementation function
mgp::Value ConvertToTreeImpl(const mgp::Value &value, const mgp::Map &config, mgp_memory *memory) {
  if (value.IsNull()) {
    return value;
  }

  // If the value is a List of Paths, convert each path and merge into a tree
  if (value.IsList()) {
    auto paths = value.ValueList();
    if (paths.Size() == 0) {
      return value;
    }

    // Get the first path to extract the root node
    auto first_path = paths[0].ValuePath();
    auto root_node = first_path.GetNodeAt(0);

    // Create root node map with properties
    mgp::Map root_map;
    auto root_props = root_node.Properties();
    auto root_labels = root_node.Labels();

    for (const auto &[key, value] : root_props) {
      if (ShouldIncludeProperty(key, config, root_labels)) {
        root_map.Insert(key, value);
      }
    }

    // Add type information
    if (root_labels.Size() == 1) {
      root_map.Insert("_type", mgp::Value(root_labels[0]));
    } else if (root_labels.Size() > 1) {
      mgp::List label_list;
      for (size_t i = 0; i < root_labels.Size(); ++i) {
        label_list.Append(mgp::Value(root_labels[i]));
      }
      root_map.Insert("_type", mgp::Value(label_list));
    }
    root_map.Insert("_id", mgp::Value(root_node.Id().AsInt()));

    // Process all paths to collect relationships
    for (size_t path_idx = 0; path_idx < paths.Size(); path_idx++) {
      auto path = paths[path_idx].ValuePath();

      // Process each relationship in the path
      for (size_t i = 0; i < path.Length(); i++) {
        auto rel = path.GetRelationshipAt(i);
        auto target_node = path.GetNodeAt(i + 1);
        std::string rel_type{rel.Type()};

        // Create or get relationship array
        mgp::List rel_list;
        if (!root_map.KeyExists(rel_type)) {
          rel_list = mgp::List();
        } else {
          rel_list = root_map.At(rel_type).ValueList();
        }

        // Create target node map
        mgp::Map target_map;

        // Add target node properties
        auto target_props = target_node.Properties();
        auto target_labels = target_node.Labels();

        // Add all properties that should be included
        for (const auto &[key, value] : target_props) {
          if (ShouldIncludeProperty(key, config, target_labels)) {
            target_map.Insert(key, value);
          }
        }

        // Add relationship properties with proper prefixing
        auto rel_props = rel.Properties();
        for (const auto &[key, value] : rel_props) {
          if (ShouldIncludeRelProperty(key, config, rel_type)) {
            std::string prefixed_key = rel_type + "." + key;
            target_map.Insert(prefixed_key, value);
          }
        }

        // Add type information
        if (target_labels.Size() == 1) {
          target_map.Insert("_type", mgp::Value(target_labels[0]));
        } else if (target_labels.Size() > 1) {
          mgp::List label_list;
          for (size_t l = 0; l < target_labels.Size(); ++l) {
            label_list.Append(mgp::Value(target_labels[l]));
          }
          target_map.Insert("_type", mgp::Value(label_list));
        }
        target_map.Insert("_id", mgp::Value(target_node.Id().AsInt()));

        // Check if this target node is already in the rel_list
        bool found = false;
        for (size_t j = 0; j < rel_list.Size(); ++j) {
          auto existing = rel_list[j].ValueMap();
          if (existing.KeyExists("_id") && existing.At("_id").ValueInt() == target_node.Id().AsInt()) {
            found = true;
            break;
          }
        }
        if (!found) {
          rel_list.AppendExtend(mgp::Value(target_map));
        }
        root_map.Update(rel_type, mgp::Value(rel_list));
      }
    }

    return mgp::Value(root_map);
  }

  // If single path, convert it to a list and process
  if (value.IsPath()) {
    auto list = mgp::List(1);
    list.Append(value);
    return ConvertToTreeImpl(mgp::Value(list), config, memory);
  }

  return value;
}

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
          "The end result is not one of the following JSON-language data types: object, array, "
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

void convert_to_tree(mgp_list *args, mgp_func_context *ctx, mgp_func_result *func_result, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);
    const auto arguments = mgp::List(args);
    const auto record = arguments[0];

    // Create a default empty config if none provided
    mgp::Map config;
    if (arguments.Size() > 1) {
      config = arguments[1].ValueMap();
    }

    // Convert the input value to tree structure
    auto result_value = ConvertToTreeImpl(record, config, memory);

    // We know the result will be a Map, so handle it directly
    mgp::Result result(func_result);
    if (result_value.IsMap()) {
      result.SetValue(result_value.ValueMap());
    } else {
      // For any other type (shouldn't happen in our case), return null
      result.SetValue();
    }

  } catch (const std::exception &e) {
    mgp::Result(func_result).SetErrorMessage(e.what());
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);

    mgp::AddFunction(str2object, "str2object", {mgp::Parameter("string", mgp::Type::String)}, module, memory);
    mgp::AddFunction(
        convert_to_tree, "to_tree",
        {mgp::Parameter("value", mgp::Type::Any), mgp::Parameter("config", mgp::Type::Map, mgp::Value(mgp::Map()))},
        module, memory);

  } catch (const std::exception &e) {
    std::cerr << "Error while initializing query module: " << e.what() << '\n';
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }

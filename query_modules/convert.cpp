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

#include <iostream>
#include <mgp.hpp>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

// Forward declarations
mgp::Value ConvertToTreeImpl(const mgp::Value &value, const mgp::Map &config, mgp_memory *memory);
std::optional<mgp::Value> ParseJsonToMgpValue(const nlohmann::json &json_obj, mgp_memory *memory);

// Helper function to check if a property should be included
// Determines if a node property should be included in the output based on config filtering rules.
bool ShouldIncludeProperty(std::string_view prop_name, const mgp::Map &config, const mgp::Labels &labels) {
  if (!config.KeyExists("nodes")) {
    // No filtering config: include all properties
    return true;
  }

  auto nodes = config.At("nodes").ValueMap();
  // Lambda to get a valid property list for a label, or nullopt if not valid
  auto get_valid_props = [&](const mgp::Map &nodes, const mgp::Labels &labels, size_t l) -> std::optional<mgp::List> {
    std::string_view label = labels[l];
    if (!nodes.KeyExists(label)) return std::nullopt;
    auto props = nodes.At(label).ValueList();
    if (props.Size() == 0) return std::nullopt;
    if (!props[0].IsString()) return std::nullopt;
    return props;
  };

  std::vector<std::string_view> excluded_props;

  // Collect excluded properties if exclusion mode is detected
  for (size_t l = 0; l < labels.Size(); ++l) {
    auto props_opt = get_valid_props(nodes, labels, l);
    if (!props_opt) continue;
    auto props = *props_opt;
    auto first_prop = props[0].ValueString();
    if (first_prop.size() > 0 && first_prop[0] == '-') {
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

  // Exclusion mode: skip if property is in excluded_props
  if (!excluded_props.empty()) {
    for (auto excluded : excluded_props) {
      if (prop_name == excluded) {
        return false;
      }
    }
    return true;
  }

  // Inclusion mode: check if property is explicitly included
  for (size_t l = 0; l < labels.Size(); ++l) {
    auto props_opt = get_valid_props(nodes, labels, l);
    if (!props_opt) continue;
    auto props = *props_opt;
    if (props[0].ValueString() == "*") return true;
    for (size_t i = 0; i < props.Size(); ++i) {
      if (props[i].ValueString() == prop_name) return true;
    }
  }
  return false;
}

// Helper function to check if a relationship property should be included
// Determines if a relationship property should be included in the output based on config filtering rules.
bool ShouldIncludeRelProperty(std::string_view prop_name, const mgp::Map &config, const std::string_view &rel_type) {
  if (!config.KeyExists("rels")) {
    // No filtering config: include all properties
    return true;
  }

  auto rels = config.At("rels").ValueMap();
  if (!rels.KeyExists(rel_type)) {
    // No config for this relationship type: include all properties
    return true;
  }

  auto props = rels.At(rel_type).ValueList();
  if (props.Size() == 0) {
    // Empty config: include all properties
    return true;
  }

  auto first_prop = props[0].ValueString();
  if (first_prop.size() > 0 && first_prop[0] == '-') {
    // Exclusion mode for relationships
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
    for (auto excluded : excluded_props) {
      if (prop_name == excluded) {
        return false;
      }
    }
    return true;
  }

  // Inclusion mode for relationships
  if (first_prop == "*") return true;
  for (size_t i = 0; i < props.Size(); ++i) {
    if (props[i].ValueString() == prop_name) return true;
  }
  return false;
}

// Main implementation function
// Converts a list of paths (or a single path) into a hierarchical tree structure.
mgp::Value ConvertToTreeImpl(const mgp::Value &value, const mgp::Map &config, mgp_memory *memory) {
  if (value.IsNull()) {
    // Null input: return as is
    return value;
  }

  // If the value is a List of Paths, convert each path and merge into a tree
  if (value.IsList()) {
    auto paths = value.ValueList();
    if (paths.Size() == 0) {
      // Empty list: return as is
      return value;
    }

    // Get the first path to extract the root node
    auto first_path = paths[0].ValuePath();
    auto root_node = first_path.GetNodeAt(0);

    // Create root node map with properties
    mgp::Map root_map;
    auto root_props = root_node.Properties();
    auto root_labels = root_node.Labels();

    // Copy all root node properties that pass filtering
    for (const auto &[key, value] : root_props) {
      if (ShouldIncludeProperty(key, config, root_labels)) {
        root_map.Insert(key, value);
      }
    }

    // Lambda to insert _type and _id from labels and node id into a map
    auto insert_type_and_id = [](mgp::Map &map, const mgp::Labels &labels, const mgp::Node &node) {
      if (labels.Size() == 1) {
        map.Insert("_type", mgp::Value(labels[0]));
      } else if (labels.Size() > 1) {
        mgp::List label_list;
        for (size_t l = 0; l < labels.Size(); ++l) {
          label_list.Append(mgp::Value(labels[l]));
        }
        map.Insert("_type", mgp::Value(label_list));
      }
      map.Insert("_id", mgp::Value(node.Id().AsInt()));
    };

    // Add type and id to root node
    insert_type_and_id(root_map, root_labels, root_node);

    // Process all paths to collect relationships and build the tree
    for (size_t path_idx = 0; path_idx < paths.Size(); path_idx++) {
      auto path = paths[path_idx].ValuePath();

      // For each relationship in the path, add the target node to the appropriate relationship array
      for (size_t i = 0; i < path.Length(); i++) {
        auto rel = path.GetRelationshipAt(i);
        auto target_node = path.GetNodeAt(i + 1);
        std::string rel_type{rel.Type()};

        // Create or get relationship array for this type
        mgp::List rel_list;
        if (!root_map.KeyExists(rel_type)) {
          rel_list = mgp::List();
        } else {
          rel_list = root_map.At(rel_type).ValueList();
        }

        // Create target node map
        mgp::Map target_map;

        // Copy all target node properties that pass filtering
        auto target_props = target_node.Properties();
        auto target_labels = target_node.Labels();
        for (const auto &[key, value] : target_props) {
          if (ShouldIncludeProperty(key, config, target_labels)) {
            target_map.Insert(key, value);
          }
        }

        // Add relationship properties with type-prefixed keys
        auto rel_props = rel.Properties();
        for (const auto &[key, value] : rel_props) {
          if (ShouldIncludeRelProperty(key, config, rel_type)) {
            std::string prefixed_key = rel_type + "." + key;
            target_map.Insert(prefixed_key, value);
          }
        }

        // Add type and id to target node
        insert_type_and_id(target_map, target_labels, target_node);

        // Only add unique target nodes to the relationship array (by _id)
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
        // Update the relationship array in the root map
        root_map.Update(rel_type, mgp::Value(rel_list));
      }
    }

    // Return the constructed tree as a map
    return mgp::Value(root_map);
  }

  // If single path, convert it to a list and process recursively
  if (value.IsPath()) {
    auto list = mgp::List(1);
    list.Append(value);
    return ConvertToTreeImpl(mgp::Value(list), config, memory);
  }

  // For other types, return as is
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

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
#include <set>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

constexpr const std::string_view kFunctionStr2Object = "str2object";
constexpr const std::string_view kProcedureToTree = "to_tree";

constexpr const std::string_view kParameterPaths = "paths";
constexpr const std::string_view kParameterConfig = "config";
constexpr const std::string_view kParameterString = "string";
constexpr const std::string_view kParameterLowerCaseRels = "lowerCaseRels";

constexpr const std::string_view kReturnValue = "value";

// Forward declarations
mgp::Value ConvertToTreeImpl(const mgp::Value &value, const bool lowerCaseRels, const mgp::Map &config,
                             mgp_memory *memory);
std::optional<mgp::Value> ParseJsonToMgpValue(const nlohmann::json &json_obj, mgp_memory *memory);

// Struct to encapsulate property filter logic for a label
struct NodePropertyFilter {
  enum class Mode { INCLUDE, EXCLUDE, WILDCARD, INVALID };
  Mode mode = Mode::INVALID;
  std::set<std::string> properties;

  NodePropertyFilter() = default;
  NodePropertyFilter(const mgp::List &props) {
    if (props.Size() == 0) {
      mode = Mode::INVALID;
      return;
    }
    // All entries must be strings
    for (size_t i = 0; i < props.Size(); ++i) {
      if (!props[i].IsString()) {
        mode = Mode::INVALID;
        return;
      }
    }
    std::string_view first = props[0].ValueString();
    if (first == "*") {
      mode = Mode::WILDCARD;
      return;
    }
    if (!first.empty() && first[0] == '-') {
      mode = Mode::EXCLUDE;
      for (size_t i = 0; i < props.Size(); ++i) {
        std::string_view val_sv = props[i].ValueString();
        std::string val(val_sv);
        if (!val.empty() && val[0] == '-') val = val.substr(1);
        properties.insert(std::move(val));
      }
      return;
    }
    // Otherwise, inclusion mode
    mode = Mode::INCLUDE;
    for (size_t i = 0; i < props.Size(); ++i) {
      std::string_view val_sv = props[i].ValueString();
      properties.insert(std::string(val_sv));
    }
  }
  bool ShouldInclude(const std::string_view &prop_name) const {
    switch (mode) {
      case Mode::WILDCARD:
        return true;
      case Mode::EXCLUDE:
        return properties.count(std::string(prop_name)) == 0;
      case Mode::INCLUDE:
        return properties.count(std::string(prop_name)) > 0;
      default:
        return false;
    }
  }
  bool IsValid() const { return mode != Mode::INVALID; }
};

// Struct to encapsulate property filter logic for a relationship type
struct RelPropertyFilter {
  enum class Mode { INCLUDE, EXCLUDE, WILDCARD, INVALID };
  Mode mode = Mode::INVALID;
  std::set<std::string> properties;

  RelPropertyFilter() = default;
  RelPropertyFilter(const mgp::List &props) {
    if (props.Size() == 0) {
      mode = Mode::INVALID;
      return;
    }
    // All entries must be strings
    for (size_t i = 0; i < props.Size(); ++i) {
      if (!props[i].IsString()) {
        mode = Mode::INVALID;
        return;
      }
    }
    std::string_view first = props[0].ValueString();
    if (first == "*") {
      mode = Mode::WILDCARD;
      return;
    }
    if (!first.empty() && first[0] == '-') {
      mode = Mode::EXCLUDE;
      for (size_t i = 0; i < props.Size(); ++i) {
        std::string_view val_sv = props[i].ValueString();
        std::string val(val_sv);
        if (!val.empty() && val[0] == '-') val = val.substr(1);
        properties.insert(std::move(val));
      }
      return;
    }
    // Otherwise, inclusion mode
    mode = Mode::INCLUDE;
    for (size_t i = 0; i < props.Size(); ++i) {
      std::string_view val_sv = props[i].ValueString();
      properties.insert(std::string(val_sv));
    }
  }
  bool ShouldInclude(const std::string_view &prop_name) const {
    switch (mode) {
      case Mode::WILDCARD:
        return true;
      case Mode::EXCLUDE:
        return properties.count(std::string(prop_name)) == 0;
      case Mode::INCLUDE:
        return properties.count(std::string(prop_name)) > 0;
      default:
        return false;
    }
  }
  bool IsValid() const { return mode != Mode::INVALID; }
};

// Config struct to hold all relationship property filters
struct RelPropertyFilterConfig {
  std::map<std::string, RelPropertyFilter> filters;
  RelPropertyFilterConfig(const mgp::Map &rels) {
    for (const auto &[key, value] : rels) {
      if (!value.IsList()) continue;
      filters.emplace(std::string(key), RelPropertyFilter(value.ValueList()));
    }
  }
  // Returns the filter for a given rel_type, or a default wildcard filter if not present
  const RelPropertyFilter &GetFilter(const std::string_view &rel_type) const {
    static RelPropertyFilter wildcard_filter;
    auto it = filters.find(std::string(rel_type));
    if (it != filters.end()) return it->second;
    // If not present, default to wildcard (include all)
    static RelPropertyFilter default_wildcard(mgp::List({mgp::Value("*")}));
    return default_wildcard;
  }
};

// Helper function to check if a property should be included
// Determines if a node property should be included in the output based on config filtering rules.
bool ShouldIncludeProperty(std::string_view prop_name, const mgp::Map &config, const mgp::Labels &labels) {
  if (!config.KeyExists("nodes")) {
    // No filtering config: include all properties
    return true;
  }
  auto nodes = config.At("nodes").ValueMap();
  std::vector<NodePropertyFilter> filters;
  for (size_t label_idx = 0; label_idx < labels.Size(); ++label_idx) {
    std::string_view label = labels[label_idx];
    if (!nodes.KeyExists(label)) continue;
    auto props = nodes.At(label).ValueList();
    NodePropertyFilter filter(props);
    if (filter.IsValid()) filters.push_back(filter);
  }
  if (filters.empty()) return false;
  // Exclusion mode takes precedence if any filter is exclusion
  for (const auto &filter : filters) {
    if (filter.mode == NodePropertyFilter::Mode::EXCLUDE) {
      return filter.ShouldInclude(prop_name);
    }
  }
  // Wildcard mode if any filter is wildcard
  for (const auto &filter : filters) {
    if (filter.mode == NodePropertyFilter::Mode::WILDCARD) {
      return true;
    }
  }
  // Otherwise, inclusion mode: include if any filter includes the property
  for (const auto &filter : filters) {
    if (filter.mode == NodePropertyFilter::Mode::INCLUDE && filter.ShouldInclude(prop_name)) {
      return true;
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
  static RelPropertyFilterConfig rel_filter_config(rels);
  const RelPropertyFilter &filter = rel_filter_config.GetFilter(rel_type);
  return filter.ShouldInclude(prop_name);
}

// Main implementation function
// Converts a list of paths (or a single path) into a hierarchical tree structure.
mgp::Value ConvertToTreeImpl(const mgp::Value &input, const bool lowerCaseRels, const mgp::Map &config,
                             mgp_memory *memory) {
  auto value = mgp::Value(input);

  if (value.IsNull()) {
    // Null input: return as is
    return mgp::Value(mgp::Map());
  }

  // If the value is a List of Paths, convert each path and merge into a tree
  if (value.IsList()) {
    auto paths = value.ValueList();
    if (paths.Size() == 0) {
      // Empty list: return as is
      return mgp::Value(mgp::Map());
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
        if (lowerCaseRels) {
          std::transform(rel_type.begin(), rel_type.end(), rel_type.begin(),
                         [](unsigned char c) { return std::tolower(c); });
        }

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
    return ConvertToTreeImpl(mgp::Value(list), lowerCaseRels, config, memory);
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

void to_tree(mgp_list *args, mgp_graph *graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto input = arguments[0];

    const bool lowerCaseRels = arguments.Size() > 1 ? arguments[1].ValueBool() : true;
    const mgp::Map config = arguments.Size() > 2 ? arguments[2].ValueMap() : mgp::Map();

    // Convert the input value to tree structure
    auto result_value = ConvertToTreeImpl(input, lowerCaseRels, config, memory);

    auto record = record_factory.NewRecord();
    record.Insert(kReturnValue.data(), result_value);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);

    mgp::AddFunction(str2object, kFunctionStr2Object, {mgp::Parameter(kParameterString, mgp::Type::String)}, module,
                     memory);
    mgp::AddProcedure(to_tree, kProcedureToTree, mgp::ProcedureType::Read,
                      {mgp::Parameter(kParameterPaths, mgp::Type::Any),
                       mgp::Parameter(kParameterLowerCaseRels, mgp::Type::Bool, true),
                       mgp::Parameter(kParameterConfig, mgp::Type::Map, mgp::Value(mgp::Map()))},
                      {mgp::Return(kReturnValue, mgp::Type::Map)}, module, memory);

  } catch (const std::exception &e) {
    std::cerr << "Error while initializing query module: " << e.what() << '\n';
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }

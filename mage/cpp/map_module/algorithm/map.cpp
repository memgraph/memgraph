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

#include <fmt/format.h>
#include <list>
#include <sstream>

#include "map.hpp"

const auto number_of_elements_in_pair = 2;

/*NOTE: FromNodes isn't 1:1 for graphQL, because first, we need to extend C and CPP API to iterate vertices using ctx
object, since the `FromNodes` procedure (function if we want to change API) needs to iterate over all graph nodes*/
void Map::FromNodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto label{arguments[0].ValueString()};
    const auto property{arguments[1].ValueString()};
    mgp::Map map_result{};

    const auto all_nodes = mgp::Graph(memgraph_graph).Nodes();
    for (const auto node : all_nodes) {
      if (!node.HasLabel(label) || !node.Properties().contains(std::string(property))) continue;

      std::ostringstream oss;
      oss << node.GetProperty(std::string(property));
      const auto key = oss.str();

      mgp::Map map{};
      map.Update("identity", mgp::Value(node.Id().AsInt()));

      mgp::List labels{};
      for (const auto &label : node.Labels()) {
        labels.AppendExtend(mgp::Value(label));
      }
      map.Update("labels", mgp::Value(std::move(labels)));

      const auto property_map = node.Properties();
      mgp::Map properties{};
      for (const auto &[key, value] : property_map) {
        properties.Insert(key, value);
      }
      map.Update("properties", mgp::Value(std::move(properties)));

      map_result.Update(key, mgp::Value(std::move(map)));
    }

    auto record = record_factory.NewRecord();
    record.Insert(std::string(kResultFromNodes).c_str(), map_result);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Map::FromValues(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    const auto values{arguments[0].ValueList()};
    mgp::Map map{};

    if (values.Size() % 2) {
      throw mgp::ValueException("List needs to have an even number of elements");
    }

    auto iterator = values.begin();
    while (iterator != values.end()) {
      const auto key_value = *iterator;
      ++iterator;
      const auto value = *iterator;
      ++iterator;

      // Skip pairs whose key is null.
      if (key_value.IsNull()) {
        continue;
      }
      std::ostringstream oss;
      oss << key_value;
      map.Update(oss.str(), value);
    }

    result.SetValue(map);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

void Map::SetKey(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    // A null map is treated as empty and a null key is a no-op; any other non-map
    // (e.g. a node) falls through to ValueMap() and throws, like the sibling functions.
    mgp::Map map = arguments[0].IsNull() ? mgp::Map() : mgp::Map(arguments[0].ValueMap());
    if (!arguments[1].IsNull()) {
      map.Update(std::string(arguments[1].ValueString()), arguments[2]);
    }
    result.SetValue(std::move(map));

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

void Map::RemoveRecursion(mgp::Map &result, bool recursive, std::string_view key) {
  for (auto element : result) {
    if (element.key == key) {
      result.Erase(element.key);
      continue;
    }
    if (element.value.IsMap() && recursive) {
      // TO-DO no need for non_const_value_map in new version of memgraph
      mgp::Map non_const_value_map = mgp::Map(element.value.ValueMap());
      RemoveRecursion(non_const_value_map, recursive, key);
      if (non_const_value_map.Empty()) {
        result.Erase(element.key);
        continue;
      }
      result.Update(element.key, mgp::Value(std::move(non_const_value_map)));
    }
  }
}

void Map::RemoveKey(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    const auto map = arguments[0].ValueMap();
    const auto key = std::string(arguments[1].ValueString());
    const auto config = arguments[2].ValueMap();
    const auto recursive = (config.At("recursive").IsBool()) ? config.At("recursive").ValueBool() : false;
    mgp::Map map_removed = mgp::Map(map);

    RemoveRecursion(map_removed, recursive, key);

    result.SetValue(std::move(map_removed));

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

void Map::FromPairs(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    const auto list = arguments[0].ValueList();

    mgp::Map pairs_map;

    for (const auto inside_list : list) {
      if (inside_list.ValueList().Size() != number_of_elements_in_pair) {
        throw mgp::IndexException(
            fmt::format("Pairs must consist of {} elements exactly.",
                        number_of_elements_in_pair));  // NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject)
      }
      if (!inside_list.ValueList()[0].IsString()) {
        throw mgp::ValueException("All keys have to be type string.");
      }
      pairs_map.Update(inside_list.ValueList()[0].ValueString(), inside_list.ValueList()[1]);
    }

    result.SetValue(std::move(pairs_map));

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

void Map::Merge(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    const auto map1 = arguments[0].IsMap() ? arguments[0].ValueMap() : mgp::Map();
    const auto map2 = arguments[1].IsMap() ? arguments[1].ValueMap() : mgp::Map();

    mgp::Map merged_map = mgp::Map(map2);
    for (const auto element : map1) {
      if (!merged_map.KeyExists(element.key)) {
        merged_map.Insert(element.key, element.value);
      }
    }

    result.SetValue(std::move(merged_map));

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

void Map::FlattenRecursion(mgp::Map &result, const mgp::Map &input, const std::string &key,
                           const std::string &delimiter) {
  for (auto element : input) {
    const std::string el_key(element.key);
    if (element.value.IsMap()) {
      std::string new_key = key;
      new_key += el_key;
      new_key += delimiter;
      FlattenRecursion(result, element.value.ValueMap(), new_key, delimiter);
    } else {
      result.Insert(key + el_key, element.value);
    }
  }
}

void Map::Flatten(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    const mgp::Map map = arguments[0].ValueMap();
    const std::string delimiter(arguments[1].ValueString());
    mgp::Map result_map = mgp::Map();
    FlattenRecursion(result_map, map, "", delimiter);
    result.SetValue(std::move(result_map));

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

void Map::FromLists(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  auto arguments = mgp::List(args);
  auto result_object = mgp::Result(res);
  try {
    mgp::List list1 = arguments[0].ValueList();
    mgp::List list2 = arguments[1].ValueList();

    const auto expected_list_size = list1.Size();
    if (expected_list_size != list2.Size()) {
      throw mgp::ValueException("Lists must be of same size");
    }
    // Empty lists yield an empty map.
    mgp::Map result = mgp::Map();
    for (size_t i = 0; i < expected_list_size; i++) {
      result.Update(list1[i].ValueString(), list2[i]);
    }
    result_object.SetValue(std::move(result));

  } catch (const std::exception &e) {
    result_object.SetErrorMessage(e.what());
    return;
  }
}

void Map::RemoveRecursionSet(mgp::Map &result, bool recursive, std::unordered_set<std::string> &set) {
  for (auto element : result) {
    bool inSet = false;
    if (set.contains(std::string(element.key))) {
      inSet = true;
    }
    if (inSet) {
      result.Erase(element.key);
      continue;
    }
    if (element.value.IsMap() && recursive) {
      mgp::Map non_const_value_map = mgp::Map(element.value.ValueMap());
      RemoveRecursionSet(non_const_value_map, recursive, set);
      if (non_const_value_map.Empty()) {
        result.Erase(element.key);
        continue;
      }
      result.Update(element.key, mgp::Value(std::move(non_const_value_map)));
    }
  }
}

void Map::RemoveKeys(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    mgp::Map map = arguments[0].ValueMap();
    const mgp::List list = arguments[1].ValueList();
    const auto config = arguments[2].ValueMap();
    const auto recursive = (config.At("recursive").IsBool()) ? config.At("recursive").ValueBool() : false;
    std::unordered_set<std::string> set;
    for (auto elem : list) {
      set.insert(std::string(elem.ValueString()));
    }
    RemoveRecursionSet(map, recursive, set);
    result.SetValue(std::move(map));

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// mgp::Result::SetValue has no generic Value overload, so dispatch on the runtime type
// (mirrors Collections::SetResult) with explicit null handling.
void Map::SetResult(mgp::Result &result, const mgp::Value &value) {
  switch (value.Type()) {
    case mgp::Type::Null:
      result.SetValue();
      break;
    case mgp::Type::Bool:
      result.SetValue(value.ValueBool());
      break;
    case mgp::Type::Int:
      result.SetValue(value.ValueInt());
      break;
    case mgp::Type::Double:
      result.SetValue(value.ValueDouble());
      break;
    case mgp::Type::String:
      result.SetValue(value.ValueString());
      break;
    case mgp::Type::List:
      result.SetValue(value.ValueList());
      break;
    case mgp::Type::Map:
      result.SetValue(value.ValueMap());
      break;
    case mgp::Type::Node:
      result.SetValue(value.ValueNode());
      break;
    case mgp::Type::Relationship:
      result.SetValue(value.ValueRelationship());
      break;
    case mgp::Type::Path:
      result.SetValue(value.ValuePath());
      break;
    case mgp::Type::Date:
      result.SetValue(value.ValueDate());
      break;
    case mgp::Type::LocalTime:
      result.SetValue(value.ValueLocalTime());
      break;
    case mgp::Type::LocalDateTime:
      result.SetValue(value.ValueLocalDateTime());
      break;
    case mgp::Type::Duration:
      result.SetValue(value.ValueDuration());
      break;
    case mgp::Type::ZonedDateTime:
      result.SetValue(value.ValueZonedDateTime());
      break;
    case mgp::Type::Point2d:
      result.SetValue(value.ValuePoint2d());
      break;
    case mgp::Type::Point3d:
      result.SetValue(value.ValuePoint3d());
      break;
    case mgp::Type::Enum:
      result.SetValue(value.ValueEnum());
      break;
    default:
      std::ostringstream oss;
      oss << value.Type();
      throw mgp::ValueException("map.get has no Result.SetValue for: " + oss.str());
  }
}

void Map::Get(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    const auto map = arguments[0].ValueMap();
    const auto key = std::string(arguments[1].ValueString());
    const auto default_value = arguments[2];
    const auto fail = arguments[3].ValueBool();

    if (map.KeyExists(key)) {
      SetResult(result, map.At(key));
      return;
    }
    if (!default_value.IsNull()) {
      SetResult(result, default_value);
      return;
    }
    if (fail) {
      std::ostringstream oss;
      oss << "Key '" << key << "' is not one of the existing keys [";
      bool first = true;
      for (const auto element : map) {
        oss << (first ? "" : ", ") << element.key;
        first = false;
      }
      oss << "]";
      throw mgp::ValueException(oss.str());
    }
    result.SetValue();

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

void Map::MergeList(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    const auto maps = arguments[0].ValueList();
    mgp::Map merged{};
    for (const auto element_map : maps) {
      for (const auto entry : element_map.ValueMap()) {
        merged.Update(entry.key, entry.value);  // last key wins
      }
    }
    result.SetValue(std::move(merged));

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

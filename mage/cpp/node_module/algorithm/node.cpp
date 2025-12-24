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

#include "node.hpp"

#include <unordered_set>

#include "mgp.hpp"

namespace {

std::unordered_map<std::string_view, uint8_t> GetTypeDirection(const mgp::Value &types) {
  std::unordered_map<std::string_view, uint8_t> result;
  for (const auto &type_value : types.ValueList()) {
    auto type = type_value.ValueString();
    if (type.starts_with('<')) {
      if (type.ends_with('>')) {
        throw mgp::ValueException("<type> format not allowed. Use type instead.");
      }
      result[type.substr(1, type.size() - 1)] |= 1;
    } else if (type.ends_with('>')) {
      result[type.substr(0, type.size() - 1)] |= 2;
    } else {
      result[type] |= 3;
    }
  }
  return result;
}

mgp::List GetRelationshipTypes(const mgp::Value &node_value, const mgp::Value &types_value) {
  auto type_direction = GetTypeDirection(types_value);

  std::unordered_set<std::string_view> types;
  const auto node = node_value.ValueNode();
  if (type_direction.empty()) {
    for (const auto relationship : node.InRelationships()) {
      types.insert(relationship.Type());
    }
    for (const auto relationship : node.OutRelationships()) {
      types.insert(relationship.Type());
    }
  } else {
    for (const auto relationship : node.InRelationships()) {
      if (type_direction[relationship.Type()] & 1) {
        types.insert(relationship.Type());
      }
    }
    for (const auto relationship : node.OutRelationships()) {
      if (type_direction[relationship.Type()] & 2) {
        types.insert(relationship.Type());
      }
    }
  }

  mgp::List result{types.size()};
  for (const auto &type : types) {
    auto value = mgp::Value(type);
    result.Append(value);
  }
  return result;
}
}  // namespace

bool Node::RelationshipExist(const mgp::Node &node, std::string &rel_type) {
  char direction{' '};
  if (rel_type[0] == '<' && rel_type[rel_type.size() - 1] == '>') {
    throw mgp::ValueException("Invalid relationship specification!");
  } else if (rel_type[rel_type.size() - 1] == '>') {
    direction = rel_type[rel_type.size() - 1];
    rel_type.pop_back();
  } else if (rel_type[0] == '<') {
    direction = rel_type[0];
    rel_type.erase(0, 1);
  }
  for (auto rel : node.OutRelationships()) {
    if (std::string(rel.Type()) == rel_type && direction != '<') {
      return true;
    }
  }
  for (auto rel : node.InRelationships()) {
    if (std::string(rel.Type()) == rel_type && direction != '>') {
      return true;
    }
  }
  return false;
}

void Node::RelationshipsExist(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const mgp::Node node = arguments[0].ValueNode();
    const mgp::List relationships = arguments[1].ValueList();
    if (relationships.Size() == 0) {
      throw mgp::ValueException("Input relationships list must not be empty!");
    }
    mgp::Map relationship_map = mgp::Map();
    for (auto rel : relationships) {
      std::string rel_type{rel.ValueString()};
      if (RelationshipExist(node, rel_type)) {
        relationship_map.Insert(rel.ValueString(), mgp::Value(true));
      } else {
        relationship_map.Insert(rel.ValueString(), mgp::Value(false));
      }
    }
    auto record = record_factory.NewRecord();
    record.Insert(std::string(kReturnRelationshipsExist).c_str(), std::move(relationship_map));

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

bool Node::FindRelationship(std::unordered_set<std::string_view> types, mgp::Relationships relationships) {
  if (types.contains("") && relationships.cbegin() != relationships.cend()) {
    return true;
  }
  for (auto relationship : relationships) {
    if (types.contains(relationship.Type())) {
      return true;
    }
  }
  return false;
}

void Node::RelationshipExists(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto node = arguments[0].ValueNode();
    auto patterns = arguments[1].ValueList();

    if (patterns.Empty()) {
      patterns.AppendExtend(mgp::Value(""));
    }

    std::unordered_set<std::string_view> in_rels;
    std::unordered_set<std::string_view> out_rels;

    for (auto pattern_value : patterns) {
      auto pattern = pattern_value.ValueString();
      if (pattern[0] == '<' && pattern[pattern.size() - 1] == '>') {
        throw mgp::ValueException("Invalid relationship specification!");
      }
      if (pattern[0] == '<') {
        in_rels.insert(pattern.substr(1, pattern.size()));
        continue;
      }
      if (pattern[pattern.size() - 1] == '>') {
        out_rels.insert(pattern.substr(0, pattern.size() - 1));
        continue;
      }
      in_rels.insert(pattern);
      out_rels.insert(pattern);
    }

    auto record = record_factory.NewRecord();
    record.Insert(
        std::string(kResultRelationshipExists).c_str(),
        FindRelationship(in_rels, node.InRelationships()) || FindRelationship(out_rels, node.OutRelationships()));
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Node::RelationshipTypes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto record = record_factory.NewRecord();
    record.Insert(std::string(kResultRelationshipTypes).c_str(), GetRelationshipTypes(arguments[0], arguments[1]));

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Node::DegreeIn(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    const auto node = arguments[0].ValueNode();
    const auto type = arguments[1].ValueString();
    if (type.size() == 0) {
      result.SetValue((int64_t)node.InDegree());
      return;
    }
    int64_t degree = 0;
    for (const auto rel : node.InRelationships()) {
      if (rel.Type() == type) {
        degree += 1;
      }
    }
    result.SetValue(degree);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

void Node::DegreeOut(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    const auto node = arguments[0].ValueNode();
    const auto type = arguments[1].ValueString();
    if (type.size() == 0) {
      result.SetValue((int64_t)node.OutDegree());
      return;
    }
    int64_t degree = 0;
    for (const auto rel : node.OutRelationships()) {
      if (rel.Type() == type) {
        degree += 1;
      }
    }
    result.SetValue(degree);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

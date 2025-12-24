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

#include "nodes.hpp"
#include <unordered_set>
#include "mgp.hpp"

bool Nodes::RelationshipExist(const mgp::Node &node, std::string &rel_type) {
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
  for (const auto rel : node.OutRelationships()) {
    if (std::string(rel.Type()) == rel_type && direction != '<') {
      return true;
    }
  }
  for (const auto rel : node.InRelationships()) {
    if (std::string(rel.Type()) == rel_type && direction != '>') {
      return true;
    }
  }
  return false;
}

void Nodes::RelationshipsExist(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const mgp::Graph graph = mgp::Graph(memgraph_graph);
    const mgp::List nodes = arguments[0].ValueList();
    const mgp::List relationships = arguments[1].ValueList();
    if (nodes.Size() == 0 || relationships.Size() == 0) {
      throw mgp::ValueException("Input lists must not be empty!");
    }

    for (auto element : nodes) {
      if (!element.IsNode() && !element.IsInt()) {
        throw mgp::ValueException("Input arguments must be nodes or their ID's");
      }
      mgp::Node node = element.IsNode() ? element.ValueNode() : graph.GetNodeById(mgp::Id::FromInt(element.ValueInt()));
      mgp::Map return_map = mgp::Map();
      mgp::Map relationship_map = mgp::Map();
      for (auto rel : relationships) {
        std::string rel_type{rel.ValueString()};
        if (RelationshipExist(node, rel_type)) {
          relationship_map.Insert(rel.ValueString(), mgp::Value(true));
        } else {
          relationship_map.Insert(rel.ValueString(), mgp::Value(false));
        }
      }
      return_map.Insert(std::string(kNodeRelationshipsExist).c_str(), mgp::Value(node));
      return_map.Insert(std::string(kRelationshipsExistStatus).c_str(), mgp::Value(std::move(relationship_map)));
      auto record = record_factory.NewRecord();
      record.Insert(std::string(kReturnRelationshipsExist).c_str(), std::move(return_map));
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

namespace {
void ThrowException(const mgp::Value &value) {
  std::ostringstream oss;
  oss << value.Type();
  throw mgp::ValueException("Unsuppported type for this operation, received type: " + oss.str());
};

mgp::Value InsertNodeRelationshipTypes(const mgp::Node &node,
                                       std::unordered_map<std::string, uint8_t> &type_direction) {
  mgp::Map result{};
  result.Insert("node", mgp::Value(node));

  std::unordered_set<std::string_view> types;
  if (type_direction.empty()) {
    for (const auto relationship : node.InRelationships()) {
      types.insert(relationship.Type());
    }
    for (const auto relationship : node.OutRelationships()) {
      types.insert(relationship.Type());
    }
  } else {
    for (const auto relationship : node.InRelationships()) {
      if (type_direction[std::string(relationship.Type())] & 1) {
        types.insert(relationship.Type());
      }
    }
    for (const auto relationship : node.OutRelationships()) {
      if (type_direction[std::string(relationship.Type())] & 2) {
        types.insert(relationship.Type());
      }
    }
  }

  mgp::List types_list{types.size()};
  for (const auto &type : types) {
    auto value = mgp::Value(type);
    types_list.Append(value);
  }
  result.Insert("types", mgp::Value(std::move(types_list)));

  return mgp::Value(std::move(result));
}

std::unordered_map<std::string, uint8_t> GetTypeDirection(const mgp::Value &types) {
  std::unordered_map<std::string, uint8_t> result;
  for (const auto &type_value : types.ValueList()) {
    // string view of temporary object ValueList(), invalid after the loop
    auto type = type_value.ValueString();
    if (type.starts_with('<')) {
      if (type.ends_with('>')) {
        throw mgp::ValueException("<type> format not allowed. Use type instead.");
      }
      auto key = std::string(type.substr(1, type.size() - 1));
      result[key] |= 1;
    } else if (type.ends_with('>')) {
      auto key = std::string(type.substr(0, type.size() - 1));
      result[key] |= 2;
    } else {
      auto key = std::string(type);
      result[key] |= 3;
    }
  }
  return result;
}

mgp::List GetRelationshipTypes(mgp_graph *memgraph_graph, const mgp::Value &argument, const mgp::Value &types) {
  mgp::List result{};
  mgp::Graph graph{memgraph_graph};
  auto type_direction = GetTypeDirection(types);

  auto ParseNode = [&](const mgp::Value &value) {
    if (value.IsNode()) {
      result.AppendExtend(InsertNodeRelationshipTypes(value.ValueNode(), type_direction));
    } else if (value.IsInt()) {
      result.AppendExtend(
          InsertNodeRelationshipTypes(graph.GetNodeById(mgp::Id::FromInt(value.ValueInt())), type_direction));
    } else {
      ThrowException(value);
    }
  };

  if (!argument.IsList()) {
    ParseNode(argument);
    return result;
  }

  for (const auto &list_item : argument.ValueList()) {
    ParseNode(list_item);
  }

  return result;
}

void DetachDeleteNode(const mgp::Value &node, mgp::Graph &graph) {
  if (node.IsInt()) {
    graph.DetachDeleteNode(graph.GetNodeById(mgp::Id::FromInt(node.ValueInt())));
  } else if (node.IsNode()) {
    graph.DetachDeleteNode(node.ValueNode());
  } else {
    ThrowException(node);
  }
}

}  // namespace

void Nodes::RelationshipTypes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto record = record_factory.NewRecord();
    record.Insert(std::string(kResultRelationshipTypes).c_str(),
                  GetRelationshipTypes(memgraph_graph, arguments[0], arguments[1]));

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Nodes::Delete(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    mgp::Graph graph{memgraph_graph};
    auto nodes{arguments[0]};

    if (!nodes.IsList()) {
      DetachDeleteNode(nodes, graph);
      return;
    }

    for (const auto &list_item : nodes.ValueList()) {
      DetachDeleteNode(list_item, graph);
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Nodes::Link(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    mgp::Graph graph = mgp::Graph(memgraph_graph);
    const mgp::List list_nodes = arguments[0].ValueList();
    const std::string_view type{arguments[1].ValueString()};
    const size_t size = list_nodes.Size();
    if (size < minimumNodeListSize) {
      throw mgp::ValueException("You need to input at least 2 nodes");
    }

    for (size_t i = 0; i < size - 1; i++) {
      graph.CreateRelationship(list_nodes[i].ValueNode(), list_nodes[i + 1].ValueNode(), type);
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

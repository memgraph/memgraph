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

#include "refactor.hpp"

#include <string_view>
#include <unordered_set>

#include <fmt/format.h>
#include <mg_utils.hpp>
#include "mgp.hpp"

namespace {
void ThrowInvalidTypeException(const mgp::Value &value) {
  std::ostringstream oss;
  oss << value.Type();
  throw mgp::ValueException(fmt::format("Unsupported type for this operation, received type: {}", oss.str()));
}
}  // namespace

namespace {
void CopyRelProperties(mgp::Relationship &to, mgp::Relationship const &from) {
  for (auto const &[k, v] : from.Properties()) {
    to.SetProperty(k, v);
  }
}
}  // namespace

void Refactor::From(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto relationship{arguments[0].ValueRelationship()};
    auto const new_from{arguments[1].ValueNode()};

    mgp::Graph graph{memgraph_graph};
    auto new_relationship = graph.CreateRelationship(new_from, relationship.To(), relationship.Type());
    CopyRelProperties(new_relationship, relationship);
    graph.DeleteRelationship(relationship);

    auto record = record_factory.NewRecord();
    record.Insert(std::string(kFromResult).c_str(), new_relationship);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Refactor::To(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto relationship{arguments[0].ValueRelationship()};
    auto const new_to{arguments[1].ValueNode()};

    mgp::Graph graph{memgraph_graph};
    auto new_relationship = graph.CreateRelationship(relationship.From(), new_to, relationship.Type());
    CopyRelProperties(new_relationship, relationship);
    graph.DeleteRelationship(relationship);

    auto record = record_factory.NewRecord();
    record.Insert(std::string(kToResult).c_str(), new_relationship);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Refactor::RenameLabel(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto old_label{arguments[0].ValueString()};
    auto new_label{arguments[1].ValueString()};
    const auto nodes{arguments[2].ValueList()};

    int64_t nodes_changed{0};
    for (const auto &node_value : nodes) {
      auto node = node_value.ValueNode();
      if (!node.HasLabel(old_label)) {
        continue;
      }

      node.RemoveLabel(old_label);
      node.AddLabel(new_label);
      nodes_changed++;
    }
    auto record = record_factory.NewRecord();
    record.Insert(std::string(kRenameLabelResult).c_str(), nodes_changed);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Refactor::RenameNodeProperty(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto old_property_name{std::string(arguments[0].ValueString())};
    const auto new_property_name{std::string(arguments[1].ValueString())};
    const auto nodes{arguments[2].ValueList()};

    int64_t nodes_changed{0};
    for (const auto &node_value : nodes) {
      auto node = node_value.ValueNode();
      auto old_property = node.GetProperty(old_property_name);
      if (old_property.IsNull()) {
        continue;
      }

      node.RemoveProperty(old_property_name);
      node.SetProperty(new_property_name, old_property);
      nodes_changed++;
    }

    auto record = record_factory.NewRecord();
    record.Insert(std::string(kRenameLabelResult).c_str(), nodes_changed);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Refactor::InsertCloneNodesRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, const int cycle_id,
                                      const int node_id) {
  auto *node = mgp::graph_get_vertex_by_id(graph, mgp_vertex_id{.as_int = static_cast<int64_t>(node_id)}, memory);
  if (!node) {
    if (mgp::graph_is_transactional(graph)) {
      throw mg_exception::InvalidIDException();
    }
    return;
  }

  auto *record = mgp::result_new_record(result);
  if (record == nullptr) throw mg_exception::NotEnoughMemoryException();

  mg_utility::InsertIntValueResult(record, std::string(kResultClonedNodeId).c_str(), cycle_id, memory);
  mg_utility::InsertNodeValueResult(record, std::string(kResultNewNode).c_str(), node, memory);
  mg_utility::InsertStringValueResult(record, std::string(kResultCloneNodeError).c_str(), "", memory);
}

mgp::Node GetStandinOrCopy(const mgp::List &standin_nodes, const mgp::Node node,
                           const std::map<mgp::Node, mgp::Node> &old_new_node_mirror) {
  for (auto pair : standin_nodes) {
    if (!pair.IsList() || !pair.ValueList()[0].IsNode() || !pair.ValueList()[1].IsNode()) {
      throw mgp::ValueException(
          "Configuration map must consist of specific keys and values described in documentation.");
    }
    if (node == pair.ValueList()[0].ValueNode()) {
      return pair.ValueList()[1].ValueNode();
    }
  }
  try {
    return old_new_node_mirror.at(node);
  } catch (const std::out_of_range &e) {
    throw mgp::ValueException("Can't clone relationship without cloning relationship's source and/or target nodes.");
  }
}

bool CheckIfStandin(const mgp::Node &node, const mgp::List &standin_nodes) {
  for (auto pair : standin_nodes) {
    if (!pair.IsList() || !pair.ValueList()[0].IsNode()) {
      throw mgp::ValueException(
          "Configuration map must consist of specific keys and values described in documentation.");
    }
    if (node == pair.ValueList()[0].ValueNode()) {
      return true;
    }
  }
  return false;
}

void CloneNodes(const std::vector<mgp::Node> &nodes, const mgp::List &standin_nodes, mgp::Graph &graph,
                const std::unordered_set<mgp::Value> &skip_props_searchable,
                std::map<mgp::Node, mgp::Node> &old_new_node_mirror, mgp_graph *memgraph_graph, mgp_result *result,
                mgp_memory *memory) {
  for (auto node : nodes) {
    if (CheckIfStandin(node, standin_nodes)) {
      continue;
    }
    mgp::Node new_node = graph.CreateNode();

    for (auto label : node.Labels()) {
      new_node.AddLabel(label);
    }

    for (auto prop : node.Properties()) {
      if (skip_props_searchable.empty() || !skip_props_searchable.contains(mgp::Value(prop.first))) {
        new_node.SetProperty(prop.first, prop.second);
      }
    }
    old_new_node_mirror.insert({node, new_node});
    Refactor::InsertCloneNodesRecord(memgraph_graph, result, memory, node.Id().AsInt(), new_node.Id().AsInt());
  }
}

void CloneRels(const std::vector<mgp::Relationship> &rels, const mgp::List &standin_nodes, mgp::Graph &graph,
               const std::unordered_set<mgp::Value> &skip_props_searchable,
               std::map<mgp::Node, mgp::Node> &old_new_node_mirror, mgp_graph *memgraph_graph, mgp_result *result,
               mgp_memory *memory) {
  for (auto rel : rels) {
    mgp::Relationship new_relationship =
        graph.CreateRelationship(GetStandinOrCopy(standin_nodes, rel.From(), old_new_node_mirror),
                                 GetStandinOrCopy(standin_nodes, rel.To(), old_new_node_mirror), rel.Type());
    for (auto prop : rel.Properties()) {
      if (skip_props_searchable.empty() || !skip_props_searchable.contains(mgp::Value(prop.first))) {
        new_relationship.SetProperty(prop.first, prop.second);
      }
    }
  }
}

void Refactor::CloneNodesAndRels(mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory,
                                 const std::vector<mgp::Node> &nodes, const std::vector<mgp::Relationship> &rels,
                                 const mgp::Map &config_map) {
  mgp::List standin_nodes;
  mgp::List skip_props;
  if ((!config_map.At("standinNodes").IsList() && !config_map.At("standinNodes").IsNull()) ||
      (!config_map.At("skipProperties").IsList() && !config_map.At("skipProperties").IsNull())) {
    throw mgp::ValueException("Configuration map must consist of specific keys and values described in documentation.");
  }
  if (!config_map.At("standinNodes").IsNull()) {
    standin_nodes = config_map.At("standinNodes").ValueList();
  }
  if (!config_map.At("skipProperties").IsNull()) {
    skip_props = config_map.At("skipProperties").ValueList();
  }
  std::unordered_set<mgp::Value> skip_props_searchable{skip_props.begin(), skip_props.end()};

  auto graph = mgp::Graph(memgraph_graph);

  std::map<mgp::Node, mgp::Node> old_new_node_mirror;
  CloneNodes(nodes, standin_nodes, graph, skip_props_searchable, old_new_node_mirror, memgraph_graph, result, memory);
  CloneRels(rels, standin_nodes, graph, skip_props_searchable, old_new_node_mirror, memgraph_graph, result, memory);
}

void Refactor::CloneSubgraphFromPaths(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result,
                                      mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  try {
    const auto paths = arguments[0].ValueList();
    const auto config_map = arguments[1].ValueMap();

    std::unordered_set<mgp::Node> distinct_nodes;
    std::unordered_set<mgp::Relationship> distinct_relationships;
    for (auto path_value : paths) {
      auto path = path_value.ValuePath();
      for (size_t index = 0; index < path.Length(); index++) {
        distinct_nodes.insert(path.GetNodeAt(index));
        distinct_relationships.insert(path.GetRelationshipAt(index));
      }
      distinct_nodes.insert(path.GetNodeAt(path.Length()));
    }
    std::vector<mgp::Node> nodes_vector{distinct_nodes.begin(), distinct_nodes.end()};
    std::vector<mgp::Relationship> rels_vector{distinct_relationships.begin(), distinct_relationships.end()};
    CloneNodesAndRels(memgraph_graph, result, memory, nodes_vector, rels_vector, config_map);

  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

void Refactor::CloneSubgraph(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  try {
    const auto nodes = arguments[0].ValueList();
    const auto rels = arguments[1].ValueList();
    const auto config_map = arguments[2].ValueMap();

    std::unordered_set<mgp::Node> distinct_nodes;
    std::unordered_set<mgp::Relationship> distinct_rels;

    for (auto node : nodes) {
      distinct_nodes.insert(node.ValueNode());
    }
    for (auto rel : rels) {
      distinct_rels.insert(rel.ValueRelationship());
    }

    if (distinct_rels.size() == 0 && distinct_nodes.size() > 0) {
      for (auto node : distinct_nodes) {
        for (auto rel : node.OutRelationships()) {
          if (distinct_nodes.contains(rel.To())) {
            distinct_rels.insert(rel);
          }
        }
      }
    }

    std::vector<mgp::Node> nodes_vector{distinct_nodes.begin(), distinct_nodes.end()};
    std::vector<mgp::Relationship> rels_vector{distinct_rels.begin(), distinct_rels.end()};

    CloneNodesAndRels(memgraph_graph, result, memory, nodes_vector, rels_vector, config_map);

  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

mgp::Node getCategoryNode(mgp::Graph &graph, std::unordered_set<mgp::Node> &created_nodes,
                          std::string_view new_prop_name_key, mgp::Value &new_node_name, std::string_view new_label) {
  for (auto node : created_nodes) {
    if (node.GetProperty(std::string(new_prop_name_key)) == new_node_name) {
      return node;
    }
  }
  mgp::Node category_node = graph.CreateNode();
  category_node.AddLabel(new_label);
  category_node.SetProperty(std::string(new_prop_name_key), new_node_name);
  created_nodes.insert(category_node);
  return category_node;
}

void Refactor::Categorize(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto graph = mgp::Graph(memgraph_graph);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto original_prop_key = arguments[0].ValueString();
    const auto rel_type = arguments[1].ValueString();
    const auto is_outgoing = arguments[2].ValueBool();
    const auto new_label = arguments[3].ValueString();
    const auto new_prop_name_key = arguments[4].ValueString();
    const auto copy_props_list = arguments[5].ValueList();

    std::unordered_set<mgp::Node> created_nodes;

    for (auto node : graph.Nodes()) {
      auto new_node_name = node.GetProperty(std::string(original_prop_key));
      if (new_node_name.IsNull()) {
        continue;
      }

      auto category_node = getCategoryNode(graph, created_nodes, new_prop_name_key, new_node_name, new_label);

      if (is_outgoing) {
        graph.CreateRelationship(node, category_node, rel_type);
      } else {
        graph.CreateRelationship(category_node, node, rel_type);
      }

      node.RemoveProperty(std::string(original_prop_key));
      for (auto key : copy_props_list) {
        auto prop_key = std::string(key.ValueString());
        auto prop_value = node.GetProperty(prop_key);
        if (prop_value.IsNull() || prop_key == new_prop_name_key) {
          continue;
        }
        category_node.SetProperty(prop_key, prop_value);
        node.RemoveProperty(prop_key);
      }
    }

    auto record = record_factory.NewRecord();
    record.Insert(std::string(kResultCategorize).c_str(), "success");
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

void Refactor::CloneNodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto graph = mgp::Graph(memgraph_graph);
  try {
    const auto nodes = arguments[0].ValueList();
    const auto clone_rels = arguments[1].ValueBool();
    const auto skip_props = arguments[2].ValueList();
    std::unordered_set<std::string_view> skip_props_searchable;

    for (const auto &property_key : skip_props) {
      skip_props_searchable.insert(property_key.ValueString());
    }

    for (const auto &node : nodes) {
      mgp::Node old_node = node.ValueNode();
      mgp::Node new_node = graph.CreateNode();

      for (auto label : old_node.Labels()) {
        new_node.AddLabel(label);
      }

      for (const auto &prop : old_node.Properties()) {
        if (skip_props.Empty() || !skip_props_searchable.contains(prop.first)) {
          new_node.SetProperty(prop.first, prop.second);
        }
      }

      if (clone_rels) {
        for (auto rel : old_node.InRelationships()) {
          graph.CreateRelationship(rel.From(), new_node, rel.Type());
        }

        for (auto rel : old_node.OutRelationships()) {
          graph.CreateRelationship(new_node, rel.To(), rel.Type());
        }
      }

      InsertCloneNodesRecord(memgraph_graph, result, memory, static_cast<int>(old_node.Id().AsInt()),
                             static_cast<int>(new_node.Id().AsInt()));
    }
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

mgp::Relationship Refactor::InvertRel(mgp::Graph &graph, mgp::Relationship &rel) {
  const auto old_from = rel.From();
  const auto old_to = rel.To();

  auto new_rel = graph.CreateRelationship(old_to, old_from, rel.Type());
  CopyRelProperties(new_rel, rel);
  graph.DeleteRelationship(rel);
  return new_rel;
}

void Refactor::Invert(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto graph = mgp::Graph(memgraph_graph);
    mgp::Relationship rel = arguments[0].ValueRelationship();

    auto relationship = InvertRel(graph, rel);
    auto record = record_factory.NewRecord();
    record.Insert(std::string(kResultIdInvert).c_str(), relationship.Id().AsInt());
    record.Insert(std::string(kResultRelationshipInvert).c_str(), relationship);
    record.Insert(std::string(kResultErrorInvert).c_str(), "");
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Refactor::TransferProperties(const mgp::Node &node, mgp::Relationship &rel) {
  for (auto &[key, value] : node.Properties()) {
    rel.SetProperty(key, value);
  }
}

void Refactor::Collapse(mgp::Graph &graph, const mgp::Node &node, const std::string &type,
                        const mgp::RecordFactory &record_factory) {
  if (node.InDegree() != 1 || node.OutDegree() != 1) {
    throw mgp::ValueException("Out and in degree of the nodes both must be 1!");
  }

  const mgp::Node from_node = (*node.InRelationships().begin()).From();
  const mgp::Node to_node = (*node.OutRelationships().begin()).To();
  if (from_node == node && to_node == node) {
    throw mgp::ValueException("Nodes with self relationships are non collapsible!");
  }
  mgp::Relationship new_rel = graph.CreateRelationship(from_node, to_node, type);
  TransferProperties(node, new_rel);

  auto record = record_factory.NewRecord();
  record.Insert(std::string(kReturnIdCollapseNode).c_str(), node.Id().AsInt());
  record.Insert(std::string(kReturnRelationshipCollapseNode).c_str(), new_rel);
  graph.DetachDeleteNode(node);
}

void Refactor::CollapseNode(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    mgp::Graph graph = mgp::Graph(memgraph_graph);
    const mgp::Value input = arguments[0];
    const std::string type{arguments[1].ValueString()};

    if (!input.IsNode() && !input.IsInt() && !input.IsList()) {
      record_factory.SetErrorMessage("Input can only be node, node ID, or list of nodes/IDs");
      return;
    }

    if (input.IsNode()) {
      const mgp::Node node = input.ValueNode();
      Collapse(graph, node, type, record_factory);
    } else if (input.IsInt()) {
      const mgp::Node node = graph.GetNodeById(mgp::Id::FromInt(input.ValueInt()));
      Collapse(graph, node, type, record_factory);
    } else if (input.IsList()) {
      for (auto elem : input.ValueList()) {
        if (elem.IsNode()) {
          const mgp::Node node = elem.ValueNode();
          Collapse(graph, node, type, record_factory);
        } else if (elem.IsInt()) {
          const mgp::Node node = graph.GetNodeById(mgp::Id::FromInt(elem.ValueInt()));
          Collapse(graph, node, type, record_factory);
        } else {
          record_factory.SetErrorMessage("Elements in the list can only be Node or ID");
          return;
        }
      }
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

Refactor::Config::Config(const mgp::Map &config) {
  auto rel_strategy_string = config.At("relationshipSelectionStrategy");
  auto prop_strategy_string = config.At("properties");

  if (rel_strategy_string.IsNull()) {
    SetRelStrategy("incoming");
    return;
  }
  SetRelStrategy(rel_strategy_string.ValueString());
  if (prop_strategy_string.IsNull()) {
    SetPropStrategy("combine");
    return;
  }
  SetPropStrategy(prop_strategy_string.ValueString());
}

void Refactor::Config::SetRelStrategy(std::string_view strategy) {
  if (strategy == "incoming") {
    rel_strategy = RelSelectStrategy::INCOMING;
  } else if (strategy == "outgoing") {
    rel_strategy = RelSelectStrategy::OUTGOING;
  } else if (strategy == "merge") {
    rel_strategy = RelSelectStrategy::MERGE;
  } else {
    throw mgp::ValueException("Invalid relationship selection strategy");
  }
}

void Refactor::Config::SetPropStrategy(std::string_view strategy) {
  if (strategy == "discard") {
    prop_strategy = PropertiesStrategy::DISCARD;
  } else if (strategy == "overwrite" || strategy == "override") {
    prop_strategy = PropertiesStrategy::OVERRIDE;
  } else if (strategy == "combine") {
    prop_strategy = PropertiesStrategy::COMBINE;
  } else {
    throw mgp::ValueException("Invalid properties selection strategy");
  }
}

void Refactor::RenameTypeProperty(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const std::string old_name{arguments[0].ValueString()};
    const std::string new_name{arguments[1].ValueString()};
    const auto rels{arguments[2].ValueList()};

    int64_t rels_changed{0};
    for (auto rel_value : rels) {
      auto rel = rel_value.ValueRelationship();
      const auto prop_value = rel.GetProperty(old_name);
      /*since there is no bug(prop map cant have null values), it is faster to just check isNull
      instead of copying entire properties map and then find*/
      if (prop_value.IsNull()) {
        continue;
      }
      rel.RemoveProperty(old_name);
      rel.SetProperty(new_name, prop_value);
      rels_changed++;
    }

    auto record = record_factory.NewRecord();
    record.Insert(std::string(kRenameTypePropertyResult).c_str(), rels_changed);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

namespace {

template <typename T>
concept GraphObject = std::is_same<T, mgp::Node>::value || std::is_same<T, mgp::Relationship>::value;

template <GraphObject NodeOrRel>
void NormalizeToBoolean(NodeOrRel object, const std::string &property_key,
                        const std::unordered_set<mgp::Value> &true_values,
                        const std::unordered_set<mgp::Value> &false_values) {
  auto old_value = object.GetProperty(property_key);
  if (old_value.IsNull()) {
    return;
  }

  bool property_in_true_vals = true_values.contains(old_value);
  bool property_in_false_vals = false_values.contains(old_value);

  if (property_in_true_vals && !property_in_false_vals) {
    object.SetProperty(property_key, mgp::Value(true));
  } else if (!property_in_true_vals && property_in_false_vals) {
    object.SetProperty(property_key, mgp::Value(false));
  } else if (!property_in_true_vals && !property_in_false_vals) {
    object.RemoveProperty(property_key);
  } else {
    throw mgp::ValueException(
        fmt::format("The value {} is contained in both true_values and false_values.", old_value.ToString()));
  }
}

}  // namespace

void Refactor::DeleteAndReconnect(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto path{arguments[0].ValuePath()};
    const auto nodes_to_delete_list{arguments[1].ValueList()};
    const auto config_map{arguments[2].ValueMap()};

    std::unordered_set<int64_t> nodes_to_delete;
    for (const auto &node : nodes_to_delete_list) {
      nodes_to_delete.insert(node.ValueNode().Id().AsInt());
    }

    Config config{config_map};
    mgp::List nodes;
    mgp::List relationships;
    int64_t prev_non_deleted_path_index{-1};
    int64_t prev_non_deleted_node_id{-1};
    std::unordered_set<mgp::Node> to_be_deleted;
    auto graph = mgp::Graph{memgraph_graph};

    for (size_t i = 0; i < path.Length() + 1; ++i) {
      auto node = path.GetNodeAt(i);
      int64_t id = node.Id().AsInt();
      auto delete_node = nodes_to_delete.contains(id);

      const auto modify_relationship = [&graph, &relationships](mgp::Relationship relationship, const mgp::Node &node,
                                                                int64_t other_node_id) {
        auto new_relationship = std::invoke([&]() {
          if (relationship.From().Id().AsInt() == other_node_id) {
            return graph.CreateRelationship(relationship.From(), node, relationship.Type());
          }
          return graph.CreateRelationship(node, relationship.To(), relationship.Type());
        });
        CopyRelProperties(new_relationship, relationship);
        graph.DeleteRelationship(relationship);
        relationships.AppendExtend(mgp::Value(new_relationship));
        return new_relationship;
      };

      const auto merge_relationships = [](mgp::Relationship &rel, mgp::Relationship &other, bool combine = false) {
        for (const auto &[key, value] : other.Properties()) {
          auto old_property = rel.GetProperty(key);
          if (!old_property.IsNull()) {
            if (combine) {
              rel.SetProperty(key, mgp::Value(mgp::List{old_property, value}));
            }
            continue;
          }
          rel.SetProperty(key, value);
        }
      };

      if (!delete_node && prev_non_deleted_path_index != -1 &&
          (prev_non_deleted_path_index != static_cast<int64_t>(i - 1))) {  // there was a deleted node in between
        if (config.rel_strategy == RelSelectStrategy::INCOMING) {
          modify_relationship(path.GetRelationshipAt(prev_non_deleted_path_index), node, prev_non_deleted_node_id);
        } else if (config.rel_strategy == RelSelectStrategy::OUTGOING) {
          modify_relationship(path.GetRelationshipAt(i - 1), path.GetNodeAt(prev_non_deleted_path_index), id);
        } else {  // RelSelectStrategy::MERGE
          auto new_rel = path.GetRelationshipAt(
              config.prop_strategy == PropertiesStrategy::OVERRIDE ? i - 1 : prev_non_deleted_path_index);
          auto old_rel = path.GetRelationshipAt(
              config.prop_strategy == PropertiesStrategy::OVERRIDE ? prev_non_deleted_path_index : i - 1);

          std::string new_type;
          if (config.prop_strategy == PropertiesStrategy::OVERRIDE) {
            new_type = std::string(new_rel.Type()) + "_" + std::string(old_rel.Type());
          } else {
            new_type = std::string(old_rel.Type()) + "_" + std::string(new_rel.Type());
          }
          auto new_relationship = graph.CreateRelationship(new_rel.From(), new_rel.To(), new_type);
          CopyRelProperties(new_relationship, new_rel);
          graph.DeleteRelationship(new_rel);

          if (config.prop_strategy == PropertiesStrategy::DISCARD) {
            auto modified_rel = modify_relationship(new_relationship, node, prev_non_deleted_node_id);
            merge_relationships(modified_rel, old_rel);
          } else if (config.prop_strategy == PropertiesStrategy::OVERRIDE) {
            auto modified_rel = modify_relationship(new_relationship, path.GetNodeAt(prev_non_deleted_path_index), id);
            merge_relationships(modified_rel, old_rel);
          } else {  // PropertiesStrategy::COMBINE
            auto modified_rel = modify_relationship(new_relationship, node, prev_non_deleted_node_id);
            merge_relationships(modified_rel, old_rel, true);
          }
        }
      } else if (!delete_node && prev_non_deleted_path_index != -1) {
        relationships.AppendExtend(mgp::Value(path.GetRelationshipAt(prev_non_deleted_path_index)));
      }

      if (!delete_node) {
        nodes.AppendExtend(mgp::Value(node));
        prev_non_deleted_path_index = static_cast<int64_t>(i);
        prev_non_deleted_node_id = id;
      } else {
        to_be_deleted.insert(node);
      }
    }

    for (const auto &node : to_be_deleted) {
      graph.DetachDeleteNode(node);
    }

    auto record = record_factory.NewRecord();
    record.Insert(std::string(kReturnDeleteAndReconnect1).c_str(), nodes);
    record.Insert(std::string(kReturnDeleteAndReconnect2).c_str(), relationships);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Refactor::NormalizeAsBoolean(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto object{arguments[0]};
    auto property_key{std::string(arguments[1].ValueString())};
    const auto true_values_list{arguments[2].ValueList()};
    const auto false_values_list{arguments[3].ValueList()};

    std::unordered_set<mgp::Value> true_values{true_values_list.begin(), true_values_list.end()};
    std::unordered_set<mgp::Value> false_values{false_values_list.begin(), false_values_list.end()};

    auto parse = [&property_key, &true_values, &false_values](const mgp::Value &object) {
      if (object.IsNode()) {
        NormalizeToBoolean(object.ValueNode(), property_key, true_values, false_values);
      } else if (object.IsRelationship()) {
        NormalizeToBoolean(object.ValueRelationship(), property_key, true_values, false_values);
      } else {
        ThrowInvalidTypeException(object);
      }
    };

    if (!object.IsList()) {
      parse(object);
      return;
    }

    for (const auto &list_item : object.ValueList()) {
      parse(list_item);
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Refactor::ExtractNode(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    mgp::Graph graph{memgraph_graph};
    mgp::Value rel_or_id{arguments[0]};
    auto labels{arguments[1].ValueList()};
    auto out_type{arguments[2].ValueString()};
    auto in_type{arguments[3].ValueString()};

    auto extract = [&graph, &labels, &record_factory, out_type, in_type](const mgp::Relationship &relationship) {
      auto new_node = graph.CreateNode();
      for (const auto &label : labels) {
        new_node.AddLabel(label.ValueString());
      }
      for (auto &[key, property] : relationship.Properties()) {
        new_node.SetProperty(key, std::move(property));
      }

      graph.CreateRelationship(relationship.From(), new_node, in_type);
      graph.CreateRelationship(new_node, relationship.To(), out_type);

      auto record = record_factory.NewRecord();
      record.Insert(std::string(kResultExtractNode1).c_str(), relationship.Id().AsInt());
      graph.DeleteRelationship(relationship);

      record.Insert(std::string(kResultExtractNode2).c_str(), new_node);
      record.Insert(std::string(kResultExtractNode3).c_str(), "");
    };

    std::unordered_set<int64_t> ids;
    auto parse = [&ids, &extract](const mgp::Value &rel_or_id) {
      if (rel_or_id.IsInt()) {
        ids.insert(rel_or_id.ValueInt());
      } else if (rel_or_id.IsRelationship()) {
        extract(rel_or_id.ValueRelationship());
      } else {
        ThrowInvalidTypeException(rel_or_id);
      }
    };

    if (!rel_or_id.IsList()) {
      parse(rel_or_id);
    } else {
      for (const auto &list_element : rel_or_id.ValueList()) {
        parse(list_element);
      }
    }

    if (ids.empty()) {
      return;
    }

    for (const auto relationship : graph.Relationships()) {
      if (ids.contains(relationship.Id().AsInt())) {
        extract(relationship);
      }
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Refactor::RenameType(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto old_type{arguments[0].ValueString()};
    const auto new_type{arguments[1].ValueString()};
    const auto relationships{arguments[2].ValueList()};
    auto graph{mgp::Graph(memgraph_graph)};

    int64_t rels_changed{0};
    for (auto relationship_value : relationships) {
      auto relationship{relationship_value.ValueRelationship()};
      if (relationship.Type() == old_type) {
        auto new_relationship = graph.CreateRelationship(relationship.From(), relationship.To(), new_type);
        CopyRelProperties(new_relationship, relationship);
        graph.DeleteRelationship(relationship);
        ++rels_changed;
      }
    }

    auto record = record_factory.NewRecord();
    record.Insert(std::string(kResultRenameType).c_str(), rels_changed);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Refactor::MergeNodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto graph = mgp::Graph(memgraph_graph);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto nodes = arguments[0].ValueList();
    if (nodes.Empty()) {
      throw mgp::ValueException(kMergeNodesEmptyListError.data());
    }

    // Verify all elements are nodes
    for (const auto &node_value : nodes) {
      if (!node_value.IsNode()) {
        throw mgp::ValueException(kMergeNodesInvalidTypeError.data());
      }
    }

    const auto config = arguments[1].ValueMap();
    const bool mergeRels = [&config, &kMergeNodesRelationshipsStrategy]() -> bool {
      if (config.KeyExists(kMergeNodesRelationshipsStrategy)) {
        if (!config.At(kMergeNodesRelationshipsStrategy).IsBool()) {
          throw mgp::ValueException(kMergeRelationshipsInvalidValueError.data());
        }

        return config.At(kMergeNodesRelationshipsStrategy).ValueBool();
      }

      return false;
    }();

    // Get properties strategy from config
    std::string prop_strategy = [&config, &kMergeNodesPropertiesStrategy, &kMergeNodesPropertiesStrategyAlternative,
                                 &kMergeNodesPropertiesCombine]() -> std::string {
      if (config.KeyExists(kMergeNodesPropertiesStrategy)) {
        return std::string(config.At(kMergeNodesPropertiesStrategy).ValueString());
      }
      if (config.KeyExists(kMergeNodesPropertiesStrategyAlternative)) {
        return std::string(config.At(kMergeNodesPropertiesStrategyAlternative).ValueString());
      }
      return std::string(kMergeNodesPropertiesCombine);
    }();

    // Convert to lowercase for case-insensitive comparison
    std::transform(prop_strategy.begin(), prop_strategy.end(), prop_strategy.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    // Validate property strategy
    if (prop_strategy != kMergeNodesPropertiesCombine && prop_strategy != kMergeNodesPropertiesDiscard &&
        prop_strategy != kMergeNodesPropertiesOverride && prop_strategy != kMergeNodesPropertiesOverwrite) {
      throw mgp::ValueException(kMergeNodesInvalidPropertyStrategyError.data());
    }

    // Get the first node as the target node
    auto target_node = nodes[0].ValueNode();

    // Process remaining nodes
    for (size_t i = 1; i < nodes.Size(); ++i) {
      auto source_node = nodes[i].ValueNode();

      // Handle properties based on strategy
      // Discard properties keeps the target properties so it's not handled in if-else
      if (prop_strategy == kMergeNodesPropertiesCombine) {
        // Combine properties from both nodes
        auto source_props = source_node.Properties();
        for (const auto &[key, value] : source_props) {
          auto target_property = target_node.GetProperty(key);
          if (target_property.IsList()) {
            auto target_list = target_property.ValueList();
            target_list.AppendExtend(source_props[key]);
            target_node.SetProperty(key, mgp::Value(std::move(target_list)));
          } else if (!target_property.IsNull()) {
            auto combined_properties = mgp::List();
            combined_properties.AppendExtend(target_property);
            combined_properties.AppendExtend(source_props[key]);
            target_node.SetProperty(key, mgp::Value(std::move(combined_properties)));
          } else {
            target_node.SetProperty(key, source_props[key]);
          }
        }
      } else if (prop_strategy == kMergeNodesPropertiesOverride || prop_strategy == kMergeNodesPropertiesOverwrite) {
        // Override/overwrite target properties with source properties
        auto source_props = source_node.Properties();
        for (const auto &[key, value] : source_props) {
          target_node.SetProperty(key, value);
        }
      }

      // Copy labels from source to target
      auto source_labels = source_node.Labels();
      for (size_t j = 0; j < source_labels.Size(); ++j) {
        target_node.AddLabel(std::move(source_labels[j]));
      }

      // Handle relationships
      // Copy all relationships from source to target
      if (mergeRels) {
        auto in_rels = source_node.InRelationships();
        for (const auto &rel : in_rels) {
          graph.CreateRelationship(rel.From(), target_node, rel.Type());
        }

        auto out_rels = source_node.OutRelationships();
        for (const auto &rel : out_rels) {
          graph.CreateRelationship(target_node, rel.To(), rel.Type());
        }
      }

      // Delete the source node
      graph.DetachDeleteNode(source_node);
    }

    // Return the merged node
    auto record = record_factory.NewRecord();
    record.Insert(kMergeNodesResult.data(), target_node);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

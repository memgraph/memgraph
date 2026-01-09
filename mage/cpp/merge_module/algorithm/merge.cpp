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

#include "merge.hpp"
#include <algorithm>
#include <optional>
#include <ranges>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include "mgp.hpp"

bool Merge::LabelsContained(const std::unordered_set<std::string_view> &labels, const mgp::Node &node) {
  bool contained = false;
  auto size = labels.size();  // this works if labels are unique, which they are
  size_t counter = 0;

  for (const auto label : node.Labels()) {
    if (labels.contains(label)) {
      counter++;
    }
  }

  if (counter == size) {
    contained = true;
  }

  return contained;
}

bool Merge::IdentProp(const mgp::Map &ident_prop, const mgp::Node &node) {
  bool match = true;
  for (const auto &[key, value] : ident_prop) {
    if (value != node.GetProperty(std::string(key))) {
      match = false;
    }
  }
  return match;
}

void Merge::Node(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto graph = mgp::Graph(memgraph_graph);
    auto labels = arguments[0].ValueList();
    auto ident_prop = arguments[1].ValueMap();
    auto create_prop = arguments[2].ValueMap();
    auto match_prop = arguments[3].ValueMap();
    std::unordered_set<std::string_view> label_set;
    std::unordered_map<std::string_view, mgp::Value> ident_prop_map;
    std::unordered_map<std::string_view, mgp::Value> create_prop_map;
    std::unordered_map<std::string_view, mgp::Value> match_prop_map;
    /*conversion of mgp::Maps to unordered_map for easier use of SetProperties(it expects an unordered map as
     * argument)*/
    auto convert_to_map = [](const auto &source, auto &destination) {
      for (const auto &[key, value] : source) {
        destination.emplace(key, value);
      }
    };
    convert_to_map(ident_prop, ident_prop_map);

    convert_to_map(create_prop, create_prop_map);

    convert_to_map(match_prop, match_prop_map);

    /*creating a set of labels for O(1) check of labels*/
    for (const auto elem : labels) {
      const auto label = elem.ValueString();
      if (label.empty()) {
        throw mgp::ValueException("List of labels cannot contain empty string!");
      }
      label_set.insert(elem.ValueString());
    }

    bool matched = false;
    for (auto node : graph.Nodes()) {
      // check if node already exists, if true, merge, if not, create
      if (LabelsContained(label_set, node) && IdentProp(ident_prop, node)) {
        matched = true;
        node.SetProperties(match_prop_map);
        auto record = record_factory.NewRecord();
        record.Insert(std::string(kNodeRes).c_str(), node);
      }
    }
    if (!matched) {
      auto node = graph.CreateNode();
      for (const auto label : label_set) {
        node.AddLabel(label);
      }
      // when merge creates it creates identyfing props also
      node.SetProperties(ident_prop_map);
      node.SetProperties(create_prop_map);
      auto record = record_factory.NewRecord();
      record.Insert(std::string(kNodeRes).c_str(), node);
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

namespace {

template <typename T>
concept GraphObject = std::is_same_v<T, mgp::Node> || std::is_same_v<T, mgp::Relationship>;

template <GraphObject NodeOrRel>
bool SameProps(const NodeOrRel &node_or_rel, const mgp::Map &props) {
  return std::all_of(props.begin(), props.end(), [&node_or_rel](const auto &kv) {
    return node_or_rel.GetProperty(std::string(kv.key)) == kv.value;
  });
}

std::vector<mgp::Relationship> MatchRelationship(const mgp::Node &from, const mgp::Node &to, std::string_view type,
                                                 const mgp::Map &props) {
  std::vector<mgp::Relationship> rels;
  for (const auto rel : from.OutRelationships()) {
    if (rel.To() != to || rel.Type() != type || !SameProps(rel, props)) {
      continue;
    }
    rels.push_back(rel);
  }
  return rels;
}

}  // namespace

void Merge::Relationship(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto graph{mgp::Graph(memgraph_graph)};

    const auto start_node{arguments[0].ValueNode()};
    const auto relationship_type{arguments[1].ValueString()};
    const auto props{arguments[2].ValueMap()};
    const auto create_props{arguments[3].ValueMap()};
    const auto end_node{arguments[4].ValueNode()};
    const auto match_props{arguments[5].ValueMap()};

    if (relationship_type.empty()) {
      throw mgp::ValueException("Relationship type can't be an empty string.");
    }

    auto convert_to_map = [](const mgp::Map &properties) {
      std::unordered_map<std::string_view, mgp::Value> map;
      for (const auto &[k, v] : properties) {
        map.emplace(k, v);
      }
      return map;
    };

    auto props_map = convert_to_map(props);
    auto create_props_map = convert_to_map(create_props);
    auto match_props_map = convert_to_map(match_props);

    auto rels = MatchRelationship(start_node, end_node, relationship_type, props);
    if (!rels.empty()) {
      for (auto &rel : rels) {
        rel.SetProperties(match_props_map);
        auto record = record_factory.NewRecord();
        record.Insert(std::string(kRelationshipResult).c_str(), rel);
      }
      return;
    }

    auto new_rel = graph.CreateRelationship(start_node, end_node, relationship_type);
    new_rel.SetProperties(props_map);
    new_rel.SetProperties(create_props_map);
    auto record = record_factory.NewRecord();
    record.Insert(std::string(kRelationshipResult).c_str(), new_rel);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

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

#include "path.hpp"

#include <algorithm>
#include <ranges>
#include <utility>

#include "mgp.hpp"

Path::PathHelper::PathHelper(const mgp::List &labels, const mgp::List &relationships, int64_t min_hops,
                             int64_t max_hops) {
  ParseLabels(labels);
  FilterLabelBoolStatus();
  ParseRelationships(relationships);
  config_.min_hops = min_hops;
  config_.max_hops = max_hops;
}

Path::PathHelper::PathHelper(const mgp::Map &config) {
  auto same_type_or_null = [](const mgp::Type type, const mgp::Type wanted_type) {
    return type == wanted_type || type == mgp::Type::Null;
  };

  if (!same_type_or_null(config.At("minHops").Type(), mgp::Type::Int) ||
      !same_type_or_null(config.At("maxHops").Type(), mgp::Type::Int) ||
      !same_type_or_null(config.At("relationshipFilter").Type(), mgp::Type::List) ||
      !same_type_or_null(config.At("labelFilter").Type(), mgp::Type::List) ||
      !same_type_or_null(config.At("filterStartNode").Type(), mgp::Type::Bool) ||
      !same_type_or_null(config.At("beginSequenceAtStart").Type(), mgp::Type::Bool) ||
      !same_type_or_null(config.At("bfs").Type(), mgp::Type::Bool)) {
    throw mgp::ValueException(
        "The config parameter needs to be a map with keys and values in line with the documentation.");
  }

  auto value = config.At("maxHops");
  if (!value.IsNull()) {
    int64_t max_hops = value.ValueInt();
    if (max_hops >= 0) {
      config_.max_hops = max_hops;
    }
  }
  value = config.At("minHops");
  if (!value.IsNull()) {
    int64_t min_hops = value.ValueInt();
    if (min_hops >= 0) {
      config_.min_hops = min_hops;
    }
  }

  value = config.At("relationshipFilter");
  if (!value.IsNull()) {
    ParseRelationships(value.ValueList());
  } else {
    ParseRelationships(mgp::List());
  }

  value = config.At("labelFilter");
  if (!value.IsNull()) {
    ParseLabels(value.ValueList());
  } else {
    ParseLabels(mgp::List());
  }
  FilterLabelBoolStatus();

  value = config.At("filterStartNode");
  config_.filter_start_node = value.IsNull() ? true : value.ValueBool();

  value = config.At("beginSequenceAtStart");
  config_.begin_sequence_at_start = value.IsNull() ? true : value.ValueBool();

  value = config.At("bfs");
  config_.bfs = value.IsNull() ? false : value.ValueBool();
}

Path::RelDirection Path::PathHelper::GetDirection(const std::string &rel_type) const {
  auto it = config_.relationship_sets.find(rel_type);
  if (it == config_.relationship_sets.end()) {
    return RelDirection::kNone;
  }
  return it->second;
}

Path::LabelBools Path::PathHelper::GetLabelBools(const mgp::Node &node) const {
  LabelBools label_bools;
  for (const auto &label : node.Labels()) {
    FilterLabel(label, label_bools);
  }
  return label_bools;
}

bool Path::PathHelper::AreLabelsValid(const LabelBools &label_bools) const {
  return !label_bools.blacklisted &&
         ((label_bools.end_node && config_.label_bools_status.end_node_activated) || label_bools.terminated ||
          (!config_.label_bools_status.termination_activated && !config_.label_bools_status.end_node_activated &&
           Whitelisted(label_bools.whitelisted)));
}

bool Path::PathHelper::ContinueExpanding(const LabelBools &label_bools, size_t path_size) const {
  return (static_cast<int64_t>(path_size) <= config_.max_hops &&
          ((!label_bools.blacklisted && !label_bools.terminated &&
            (label_bools.end_node || Whitelisted(label_bools.whitelisted))) ||
           (path_size == 1 && !config_.filter_start_node)));
}

bool Path::PathHelper::PathSizeOk(const int64_t path_size) const {
  return (path_size <= config_.max_hops) && (path_size >= config_.min_hops);
}

bool Path::PathHelper::PathTooBig(const int64_t path_size) const { return path_size > config_.max_hops; }

bool Path::PathHelper::Whitelisted(const bool whitelisted) const {
  return (config_.label_bools_status.whitelist_empty || whitelisted);
}

void Path::PathHelper::FilterLabelBoolStatus() {
  config_.label_bools_status.end_node_activated = !config_.label_sets.end_list.empty();
  config_.label_bools_status.whitelist_empty = config_.label_sets.whitelist.empty();
  config_.label_bools_status.termination_activated = !config_.label_sets.termination_list.empty();
}

/*function to set appropriate parameters for filtering*/
void Path::PathHelper::FilterLabel(std::string_view label, LabelBools &label_bools) const {
  if (config_.label_sets.blacklist.contains(label)) {
    label_bools.blacklisted = true;
  }

  if (config_.label_sets.termination_list.contains(label)) {
    label_bools.terminated = true;
  }

  if (config_.label_sets.end_list.contains(label)) {
    label_bools.end_node = true;
  }

  if (config_.label_sets.whitelist.contains(label)) {
    label_bools.whitelisted = true;
  }
}

// Function that takes input list of labels, and sorts them into appropriate category
// sets were used so when filtering is done, its done in O(1)
void Path::PathHelper::ParseLabels(const mgp::List &list_of_labels) {
  for (const auto &label : list_of_labels) {
    std::string_view label_string = label.ValueString();
    const char first_elem = label_string.front();
    switch (first_elem) {
      case '-':
        label_string.remove_prefix(1);
        config_.label_sets.blacklist.insert(label_string);
        break;
      case '>':
        label_string.remove_prefix(1);
        config_.label_sets.end_list.insert(label_string);
        break;
      case '+':
        label_string.remove_prefix(1);
        config_.label_sets.whitelist.insert(label_string);
        break;
      case '/':
        label_string.remove_prefix(1);
        config_.label_sets.termination_list.insert(label_string);
        break;
      default:
        config_.label_sets.whitelist.insert(label_string);
        break;
    }
  }
}

// Function that takes input list of relationships, and sorts them into appropriate categories
// sets were also used to reduce complexity
void Path::PathHelper::ParseRelationships(const mgp::List &list_of_relationships) {
  if (list_of_relationships.Size() ==
      0) {  // if no relationships were passed as arguments, all relationships are allowed
    config_.any_outgoing = true;
    config_.any_incoming = true;
    return;
  }

  for (const auto &rel : list_of_relationships) {
    const std::string rel_type{std::string(rel.ValueString())};
    const bool starts_with = rel_type.starts_with('<');
    const bool ends_with = rel_type.ends_with('>');

    if (rel_type.size() == 1) {
      if (starts_with) {
        config_.any_incoming = true;
      } else if (ends_with) {
        config_.any_outgoing = true;
      } else {
        config_.relationship_sets[rel_type] = RelDirection::kAny;
      }
      continue;
    }

    if (starts_with && ends_with) {  // <type>
      config_.relationship_sets[rel_type.substr(1, rel_type.size() - 2)] = RelDirection::kBoth;
    } else if (starts_with) {  // <type
      config_.relationship_sets[rel_type.substr(1)] = RelDirection::kIncoming;
    } else if (ends_with) {  // type>
      config_.relationship_sets[rel_type.substr(0, rel_type.size() - 1)] = RelDirection::kOutgoing;
    } else {  // type
      config_.relationship_sets[rel_type] = RelDirection::kAny;
    }
  }
}

void Path::Elements(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    const auto path{arguments[0].ValuePath()};
    const size_t path_length = path.Length();
    mgp::List split_path((path_length * 2) + 1);
    for (size_t i = 0; i < path_length; ++i) {
      split_path.Append(mgp::Value(path.GetNodeAt(i)));
      split_path.Append(mgp::Value(path.GetRelationshipAt(i)));
    }
    split_path.Append(mgp::Value(path.GetNodeAt(path.Length())));
    result.SetValue(std::move(split_path));

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
  }
}

void Path::Combine(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    auto path1{arguments[0].ValuePath()};
    const auto path2{arguments[1].ValuePath()};

    for (int i = 0; i < path2.Length(); ++i) {
      // Expand will throw an exception if it can't connect
      path1.Expand(path2.GetRelationshipAt(i));
    }

    result.SetValue(path1);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
  }
}

void Path::Slice(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    const auto path{arguments[0].ValuePath()};
    const auto offset{arguments[1].ValueInt()};
    const auto length{arguments[2].ValueInt()};

    mgp::Path new_path{path.GetNodeAt(offset)};
    const size_t old_path_length = path.Length();
    const size_t max_iteration = std::min((length == -1 ? old_path_length : offset + length), old_path_length);
    for (size_t i = offset; i < max_iteration; ++i) {
      new_path.Expand(path.GetRelationshipAt(i));
    }

    result.SetValue(new_path);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
  }
}

void Path::Create(mgp_list *args, mgp_graph * /*memgraph_graph*/, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto start_node{arguments[0].ValueNode()};
    auto relationships{arguments[1].ValueMap()};

    mgp::Path path{start_node};
    for (const auto &relationship : relationships["rel"].ValueList()) {
      if (relationship.IsNull()) {
        break;
      }
      if (!relationship.IsRelationship()) {
        std::ostringstream oss;
        oss << relationship.Type();
        throw mgp::ValueException("Expected relationship or null type, got " + oss.str());
      }

      const auto rel = relationship.ValueRelationship();
      auto last_node = path.GetNodeAt(path.Length());

      bool endpoint_is_from = false;

      if (last_node.Id() == rel.From().Id()) {
        endpoint_is_from = true;
      }

      auto contains = [](mgp::Relationships relationships, const mgp::Id id) {
        // NOLINTNEXTLINE(modernize-use-ranges,boost-use-ranges)
        return std::any_of(relationships.begin(), relationships.end(),
                           [&id](const auto &relationship) { return relationship.To().Id() == id; });
      };

      if ((endpoint_is_from && !contains(rel.From().OutRelationships(), rel.To().Id())) ||
          (!endpoint_is_from && !contains(rel.To().OutRelationships(), rel.From().Id()))) {
        break;
      }

      path.Expand(rel);
    }

    auto record = record_factory.NewRecord();
    record.Insert(std::string(kResultCreate).c_str(), path);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Path::PathExpand::ExpandPath(mgp::Path &path, const mgp::Relationship &relationship, int64_t path_size) {
  path.Expand(relationship);
  path_data_.visited_.insert(relationship.Id().AsInt());
  DFS(path, path_size + 1);
  path_data_.visited_.erase(relationship.Id().AsInt());
  path.Pop();
}

void Path::PathExpand::ExpandFromRelationships(mgp::Path &path, mgp::Relationships relationships, bool outgoing,
                                               int64_t path_size,
                                               std::set<std::pair<std::string_view, int64_t>> &seen) {
  for (const auto relationship : relationships) {
    auto type = std::string(relationship.Type());
    auto wanted_direction = path_data_.helper_.GetDirection(type);

    if ((wanted_direction == RelDirection::kNone && !path_data_.helper_.AnyDirected(outgoing)) ||
        path_data_.visited_.contains(relationship.Id().AsInt())) {
      continue;
    }

    const RelDirection curr_direction = outgoing ? RelDirection::kOutgoing : RelDirection::kIncoming;

    if (wanted_direction == RelDirection::kAny || curr_direction == wanted_direction ||
        path_data_.helper_.AnyDirected(outgoing)) {
      ExpandPath(path, relationship, path_size);
    } else if (wanted_direction == RelDirection::kBoth) {
      if (outgoing && seen.contains({type, relationship.To().Id().AsInt()})) {
        ExpandPath(path, relationship, path_size);
      } else {
        seen.insert({type, relationship.From().Id().AsInt()});
      }
    }
  }
}

/*function used for traversal and filtering*/
void Path::PathExpand::DFS(mgp::Path &path, int64_t path_size) {
  const mgp::Node node{path.GetNodeAt(path_size)};

  const LabelBools label_bools = path_data_.helper_.GetLabelBools(node);
  if (path_data_.helper_.PathSizeOk(path_size) && path_data_.helper_.AreLabelsValid(label_bools)) {
    auto record = path_data_.record_factory_.NewRecord();
    record.Insert(std::string(kResultExpand).c_str(), path);
  }

  if (!path_data_.helper_.ContinueExpanding(label_bools, path_size + 1)) {
    return;
  }

  std::set<std::pair<std::string_view, int64_t>> seen;
  this->ExpandFromRelationships(path, node.InRelationships(), false, path_size, seen);
  this->ExpandFromRelationships(path, node.OutRelationships(), true, path_size, seen);
}

void Path::PathExpand::StartAlgorithm(const mgp::Node node) {
  mgp::Path path = mgp::Path(node);
  DFS(path, 0);
}

void Path::PathExpand::Parse(const mgp::Value &value) {
  if (value.IsNode()) {
    path_data_.start_nodes_.insert((value.ValueNode()));
  } else if (value.IsInt()) {
    path_data_.start_nodes_.insert((path_data_.graph_.GetNodeById(mgp::Id::FromInt(value.ValueInt()))));
  } else {
    throw mgp::ValueException("Invalid start type. Expected Node, Int, List[Node, Int]");
  }
}

void Path::PathExpand::RunAlgorithm() {
  for (const auto &node : path_data_.start_nodes_) {
    StartAlgorithm(node);
  }
}

void Path::Expand(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto graph = mgp::Graph(memgraph_graph);
    const mgp::Value start_value = arguments[0];
    const mgp::List relationships{arguments[1].ValueList()};
    const mgp::List labels{arguments[2].ValueList()};
    const int64_t min_hops{arguments[3].ValueInt()};
    const int64_t max_hops{arguments[4].ValueInt()};

    PathExpand path_expand{PathData(PathHelper{labels, relationships, min_hops, max_hops}, record_factory, graph)};

    if (!start_value.IsList()) {
      path_expand.Parse(start_value);
    } else {
      for (const auto &list_item : start_value.ValueList()) {
        path_expand.Parse(list_item);
      }
    }

    path_expand.RunAlgorithm();

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Path::PathSubgraph::Parse(const mgp::Value &value) {
  if (!(value.IsNode() || value.IsInt())) {
    throw mgp::ValueException("The first argument needs to be a node, an integer ID, or a list thereof.");
  }
  if (value.IsNode()) {
    path_data_.start_nodes_.insert(value.ValueNode());
    return;
  }
  path_data_.start_nodes_.insert(path_data_.graph_.GetNodeById(mgp::Id::FromInt(value.ValueInt())));
}

void Path::PathSubgraph::ExpandFromRelationships(const std::pair<mgp::Node, int64_t> &pair,
                                                 const mgp::Relationships relationships, bool outgoing,
                                                 std::queue<std::pair<mgp::Node, int64_t>> &queue,
                                                 std::set<std::pair<std::string_view, int64_t>> &seen) {
  for (const auto relationship : relationships) {
    auto next_node = outgoing ? relationship.To() : relationship.From();

    if (path_data_.visited_.contains(next_node.Id().AsInt())) {
      continue;
    }

    auto type = std::string(relationship.Type());
    auto wanted_direction = path_data_.helper_.GetDirection(type);

    if (path_data_.helper_.IsNotStartOrFilterStartRel(pair.second == 0)) {
      if (wanted_direction == RelDirection::kNone && !path_data_.helper_.AnyDirected(outgoing)) {
        continue;
      }
    }

    const RelDirection curr_direction = outgoing ? RelDirection::kOutgoing : RelDirection::kIncoming;

    if (wanted_direction == RelDirection::kAny || curr_direction == wanted_direction ||
        path_data_.helper_.AnyDirected(outgoing)) {
      path_data_.visited_.insert(next_node.Id().AsInt());
      queue.emplace(next_node, pair.second + 1);
    } else if (wanted_direction == RelDirection::kBoth) {
      if (outgoing && seen.contains({type, relationship.To().Id().AsInt()})) {
        path_data_.visited_.insert(next_node.Id().AsInt());
        queue.emplace(next_node, pair.second + 1);
        to_be_returned_nodes_.AppendExtend(mgp::Value{next_node});
      } else {
        seen.insert({type, relationship.From().Id().AsInt()});
      }
    }
  }
}

void Path::PathSubgraph::TryInsertNode(const mgp::Node &node, int64_t hop_count, LabelBools &label_bools) {
  if (path_data_.helper_.IsNotStartOrFiltersStartNode(hop_count == 0)) {
    if (path_data_.helper_.AreLabelsValid(label_bools)) {
      to_be_returned_nodes_.AppendExtend(mgp::Value(node));
    }
    return;
  }

  to_be_returned_nodes_.AppendExtend(mgp::Value(node));
}

mgp::List Path::PathSubgraph::BFS() {
  std::queue<std::pair<mgp::Node, int64_t>> queue;

  for (const auto &node : path_data_.start_nodes_) {
    queue.push({node, 0});
    path_data_.visited_.insert(node.Id().AsInt());
  }

  while (!queue.empty()) {
    auto pair = queue.front();
    queue.pop();

    if (path_data_.helper_.PathTooBig(pair.second)) {
      continue;
    }

    LabelBools label_bools = path_data_.helper_.GetLabelBools(pair.first);
    TryInsertNode(pair.first, pair.second, label_bools);
    if (!path_data_.helper_.ContinueExpanding(label_bools, pair.second + 1)) {
      continue;
    }

    std::set<std::pair<std::string_view, int64_t>> seen;
    this->ExpandFromRelationships(pair, pair.first.InRelationships(), false, queue, seen);
    this->ExpandFromRelationships(pair, pair.first.OutRelationships(), true, queue, seen);
  }

  return to_be_returned_nodes_;
}

void Path::SubgraphNodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto graph = mgp::Graph(memgraph_graph);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto config = arguments[1].ValueMap();
    PathSubgraph path_subgraph{PathData(PathHelper{config}, record_factory, graph)};

    auto start_value = arguments[0];
    if (!start_value.IsList()) {
      path_subgraph.Parse(start_value);
    } else {
      for (const auto &list_item : start_value.ValueList()) {
        path_subgraph.Parse(list_item);
      }
    }

    auto to_be_returned_nodes = path_subgraph.BFS();

    for (const auto &node : to_be_returned_nodes) {
      auto record = record_factory.NewRecord();
      record.Insert(std::string(kResultSubgraphNodes).c_str(), node);
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Path::SubgraphAll(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto graph = mgp::Graph(memgraph_graph);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto config = arguments[1].ValueMap();
    PathSubgraph path_subgraph{PathData(PathHelper{config}, record_factory, graph)};

    auto start_value = arguments[0];
    if (!start_value.IsList()) {
      path_subgraph.Parse(start_value);
    } else {
      for (const auto &list_item : start_value.ValueList()) {
        path_subgraph.Parse(list_item);
      }
    }

    const auto to_be_returned_nodes = path_subgraph.BFS();

    std::unordered_set<mgp::Node> to_be_returned_nodes_searchable;

    for (const auto &node : to_be_returned_nodes) {
      to_be_returned_nodes_searchable.insert(node.ValueNode());
    }

    mgp::List to_be_returned_rels;
    for (auto node : to_be_returned_nodes) {
      for (auto rel : node.ValueNode().OutRelationships()) {
        if (to_be_returned_nodes_searchable.contains(rel.To())) {
          to_be_returned_rels.AppendExtend(mgp::Value(rel));
        }
      }
    }

    auto record = record_factory.NewRecord();
    record.Insert(std::string(kResultNodesSubgraphAll).c_str(), to_be_returned_nodes);
    record.Insert(std::string(kResultRelsSubgraphAll).c_str(), to_be_returned_rels);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

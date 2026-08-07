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
#include <array>
#include <limits>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>

#include "mgp.hpp"

namespace {

// -1 means "no limit" for an upper hop bound; every other value is used as given, so a bound of -5
// legitimately matches nothing. A negative lower bound needs no such rule: it is trivially satisfied.
constexpr int64_t MaxHopsOrNoLimit(int64_t max_hops) noexcept {
  return max_hops == -1 ? std::numeric_limits<int64_t>::max() : max_hops;
}

// The expand walk recurses once per hop, so an unbounded upper hop bound would exhaust the stack and
// take the whole process down on a long chain. Refuse the walk instead of crashing.
constexpr int64_t kMaxExpandDepth = 5000;

// An unfiltered start node is exempt from the label filters: treat it as a plain whitelisted node.
constexpr Path::LabelBools kExemptStart{.whitelisted = true};

}  // namespace

Path::PathHelper::PathHelper(const mgp::List &labels, const mgp::List &relationships, int64_t min_hops,
                             int64_t max_hops) {
  ParseLabels(labels);
  FilterLabelBoolStatus();
  ParseRelationships(relationships);
  config_.min_hops = min_hops;
  config_.max_hops = MaxHopsOrNoLimit(max_hops);
}

namespace {

// Every config key the path procedures honour. An unrecognized key is rejected rather than ignored:
// silently dropping something like `limit` or `endNodes` returns a different result set than was asked for.
constexpr std::array<std::string_view, 13> kConfigKeys{"minHops",
                                                       "maxHops",
                                                       "minLevel",
                                                       "maxLevel",
                                                       "relationshipFilter",
                                                       "labelFilter",
                                                       "filterStartNode",
                                                       "beginSequenceAtStart",
                                                       "bfs",
                                                       "allowlistNodes",
                                                       "denylistNodes",
                                                       "whitelistNodes",
                                                       "blacklistNodes"};

void ValidateConfigKeys(const mgp::Map &config) {
  for (const auto &item : config) {
    if (std::ranges::find(kConfigKeys, item.key) == kConfigKeys.end()) {
      throw mgp::ValueException("Unrecognized config key '" + std::string(item.key) + "'.");
    }
  }
}

// Reads `key`, falling back to its alias. Supplying both is ambiguous, so it throws instead of picking one.
mgp::Value AliasedValue(const mgp::Map &config, std::string_view key, std::string_view alias) {
  auto value = config.At(key);
  auto alias_value = config.At(alias);
  if (value.IsNull()) {
    return alias_value;
  }
  if (!alias_value.IsNull()) {
    throw mgp::ValueException("Config keys '" + std::string(key) + "' and '" + std::string(alias) +
                              "' mean the same thing; supply only one.");
  }
  return value;
}

int64_t NodeFilterId(const mgp::Value &value, const mgp::Graph &graph, std::string_view type_error) {
  if (value.IsNode()) {
    return value.ValueNode().Id().AsInt();
  }
  if (value.IsInt()) {
    const int64_t id = value.ValueInt();
    // A negative ID has no node behind it, and reporting it raw beats the unsigned wraparound the lookup prints.
    if (id < 0) {
      throw mgp::ValueException("Node filters need a non-negative node ID, got " + std::to_string(id) + ".");
    }
    // Resolve the ID so a node that doesn't exist is reported instead of silently never matching.
    return graph.GetNodeById(mgp::Id::FromInt(id)).Id().AsInt();
  }
  throw mgp::ValueException(std::string(type_error));
}

// Collects node IDs from a node, an integer ID, or a list thereof. A null value collects nothing.
std::unordered_set<int64_t> CollectNodeIds(const mgp::Value &value, const mgp::Graph &graph) {
  std::unordered_set<int64_t> ids;
  if (value.IsNull()) {
    return ids;
  }
  if (!value.IsList()) {
    ids.insert(NodeFilterId(value, graph, "Node filters need to be a node, an integer ID, or a list thereof."));
    return ids;
  }
  for (const auto &item : value.ValueList()) {
    ids.insert(NodeFilterId(item, graph, "Node filter list entries need to be a node or an integer ID."));
  }
  return ids;
}

}  // namespace

Path::PathHelper::PathHelper(const mgp::Map &config, const mgp::Graph &graph) {
  ValidateConfigKeys(config);

  auto same_type_or_null = [](const mgp::Value &value, const mgp::Type wanted_type) {
    return value.Type() == wanted_type || value.IsNull();
  };

  const auto min_hops_value = AliasedValue(config, "minHops", "minLevel");
  const auto max_hops_value = AliasedValue(config, "maxHops", "maxLevel");

  if (!same_type_or_null(min_hops_value, mgp::Type::Int) || !same_type_or_null(max_hops_value, mgp::Type::Int) ||
      !same_type_or_null(config.At("relationshipFilter"), mgp::Type::List) ||
      !same_type_or_null(config.At("labelFilter"), mgp::Type::List) ||
      !same_type_or_null(config.At("filterStartNode"), mgp::Type::Bool) ||
      !same_type_or_null(config.At("beginSequenceAtStart"), mgp::Type::Bool) ||
      !same_type_or_null(config.At("bfs"), mgp::Type::Bool)) {
    throw mgp::ValueException(
        "The config parameter needs to be a map with keys and values in line with the documentation.");
  }

  if (!max_hops_value.IsNull()) {
    config_.max_hops = MaxHopsOrNoLimit(max_hops_value.ValueInt());
  }
  if (!min_hops_value.IsNull()) {
    config_.min_hops = min_hops_value.ValueInt();
  }

  auto value = config.At("relationshipFilter");
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
  config_.filter_start_node = value.IsNull() ? false : value.ValueBool();

  value = config.At("beginSequenceAtStart");
  config_.begin_sequence_at_start = value.IsNull() ? true : value.ValueBool();

  value = config.At("bfs");
  config_.bfs = value.IsNull() ? false : value.ValueBool();

  ParseNodeFilters(config, graph);
}

// `allowlistNodes` and `denylistNodes` supersede the deprecated `whitelistNodes` and `blacklistNodes`;
// an absent or empty list falls back to the deprecated key. The deprecated key is read only when the
// preferred one came back empty, so a stale ID in a superseded list cannot abort the query.
void Path::PathHelper::ParseNodeFilters(const mgp::Map &config, const mgp::Graph &graph) {
  config_.allowlist_nodes = CollectNodeIds(config.At("allowlistNodes"), graph);
  if (config_.allowlist_nodes.empty()) {
    config_.allowlist_nodes = CollectNodeIds(config.At("whitelistNodes"), graph);
  }

  config_.denylist_nodes = CollectNodeIds(config.At("denylistNodes"), graph);
  if (config_.denylist_nodes.empty()) {
    config_.denylist_nodes = CollectNodeIds(config.At("blacklistNodes"), graph);
  }
}

Path::RelDirection Path::PathHelper::GetDirection(std::string_view rel_type) const {
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
  return (std::cmp_less_equal(path_size, config_.max_hops) &&
          ((!label_bools.blacklisted && !label_bools.terminated &&
            (label_bools.end_node || Whitelisted(label_bools.whitelisted))) ||
           (path_size == 1 && !config_.filter_start_node)));
}

// Evaluated independently of the label filters: a node the lists reject is never returned nor expanded through.
bool Path::PathHelper::NodeFilterAllows(const mgp::Node &node, const bool is_start) const {
  if (config_.allowlist_nodes.empty() && config_.denylist_nodes.empty()) {
    return true;
  }

  // An unfiltered start node is exempt, as it is for the label filters.
  if (!IsNotStartOrFiltersStartNode(is_start)) {
    return true;
  }

  const auto id = node.Id().AsInt();
  return !config_.denylist_nodes.contains(id) &&
         (config_.allowlist_nodes.empty() || config_.allowlist_nodes.contains(id));
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
  constexpr std::string_view kLabelPrefixes = "-+>/";
  for (const auto &label : list_of_labels) {
    if (label.Type() != mgp::Type::String) {
      throw mgp::ValueException("Label filter entries need to be strings.");
    }
    std::string_view label_string = label.ValueString();
    // Reject an empty entry, or a prefix with no label behind it: either would filter on a label no node
    // can carry, silently emptying or unbounding the result instead of reporting the mistake.
    if (label_string.empty() || (label_string.size() == 1 && kLabelPrefixes.contains(label_string.front()))) {
      throw mgp::ValueException("Invalid labelFilter entry '" + std::string(label_string) +
                                "': expected a label, optionally prefixed with '+', '-', '>' or '/'.");
    }
    const char first_elem = label_string.front();
    switch (first_elem) {
      case '-':
        label_string.remove_prefix(1);
        config_.label_sets.blacklist.emplace(label_string);
        break;
      case '>':
        label_string.remove_prefix(1);
        config_.label_sets.end_list.emplace(label_string);
        break;
      case '+':
        label_string.remove_prefix(1);
        config_.label_sets.whitelist.emplace(label_string);
        break;
      case '/':
        label_string.remove_prefix(1);
        config_.label_sets.termination_list.emplace(label_string);
        break;
      default:
        config_.label_sets.whitelist.emplace(label_string);
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
    if (rel.Type() != mgp::Type::String) {
      throw mgp::ValueException("Relationship filter entries need to be strings.");
    }
    const std::string rel_type{std::string(rel.ValueString())};
    // Reject an entry with no type behind the direction markers: it can never match, so it would silently
    // block every relationship instead of reporting the mistake. A bare '<' or '>' is a real any-direction
    // filter and stays valid; an empty *list* already means "no filter" and is handled above.
    if (rel_type.empty() || rel_type == "<>") {
      throw mgp::ValueException(
          "Invalid relationshipFilter entry '" + rel_type +
          "': expected a relationship type, optionally wrapped in '<' and '>', or a bare '<' or '>'.");
    }
    std::string_view type{rel_type};
    RelDirection direction = RelDirection::kAny;

    if (type.size() == 1) {  // a bare marker: every type, in that direction
      if (type.front() == '<') {
        config_.any_incoming = true;
        continue;
      }
      if (type.front() == '>') {
        config_.any_outgoing = true;
        continue;
      }
    } else if (type.starts_with('<') && type.ends_with('>')) {  // <type>
      direction = RelDirection::kBoth;
      type = type.substr(1, type.size() - 2);
    } else if (type.starts_with('<')) {  // <type
      direction = RelDirection::kIncoming;
      type.remove_prefix(1);
    } else if (type.starts_with('>')) {  // >type -- the outgoing marker may lead as well as trail
      direction = RelDirection::kOutgoing;
      type.remove_prefix(1);
      if (type.ends_with('>')) {  // >type>
        type.remove_suffix(1);
      }
    } else if (type.ends_with('>')) {  // type>
      direction = RelDirection::kOutgoing;
      type = type.substr(0, type.size() - 1);
    }

    // Stripping a leading marker can leave another one behind ('>>', '><'), which is the bare-marker
    // form again: every type, in the direction the remaining marker names.
    if (type == "<") {
      config_.any_incoming = true;
      continue;
    }
    if (type == ">" || type.empty()) {
      config_.any_outgoing = true;
      continue;
    }

    AddRelationshipDirection(std::string(type), direction);
  }
}

// Two entries for the same type merge rather than overwrite: 'R>' together with '<R' means the type is
// traversable either way, which is exactly what an unqualified type already means. Overwriting would
// silently drop one of the two directions, and which one would depend on the order they were listed in.
void Path::PathHelper::AddRelationshipDirection(std::string type, RelDirection direction) {
  const auto [it, inserted] = config_.relationship_sets.try_emplace(std::move(type), direction);
  if (inserted || it->second == direction) {
    return;
  }
  // The reciprocal mode is not a direction, so it has nothing to merge with; keep the later entry.
  it->second = (it->second == RelDirection::kBoth || direction == RelDirection::kBoth) ? direction : RelDirection::kAny;
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

void Path::Create(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto graph = mgp::Graph(memgraph_graph);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto start_node{arguments[0].ValueNode()};
    auto relationships{arguments[1].ValueMap()};

    // Each entry scans an endpoint's relationships, so a long list over dense nodes needs polling too.
    uint64_t abort_poll_counter = 0;
    mgp::Path path{start_node};
    for (const auto &relationship : relationships["rel"].ValueList()) {
      PollAbort(graph, abort_poll_counter);
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
        return std::any_of(relationships.begin(), relationships.end(), [&id](const auto &relationship) {
          return relationship.To().Id() == id;
        });
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
                                               int64_t path_size, std::set<std::pair<std::string, int64_t>> &seen) {
  for (const auto relationship : relationships) {
    // A node whose relationships are all filtered out does no other work, so without a poll here the
    // walk is unstoppable for the whole adjacency list of a supernode.
    path_data_.MaybeAbort();

    // string_view keeps the GetDirection lookup allocation-free; `seen` below owns its key.
    const std::string_view type = relationship.Type();
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
      if (outgoing && seen.contains({std::string(type), relationship.To().Id().AsInt()})) {
        ExpandPath(path, relationship, path_size);
      } else {
        seen.insert({std::string(type), relationship.From().Id().AsInt()});
      }
    }
  }
}

/*function used for traversal and filtering*/
void Path::PathExpand::DFS(mgp::Path &path, int64_t path_size) {
  // The walk enumerates paths, not nodes, so it can run far longer than the graph is big -- and with a
  // high enough minHops it does so without emitting anything, where no memory limit can stop it.
  path_data_.MaybeAbort();

  // One frame per hop, so an unbounded upper hop bound would overflow the stack rather than return.
  if (path_size > kMaxExpandDepth) {
    throw mgp::ValueException("Path expansion exceeded the maximum depth of " + std::to_string(kMaxExpandDepth) +
                              "; set a smaller maxHops to bound the traversal.");
  }

  const mgp::Node node{path.GetNodeAt(path_size)};

  // A node the identity filters reject ends the walk here: no record and no expansion.
  if (!path_data_.helper_.NodeFilterAllows(node, path_size == 0)) {
    return;
  }

  const LabelBools label_bools = path_data_.helper_.GetLabelBools(node);
  const LabelBools &inclusion_bools =
      path_data_.helper_.IsNotStartOrFiltersStartNode(path_size == 0) ? label_bools : kExemptStart;

  if (path_data_.helper_.PathSizeOk(path_size) && path_data_.helper_.AreLabelsValid(inclusion_bools)) {
    auto record = path_data_.record_factory_.NewRecord();
    record.Insert(std::string(kResultExpand).c_str(), path);
  }

  // Expansion uses the real labels. That is equivalent to passing inclusion_bools today, because the two
  // differ only at an unfiltered start, where ContinueExpanding's `path_size == 1 && !filter_start_node`
  // clause already decides the answer -- but it stops being equivalent if that clause is ever removed.
  if (!path_data_.helper_.ContinueExpanding(label_bools, path_size + 1)) {
    return;
  }

  std::set<std::pair<std::string, int64_t>> seen;
  this->ExpandFromRelationships(path, node.InRelationships(), false, path_size, seen);
  this->ExpandFromRelationships(path, node.OutRelationships(), true, path_size, seen);
}

void Path::PathExpand::StartAlgorithm(const mgp::Node &node) {
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

namespace {

void RunExpand(Path::PathHelper &&helper, const mgp::Value &start_value, const mgp::RecordFactory &record_factory,
               const mgp::Graph &graph) {
  Path::PathExpand path_expand{Path::PathData(std::move(helper), record_factory, graph)};

  if (!start_value.IsList()) {
    path_expand.Parse(start_value);
  } else {
    for (const auto &list_item : start_value.ValueList()) {
      path_expand.Parse(list_item);
    }
  }

  path_expand.RunAlgorithm();
}

}  // namespace

void Path::Expand(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto graph = mgp::Graph(memgraph_graph);
    const mgp::List relationships{arguments[1].ValueList()};
    const mgp::List labels{arguments[2].ValueList()};
    const int64_t min_hops{arguments[3].ValueInt()};
    const int64_t max_hops{arguments[4].ValueInt()};

    RunExpand(PathHelper{labels, relationships, min_hops, max_hops}, arguments[0], record_factory, graph);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Path::ExpandConfig(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto graph = mgp::Graph(memgraph_graph);
    const auto config = arguments[1].ValueMap();

    RunExpand(PathHelper{config, graph}, arguments[0], record_factory, graph);

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
                                                 std::set<std::pair<std::string, int64_t>> &seen) {
  for (const auto relationship : relationships) {
    // As in the expand walk: a supernode whose relationships are all filtered out would otherwise be
    // traversed without ever reaching the dequeue poll above.
    path_data_.MaybeAbort();

    auto next_node = outgoing ? relationship.To() : relationship.From();

    if (path_data_.visited_.contains(next_node.Id().AsInt())) {
      continue;
    }

    // string_view keeps the GetDirection lookup allocation-free; `seen` below owns its key.
    const std::string_view type = relationship.Type();
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
      queue.emplace(std::move(next_node), pair.second + 1);
    } else if (wanted_direction == RelDirection::kBoth) {
      if (outgoing && seen.contains({std::string(type), relationship.To().Id().AsInt()})) {
        // Enqueue only; TryInsertNode emits it once on dequeue with hop/label filters applied.
        path_data_.visited_.insert(next_node.Id().AsInt());
        queue.emplace(std::move(next_node), pair.second + 1);
      } else {
        seen.insert({std::string(type), relationship.From().Id().AsInt()});
      }
    }
  }
}

void Path::PathSubgraph::TryInsertNode(const mgp::Node &node, int64_t hop_count, const LabelBools &label_bools) {
  // Nodes closer than minHops are still traversed but must not be returned.
  if (!path_data_.helper_.PathSizeOk(hop_count)) {
    return;
  }

  if (path_data_.helper_.IsNotStartOrFiltersStartNode(hop_count == 0)) {
    if (path_data_.helper_.AreLabelsValid(label_bools)) {
      to_be_returned_nodes_.AppendExtend(mgp::Value(node));
    }
    return;
  }

  if (path_data_.helper_.AreLabelsValid(kExemptStart)) {
    to_be_returned_nodes_.AppendExtend(mgp::Value(node));
  }
}

mgp::List Path::PathSubgraph::BFS() {
  std::queue<std::pair<mgp::Node, int64_t>> queue;

  for (const auto &node : path_data_.start_nodes_) {
    queue.emplace(node, 0);
    path_data_.visited_.insert(node.Id().AsInt());
  }

  while (!queue.empty()) {
    path_data_.MaybeAbort();

    auto pair = std::move(queue.front());
    queue.pop();

    if (path_data_.helper_.PathTooBig(pair.second)) {
      continue;
    }

    if (!path_data_.helper_.NodeFilterAllows(pair.first, pair.second == 0)) {
      continue;
    }

    LabelBools label_bools = path_data_.helper_.GetLabelBools(pair.first);
    TryInsertNode(pair.first, pair.second, label_bools);
    if (!path_data_.helper_.ContinueExpanding(label_bools, pair.second + 1)) {
      continue;
    }

    std::set<std::pair<std::string, int64_t>> seen;
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
    PathSubgraph path_subgraph{PathData(PathHelper{config, graph}, record_factory, graph)};

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
    PathSubgraph path_subgraph{PathData(PathHelper{config, graph}, record_factory, graph)};

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

    // Collecting the relationships is a second O(nodes * degree) pass outside the walk, so it needs its
    // own polling to stay abortable.
    uint64_t abort_poll_counter = 0;
    mgp::List to_be_returned_rels;
    for (auto node : to_be_returned_nodes) {
      for (auto rel : node.ValueNode().OutRelationships()) {
        PollAbort(graph, abort_poll_counter);
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

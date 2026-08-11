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
#include <cctype>
#include <limits>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>

#include "mgp.hpp"

namespace {

// -1 means "no limit"; any other value is used as given, so -5 legitimately matches nothing.
constexpr int64_t MaxHopsOrNoLimit(int64_t max_hops) noexcept {
  return max_hops == -1 ? std::numeric_limits<int64_t>::max() : max_hops;
}

// One frame per hop, so an unbounded bound would exhaust the stack. Refuse rather than crash.
constexpr int64_t kMaxExpandDepth = 5000;

}  // namespace

Path::PathHelper::PathHelper(const mgp::List &labels, const mgp::List &relationships, int64_t min_hops,
                             int64_t max_hops) {
  ParseLabels(labels);
  FilterLabelBoolStatus();
  ParseRelationships(relationships);
  config_.min_hops = min_hops;
  config_.max_hops = MaxHopsOrNoLimit(max_hops);
  // No `bfs` argument in this form, so a caller could not opt out of the per-depth re-walk.
  config_.bfs = false;
}

namespace {

// Every accepted config key. An unrecognized one is rejected rather than ignored: dropping `limit` or
// `endNodes` would return a different result set than was asked for.
constexpr std::array<std::string_view, 17> kConfigKeys{"minHops",
                                                       "maxHops",
                                                       "minLevel",
                                                       "maxLevel",
                                                       "relationshipFilter",
                                                       "labelFilter",
                                                       "filterStartNode",
                                                       "beginSequenceAtStart",
                                                       "bfs",
                                                       "limit",
                                                       "uniqueness",
                                                       "endNodes",
                                                       "terminatorNodes",
                                                       "allowlistNodes",
                                                       "denylistNodes",
                                                       "whitelistNodes",
                                                       "blacklistNodes"};

void ValidateConfigKeys(const mgp::Map &config) {
  // Counting by hash lookup equals the map's size exactly when it holds nothing else, so the common
  // case never iterates -- which would deep-copy every value twice just to check spellings.
  const auto recognized =
      std::ranges::count_if(kConfigKeys, [&config](std::string_view key) { return config.KeyExists(key); });
  if (std::cmp_equal(recognized, config.Size())) {
    return;
  }

  for (const auto &item : config) {
    if (std::ranges::find(kConfigKeys, item.key) == kConfigKeys.end()) {
      throw mgp::ValueException("Unrecognized config key '" + std::string(item.key) + "'.");
    }
  }
}

// A config value plus the spelling the caller wrote, so an error can name that one.
struct AliasedConfigValue {
  mgp::Value value;
  std::string_view key;
};

// Reads `key` or its alias; supplying both is ambiguous and throws.
AliasedConfigValue AliasedValue(const mgp::Map &config, std::string_view key, std::string_view alias) {
  auto value = config.At(key);
  auto alias_value = config.At(alias);
  if (value.IsNull()) {
    return {.value = std::move(alias_value), .key = alias};
  }
  if (!alias_value.IsNull()) {
    throw mgp::ValueException("Config keys '" + std::string(key) + "' and '" + std::string(alias) +
                              "' mean the same thing; supply only one.");
  }
  return {.value = std::move(value), .key = key};
}

int64_t NodeFilterId(const mgp::Value &value, const mgp::Graph &graph, std::string_view type_error) {
  if (value.IsNode()) {
    return value.ValueNode().Id().AsInt();
  }
  if (value.IsInt()) {
    const int64_t id = value.ValueInt();
    // Reporting it raw beats the unsigned wraparound the lookup would print.
    if (id < 0) {
      throw mgp::ValueException("Node filters need a non-negative node ID, got " + std::to_string(id) + ".");
    }
    // Resolve so a missing node is reported instead of silently never matching.
    return graph.GetNodeById(mgp::Id::FromInt(id)).Id().AsInt();
  }
  throw mgp::ValueException(std::string(type_error));
}

// One entry per list element, or a single '|'-separated string. Empty pieces are dropped.
mgp::List FilterEntries(const mgp::Value &value, std::string_view key) {
  if (value.IsNull()) {
    return mgp::List{};
  }
  if (value.IsList()) {
    return value.ValueList();
  }
  if (!value.IsString()) {
    throw mgp::ValueException("Config key '" + std::string(key) + "' needs to be a string or a list of strings.");
  }

  const std::string_view text = value.ValueString();
  mgp::List entries;
  for (size_t start = 0; start <= text.size();) {
    const size_t separator = text.find('|', start);
    const size_t end = separator == std::string_view::npos ? text.size() : separator;
    if (end > start) {
      entries.AppendExtend(mgp::Value(std::string(text.substr(start, end - start))));
    }
    if (separator == std::string_view::npos) {
      break;
    }
    start = separator + 1;
  }
  return entries;
}

// Case-insensitive. The modes left out are the ones whose surviving result set depends on the order
// relationships happen to be iterated in, which is not a rule a caller can reason about.
Path::Uniqueness ParseUniqueness(std::string_view name) {
  std::string upper;
  upper.reserve(name.size());
  for (const char character : name) {
    upper.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(character))));
  }

  if (upper == "RELATIONSHIP_PATH") return Path::Uniqueness::kRelationshipPath;
  if (upper == "NODE_PATH") return Path::Uniqueness::kNodePath;
  if (upper == "NODE_GLOBAL") return Path::Uniqueness::kNodeGlobal;

  throw mgp::ValueException("Unrecognized uniqueness '" + std::string(name) +
                            "'. Expected one of: RELATIONSHIP_PATH, NODE_PATH, NODE_GLOBAL.");
}

// Node IDs from a node, an integer ID, or a list thereof; null collects nothing. Polls, since a
// caller-supplied list is unbounded and each integer entry costs a lookup.
std::unordered_set<int64_t> CollectNodeIds(const mgp::Value &value, const mgp::Graph &graph) {
  std::unordered_set<int64_t> ids;
  if (value.IsNull()) {
    return ids;
  }
  if (!value.IsList()) {
    ids.insert(NodeFilterId(value, graph, "Node filters need to be a node, an integer ID, or a list thereof."));
    return ids;
  }
  uint64_t abort_poll_counter = 0;
  for (const auto &item : value.ValueList()) {
    Path::PollAbort(graph, abort_poll_counter);
    ids.insert(NodeFilterId(item, graph, "Node filter list entries need to be a node or an integer ID."));
  }
  return ids;
}

}  // namespace

Path::PathHelper::PathHelper(const mgp::Map &config, const mgp::Graph &graph, const ProcedureKind kind) {
  ValidateConfigKeys(config);

  // Name the offending key; otherwise the caller has to bisect the map to find it.
  auto require_type =
      [](std::string_view key, const mgp::Value &value, const mgp::Type wanted_type, std::string_view wanted_name) {
        if (!value.IsNull() && value.Type() != wanted_type) {
          throw mgp::ValueException("Config key '" + std::string(key) + "' needs to be " + std::string(wanted_name) +
                                    ".");
        }
      };

  const auto min_hops_value = AliasedValue(config, "minHops", "minLevel");
  const auto max_hops_value = AliasedValue(config, "maxHops", "maxLevel");

  // Report the spelling the caller wrote.
  require_type(min_hops_value.key, min_hops_value.value, mgp::Type::Int, "an integer");
  require_type(max_hops_value.key, max_hops_value.value, mgp::Type::Int, "an integer");
  require_type("filterStartNode", config.At("filterStartNode"), mgp::Type::Bool, "a boolean");
  require_type("limit", config.At("limit"), mgp::Type::Int, "an integer");
  // Only the expand walk can act on these. The subgraph walk already does what they would ask for, so
  // it accepts and ignores them rather than failing the query.
  if (kind == ProcedureKind::kExpand) {
    require_type("bfs", config.At("bfs"), mgp::Type::Bool, "a boolean");
    require_type("uniqueness", config.At("uniqueness"), mgp::Type::String, "a string");
  }

  if (!max_hops_value.value.IsNull()) {
    config_.max_hops = MaxHopsOrNoLimit(max_hops_value.value.ValueInt());
  }
  if (!min_hops_value.value.IsNull()) {
    config_.min_hops = min_hops_value.value.ValueInt();
  }

  // -1 is "no limit"; any other negative would silently return nothing.
  auto limit_value = config.At("limit");
  if (!limit_value.IsNull()) {
    const int64_t limit = limit_value.ValueInt();
    if (limit < kNoLimit) {
      throw mgp::ValueException("Config key 'limit' needs to be non-negative, or -1 for no limit, got " +
                                std::to_string(limit) + ".");
    }
    config_.limit = limit;
  }

  ParseRelationships(FilterEntries(config.At("relationshipFilter"), "relationshipFilter"));
  ParseLabels(FilterEntries(config.At("labelFilter"), "labelFilter"));
  FilterLabelBoolStatus();

  mgp::Value value{};

  value = config.At("filterStartNode");
  config_.filter_start_node = value.IsNull() ? false : value.ValueBool();

  if (kind == ProcedureKind::kSubgraph) {
    // Always breadth-first and visits each node once, so neither key is read.
    config_.uniqueness = Uniqueness::kNodeGlobal;
  } else {
    value = config.At("bfs");
    config_.bfs = value.IsNull() ? true : value.ValueBool();

    value = config.At("uniqueness");
    if (!value.IsNull()) {
      config_.uniqueness = ParseUniqueness(value.ValueString());
    }
  }

  ParseNodeFilters(config, graph);
}

// The preferred keys supersede the deprecated ones, which are read only when the preferred came back
// empty -- so a stale ID in a superseded list cannot abort the query.
void Path::PathHelper::ParseNodeFilters(const mgp::Map &config, const mgp::Graph &graph) {
  config_.allowlist_nodes = CollectNodeIds(config.At("allowlistNodes"), graph);
  if (config_.allowlist_nodes.empty()) {
    config_.allowlist_nodes = CollectNodeIds(config.At("whitelistNodes"), graph);
  }

  config_.denylist_nodes = CollectNodeIds(config.At("denylistNodes"), graph);
  if (config_.denylist_nodes.empty()) {
    config_.denylist_nodes = CollectNodeIds(config.At("blacklistNodes"), graph);
  }

  config_.end_nodes = CollectNodeIds(config.At("endNodes"), graph);
  config_.terminator_nodes = CollectNodeIds(config.At("terminatorNodes"), graph);

  // Allowlisted implicitly, or an allowlist not naming them would starve the filter it is paired with.
  if (!config_.allowlist_nodes.empty()) {
    config_.allowlist_nodes.insert(config_.end_nodes.begin(), config_.end_nodes.end());
    config_.allowlist_nodes.insert(config_.terminator_nodes.begin(), config_.terminator_nodes.end());
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

// The label filter on its own. First match wins, in this order: deny, terminator, end, allow.
Path::Evaluation Path::PathHelper::EvaluateLabels(const mgp::Node &node, const int64_t depth) const {
  // An unfiltered start node bypasses the filter entirely. `filterStartNode` alone decides this, here
  // and in the two filters alongside, so a start node cannot be filtered by one and not the other.
  if (depth == 0 && !config_.filter_start_node) {
    return {.include = !EndNodesOnly(), .expand = true};
  }

  const LabelBools label_bools = GetLabelBools(node);
  // Below the lower bound a node cannot be returned, but the walk still passes through it -- including
  // through a terminator, which ends the walk only once it could be returned.
  const bool below_min_hops = depth < config_.min_hops;

  if (label_bools.blacklisted) {
    return {.include = false, .expand = false};
  }
  if (label_bools.terminated) {
    return {.include = !below_min_hops, .expand = below_min_hops};
  }
  if (label_bools.end_node) {
    return {.include = !below_min_hops, .expand = true};
  }
  if (Whitelisted(label_bools.whitelisted)) {
    // With an end or termination label in play, only those are returned.
    return {.include = !(EndNodesOnly() || below_min_hops), .expand = true};
  }
  return {.include = false, .expand = false};
}

// Identity counterpart of the '>' and '/' label sets.
Path::Evaluation Path::PathHelper::EvaluateEndAndTerminatorNodes(const mgp::Node &node, const int64_t depth) const {
  if ((depth == 0 && !config_.filter_start_node) || depth < config_.min_hops) {
    return {.include = false, .expand = true};
  }
  const auto id = node.Id().AsInt();
  const bool is_terminator = config_.terminator_nodes.contains(id);
  return {.include = is_terminator || config_.end_nodes.contains(id), .expand = !is_terminator};
}

// allowlistNodes / denylistNodes: a node either list rejects is neither returned nor expanded through.
Path::Evaluation Path::PathHelper::EvaluateNodeLists(const mgp::Node &node, const int64_t depth) const {
  // Exempt by position, not identity: the same node re-entered deeper in the walk is filtered.
  if (depth == 0 && !config_.filter_start_node) {
    return {};
  }

  const auto id = node.Id().AsInt();
  if (config_.denylist_nodes.contains(id)) {
    return {.include = false, .expand = false};
  }
  if (!config_.allowlist_nodes.empty() && !config_.allowlist_nodes.contains(id)) {
    return {.include = false, .expand = false};
  }
  return {};
}

// Every filter must agree before a node is returned; any one may stop the walk.
Path::Evaluation Path::PathHelper::Evaluate(const mgp::Node &node, const int64_t depth) const {
  Evaluation evaluation = EvaluateLabels(node, depth);
  if (!config_.end_nodes.empty() || !config_.terminator_nodes.empty()) {
    evaluation &= EvaluateEndAndTerminatorNodes(node, depth);
  }
  if (!config_.allowlist_nodes.empty() || !config_.denylist_nodes.empty()) {
    evaluation &= EvaluateNodeLists(node, depth);
  }
  return evaluation;
}

bool Path::PathHelper::PathSizeOk(const int64_t path_size) const {
  // Each pass emits its own depth only, so the passes partition the results.
  if (config_.pass_depth >= 0 && path_size != config_.pass_depth) {
    return false;
  }
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
    // Either would filter on a label no node can carry, silently emptying or unbounding the result.
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
    const std::string_view entry{rel.ValueString()};
    // An empty *list* means "no filter"; an empty entry inside one is a mistake.
    if (entry.empty()) {
      throw mgp::ValueException(
          "Invalid relationshipFilter entry '': expected a relationship type, optionally marked with '<' or '>'.");
    }

    // A marker counts wherever it appears, so '<R', 'R<' and '<R>' are all the incoming filter;
    // '<' wins when both are present.
    const bool incoming = entry.contains('<');
    const bool outgoing = !incoming && entry.contains('>');

    // The type is whatever is left once the markers are stripped.
    std::string type;
    type.reserve(entry.size());
    for (const char character : entry) {
      if (character != '<' && character != '>' && character != ':') {
        type.push_back(character);
      }
    }

    // Nothing left means a direction-only entry, applying to every type -- but only if a marker is what
    // emptied it. Separators alone name neither, and would filter out every relationship in the graph.
    if (type.empty()) {
      if (!incoming && !outgoing) {
        throw mgp::ValueException("Invalid relationshipFilter entry '" + std::string(entry) +
                                  "': expected a relationship type, optionally marked with '<' or '>'.");
      }
      config_.any_incoming = config_.any_incoming || incoming;
      config_.any_outgoing = config_.any_outgoing || outgoing;
      continue;
    }

    auto direction = RelDirection::kAny;
    if (incoming) {
      direction = RelDirection::kIncoming;
    } else if (outgoing) {
      direction = RelDirection::kOutgoing;
    }
    AddRelationshipDirection(std::move(type), direction);
  }
}

// Merge rather than overwrite: 'R>' with '<R' means either way. Overwriting would drop one direction,
// and which one would depend on the order they were listed in.
void Path::PathHelper::AddRelationshipDirection(std::string type, RelDirection direction) {
  const auto [it, inserted] = config_.relationship_sets.try_emplace(std::move(type), direction);
  if (!inserted && it->second != direction) {
    it->second = RelDirection::kAny;
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

void Path::Create(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto graph = mgp::Graph(memgraph_graph);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto start_value = arguments[0];
    // Nothing to build a path from, so no rows rather than an error.
    if (start_value.IsNull()) {
      return;
    }
    if (!start_value.IsNode()) {
      throw mgp::ValueException("The start node needs to be a node.");
    }
    auto start_node{start_value.ValueNode()};
    auto relationships{arguments[1].ValueMap()};

    // At() yields a null value for a missing key; operator[] yields a null handle that the conversion
    // below would dereference, taking the process down.
    const auto relationships_value = relationships.At("rel");
    if (!relationships_value.IsNull() && !relationships_value.IsList()) {
      throw mgp::ValueException("The 'rel' entry needs to be a list of relationships.");
    }
    const auto relationship_list = relationships_value.IsNull() ? mgp::List{} : relationships_value.ValueList();

    // Each entry scans an endpoint's relationships, so a long list needs polling too.
    uint64_t abort_poll_counter = 0;
    mgp::Path path{start_node};
    for (const auto &relationship : relationship_list) {
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
    record.Insert(kResultCreate, path);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

int64_t Path::PathExpand::UniquenessKey(const mgp::Relationship &relationship, const bool outgoing) const {
  if (IsNodeUniqueness(path_data_.helper_.GetUniqueness())) {
    return (outgoing ? relationship.To() : relationship.From()).Id().AsInt();
  }
  return relationship.Id().AsInt();
}

void Path::PathExpand::ExpandPath(mgp::Path &path, const mgp::Relationship &relationship, int64_t path_size,
                                  const int64_t uniqueness_key) {
  path.Expand(relationship);
  path_data_.visited_.insert(uniqueness_key);
  DFS(path, path_size + 1);
  // A path-scoped rule releases the mark on the way back out; a global one holds it for the whole walk.
  if (!path_data_.helper_.GlobalUniqueness()) {
    path_data_.visited_.erase(uniqueness_key);
  }
  path.Pop();
}

void Path::PathExpand::ExpandFromRelationships(mgp::Path &path, mgp::Relationships relationships, bool outgoing,
                                               int64_t path_size) {
  const RelDirection curr_direction = outgoing ? RelDirection::kOutgoing : RelDirection::kIncoming;

  for (const auto relationship : relationships) {
    if (path_data_.LimitReached()) {
      return;
    }
    // A node whose relationships are all filtered out does no other work, so without this poll a
    // supernode's whole adjacency list is uninterruptible.
    path_data_.MaybeAbort();

    const std::string_view type = relationship.Type();
    const auto wanted_direction = path_data_.helper_.GetDirection(type);

    const int64_t uniqueness_key = UniquenessKey(relationship, outgoing);
    if ((wanted_direction == RelDirection::kNone && !path_data_.helper_.AnyDirected(outgoing)) ||
        path_data_.visited_.contains(uniqueness_key)) {
      continue;
    }

    if (wanted_direction == RelDirection::kAny || curr_direction == wanted_direction ||
        path_data_.helper_.AnyDirected(outgoing)) {
      ExpandPath(path, relationship, path_size, uniqueness_key);
    }
  }
}

void Path::PathExpand::Emit(const mgp::Path &path) {
  auto record = path_data_.record_factory_.NewRecord();
  record.Insert(kResultExpand, path);
  ++path_data_.emitted_;
}

/*function used for traversal and filtering*/
void Path::PathExpand::DFS(mgp::Path &path, int64_t path_size) {
  if (path_data_.LimitReached()) {
    return;
  }
  // Enumerates paths, not nodes, so it can run far longer than the graph is big -- and with a high
  // minHops it does so without emitting anything, where no memory limit would stop it.
  path_data_.MaybeAbort();

  // One frame per hop; refuse rather than overflow the stack.
  if (path_size > kMaxExpandDepth) {
    throw mgp::ValueException("Path expansion exceeded the maximum depth of " + std::to_string(kMaxExpandDepth) +
                              "; lower the upper hop bound to bound the traversal.");
  }

  const mgp::Node node{path.GetNodeAt(path_size)};

  // Counts whether or not the filters keep it: this tells the driver a deeper pass has somewhere to go.
  deepest_reached_ = std::max(deepest_reached_, path_size);

  const Evaluation evaluation = path_data_.helper_.Evaluate(node, path_size);

  if (evaluation.include && path_data_.helper_.PathSizeOk(path_size)) {
    Emit(path);
    if (path_data_.LimitReached()) {
      return;
    }
  }

  if (!evaluation.expand || std::cmp_greater(path_size + 1, path_data_.helper_.ExpansionCeiling())) {
    return;
  }

  this->ExpandFromRelationships(path, node.InRelationships(), false, path_size);
  this->ExpandFromRelationships(path, node.OutRelationships(), true, path_size);
}

void Path::PathExpand::StartAlgorithm(const mgp::Node &node) {
  mgp::Path path = mgp::Path(node);
  // A node-keyed rule has to count the start node, or a walk could return to it.
  const bool mark_start = IsNodeUniqueness(path_data_.helper_.GetUniqueness());
  if (mark_start) {
    path_data_.visited_.insert(node.Id().AsInt());
  }
  DFS(path, 0);
  if (mark_start && !path_data_.helper_.GlobalUniqueness()) {
    path_data_.visited_.erase(node.Id().AsInt());
  }
}

void Path::PathExpand::Parse(const mgp::Value &value) {
  if (value.IsNode()) {
    path_data_.AddStartNode(value.ValueNode());
  } else if (value.IsInt()) {
    path_data_.AddStartNode(path_data_.graph_.GetNodeById(mgp::Id::FromInt(value.ValueInt())));
  } else {
    throw mgp::ValueException("Invalid start type. Expected Node, Int, List[Node, Int]");
  }
}

void Path::PathExpand::RunAllStarts() {
  for (const auto &node : path_data_.start_nodes_) {
    if (path_data_.LimitReached()) {
      return;
    }
    StartAlgorithm(node);
  }
}

// The relationships reaching `index`, walked back to a start node and replayed forwards.
mgp::Path Path::PathExpand::PathTo(const int64_t index) const {
  std::vector<int64_t> chain;
  for (int64_t at = index; at >= 0; at = tree_[at].parent) {
    chain.push_back(at);
  }

  mgp::Path path{tree_[chain.back()].node};
  // Back to front: the last entry is the start node, whose own `from_parent` is empty.
  for (const int64_t at : std::ranges::reverse_view(chain)) {
    if (tree_[at].from_parent.has_value()) {
      path.Expand(*tree_[at].from_parent);
    }
  }
  return path;
}

void Path::PathExpand::ExpandTreeEntry(const int64_t index, const int64_t depth, mgp::Relationships relationships,
                                       const bool outgoing, std::queue<int64_t> &frontier) {
  const RelDirection curr_direction = outgoing ? RelDirection::kOutgoing : RelDirection::kIncoming;

  for (const auto relationship : relationships) {
    if (path_data_.LimitReached()) {
      return;
    }
    // As in the other walks: a fully filtered supernode never reaches the poll at the dequeue.
    path_data_.MaybeAbort();

    auto next_node = outgoing ? relationship.To() : relationship.From();
    if (path_data_.visited_.contains(next_node.Id().AsInt())) {
      continue;
    }

    const auto wanted_direction = path_data_.helper_.GetDirection(relationship.Type());
    if (wanted_direction == RelDirection::kNone && !path_data_.helper_.AnyDirected(outgoing)) {
      continue;
    }
    if (!(wanted_direction == RelDirection::kAny || curr_direction == wanted_direction ||
          path_data_.helper_.AnyDirected(outgoing))) {
      continue;
    }

    // Marked here rather than at the dequeue, so the first relationship to reach a node is the one that
    // keeps it -- which is what makes the tree breadth-first, and holds even for a node a filter then
    // rejects: it is spent either way.
    path_data_.visited_.insert(next_node.Id().AsInt());
    tree_.push_back({.node = std::move(next_node), .from_parent = relationship, .parent = index, .depth = depth + 1});
    frontier.push(static_cast<int64_t>(tree_.size()) - 1);
  }
}

// The node-global rule returns one path per reachable node, so the frontier is bounded by the graph
// rather than by the paths through it -- a real queue can hold it, and the tree it builds gives each
// node its parent. Every start node enters at depth 0 sharing one visited set, so a start reached from
// another start is not returned twice.
void Path::PathExpand::RunNodeGlobalBfs() {
  std::queue<int64_t> frontier;
  for (const auto &node : path_data_.start_nodes_) {
    path_data_.visited_.insert(node.Id().AsInt());
    tree_.push_back({.node = node, .from_parent = std::nullopt, .parent = -1, .depth = 0});
    frontier.push(static_cast<int64_t>(tree_.size()) - 1);
  }

  while (!frontier.empty()) {
    if (path_data_.LimitReached()) {
      return;
    }
    path_data_.MaybeAbort();

    const int64_t index = frontier.front();
    frontier.pop();
    const int64_t depth = tree_[index].depth;
    // Copied out: appending to the tree may move the entry while its relationships are being iterated.
    const mgp::Node node{tree_[index].node};

    const Evaluation evaluation = path_data_.helper_.Evaluate(node, depth);
    if (evaluation.include && path_data_.helper_.PathSizeOk(depth)) {
      Emit(PathTo(index));
      if (path_data_.LimitReached()) {
        return;
      }
    }

    if (!evaluation.expand || std::cmp_greater(depth + 1, path_data_.helper_.ExpansionCeiling())) {
      continue;
    }

    ExpandTreeEntry(index, depth, node.InRelationships(), false, frontier);
    ExpandTreeEntry(index, depth, node.OutRelationships(), true, frontier);
  }
}

// Breadth-first emits every path of one length before any longer one, which is what makes a `limit`
// return the shortest paths. It is driven by re-walking once per depth, so the traversal, the filters
// and the uniqueness rule stay the ones the depth-first walk uses. A queue of partial paths would
// instead hold the whole frontier, which here is as large as the result set it is about to build --
// except under the node-global rule, which bounds it by the graph and gets its own walk.
void Path::PathExpand::RunAlgorithm() {
  if (path_data_.helper_.GlobalUniqueness() && path_data_.helper_.Bfs()) {
    RunNodeGlobalBfs();
    return;
  }

  if (!path_data_.helper_.Bfs()) {
    RunAllStarts();
    return;
  }

  const int64_t min_hops = path_data_.helper_.MinHops();
  const int64_t max_hops = path_data_.helper_.MaxHops();

  for (int64_t depth = std::max(min_hops, int64_t{0}); depth <= max_hops; ++depth) {
    deepest_reached_ = -1;
    path_data_.helper_.SetPassDepth(depth);
    RunAllStarts();
    // Stop on the limit, or when nothing reached this depth -- nothing can then reach a greater one,
    // which is what bounds the loop with no upper hop bound.
    if (path_data_.LimitReached() || deepest_reached_ < depth) {
      break;
    }
  }

  path_data_.helper_.ClearPassDepth();
}

namespace {

void RunExpand(Path::PathHelper &&helper, const mgp::Value &start_value, const mgp::RecordFactory &record_factory,
               const mgp::Graph &graph) {
  // Nowhere to walk from, so no paths rather than an error.
  if (start_value.IsNull()) {
    return;
  }

  Path::PathExpand path_expand{Path::PathData(std::move(helper), record_factory, graph)};

  if (!start_value.IsList()) {
    path_expand.Parse(start_value);
  } else {
    // Caller-supplied and each entry may cost a lookup, so it polls too.
    uint64_t abort_poll_counter = 0;
    for (const auto &list_item : start_value.ValueList()) {
      Path::PollAbort(graph, abort_poll_counter);
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

    RunExpand(PathHelper{config, graph, ProcedureKind::kExpand}, arguments[0], record_factory, graph);

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
    path_data_.AddStartNode(value.ValueNode());
    return;
  }
  path_data_.AddStartNode(path_data_.graph_.GetNodeById(mgp::Id::FromInt(value.ValueInt())));
}

void Path::PathSubgraph::ExpandFromRelationships(const std::pair<mgp::Node, int64_t> &pair,
                                                 const mgp::Relationships relationships, bool outgoing,
                                                 std::queue<std::pair<mgp::Node, int64_t>> &queue) {
  const RelDirection curr_direction = outgoing ? RelDirection::kOutgoing : RelDirection::kIncoming;

  for (const auto relationship : relationships) {
    // As in the expand walk: a fully filtered supernode never reaches the dequeue poll above.
    path_data_.MaybeAbort();

    auto next_node = outgoing ? relationship.To() : relationship.From();

    if (path_data_.visited_.contains(next_node.Id().AsInt())) {
      continue;
    }

    const std::string_view type = relationship.Type();
    const auto wanted_direction = path_data_.helper_.GetDirection(type);

    if (wanted_direction == RelDirection::kNone && !path_data_.helper_.AnyDirected(outgoing)) {
      continue;
    }

    if (wanted_direction == RelDirection::kAny || curr_direction == wanted_direction ||
        path_data_.helper_.AnyDirected(outgoing)) {
      // Enqueue only; TryInsertNode emits it on dequeue, once the checks are applied.
      path_data_.visited_.insert(next_node.Id().AsInt());
      queue.emplace(std::move(next_node), pair.second + 1);
    }
  }
}

void Path::PathSubgraph::TryInsertNode(const mgp::Node &node, int64_t hop_count, const Evaluation &evaluation) {
  // Closer than minHops: traversed, but not returned.
  if (!path_data_.helper_.PathSizeOk(hop_count)) {
    return;
  }

  if (evaluation.include) {
    to_be_returned_nodes_.AppendExtend(mgp::Value(node));
    ++path_data_.emitted_;
  }
}

mgp::List Path::PathSubgraph::BFS() {
  std::queue<std::pair<mgp::Node, int64_t>> queue;

  for (const auto &node : path_data_.start_nodes_) {
    queue.emplace(node, 0);
    path_data_.visited_.insert(node.Id().AsInt());
  }

  while (!queue.empty()) {
    if (path_data_.LimitReached()) {
      break;
    }
    path_data_.MaybeAbort();

    auto pair = std::move(queue.front());
    queue.pop();

    if (path_data_.helper_.PathTooBig(pair.second)) {
      continue;
    }

    const Evaluation evaluation = path_data_.helper_.Evaluate(pair.first, pair.second);
    TryInsertNode(pair.first, pair.second, evaluation);
    if (!evaluation.expand || std::cmp_greater(pair.second + 1, path_data_.helper_.MaxHops())) {
      continue;
    }

    this->ExpandFromRelationships(pair, pair.first.InRelationships(), false, queue);
    this->ExpandFromRelationships(pair, pair.first.OutRelationships(), true, queue);
  }

  return to_be_returned_nodes_;
}

void Path::SubgraphNodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto graph = mgp::Graph(memgraph_graph);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    // Read the config first: a null start must not decide whether a bad key is reported, or the same
    // call would fail only on some rows.
    auto config = arguments[1].ValueMap();
    PathHelper helper{config, graph, ProcedureKind::kSubgraph};

    auto start_value = arguments[0];
    // Nowhere to walk from, so no nodes rather than an error.
    if (start_value.IsNull()) {
      return;
    }

    PathSubgraph path_subgraph{PathData(std::move(helper), record_factory, graph)};

    if (!start_value.IsList()) {
      path_subgraph.Parse(start_value);
    } else {
      uint64_t abort_poll_counter = 0;
      for (const auto &list_item : start_value.ValueList()) {
        PollAbort(graph, abort_poll_counter);
        path_subgraph.Parse(list_item);
      }
    }

    auto to_be_returned_nodes = path_subgraph.BFS();

    for (const auto &node : to_be_returned_nodes) {
      auto record = record_factory.NewRecord();
      record.Insert(kResultSubgraphNodes, node);
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
    // Read the config first: a null start must not decide whether a bad key is reported, or the same
    // call would fail only on some rows.
    auto config = arguments[1].ValueMap();
    PathHelper helper{config, graph, ProcedureKind::kSubgraph};

    auto start_value = arguments[0];
    // This procedure always returns one record; here it is the empty one.
    if (start_value.IsNull()) {
      auto record = record_factory.NewRecord();
      record.Insert(kResultNodesSubgraphAll, mgp::List{});
      record.Insert(kResultRelsSubgraphAll, mgp::List{});
      return;
    }

    PathSubgraph path_subgraph{PathData(std::move(helper), record_factory, graph)};

    if (!start_value.IsList()) {
      path_subgraph.Parse(start_value);
    } else {
      uint64_t abort_poll_counter = 0;
      for (const auto &list_item : start_value.ValueList()) {
        PollAbort(graph, abort_poll_counter);
        path_subgraph.Parse(list_item);
      }
    }

    const auto to_be_returned_nodes = path_subgraph.BFS();

    std::unordered_set<mgp::Node> to_be_returned_nodes_searchable;

    for (const auto &node : to_be_returned_nodes) {
      to_be_returned_nodes_searchable.insert(node.ValueNode());
    }

    // A second O(nodes * degree) pass outside the walk, so it polls too.
    uint64_t abort_poll_counter = 0;
    mgp::List to_be_returned_rels;
    for (auto node : to_be_returned_nodes) {
      // A sink never reaches the inner poll, so a result set of them would be uninterruptible.
      PollAbort(graph, abort_poll_counter);
      for (auto rel : node.ValueNode().OutRelationships()) {
        PollAbort(graph, abort_poll_counter);
        if (to_be_returned_nodes_searchable.contains(rel.To())) {
          to_be_returned_rels.AppendExtend(mgp::Value(rel));
        }
      }
    }

    auto record = record_factory.NewRecord();
    record.Insert(kResultNodesSubgraphAll, to_be_returned_nodes);
    record.Insert(kResultRelsSubgraphAll, to_be_returned_rels);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

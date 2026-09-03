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

// The depth-first walk recurses once per hop, so refuse rather than overflow the stack. Measured at
// 288 bytes of frame per hop in a release build and 650 under a sanitizer, so this keeps a wide margin.
constexpr int64_t kMaxExpandDepth = 5000;

void RefuseIfTooDeep(const int64_t path_size) {
  if (path_size > kMaxExpandDepth) {
    throw mgp::ValueException("Path expansion exceeded the maximum depth of " + std::to_string(kMaxExpandDepth) +
                              "; lower the upper hop bound to bound the traversal.");
  }
}

// An end or termination filter anywhere in the step means only those nodes are returned.
bool StepEndsNodesOnly(const Path::LabelStep &step) {
  return !step.sets.end_list.empty() || !step.sets.termination_list.empty() || step.wildcards.end_list ||
         step.wildcards.termination;
}

bool StepAdmits(const Path::RelStep &step, const bool outgoing) {
  if (outgoing ? step.any_outgoing : step.any_incoming) {
    return true;
  }
  const auto wanted = outgoing ? Path::RelDirection::kOutgoing : Path::RelDirection::kIncoming;
  return std::ranges::any_of(step.types, [wanted](const auto &entry) {
    return entry.second == Path::RelDirection::kAny || entry.second == wanted;
  });
}

}  // namespace

Path::PathHelper::PathHelper(const mgp::List &labels, const mgp::List &relationships, int64_t min_hops,
                             int64_t max_hops) {
  // One step each: the positional form takes lists of alternatives, which cannot spell a sequence.
  config_.label_steps.push_back(ParseLabelStep(labels));
  config_.rel_steps.push_back(ParseRelStep(relationships));
  config_.end_nodes_only = StepEndsNodesOnly(config_.label_steps.front());
  config_.min_hops = min_hops;
  config_.max_hops = MaxHopsOrNoLimit(max_hops);
  // No `bfs` argument in this form; depth-first is the cheaper default.
  config_.bfs = false;
  SizeLabelCache();
}

namespace {

// Every accepted config key. An unrecognized one is rejected rather than ignored: dropping `limit` or
// `endNodes` would return a different result set than was asked for.
constexpr std::array<std::string_view, 18> kConfigKeys{"minHops",
                                                       "maxHops",
                                                       "minLevel",
                                                       "maxLevel",
                                                       "relationshipFilter",
                                                       "labelFilter",
                                                       "sequence",
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

// One entry per '|'-separated piece of one sequence step. Empty pieces are dropped.
mgp::List SplitAlternatives(std::string_view text) {
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

std::string_view Trimmed(std::string_view text) {
  constexpr std::string_view kSpace = " \t\n\r";
  const size_t first = text.find_first_not_of(kSpace);
  if (first == std::string_view::npos) {
    return {};
  }
  return text.substr(first, text.find_last_not_of(kSpace) - first + 1);
}

// A filter's steps: one per comma in the string form. A step that names no filter would match
// everything, which reads as a typo rather than as a filter, so it is named instead of honoured --
// whether it is blank or its alternatives are all empty, since '|' alone leaves nothing behind either.
std::vector<mgp::List> SplitSteps(std::string_view text, std::string_view key) {
  std::vector<mgp::List> steps;
  for (size_t start = 0; start <= text.size();) {
    const size_t separator = text.find(',', start);
    const size_t end = separator == std::string_view::npos ? text.size() : separator;
    const std::string_view step = Trimmed(text.substr(start, end - start));
    if (step.empty()) {
      throw mgp::ValueException("Config key '" + std::string(key) + "' has a blank step at position " +
                                std::to_string(steps.size() + 1) + "; every step needs a filter.");
    }
    // Empty '|' pieces are dropped, so a step of bare separators reaches here non-empty and would then
    // parse as the step that allows everything.
    mgp::List alternatives = SplitAlternatives(step);
    if (alternatives.Size() == 0) {
      throw mgp::ValueException("Config key '" + std::string(key) + "' has a step with no alternatives at position " +
                                std::to_string(steps.size() + 1) + "; every step needs a filter.");
    }
    steps.push_back(std::move(alternatives));
    if (separator == std::string_view::npos) {
      break;
    }
    start = separator + 1;
  }
  return steps;
}

// The steps of `labelFilter` / `relationshipFilter`: a string may spell a sequence, a list is one step
// of alternatives, as it has always been. Absent leaves the caller to supply the unfiltered step.
std::vector<mgp::List> FilterSteps(const mgp::Value &value, std::string_view key) {
  if (value.IsNull()) {
    return {};
  }
  if (value.IsList()) {
    const mgp::List entries = value.ValueList();
    // An empty list is a filter that was not given, so it must not leave a step behind for
    // `beginSequenceAtStart` to spend on the first hop -- the same as leaving the key out.
    if (entries.Size() == 0) {
      return {};
    }
    for (const auto &entry : entries) {
      if (entry.IsString() && entry.ValueString().find(',') != std::string_view::npos) {
        throw mgp::ValueException("Config key '" + std::string(key) + "' entry '" + std::string(entry.ValueString()) +
                                  "' contains a ','. A list entry is one alternative; give the whole filter as a "
                                  "string to spell a sequence of steps.");
      }
    }
    return {entries};
  }
  if (!value.IsString()) {
    throw mgp::ValueException("Config key '" + std::string(key) + "' needs to be a string or a list of strings.");
  }
  const std::string_view text = Trimmed(value.ValueString());
  if (text.empty()) {
    return {};
  }
  return SplitSteps(text, key);
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
  require_type("beginSequenceAtStart", config.At("beginSequenceAtStart"), mgp::Type::Bool, "a boolean");
  require_type("sequence", config.At("sequence"), mgp::Type::String, "a string");
  require_type("limit", config.At("limit"), mgp::Type::Int, "an integer");

  // Either spelling is allowed, so `require_type` cannot say it. Checked here rather than only where the
  // steps are read: `sequence` supersedes these two keys' values, but not their type -- a caller who
  // mistypes one alongside a sequence is still told which key is wrong.
  auto require_filter_type = [](std::string_view key, const mgp::Value &value) {
    if (!value.IsNull() && !value.IsString() && !value.IsList()) {
      throw mgp::ValueException("Config key '" + std::string(key) + "' needs to be a string or a list of strings.");
    }
  };
  require_filter_type("labelFilter", config.At("labelFilter"));
  require_filter_type("relationshipFilter", config.At("relationshipFilter"));

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

  mgp::Value value{};

  value = config.At("filterStartNode");
  config_.filter_start_node = value.IsNull() ? false : value.ValueBool();

  value = config.At("beginSequenceAtStart");
  config_.begin_sequence_at_start = value.IsNull() || value.ValueBool();

  // After the two flags above: which step a filter's first piece becomes depends on both.
  ParseSequences(config);

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
  SizeLabelCache();
}

// `sequence` spells one alternating string; the two filter keys spell a label and a relationship
// sequence separately. Either way the result is a list of steps per kind, and a filter written without
// commas is the single step it has always been.
void Path::PathHelper::ParseSequences(const mgp::Map &config) {
  const auto sequence = config.At("sequence");
  std::vector<mgp::List> label_steps;
  std::vector<mgp::List> rel_steps;
  bool rel_filter_given = false;
  // A blank `sequence` is no sequence at all, so the two filter keys still apply -- and an error below
  // has to name the key the caller actually wrote.
  const bool sequence_given = !sequence.IsNull() && !Trimmed(sequence.ValueString()).empty();

  if (sequence_given) {
    // Alternating, labels first -- so with the sequence starting one node out, the leading step is the
    // relationship reaching the node the sequence starts at.
    const auto steps = SplitSteps(Trimmed(sequence.ValueString()), "sequence");
    for (int64_t index = 0; std::cmp_less(index, steps.size()); ++index) {
      const int64_t position = config_.begin_sequence_at_start ? index : index - 1;
      (position % 2 == 0 ? label_steps : rel_steps).push_back(steps[index]);
    }
    if (label_steps.empty() || rel_steps.empty()) {
      throw mgp::ValueException(
          "Config key 'sequence' needs at least one label step and one relationship step; it alternates between them, "
          "starting with a label filter.");
    }
    rel_filter_given = true;
  } else {
    label_steps = FilterSteps(config.At("labelFilter"), "labelFilter");
    rel_steps = FilterSteps(config.At("relationshipFilter"), "relationshipFilter");
    rel_filter_given = !rel_steps.empty();
  }

  for (const auto &step : label_steps) {
    config_.label_steps.push_back(ParseLabelStep(step));
    config_.end_nodes_only = config_.end_nodes_only || StepEndsNodesOnly(config_.label_steps.back());
  }
  for (const auto &step : rel_steps) {
    config_.rel_steps.push_back(ParseRelStep(step));
  }

  // Only a filter that was actually given has a step to spend on the first hop.
  if (!config_.begin_sequence_at_start && rel_filter_given) {
    config_.initial_rel_step = std::move(config_.rel_steps.front());
    config_.rel_steps.erase(config_.rel_steps.begin());
    if (config_.rel_steps.empty()) {
      throw mgp::ValueException(
          "With 'beginSequenceAtStart' false the first relationship step is spent on the hop out of the start node, so "
          "the relationship steps of '" +
          std::string(sequence_given ? "sequence" : "relationshipFilter") +
          "' need a further one to repeat; give another, or leave 'beginSequenceAtStart' at true.");
    }
  }

  // An absent filter is the step that allows everything, so the walk has a step at every depth.
  if (config_.label_steps.empty()) {
    config_.label_steps.emplace_back();
  }
  if (config_.rel_steps.empty()) {
    config_.rel_steps.push_back(ParseRelStep(mgp::List{}));
  }
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

// Cyclic, so the sequence repeats for as long as the walk goes on.
int64_t Path::PathHelper::LabelStepIndexAt(const int64_t depth) const {
  const int64_t position = config_.begin_sequence_at_start ? depth : depth - 1;
  // Depth 0 with the sequence starting one node out never reaches here: EvaluateLabels bypasses it.
  return position % static_cast<int64_t>(config_.label_steps.size());
}

Path::LabelBools Path::PathHelper::CachedLabelBools(const mgp::Node &node, const int64_t step_index) const {
  // A graph-wide rule reaches each node once, so a cache would only ever be written to.
  if (GlobalUniqueness()) {
    return GetLabelBools(node, config_.label_steps[static_cast<size_t>(step_index)]);
  }
  auto &cache = label_bools_cache_[static_cast<size_t>(step_index)];
  const int64_t id = node.Id().AsInt();
  if (const auto it = cache.find(id); it != cache.end()) {
    return it->second;
  }
  return cache.emplace(id, GetLabelBools(node, config_.label_steps[static_cast<size_t>(step_index)])).first->second;
}

void Path::PathHelper::SizeLabelCache() {
  if (GlobalUniqueness()) {
    return;
  }
  label_bools_cache_.resize(config_.label_steps.size());
}

const Path::RelStep &Path::PathHelper::RelStepAt(const int64_t depth) const {
  if (depth == 0 && config_.initial_rel_step.has_value()) {
    return *config_.initial_rel_step;
  }
  const int64_t position = config_.initial_rel_step.has_value() ? depth - 1 : depth;
  return config_.rel_steps[position % static_cast<int64_t>(config_.rel_steps.size())];
}

// RelationshipAdmitted's direction test, hoisted: it depends on the step alone, so it can rule out a
// whole adjacency list.
bool Path::PathHelper::StepAdmitsDirection(const int64_t depth, const bool outgoing) const {
  const RelStep &step = RelStepAt(depth);
  return outgoing ? step.admits_outgoing : step.admits_incoming;
}

bool Path::PathHelper::RelationshipAdmitted(std::string_view rel_type, const bool outgoing, const int64_t depth) const {
  const RelStep &step = RelStepAt(depth);
  const bool any_directed = outgoing ? step.any_outgoing : step.any_incoming;

  const auto it = step.types.find(rel_type);
  const auto wanted_direction = it == step.types.end() ? RelDirection::kNone : it->second;
  if (wanted_direction == RelDirection::kNone && !any_directed) {
    return false;
  }

  const RelDirection curr_direction = outgoing ? RelDirection::kOutgoing : RelDirection::kIncoming;
  return wanted_direction == RelDirection::kAny || curr_direction == wanted_direction || any_directed;
}

Path::LabelBools Path::PathHelper::GetLabelBools(const mgp::Node &node, const LabelStep &step) {
  // A '*' in a category answers for every label, so it is the starting point rather than a lookup.
  LabelBools label_bools{.blacklisted = step.wildcards.blacklist,
                         .terminated = step.wildcards.termination,
                         .end_node = step.wildcards.end_list,
                         .whitelisted = step.wildcards.whitelist};
  for (const auto &label : node.Labels()) {
    FilterLabel(label, step, label_bools);
  }
  return label_bools;
}

// The label filter on its own. First match wins, in this order: deny, terminator, end, allow.
Path::Evaluation Path::PathHelper::EvaluateLabels(const mgp::Node &node, const int64_t depth) const {
  // An unfiltered start node bypasses the filter entirely, and so does one the sequence does not start
  // at -- there is no step to test it against. `filterStartNode` alone decides the two filters
  // alongside, so those still apply to a start node that asked to be filtered.
  if (depth == 0 && (!config_.filter_start_node || !config_.begin_sequence_at_start)) {
    return {.include = !EndNodesOnly(), .expand = true};
  }

  const int64_t step_index = LabelStepIndexAt(depth);
  const LabelStep &step = config_.label_steps[static_cast<size_t>(step_index)];
  const bool below_min_hops = depth < config_.min_hops;
  if (step.constrains_nothing) {
    return {.include = !(EndNodesOnly() || below_min_hops), .expand = true};
  }
  const LabelBools label_bools = CachedLabelBools(node, step_index);

  if (label_bools.blacklisted) {
    return {.include = false, .expand = false};
  }
  if (label_bools.terminated) {
    return {.include = !below_min_hops, .expand = below_min_hops};
  }
  if (label_bools.end_node) {
    return {.include = !below_min_hops, .expand = true};
  }
  if (Whitelisted(step, label_bools.whitelisted)) {
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
  return (path_size <= config_.max_hops) && (path_size >= config_.min_hops);
}

bool Path::PathHelper::PathTooBig(const int64_t path_size) const { return path_size > config_.max_hops; }

bool Path::PathHelper::Whitelisted(const LabelStep &step, const bool whitelisted) {
  return step.whitelist_empty || whitelisted;
}

/*function to set appropriate parameters for filtering*/
void Path::PathHelper::FilterLabel(std::string_view label, const LabelStep &step, LabelBools &label_bools) {
  if (step.sets.blacklist.contains(label)) {
    label_bools.blacklisted = true;
  }

  if (step.sets.termination_list.contains(label)) {
    label_bools.terminated = true;
  }

  if (step.sets.end_list.contains(label)) {
    label_bools.end_node = true;
  }

  if (step.sets.whitelist.contains(label)) {
    label_bools.whitelisted = true;
  }
}

// One step's labels, sorted into the four categories; sets so that filtering is O(1). A '*' entry stands
// for every label, so it is recorded on the category rather than in its set.
Path::LabelStep Path::PathHelper::ParseLabelStep(const mgp::List &list_of_labels) {
  constexpr std::string_view kLabelPrefixes = "-+>/";
  LabelStep step;
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

    LabelSet *set = &step.sets.whitelist;
    bool *wildcard = &step.wildcards.whitelist;
    switch (label_string.front()) {
      case '-':
        set = &step.sets.blacklist;
        wildcard = &step.wildcards.blacklist;
        label_string.remove_prefix(1);
        break;
      case '>':
        set = &step.sets.end_list;
        wildcard = &step.wildcards.end_list;
        label_string.remove_prefix(1);
        break;
      case '/':
        set = &step.sets.termination_list;
        wildcard = &step.wildcards.termination;
        label_string.remove_prefix(1);
        break;
      case '+':
        label_string.remove_prefix(1);
        break;
      default:
        break;
    }

    if (label_string == "*") {
      *wildcard = true;
    } else {
      set->emplace(label_string);
    }
  }

  step.whitelist_empty = step.sets.whitelist.empty() && !step.wildcards.whitelist;
  step.constrains_nothing = step.whitelist_empty && step.sets.blacklist.empty() && step.sets.termination_list.empty() &&
                            step.sets.end_list.empty() && !step.wildcards.blacklist && !step.wildcards.termination &&
                            !step.wildcards.end_list;
  return step;
}

// One step's relationship types, sorted by direction.
Path::RelStep Path::PathHelper::ParseRelStep(const mgp::List &list_of_relationships) {
  RelStep step;
  if (list_of_relationships.Size() == 0) {  // no relationships given, so every relationship is allowed
    step.any_outgoing = true;
    step.any_incoming = true;
    step.admits_incoming = true;
    step.admits_outgoing = true;
    return step;
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

    // '*' is a label wildcard only. Reading it as a type name would match nothing at all, silently, so
    // name the spelling that does mean "any type here".
    if (type == "*") {
      throw mgp::ValueException("Invalid relationshipFilter entry '" + std::string(entry) +
                                "': '*' is not a wildcard for relationship types; use '>' for any outgoing type, "
                                "'<' for any incoming, or '<|>' for either.");
    }

    // Nothing left means a direction-only entry, applying to every type -- but only if a marker is what
    // emptied it. Separators alone name neither, and would filter out every relationship in the graph.
    if (type.empty()) {
      if (!incoming && !outgoing) {
        throw mgp::ValueException("Invalid relationshipFilter entry '" + std::string(entry) +
                                  "': expected a relationship type, optionally marked with '<' or '>'.");
      }
      step.any_incoming = step.any_incoming || incoming;
      step.any_outgoing = step.any_outgoing || outgoing;
      continue;
    }

    auto direction = RelDirection::kAny;
    if (incoming) {
      direction = RelDirection::kIncoming;
    } else if (outgoing) {
      direction = RelDirection::kOutgoing;
    }
    AddRelationshipDirection(step, std::move(type), direction);
  }

  step.admits_incoming = StepAdmits(step, false);
  step.admits_outgoing = StepAdmits(step, true);
  return step;
}

// Merge rather than overwrite: 'R>' with '<R' means either way. Overwriting would drop one direction,
// and which one would depend on the order they were listed in.
void Path::PathHelper::AddRelationshipDirection(RelStep &step, std::string type, RelDirection direction) {
  const auto [it, inserted] = step.types.try_emplace(std::move(type), direction);
  if (!inserted && it->second != direction) {
    it->second = RelDirection::kAny;
  }
}

void Path::Elements(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    if (arguments[0].IsNull()) {
      result.SetValue();
      return;
    }

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
    // A missing path leaves nothing to combine, so the other one is the answer.
    if (arguments[0].IsNull() && arguments[1].IsNull()) {
      result.SetValue();
      return;
    }
    if (arguments[0].IsNull() || arguments[1].IsNull()) {
      result.SetValue(arguments[0].IsNull() ? arguments[1].ValuePath() : arguments[0].ValuePath());
      return;
    }

    auto path1{arguments[0].ValuePath()};
    const auto path2{arguments[1].ValuePath()};

    for (size_t i = 0; i < path2.Length(); ++i) {
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
    if (arguments[0].IsNull()) {
      result.SetValue();
      return;
    }

    const auto path{arguments[0].ValuePath()};
    const auto path_length = static_cast<int64_t>(path.Length());

    // Offsets and lengths outside the path are clamped rather than rejected: a slice that runs off the
    // end yields the end node, so a caller computing bounds does not have to guard every call.
    const int64_t offset = std::clamp(arguments[1].ValueInt(), int64_t{0}, path_length);
    int64_t length = arguments[2].ValueInt();
    if (length == kSliceToEnd) {
      length = path_length - offset;
    }
    // Clamped against what is left, so the loop bound cannot overflow.
    const int64_t hops = std::clamp(length, int64_t{0}, path_length - offset);

    mgp::Path new_path{path.GetNodeAt(offset)};
    for (int64_t i = offset; i < offset + hops; ++i) {
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

namespace {
// mgp::Node and mgp::Relationship copy out of storage on construction. Iterating the C handles defers
// that to the candidates actually taken; the handles are borrowed and valid until the next step.
class BorrowedEdges {
 public:
  BorrowedEdges(mgp_vertex *vertex, bool outgoing)
      : iterator_(outgoing ? mgp::MemHandlerCallback(mgp::vertex_iter_out_edges, vertex)
                           : mgp::MemHandlerCallback(mgp::vertex_iter_in_edges, vertex)) {
    if (iterator_ == nullptr) {
      throw mg_exception::NotEnoughMemoryException();
    }
  }

  BorrowedEdges(const BorrowedEdges &) = delete;
  BorrowedEdges &operator=(const BorrowedEdges &) = delete;
  BorrowedEdges(BorrowedEdges &&) = delete;
  BorrowedEdges &operator=(BorrowedEdges &&) = delete;

  ~BorrowedEdges() { mgp::edges_iterator_destroy(iterator_); }

  mgp_edge *First() const { return mgp::edges_iterator_get(iterator_); }

  mgp_edge *Next() const { return mgp::edges_iterator_next(iterator_); }

 private:
  mgp_edges_iterator *iterator_;
};
}  // namespace

void Path::PathExpand::ExpandPath(mgp::Path &path, const mgp::Relationship &relationship, int64_t path_size,
                                  const int64_t uniqueness_key, const mgp::Node &next_node) {
  path.Expand(relationship);
  path_data_.visited_.insert(uniqueness_key);
  DFS(path, path_size + 1, next_node);
  // A path-scoped rule releases the mark on the way back out; a global one holds it for the whole walk.
  if (!path_data_.helper_.GlobalUniqueness()) {
    path_data_.visited_.erase(uniqueness_key);
  }
  path.Pop();
}

void Path::PathExpand::ExpandFromRelationships(mgp::Path &path, mgp_vertex *vertex, const bool outgoing,
                                               const int64_t path_size) {
  const bool node_keyed = IsNodeUniqueness(path_data_.helper_.GetUniqueness());

  const BorrowedEdges edges{vertex, outgoing};
  for (auto *edge = edges.First(); edge != nullptr; edge = edges.Next()) {
    if (path_data_.LimitReached()) {
      return;
    }
    // A node whose relationships are all filtered out does no other work, so without this poll a
    // supernode's whole adjacency list is uninterruptible.
    path_data_.MaybeAbort();

    // Only the type name is needed here; everything below this copies.
    if (!path_data_.helper_.RelationshipAdmitted(mgp::edge_get_type(edge).name, outgoing, path_size)) {
      continue;
    }

    auto *next_vertex = outgoing ? mgp::edge_get_to(edge) : mgp::edge_get_from(edge);
    const int64_t uniqueness_key = node_keyed ? mgp::vertex_get_id(next_vertex).as_int : mgp::edge_get_id(edge).as_int;
    if (path_data_.visited_.contains(uniqueness_key)) {
      continue;
    }

    ExpandPath(path, mgp::Relationship(edge), path_size, uniqueness_key, mgp::Node(next_vertex));
  }
}

void Path::PathExpand::Emit(const mgp::Path &path) {
  auto record = path_data_.record_factory_.NewRecord();
  record.Insert(kResultExpand, path);
  ++path_data_.emitted_;
}

/*function used for traversal and filtering*/
void Path::PathExpand::DFS(mgp::Path &path, int64_t path_size, const mgp::Node &node) {
  if (path_data_.LimitReached()) {
    return;
  }
  // Enumerates paths, not nodes, so it can run far longer than the graph is big -- and with a high
  // minHops it does so without emitting anything, where no memory limit would stop it.
  path_data_.MaybeAbort();

  // One frame per hop; refuse rather than overflow the stack.
  RefuseIfTooDeep(path_size);

  const Evaluation evaluation = path_data_.helper_.Evaluate(node, path_size);

  if (evaluation.include && path_data_.helper_.PathSizeOk(path_size)) {
    Emit(path);
    if (path_data_.LimitReached()) {
      return;
    }
  }

  if (!evaluation.expand || std::cmp_greater(path_size + 1, path_data_.helper_.MaxHops())) {
    return;
  }

  // Skip an adjacency list the step admits nothing from.
  if (path_data_.helper_.StepAdmitsDirection(path_size, false)) {
    this->ExpandFromRelationships(path, node.GetPtr(), false, path_size);
  }
  if (path_data_.helper_.StepAdmitsDirection(path_size, true)) {
    this->ExpandFromRelationships(path, node.GetPtr(), true, path_size);
  }
}

void Path::PathExpand::StartAlgorithm(const mgp::Node &node) {
  mgp::Path path = mgp::Path(node);
  // A node-keyed rule has to count the start node, or a walk could return to it.
  const bool mark_start = IsNodeUniqueness(path_data_.helper_.GetUniqueness());
  if (mark_start) {
    path_data_.visited_.insert(node.Id().AsInt());
  }
  DFS(path, 0, node);
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
  // Under the global rule every start is marked before any of them is walked, as the queue walk does when
  // it seeds the tree. Marking them one at a time instead would let an earlier start's walk reach a later
  // one and return it on that path, and the later start would then return it again as its own root -- the
  // one node, two paths. Only the global rule needs this: the path-scoped modes release their marks on
  // the way back out, so each start begins with an empty set either way.
  if (path_data_.helper_.GlobalUniqueness()) {
    for (const auto &node : path_data_.start_nodes_) {
      path_data_.visited_.insert(node.Id().AsInt());
    }
  }

  for (const auto &node : path_data_.start_nodes_) {
    if (path_data_.LimitReached()) {
      return;
    }
    StartAlgorithm(node);
  }
}

// The relationships reaching `index`, walked back to a start node and replayed forwards. Polls: one call
// is as long as the path, and on a deep chain the walk emits one per node, so without this a single
// rebuild outlives the poll at the dequeue.
mgp::Path Path::PathExpand::PathTo(const int64_t index) {
  std::vector<int64_t> chain;
  for (int64_t at = index; at >= 0; at = tree_[at].parent) {
    path_data_.MaybeAbort();
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

void Path::PathExpand::ExpandTreeEntry(const int64_t index, const int64_t depth, mgp_vertex *vertex,
                                       const bool outgoing, std::queue<int64_t> &frontier) {
  const BorrowedEdges edges{vertex, outgoing};
  for (auto *edge = edges.First(); edge != nullptr; edge = edges.Next()) {
    if (path_data_.LimitReached()) {
      return;
    }
    // As in the other walks: a fully filtered supernode never reaches the poll at the dequeue.
    path_data_.MaybeAbort();

    auto *next_vertex = outgoing ? mgp::edge_get_to(edge) : mgp::edge_get_from(edge);
    const int64_t next_id = mgp::vertex_get_id(next_vertex).as_int;
    if (path_data_.visited_.contains(next_id)) {
      continue;
    }

    if (!path_data_.helper_.RelationshipAdmitted(mgp::edge_get_type(edge).name, outgoing, depth)) {
      continue;
    }

    // Marked here rather than at the dequeue, so the first relationship to reach a node is the one that
    // keeps it -- which is what makes the tree breadth-first, and holds even for a node a filter then
    // rejects: it is spent either way.
    path_data_.visited_.insert(next_id);
    tree_.push_back(
        {.node = mgp::Node(next_vertex), .from_parent = mgp::Relationship(edge), .parent = index, .depth = depth + 1});
    frontier.push(static_cast<int64_t>(tree_.size()) - 1);
  }
}

// The node-global rule returns one path per reachable node, so the frontier is bounded by the graph
// rather than by the paths through it -- a real queue can hold it, and the tree it builds gives each
// node its parent. Every start node is seeded at depth 0 into one visited set before any of them is
// expanded, so a start reached from another start is refused there and returned once, as its own root.
// `RunAllStarts` marks them up front for the same reason.
void Path::PathExpand::RunNodeGlobalBfs() {
  std::queue<int64_t> frontier;
  // Seeding copies a node per start, and the caller's list is unbounded, so it polls rather than making
  // the first check wait until the whole list is in the tree.
  for (const auto &node : path_data_.start_nodes_) {
    if (path_data_.LimitReached()) {
      return;
    }
    path_data_.MaybeAbort();
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
    // Copied out, not bound by reference: the two ExpandTreeEntry calls below append to the tree, so the
    // first one can reallocate it out from under the handle the second one is passed.
    const mgp::Node node{tree_[index].node};

    const Evaluation evaluation = path_data_.helper_.Evaluate(node, depth);
    if (evaluation.include && path_data_.helper_.PathSizeOk(depth)) {
      Emit(PathTo(index));
      if (path_data_.LimitReached()) {
        return;
      }
    }

    if (!evaluation.expand || std::cmp_greater(depth + 1, path_data_.helper_.MaxHops())) {
      continue;
    }

    ExpandTreeEntry(index, depth, node.GetPtr(), false, frontier);
    ExpandTreeEntry(index, depth, node.GetPtr(), true, frontier);
  }
}

bool Path::PathExpand::OnBranch(const int64_t index, const int64_t key) const {
  const bool node_keyed = IsNodeUniqueness(path_data_.helper_.GetUniqueness());
  for (int64_t i = index; i != kNoParent; i = branches_[i].parent) {
    const int64_t seen = node_keyed ? branches_[i].node_id : branches_[i].relationship_id;
    if (seen == key) {
      return true;
    }
  }
  return false;
}

mgp::Path Path::PathExpand::BranchPath(const int64_t index) {
  std::vector<int64_t> chain;
  for (int64_t i = index; i != kNoParent; i = branches_[i].parent) {
    // One call is as long as the path, so the poll at the dequeue is not enough on its own.
    path_data_.MaybeAbort();
    chain.push_back(i);
  }
  std::ranges::reverse(chain);

  // Roots are seeded in start-node order before anything expands, so a root's index is its start node's.
  mgp::Path path{path_data_.start_nodes_[static_cast<size_t>(chain.front())]};
  for (size_t step = 1; step < chain.size(); ++step) {
    path.Expand(*branches_[chain[step]].from_parent);
  }
  return path;
}

void Path::PathExpand::ExpandBranch(const int64_t index, mgp_vertex *vertex, const bool outgoing,
                                    std::queue<std::pair<int64_t, mgp::Node>> &frontier) {
  // Read before the loop: pushing a branch can reallocate the vector out from under a reference.
  const int64_t depth = branches_[index].depth;
  const bool node_keyed = IsNodeUniqueness(path_data_.helper_.GetUniqueness());

  const BorrowedEdges edges{vertex, outgoing};
  for (auto *edge = edges.First(); edge != nullptr; edge = edges.Next()) {
    if (path_data_.LimitReached()) {
      return;
    }
    path_data_.MaybeAbort();

    if (!path_data_.helper_.RelationshipAdmitted(mgp::edge_get_type(edge).name, outgoing, depth)) {
      continue;
    }

    auto *next_vertex = outgoing ? mgp::edge_get_to(edge) : mgp::edge_get_from(edge);
    const int64_t next_id = mgp::vertex_get_id(next_vertex).as_int;
    const int64_t key = node_keyed ? next_id : mgp::edge_get_id(edge).as_int;
    if (OnBranch(index, key)) {
      continue;
    }

    branches_.push_back({.node_id = next_id,
                         .relationship_id = mgp::edge_get_id(edge).as_int,
                         .parent = index,
                         .depth = depth + 1,
                         .from_parent = mgp::Relationship(edge)});
    // The loop's only node copy, and only for a branch that will be followed.
    frontier.emplace(static_cast<int64_t>(branches_.size()) - 1, mgp::Node(next_vertex));
  }
}

void Path::PathExpand::RunPathScopedBfs() {
  std::queue<std::pair<int64_t, mgp::Node>> frontier;
  for (const auto &node : path_data_.start_nodes_) {
    if (path_data_.LimitReached()) {
      return;
    }
    path_data_.MaybeAbort();
    branches_.push_back({.node_id = node.Id().AsInt(),
                         .relationship_id = kNoRelationship,
                         .parent = kNoParent,
                         .depth = 0,
                         .from_parent = std::nullopt});
    frontier.emplace(static_cast<int64_t>(branches_.size()) - 1, node);
  }

  while (!frontier.empty()) {
    if (path_data_.LimitReached()) {
      return;
    }
    path_data_.MaybeAbort();

    auto entry = std::move(frontier.front());
    frontier.pop();
    const int64_t index = entry.first;
    const mgp::Node &node = entry.second;
    const int64_t depth = branches_[index].depth;

    const Evaluation evaluation = path_data_.helper_.Evaluate(node, depth);
    if (evaluation.include && path_data_.helper_.PathSizeOk(depth)) {
      Emit(BranchPath(index));
      if (path_data_.LimitReached()) {
        return;
      }
    }

    if (!evaluation.expand || std::cmp_greater(depth + 1, path_data_.helper_.MaxHops())) {
      continue;
    }

    if (path_data_.helper_.StepAdmitsDirection(depth, false)) {
      ExpandBranch(index, node.GetPtr(), false, frontier);
    }
    if (path_data_.helper_.StepAdmitsDirection(depth, true)) {
      ExpandBranch(index, node.GetPtr(), true, frontier);
    }
  }
}

// Breadth-first emits every path of one length before any longer one, which is what makes a `limit`
// return the shortest paths. Both walks below hold a tree of what they have reached rather than a
// frontier of whole paths; they differ in what may not repeat, and so in how big that tree gets.
void Path::PathExpand::RunAlgorithm() {
  // No path length satisfies both bounds.
  if (path_data_.helper_.MinHops() > path_data_.helper_.MaxHops()) {
    return;
  }
  if (!path_data_.helper_.Bfs()) {
    RunAllStarts();
    return;
  }
  if (path_data_.helper_.GlobalUniqueness()) {
    RunNodeGlobalBfs();
    return;
  }
  RunPathScopedBfs();
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

void Path::PathSubgraph::ExpandFromRelationships(const std::pair<mgp::Node, int64_t> &pair, mgp_vertex *vertex,
                                                 bool outgoing, std::queue<std::pair<mgp::Node, int64_t>> &queue) {
  const BorrowedEdges edges{vertex, outgoing};
  for (auto *edge = edges.First(); edge != nullptr; edge = edges.Next()) {
    // As in the expand walk: a fully filtered supernode never reaches the dequeue poll above.
    path_data_.MaybeAbort();

    auto *next_vertex = outgoing ? mgp::edge_get_to(edge) : mgp::edge_get_from(edge);
    const int64_t next_id = mgp::vertex_get_id(next_vertex).as_int;

    if (path_data_.visited_.contains(next_id)) {
      continue;
    }

    if (path_data_.helper_.RelationshipAdmitted(mgp::edge_get_type(edge).name, outgoing, pair.second)) {
      // Enqueue only; TryInsertNode emits it on dequeue, once the checks are applied.
      path_data_.visited_.insert(next_id);
      queue.emplace(mgp::Node(next_vertex), pair.second + 1);
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

    if (path_data_.helper_.StepAdmitsDirection(pair.second, false)) {
      this->ExpandFromRelationships(pair, pair.first.GetPtr(), false, queue);
    }
    if (path_data_.helper_.StepAdmitsDirection(pair.second, true)) {
      this->ExpandFromRelationships(pair, pair.first.GetPtr(), true, queue);
    }
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

    // Keyed by id: the membership test is the only thing asked of it, and a node key copies a vertex
    // out of storage to store and another to look up.
    std::unordered_set<int64_t> to_be_returned_nodes_searchable;

    for (const auto &node : to_be_returned_nodes) {
      to_be_returned_nodes_searchable.insert(node.ValueNode().Id().AsInt());
    }

    // A second O(nodes * degree) pass outside the walk, so it polls too.
    uint64_t abort_poll_counter = 0;
    mgp::List to_be_returned_rels;
    for (const auto &node : to_be_returned_nodes) {
      // A sink never reaches the inner poll, so a result set of them would be uninterruptible.
      PollAbort(graph, abort_poll_counter);
      const mgp::Node from = node.ValueNode();
      const BorrowedEdges edges{from.GetPtr(), true};
      for (auto *edge = edges.First(); edge != nullptr; edge = edges.Next()) {
        PollAbort(graph, abort_poll_counter);
        if (to_be_returned_nodes_searchable.contains(mgp::vertex_get_id(mgp::edge_get_to(edge)).as_int)) {
          to_be_returned_rels.AppendExtend(mgp::Value(mgp::Relationship(edge)));
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

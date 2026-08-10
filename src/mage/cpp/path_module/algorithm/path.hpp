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

#pragma once

#include <mgp.hpp>

#include <cstdint>
#include <functional>
#include <limits>
#include <queue>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace Path {

/* elements constants */
constexpr const char *kProcedureElements = "elements";
constexpr const char *kElementsArg1 = "path";

/* combine constants */
constexpr const char *kProcedureCombine = "combine";
constexpr const char *kCombineArg1 = "first";
constexpr const char *kCombineArg2 = "second";

/* slice constants */
constexpr const char *kProcedureSlice = "slice";
constexpr const char *kSliceArg1 = "path";
constexpr const char *kSliceArg2 = "offset";
constexpr const char *kSliceArg3 = "length";

/* create constants */
constexpr const char *kProcedureCreate = "create";
constexpr const char *kCreateArg1 = "start_node";
constexpr const char *kCreateArg2 = "relationships";
constexpr const char *kResultCreate = "path";

/* expand constants */
constexpr const char *kProcedureExpand = "expand";
constexpr const char *kArgumentStartExpand = "start";
constexpr const char *kArgumentRelationshipsExpand = "relationships";
constexpr const char *kArgumentLabelsExpand = "labels";
constexpr const char *kArgumentMinHopsExpand = "min_hops";
constexpr const char *kArgumentMaxHopsExpand = "max_hops";
constexpr const char *kResultExpand = "result";

/* expand_config constants */
constexpr const char *kProcedureExpandConfig = "expand_config";
constexpr const char *kArgumentConfigExpandConfig = "config";

/* subgraph_nodes constants */
constexpr const char *kReturnSubgraphNodes = "nodes";
constexpr const char *kProcedureSubgraphNodes = "subgraph_nodes";
constexpr const char *kArgumentsStart = "start_node";
constexpr const char *kArgumentsConfig = "config";
constexpr const char *kResultSubgraphNodes = "nodes";

/* subgraph_all constants */
constexpr const char *kReturnNodesSubgraphAll = "nodes";
constexpr const char *kReturnRelsSubgraphAll = "rels";
constexpr const char *kProcedureSubgraphAll = "subgraph_all";
constexpr const char *kResultNodesSubgraphAll = "nodes";
constexpr const char *kResultRelsSubgraphAll = "rels";

// Heterogeneous lookup: find by string_view without allocating a string.
struct TransparentStringHash {
  using is_transparent = void;

  [[nodiscard]] size_t operator()(std::string_view sv) const noexcept { return std::hash<std::string_view>{}(sv); }
};

// Owning: the config outlives the mgp::List the labels came from, so a view would dangle.
using LabelSet = std::unordered_set<std::string, TransparentStringHash, std::equal_to<>>;

struct LabelSets {
  LabelSet termination_list;
  LabelSet blacklist;
  LabelSet whitelist;
  LabelSet end_list;
};

struct LabelBools {
  // no node in the path will be blacklisted
  bool blacklisted = false;
  // returned paths end with a termination node but don't continue to be expanded further,
  // takes precedence over end nodes
  bool terminated = false;
  // returned paths end with an end node but continue to be expanded further
  bool end_node = false;
  // all nodes in the path will be whitelisted (except end and termination nodes)
  // end and termination nodes don't have to respect whitelists and blacklists
  bool whitelisted = false;
};

struct LabelBoolsStatus {
  // true if there is an end node -> only paths ending with it can be saved as result,
  // but they can be expanded further
  bool end_node_activated = false;
  // true if no whitelist is given -> all nodes are whitelisted
  bool whitelist_empty = false;
  // true if there is a termination node -> only paths ending with it are allowed
  bool termination_activated = false;
};

enum class RelDirection : std::int8_t { kNone = -1, kAny = 0, kIncoming = 1, kOutgoing = 2 };

// What may not repeat during a walk. The `*Path` forms forbid a repeat within the current path only,
// and are the only values the expand walk accepts: it reaches a depth by re-walking from the start, so
// anything marked for the whole traversal would block the next pass. kNodeGlobal is the subgraph walk's
// own rule, not a caller-selectable one.
enum class Uniqueness : std::uint8_t { kRelationshipPath, kNodePath, kNodeGlobal };

[[nodiscard]] constexpr bool IsNodeUniqueness(Uniqueness uniqueness) {
  return uniqueness == Uniqueness::kNodePath || uniqueness == Uniqueness::kNodeGlobal;
}

// Which procedure got the config map: the two families accept different keys.
enum class ProcedureKind : std::uint8_t { kExpand, kSubgraph };

// One filter's answer about a node. Combined with `&=`: every filter must agree to include it, and
// any one of them may stop the walk.
struct Evaluation {
  bool include = true;
  bool expand = true;

  Evaluation &operator&=(const Evaluation &other) {
    include = include && other.include;
    expand = expand && other.expand;
    return *this;
  }
};

inline constexpr int64_t kNoLimit = -1;

struct Config {
  LabelBoolsStatus label_bools_status;
  std::unordered_map<std::string, RelDirection, TransparentStringHash, std::equal_to<>> relationship_sets;
  LabelSets label_sets;
  std::unordered_set<int64_t> allowlist_nodes;
  std::unordered_set<int64_t> denylist_nodes;
  // Identity counterparts of the '>' and '/' label sets.
  std::unordered_set<int64_t> end_nodes;
  std::unordered_set<int64_t> terminator_nodes;
  int64_t min_hops = 0;
  int64_t max_hops = std::numeric_limits<int64_t>::max();
  int64_t limit = kNoLimit;
  // The depth the breadth-first driver is walking, or -1 outside it. Deliberately does not touch
  // min_hops, which the label filters read to decide whether a terminator ends the walk.
  int64_t pass_depth = -1;
  Uniqueness uniqueness = Uniqueness::kRelationshipPath;
  bool any_incoming = false;
  bool any_outgoing = false;
  bool filter_start_node = false;
  // Shortest paths first, so a `limit` returns those. Only the config form defaults to it: the
  // positional `expand` cannot express `bfs`, so it would pay the re-walk with no way to opt out.
  bool bfs = true;
};

class PathHelper {
 public:
  explicit PathHelper(const mgp::List &labels, const mgp::List &relationships, int64_t min_hops, int64_t max_hops);
  explicit PathHelper(const mgp::Map &config, const mgp::Graph &graph, ProcedureKind kind);

  RelDirection GetDirection(std::string_view rel_type) const;
  LabelBools GetLabelBools(const mgp::Node &node) const;

  bool AnyDirected(bool outgoing) const { return outgoing ? config_.any_outgoing : config_.any_incoming; }

  // Whether to return the node, and whether to walk on through it.
  [[nodiscard]] Evaluation Evaluate(const mgp::Node &node, int64_t depth) const;

  bool PathSizeOk(int64_t path_size) const;
  bool PathTooBig(int64_t path_size) const;
  bool Whitelisted(bool whitelisted) const;

  [[nodiscard]] bool Bfs() const { return config_.bfs; }

  [[nodiscard]] int64_t MinHops() const { return config_.min_hops; }

  [[nodiscard]] int64_t MaxHops() const { return config_.max_hops; }

  [[nodiscard]] int64_t Limit() const { return config_.limit; }

  [[nodiscard]] bool HasLimit() const { return config_.limit != kNoLimit; }

  [[nodiscard]] Uniqueness GetUniqueness() const { return config_.uniqueness; }

  void SetPassDepth(int64_t depth) { config_.pass_depth = depth; }

  void ClearPassDepth() { config_.pass_depth = -1; }

  // The upper hop bound, narrowed to the depth being walked.
  [[nodiscard]] int64_t ExpansionCeiling() const {
    return config_.pass_depth < 0 ? config_.max_hops : std::min(config_.max_hops, config_.pass_depth);
  }

  void FilterLabelBoolStatus();
  void FilterLabel(std::string_view label, LabelBools &label_bools) const;
  void ParseLabels(const mgp::List &list_of_labels);
  void ParseRelationships(const mgp::List &list_of_relationships);
  void AddRelationshipDirection(std::string type, RelDirection direction);
  void ParseNodeFilters(const mgp::Map &config, const mgp::Graph &graph);

 private:
  // First match wins: deny, terminator, end, allow.
  [[nodiscard]] Evaluation EvaluateLabels(const mgp::Node &node, int64_t depth) const;
  [[nodiscard]] Evaluation EvaluateEndAndTerminatorNodes(const mgp::Node &node, int64_t depth) const;
  [[nodiscard]] Evaluation EvaluateNodeLists(const mgp::Node &node, int64_t depth) const;

  [[nodiscard]] bool EndNodesOnly() const {
    return config_.label_bools_status.end_node_activated || config_.label_bools_status.termination_activated;
  }

  Config config_;
};

// A walk can run for a long time without allocating, so the memory tracker cannot stop it and only
// polling observes the timeout, TERMINATE TRANSACTIONS and shutdown. Poll every N steps so the check
// does not dominate the per-relationship test it guards.
inline constexpr uint64_t kAbortPollInterval = 64;

// Throws if the query is terminated, timed out, or the server is shutting down.
inline void PollAbort(const mgp::Graph &graph, uint64_t &poll_counter) {
  if (poll_counter++ % kAbortPollInterval == 0) {
    graph.CheckMustAbort();
  }
}

struct PathData {
  explicit PathData(PathHelper &&helper, const mgp::RecordFactory &record_factory, const mgp::Graph &graph)
      : helper_(std::move(helper)), record_factory_(record_factory), graph_(graph) {}

  void MaybeAbort() { PollAbort(graph_, abort_poll_counter_); }

  [[nodiscard]] bool LimitReached() const {
    return helper_.HasLimit() && std::cmp_greater_equal(emitted_, helper_.Limit());
  }

  // Keeps the caller's order, ignoring a repeat.
  void AddStartNode(mgp::Node node) {
    if (start_ids_.insert(node.Id().AsInt()).second) {
      start_nodes_.push_back(std::move(node));
    }
  }

  PathHelper helper_;
  const mgp::RecordFactory &record_factory_;
  const mgp::Graph &graph_;
  std::unordered_set<int64_t> visited_;
  // Ordered, not a set: `limit` stops early, so the order the caller listed decides which paths come
  // back. `start_ids_` only stops a repeated start being walked twice.
  std::vector<mgp::Node> start_nodes_;
  std::unordered_set<int64_t> start_ids_;
  uint64_t abort_poll_counter_ = 0;
  int64_t emitted_ = 0;
};

class PathExpand {
 public:
  explicit PathExpand(PathData &&path_data) : path_data_(std::move(path_data)) {}

  // The id the uniqueness rule keys on when crossing `relationship`.
  [[nodiscard]] int64_t UniquenessKey(const mgp::Relationship &relationship, bool outgoing) const;

  void ExpandPath(mgp::Path &path, const mgp::Relationship &relationship, int64_t path_size, int64_t uniqueness_key);
  void ExpandFromRelationships(mgp::Path &path, mgp::Relationships relationships, bool outgoing, int64_t path_size);
  void StartAlgorithm(const mgp::Node &node);
  void Parse(const mgp::Value &value);
  void DFS(mgp::Path &path, int64_t path_size);
  void RunAlgorithm();

 private:
  void RunAllStarts();
  void Emit(const mgp::Path &path);

  PathData path_data_;
  // Deepest path reached this pass; bounds the driver when no upper hop bound was given.
  int64_t deepest_reached_ = -1;
};

class PathSubgraph {
 public:
  explicit PathSubgraph(PathData &&path_data) : path_data_(std::move(path_data)) {}

  void ExpandFromRelationships(const std::pair<mgp::Node, int64_t> &pair, mgp::Relationships relationships,
                               bool outgoing, std::queue<std::pair<mgp::Node, int64_t>> &queue);
  void Parse(const mgp::Value &value);
  void TryInsertNode(const mgp::Node &node, int64_t hop_count, const Evaluation &evaluation);
  mgp::List BFS();

 private:
  PathData path_data_;
  mgp::List to_be_returned_nodes_;
};

void Elements(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Combine(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Slice(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Create(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void Expand(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void ExpandConfig(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void SubgraphNodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void SubgraphAll(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

}  // namespace Path

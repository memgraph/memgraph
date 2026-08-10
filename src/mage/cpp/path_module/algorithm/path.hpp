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
constexpr const std::string_view kProcedureElements = "elements";
constexpr const std::string_view kElementsArg1 = "path";

/* combine constants */
constexpr const std::string_view kProcedureCombine = "combine";
constexpr const std::string_view kCombineArg1 = "first";
constexpr const std::string_view kCombineArg2 = "second";

/* slice constants */
constexpr const std::string_view kProcedureSlice = "slice";
constexpr const std::string_view kSliceArg1 = "path";
constexpr const std::string_view kSliceArg2 = "offset";
constexpr const std::string_view kSliceArg3 = "length";

/* create constants */
constexpr const std::string_view kProcedureCreate = "create";
constexpr const std::string_view kCreateArg1 = "start_node";
constexpr const std::string_view kCreateArg2 = "relationships";
constexpr const std::string_view kResultCreate = "path";

/* expand constants */
constexpr std::string_view kProcedureExpand = "expand";
constexpr std::string_view kArgumentStartExpand = "start";
constexpr std::string_view kArgumentRelationshipsExpand = "relationships";
constexpr std::string_view kArgumentLabelsExpand = "labels";
constexpr std::string_view kArgumentMinHopsExpand = "min_hops";
constexpr std::string_view kArgumentMaxHopsExpand = "max_hops";
constexpr std::string_view kResultExpand = "result";

/* expand_config constants */
constexpr std::string_view kProcedureExpandConfig = "expand_config";
constexpr std::string_view kArgumentConfigExpandConfig = "config";

/* subgraph_nodes constants */
constexpr std::string_view kReturnSubgraphNodes = "nodes";
constexpr std::string_view kProcedureSubgraphNodes = "subgraph_nodes";
constexpr std::string_view kArgumentsStart = "start_node";
constexpr std::string_view kArgumentsConfig = "config";
constexpr std::string_view kResultSubgraphNodes = "nodes";

/* subgraph_all constants */
constexpr std::string_view kReturnNodesSubgraphAll = "nodes";
constexpr std::string_view kReturnRelsSubgraphAll = "rels";
constexpr std::string_view kProcedureSubgraphAll = "subgraph_all";
constexpr std::string_view kResultNodesSubgraphAll = "nodes";
constexpr std::string_view kResultRelsSubgraphAll = "rels";

// Enables heterogeneous string_view lookup so labels and relationship types can be looked up without allocating.
struct TransparentStringHash {
  using is_transparent = void;

  [[nodiscard]] size_t operator()(std::string_view sv) const noexcept { return std::hash<std::string_view>{}(sv); }
};

// Owning: the parsed config outlives the mgp::List the labels were read from, so a view into it would dangle.
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

enum class RelDirection { kNone = -1, kAny = 0, kIncoming = 1, kOutgoing = 2 };

// What may not repeat during a walk. Both `*Path` forms forbid a repeat within the current path only,
// so a node or relationship can still appear in other paths; what a walk marks is released again when
// it backtracks. Those two are what the expand walk offers, and the only values it accepts: it reaches
// a depth by re-walking from the start, so anything marked for the whole traversal would block the next
// pass. kNodeGlobal is the subgraph walk's own rule, not something a caller can select -- it records
// what that walk does rather than configuring it.
enum class Uniqueness { kRelationshipPath, kNodePath, kNodeGlobal };

// Whether a uniqueness mode is keyed on nodes rather than relationships.
[[nodiscard]] constexpr bool IsNodeUniqueness(Uniqueness uniqueness) {
  return uniqueness == Uniqueness::kNodePath || uniqueness == Uniqueness::kNodeGlobal;
}

// Which procedure a config map was handed to. The two families accept different keys and honour
// `bfs` differently, so a key that would silently do nothing can be rejected instead.
enum class ProcedureKind { kExpand, kSubgraph };

// One filter's answer about a node, mirroring the traversal evaluations the reference implementation
// composes: `include` decides whether the node is returned, `expand` whether the walk continues
// through it. Independent filters are combined with `&=`, i.e. every filter must agree to include,
// and any filter may stop the walk.
struct Evaluation {
  bool include = true;
  bool expand = true;

  Evaluation &operator&=(const Evaluation &other) {
    include = include && other.include;
    expand = expand && other.expand;
    return *this;
  }
};

// No upper bound on the number of emitted records.
inline constexpr int64_t kNoLimit = -1;

struct Config {
  LabelBoolsStatus label_bools_status;
  std::unordered_map<std::string, RelDirection, TransparentStringHash, std::equal_to<>> relationship_sets;
  LabelSets label_sets;
  // Node-identity filters, kept separate from the label filters they are evaluated alongside.
  std::unordered_set<int64_t> allowlist_nodes;
  std::unordered_set<int64_t> denylist_nodes;
  // Identity counterparts of the '>' and '/' label sets: end nodes are returned and expanded through,
  // terminator nodes are returned and end the walk.
  std::unordered_set<int64_t> end_nodes;
  std::unordered_set<int64_t> terminator_nodes;
  int64_t min_hops = 0;
  int64_t max_hops = std::numeric_limits<int64_t>::max();
  int64_t limit = kNoLimit;
  // The single depth the breadth-first driver is currently walking, or -1 outside it. It narrows which
  // paths are emitted and how far the walk goes, and deliberately does not touch min_hops: the label
  // filters read that to decide whether a terminator node still ends the walk.
  int64_t pass_depth = -1;
  Uniqueness uniqueness = Uniqueness::kRelationshipPath;
  bool any_incoming = false;
  bool any_outgoing = false;
  bool filter_start_node = false;
  // Emit every path of one length before any longer one, so a `limit` returns the shortest paths
  // rather than whichever branch happened to be walked first. Only the config form defaults to this:
  // the positional `expand` cannot express `bfs`, and the breadth-first driver costs a re-walk per
  // depth, so defaulting it there would slow every existing caller down with no way to opt out.
  bool bfs = true;
};

class PathHelper {
 public:
  explicit PathHelper(const mgp::List &labels, const mgp::List &relationships, int64_t min_hops, int64_t max_hops);
  explicit PathHelper(const mgp::Map &config, const mgp::Graph &graph, ProcedureKind kind);

  RelDirection GetDirection(std::string_view rel_type) const;
  LabelBools GetLabelBools(const mgp::Node &node) const;

  bool AnyDirected(bool outgoing) const { return outgoing ? config_.any_outgoing : config_.any_incoming; }

  // The whole verdict on a node at `depth`: whether to return it and whether to walk on through it.
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

  // Used to walk one depth at a time; see PathExpand::RunAlgorithm.
  void SetPassDepth(int64_t depth) { config_.pass_depth = depth; }

  void ClearPassDepth() { config_.pass_depth = -1; }

  // How deep the walk may go right now: the configured upper bound, narrowed to the depth the
  // breadth-first driver is currently walking.
  [[nodiscard]] int64_t ExpansionCeiling() const {
    return config_.pass_depth < 0 ? config_.max_hops : std::min(config_.max_hops, config_.pass_depth);
  }

  // methods for parsing config
  void FilterLabelBoolStatus();
  void FilterLabel(std::string_view label, LabelBools &label_bools) const;
  void ParseLabels(const mgp::List &list_of_labels);
  void ParseRelationships(const mgp::List &list_of_relationships);
  void AddRelationshipDirection(std::string type, RelDirection direction);
  void ParseNodeFilters(const mgp::Map &config, const mgp::Graph &graph);

 private:
  // The label filter on its own, mirroring the reference matcher's first-match-wins order.
  [[nodiscard]] Evaluation EvaluateLabels(const mgp::Node &node, int64_t depth) const;
  // The identity counterpart of the '>' / '/' label sets.
  [[nodiscard]] Evaluation EvaluateEndAndTerminatorNodes(const mgp::Node &node, int64_t depth) const;
  // allowlistNodes / denylistNodes.
  [[nodiscard]] Evaluation EvaluateNodeLists(const mgp::Node &node, int64_t depth) const;

  [[nodiscard]] bool EndNodesOnly() const {
    return config_.label_bools_status.end_node_activated || config_.label_bools_status.termination_activated;
  }

  Config config_;
};

// A traversal can run for a long time without allocating anything, so nothing else can stop it: the
// memory tracker only fires on allocation, and the query timeout, TERMINATE TRANSACTIONS and shutdown
// are only observed by a procedure that polls for them. Poll every kAbortPollInterval steps -- the
// check is a few atomic loads, which would otherwise dominate the per-relationship test it guards.
inline constexpr uint64_t kAbortPollInterval = 64;

// Throws if the query is being terminated, has timed out, or the server is shutting down.
inline void PollAbort(const mgp::Graph &graph, uint64_t &poll_counter) {
  if (poll_counter++ % kAbortPollInterval == 0) {
    graph.CheckMustAbort();
  }
}

struct PathData {
  explicit PathData(PathHelper &&helper, const mgp::RecordFactory &record_factory, const mgp::Graph &graph)
      : helper_(std::move(helper)), record_factory_(record_factory), graph_(graph) {}

  void MaybeAbort() { PollAbort(graph_, abort_poll_counter_); }

  // True once as many records have been produced as the caller asked for.
  [[nodiscard]] bool LimitReached() const {
    return helper_.HasLimit() && std::cmp_greater_equal(emitted_, helper_.Limit());
  }

  // Records a start node the first time it is named, keeping the order the caller listed them in.
  void AddStartNode(mgp::Node node) {
    if (start_ids_.insert(node.Id().AsInt()).second) {
      start_nodes_.push_back(std::move(node));
    }
  }

  PathHelper helper_;
  const mgp::RecordFactory &record_factory_;
  const mgp::Graph &graph_;
  std::unordered_set<int64_t> visited_;
  // Ordered, not a set: `limit` stops the walk early, so which start nodes were walked first decides
  // which paths come back. Hash order would make that answer depend on the ids the graph happens to
  // hold. `start_ids_` only keeps a repeated start from being walked twice.
  std::vector<mgp::Node> start_nodes_;
  std::unordered_set<int64_t> start_ids_;
  uint64_t abort_poll_counter_ = 0;
  int64_t emitted_ = 0;
};

class PathExpand {
 public:
  explicit PathExpand(PathData &&path_data) : path_data_(std::move(path_data)) {}

  // The id the uniqueness rule keys on when crossing `relationship` towards `next_node`.
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
  // Deepest path reached in the current pass; tells the breadth-first driver when it has run out of
  // depth to explore, which is what bounds it when no upper hop bound was given.
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

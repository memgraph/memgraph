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

// An unfiltered start node is exempt from the label filters: treat it as a plain whitelisted node.
inline constexpr LabelBools kExemptStart{.whitelisted = true};

struct LabelBoolsStatus {
  // true if there is an end node -> only paths ending with it can be saved as result,
  // but they can be expanded further
  bool end_node_activated = false;
  // true if no whitelist is given -> all nodes are whitelisted
  bool whitelist_empty = false;
  // true if there is a termination node -> only paths ending with it are allowed
  bool termination_activated = false;
};

enum class RelDirection { kNone = -1, kAny = 0, kIncoming = 1, kOutgoing = 2, kBoth = 3 };

struct Config {
  LabelBoolsStatus label_bools_status;
  std::unordered_map<std::string, RelDirection, TransparentStringHash, std::equal_to<>> relationship_sets;
  LabelSets label_sets;
  // Node-identity filters, kept separate from the label filters they are evaluated alongside.
  std::unordered_set<int64_t> allowlist_nodes;
  std::unordered_set<int64_t> denylist_nodes;
  int64_t min_hops = 0;
  int64_t max_hops = std::numeric_limits<int64_t>::max();
  bool any_incoming = false;
  bool any_outgoing = false;
  bool filter_start_node = false;
  bool begin_sequence_at_start = true;
  // Emit every path of one length before any longer one. The default, so a LIMIT returns the
  // shortest paths rather than whichever branch happened to be walked first.
  bool bfs = true;
};

class PathHelper {
 public:
  explicit PathHelper(const mgp::List &labels, const mgp::List &relationships, int64_t min_hops, int64_t max_hops);
  explicit PathHelper(const mgp::Map &config, const mgp::Graph &graph);

  RelDirection GetDirection(std::string_view rel_type) const;
  LabelBools GetLabelBools(const mgp::Node &node) const;

  bool AnyDirected(bool outgoing) const { return outgoing ? config_.any_outgoing : config_.any_incoming; }

  bool IsNotStartOrFiltersStartNode(bool is_start) const { return (config_.filter_start_node || !is_start); }

  bool IsNotStartOrFilterStartRel(bool is_start) const { return (config_.begin_sequence_at_start || !is_start); }

  bool AreLabelsValid(const LabelBools &label_bools) const;
  bool ContinueExpanding(const LabelBools &label_bools, size_t path_size) const;

  // The labels to judge a node's *inclusion* by. An unfiltered start node is exempt from the label
  // filters, so its own labels are not consulted. Expansion still uses the real labels.
  [[nodiscard]] const LabelBools &InclusionLabelBools(const LabelBools &label_bools, bool is_start) const {
    return IsNotStartOrFiltersStartNode(is_start) ? label_bools : kExemptStart;
  }

  // False means the node is neither returned nor expanded through.
  [[nodiscard]] bool NodeFilterAllows(const mgp::Node &node, bool is_start) const;

  bool PathSizeOk(int64_t path_size) const;
  bool PathTooBig(int64_t path_size) const;
  bool Whitelisted(bool whitelisted) const;

  [[nodiscard]] bool Bfs() const { return config_.bfs; }

  [[nodiscard]] int64_t MinHops() const { return config_.min_hops; }

  [[nodiscard]] int64_t MaxHops() const { return config_.max_hops; }

  // Used to walk one depth at a time; see PathExpand::RunAlgorithm.
  void SetHopBounds(int64_t min_hops, int64_t max_hops) {
    config_.min_hops = min_hops;
    config_.max_hops = max_hops;
  }

  // methods for parsing config
  void FilterLabelBoolStatus();
  void FilterLabel(std::string_view label, LabelBools &label_bools) const;
  void ParseLabels(const mgp::List &list_of_labels);
  void ParseRelationships(const mgp::List &list_of_relationships);
  void AddRelationshipDirection(std::string type, RelDirection direction);
  void ParseNodeFilters(const mgp::Map &config, const mgp::Graph &graph);

 private:
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

  PathHelper helper_;
  const mgp::RecordFactory &record_factory_;
  const mgp::Graph &graph_;
  std::unordered_set<int64_t> visited_;
  std::unordered_set<mgp::Node> start_nodes_;
  uint64_t abort_poll_counter_ = 0;
};

class PathExpand {
 public:
  explicit PathExpand(PathData &&path_data) : path_data_(std::move(path_data)) {}

  void ExpandPath(mgp::Path &path, const mgp::Relationship &relationship, int64_t path_size);
  void ExpandFromRelationships(mgp::Path &path, mgp::Relationships relationships, bool outgoing, int64_t path_size,
                               std::set<std::pair<std::string, int64_t>> &seen);
  void StartAlgorithm(const mgp::Node &node);
  void Parse(const mgp::Value &value);
  void DFS(mgp::Path &path, int64_t path_size);
  void RunAlgorithm();

 private:
  void RunAllStarts();

  PathData path_data_;
  // Deepest path reached in the current pass; tells the breadth-first driver when it has run out of
  // depth to explore, which is what bounds it when no upper hop bound was given.
  int64_t deepest_reached_ = -1;
};

class PathSubgraph {
 public:
  explicit PathSubgraph(PathData &&path_data) : path_data_(std::move(path_data)) {}

  void ExpandFromRelationships(const std::pair<mgp::Node, int64_t> &pair, mgp::Relationships relationships,
                               bool outgoing, std::queue<std::pair<mgp::Node, int64_t>> &queue,
                               std::set<std::pair<std::string, int64_t>> &seen);
  void Parse(const mgp::Value &value);
  void TryInsertNode(const mgp::Node &node, int64_t hop_count, const LabelBools &label_bools);
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

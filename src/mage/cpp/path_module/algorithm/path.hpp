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
#include <optional>
#include <queue>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
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
// Length sentinel: take the rest of the path. Any other negative length takes nothing.
constexpr int64_t kSliceToEnd = -1;

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

// A '*' entry matches every label, so it is a property of the category rather than a member of its set.
struct LabelWildcards {
  bool termination = false;
  bool blacklist = false;
  bool whitelist = false;
  bool end_list = false;
};

// One step of a repeating label sequence. A filter without a sequence is the single-step case.
struct LabelStep {
  LabelSets sets;
  LabelWildcards wildcards;
  // No allowlist in this step, so every label is allowed by it.
  bool whitelist_empty = true;
  bool constrains_nothing = true;
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

enum class RelDirection : std::int8_t { kNone = -1, kAny = 0, kIncoming = 1, kOutgoing = 2 };

// One step of a repeating relationship sequence, in the same single-step sense as LabelStep.
struct RelStep {
  std::unordered_map<std::string, RelDirection, TransparentStringHash, std::equal_to<>> types;
  bool any_incoming = false;
  bool any_outgoing = false;
  bool admits_incoming = false;
  bool admits_outgoing = false;
};

// What may not repeat during a walk. The `*Path` forms forbid a repeat within the current path only;
// kNodeGlobal forbids one for the whole traversal, so it returns a single path per reachable node. The
// subgraph walk uses kNodeGlobal too, and does not let the caller pick.
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
  // Both hold at least one step; a filter given without commas is that one step. The step a node or a
  // relationship is tested against is chosen by the depth it sits at, so the sequence repeats.
  std::vector<LabelStep> label_steps;
  std::vector<RelStep> rel_steps;
  // `beginSequenceAtStart:false` spends the first relationship step on the hop out of the start node
  // only, and the rest repeat from there.
  std::optional<RelStep> initial_rel_step;
  std::unordered_set<int64_t> allowlist_nodes;
  std::unordered_set<int64_t> denylist_nodes;
  // Identity counterparts of the '>' and '/' label sets.
  std::unordered_set<int64_t> end_nodes;
  std::unordered_set<int64_t> terminator_nodes;
  int64_t min_hops = 0;
  int64_t max_hops = std::numeric_limits<int64_t>::max();
  int64_t limit = kNoLimit;
  Uniqueness uniqueness = Uniqueness::kRelationshipPath;
  // An end or termination filter in any step puts every step in end-nodes-only mode, so a node a step
  // merely allowlists is walked through rather than returned.
  bool end_nodes_only = false;
  // Whether the sequence starts at the start node or one node out from it.
  bool begin_sequence_at_start = true;
  bool filter_start_node = false;
  // Shortest paths first, so a `limit` returns those. Only the config form defaults to it: the
  // positional `expand` cannot express `bfs`, so it would pay the re-walk with no way to opt out.
  bool bfs = true;
};

class PathHelper {
 public:
  explicit PathHelper(const mgp::List &labels, const mgp::List &relationships, int64_t min_hops, int64_t max_hops);
  explicit PathHelper(const mgp::Map &config, const mgp::Graph &graph, ProcedureKind kind);

  // The step a relationship out of a node at `depth` is tested against.
  [[nodiscard]] const RelStep &RelStepAt(int64_t depth) const;
  // Identifies that step, so depths that share it share a cached answer.
  [[nodiscard]] int64_t RelStepIndexAt(int64_t depth) const;
  // Whether a relationship of this type, traversed this way, may be followed under `step`.
  [[nodiscard]] bool RelationshipAdmitted(const RelStep &step, std::string_view rel_type, bool outgoing) const;

  // True when the step names no type for this direction, so the type need not be read.
  [[nodiscard]] static bool AdmitsEveryType(const RelStep &step, const bool outgoing) noexcept {
    return outgoing ? step.any_outgoing : step.any_incoming;
  }

  [[nodiscard]] bool StepAdmitsDirection(int64_t depth, bool outgoing) const;

  [[nodiscard]] static LabelBools GetLabelBools(mgp_vertex *vertex, const LabelStep &step);
  // A path-scoped walk re-enters a node once per path reaching it, and the verdict is the same each
  // time, so it is kept rather than re-read from storage.
  [[nodiscard]] LabelBools CachedLabelBools(mgp_vertex *vertex, int64_t id, int64_t step_index) const;

  // Whether to return the node, and whether to walk on through it.
  [[nodiscard]] Evaluation Evaluate(mgp_vertex *vertex, int64_t id, int64_t depth) const;

  [[nodiscard]] Evaluation Evaluate(const mgp::Node &node, int64_t depth) const {
    return Evaluate(node.GetPtr(), node.Id().AsInt(), depth);
  }

  bool PathSizeOk(int64_t path_size) const;
  bool PathTooBig(int64_t path_size) const;
  static bool Whitelisted(const LabelStep &step, bool whitelisted);

  [[nodiscard]] bool Bfs() const { return config_.bfs; }

  [[nodiscard]] int64_t MinHops() const { return config_.min_hops; }

  [[nodiscard]] int64_t MaxHops() const { return config_.max_hops; }

  [[nodiscard]] int64_t Limit() const { return config_.limit; }

  [[nodiscard]] bool HasLimit() const { return config_.limit != kNoLimit; }

  [[nodiscard]] Uniqueness GetUniqueness() const { return config_.uniqueness; }

  // Whether a mark survives the walk back out of a path, rather than being released with it.
  [[nodiscard]] bool GlobalUniqueness() const { return config_.uniqueness == Uniqueness::kNodeGlobal; }

  static void FilterLabel(std::string_view label, const LabelStep &step, LabelBools &label_bools);
  static LabelStep ParseLabelStep(const mgp::List &list_of_labels);
  static RelStep ParseRelStep(const mgp::List &list_of_relationships);
  static void AddRelationshipDirection(RelStep &step, std::string type, RelDirection direction);
  void ParseSequences(const mgp::Map &config);
  void ParseNodeFilters(const mgp::Map &config, const mgp::Graph &graph);

 private:
  // First match wins: deny, terminator, end, allow.
  [[nodiscard]] Evaluation EvaluateLabels(mgp_vertex *vertex, int64_t id, int64_t depth) const;
  [[nodiscard]] Evaluation EvaluateEndAndTerminatorNodes(int64_t id, int64_t depth) const;
  [[nodiscard]] Evaluation EvaluateNodeLists(int64_t id, int64_t depth) const;

  // The step a node at `depth` is tested against.
  [[nodiscard]] int64_t LabelStepIndexAt(int64_t depth) const;
  void SizeLabelCache();

  [[nodiscard]] bool EndNodesOnly() const { return config_.end_nodes_only; }

  Config config_;
  // One map per label step; empty under a graph-wide uniqueness rule, which visits a node once.
  mutable std::vector<std::unordered_map<int64_t, LabelBools>> label_bools_cache_;
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

// Open addressing with linear probing and a power-of-two capacity. The key is its own hash, so ids
// minted by a counter keep their order in the table and neighbouring ids share cache lines. That is
// why it beats `boost::unordered_flat_map`, which must mix the hash: do not add a mixing step here.
struct IdentityHash {
  [[nodiscard]] constexpr size_t operator()(const int64_t id) const noexcept { return static_cast<size_t>(id); }
};

template <typename Key, typename Value, typename Hash = IdentityHash>
class FlatMap {
  // A rehash moves the entries; borrowed vertex handles survive only a nothrow move.
  static_assert(std::is_nothrow_move_constructible_v<Value>);

 public:
  [[nodiscard]] Value *Find(const Key &key) noexcept {
    if (slots_.empty()) {
      return nullptr;
    }
    for (size_t slot = Start(key);; slot = Next(slot)) {
      if (!slots_[slot].has_value()) {
        return nullptr;
      }
      if (slots_[slot]->first == key) {
        return &slots_[slot]->second;
      }
    }
  }

  // Constructs the value only if the key is new. The reference is valid until the next insert.
  template <typename... Args>
  Value &Emplace(const Key &key, Args &&...args) {
    if (2 * (size_ + 1) > slots_.size()) {
      Grow();
    }
    for (size_t slot = Start(key);; slot = Next(slot)) {
      if (!slots_[slot].has_value()) {
        slots_[slot].emplace(
            std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple(std::forward<Args>(args)...));
        ++size_;
        return slots_[slot]->second;
      }
      if (slots_[slot]->first == key) {
        return slots_[slot]->second;
      }
    }
  }

  // The key must be present; throws if it is not.
  [[nodiscard]] Value &At(const Key &key) {
    Value *value = Find(key);
    if (value == nullptr) {
      throw std::out_of_range("FlatMap::At: no such key");
    }
    return *value;
  }

  [[nodiscard]] size_t Size() const noexcept { return size_; }

 private:
  using Slot = std::optional<std::pair<Key, Value>>;

  [[nodiscard]] size_t Start(const Key &key) const noexcept { return Hash{}(key) & (slots_.size() - 1U); }

  [[nodiscard]] size_t Next(const size_t slot) const noexcept { return (slot + 1U) & (slots_.size() - 1U); }

  void Grow() {
    std::vector<Slot> bigger(slots_.empty() ? kInitialSlots : slots_.size() * 2U);
    for (Slot &slot : slots_) {
      if (!slot.has_value()) {
        continue;
      }
      size_t target = Hash{}(slot->first) & (bigger.size() - 1U);
      while (bigger[target].has_value()) {
        target = (target + 1U) & (bigger.size() - 1U);
      }
      bigger[target] = std::move(slot);
    }
    slots_ = std::move(bigger);
  }

  static constexpr size_t kInitialSlots = 1U << 10U;

  std::vector<Slot> slots_;
  size_t size_ = 0;
};

class PathExpand {
 public:
  explicit PathExpand(PathData &&path_data) : path_data_(std::move(path_data)) {}

  void ExpandPath(mgp::Path &path, const mgp::Relationship &relationship, int64_t path_size, int64_t uniqueness_key,
                  const mgp::Node &next_node);
  void ExpandFromRelationships(mgp::Path &path, mgp_vertex *vertex, bool outgoing, int64_t path_size);
  void StartAlgorithm(const mgp::Node &node);
  void Parse(const mgp::Value &value);
  // Takes the node the path now ends at: the caller has just built it to key uniqueness on.
  void DFS(mgp::Path &path, int64_t path_size, const mgp::Node &node);
  void RunAlgorithm();

 private:
  void RunAllStarts();
  void Emit(const mgp::Path &path);

  // One node of the breadth-first tree the node-global walk builds: the node, the relationship that
  // reached it, and where that came from. Holding the tree rather than a frontier of paths keeps the
  // walk linear in the graph -- under this rule each node is reached exactly once, so it has one parent.
  struct TreeEntry {
    mgp::Node node;
    std::optional<mgp::Relationship> from_parent;
    int64_t parent;
    int64_t depth;
  };

  // One partial path of the path-scoped breadth-first walk. A node can sit on many paths at once, so
  // unlike TreeEntry there is one per partial path rather than per node -- hence ids, not accessors.
  struct Branch {
    int64_t node_id;
    int64_t relationship_id;  // kNoRelationship on a start node
    int64_t parent;           // index into branches_, kNoParent on a start node
    int64_t depth;
    // The keys on this branch's path, one bit per `key & 63`, keyed as OnBranch compares.
    uint64_t key_bits;
  };

  static constexpr uint64_t KeyBit(const int64_t key) noexcept {
    return uint64_t{1} << (static_cast<uint64_t>(key) & 63U);
  }

  static constexpr int64_t kNoParent = -1;
  static constexpr int64_t kNoRelationship = std::numeric_limits<int64_t>::min();

  // An admitted relationship and the node it leads to; `next_vertex` is owned by `nodes_`.
  struct AdmittedEdge {
    int64_t next_id;
    int64_t relationship_id;
    mgp_vertex *next_vertex;
  };

  // What an adjacency admits depends on the node, the step and the direction, never on the path.
  struct NeighbourhoodKey {
    int64_t node_id;
    int64_t step_index;
    bool outgoing;
    bool operator==(const NeighbourhoodKey &other) const = default;
  };

  struct NeighbourhoodHash {
    size_t operator()(const NeighbourhoodKey &key) const noexcept {
      size_t hash = std::hash<int64_t>{}(key.node_id);
      hash ^= std::hash<int64_t>{}(key.step_index) + 0x9e3779b9UL + (hash << 6U) + (hash >> 2U);
      return hash ^ static_cast<size_t>(key.outgoing);
    }
  };

  void RunPathScopedBfs();
  void ExpandBranch(int64_t index, mgp_vertex *vertex, bool outgoing);
  // Emits the neighbours at the hop bound without making them branches.
  void EmitTerminalNeighbours(int64_t index, mgp_vertex *vertex, bool outgoing, int64_t depth);
  // Emits the path reaching `parent` extended by one relationship.
  void EmitChildOf(int64_t parent, int64_t relationship_id);
  // Stores the answer on its second ask. The reference is invalidated by the next call.
  [[nodiscard]] const std::vector<AdmittedEdge> &AdmittedNeighbours(int64_t node_id, mgp_vertex *vertex, bool outgoing,
                                                                    int64_t depth);
  // One bit per key. A collision only stores an entry one ask early.
  [[nodiscard]] bool AskedBefore(size_t hash);
  // Walks the parent chain rather than a visited set: the rule is scoped to this path, not the walk.
  [[nodiscard]] bool OnBranch(int64_t index, int64_t key) const;
  // Rebuilds the path a branch stands for. Only emitted branches pay for it.
  [[nodiscard]] mgp::Path BranchPath(int64_t index);
  // Emits a branch's path, keeping its parent's path for the next sibling.
  void EmitBranch(int64_t index);

  void RunNodeGlobalBfs();
  void ExpandTreeEntry(int64_t index, int64_t depth, mgp_vertex *vertex, bool outgoing, std::queue<int64_t> &frontier);
  // Not const: it polls the abort signal, which advances the poll counter.
  [[nodiscard]] mgp::Path PathTo(int64_t index);

  PathData path_data_;
  std::vector<TreeEntry> tree_;
  std::vector<Branch> branches_;
  // Filter verdicts for this level's branches and for the next level's, asked once per branch.
  std::vector<Evaluation> verdicts_;
  std::vector<Evaluation> next_verdicts_;
  // One copy of each node and relationship reached; branches hold only ids.
  FlatMap<int64_t, mgp::Node> nodes_;
  FlatMap<int64_t, mgp::Relationship> relationships_;
  // The admitted adjacency per (node, step, direction), shared by every branch that reaches it.
  FlatMap<NeighbourhoodKey, std::vector<AdmittedEdge>, NeighbourhoodHash> admitted_;
  // Neighbourhoods asked for once. A first ask is answered from `scratch_` and only recorded here.
  // The filter doubles with the walk and clears in place at the cap, so it never fills.
  static constexpr size_t kMinAskedBits = size_t{1} << 16U;
  static constexpr size_t kMaxAskedBits = size_t{1} << 26U;
  static constexpr size_t kAskedBitsPerKey = 8U;
  std::vector<uint64_t> asked_;
  size_t asked_set_ = 0;
  std::vector<AdmittedEdge> scratch_;
  // The path of the last parent emitted from. Siblings are emitted one after another, so they reuse it.
  std::optional<mgp::Path> emitted_prefix_;
  int64_t emitted_prefix_parent_ = kNoParent;
};

class PathSubgraph {
 public:
  explicit PathSubgraph(PathData &&path_data) : path_data_(std::move(path_data)) {}

  void ExpandFromRelationships(const std::pair<mgp::Node, int64_t> &pair, mgp_vertex *vertex, bool outgoing,
                               std::queue<std::pair<mgp::Node, int64_t>> &queue);
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

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

#include <algorithm>
#include <array>
#include <limits>
#include <map>
#include <optional>
#include <queue>
#include <random>

#include "gtest/gtest.h"

#include "auth/models.hpp"
#include "glue/auth_checker.hpp"
#include "query/context.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"
#include "query_common.hpp"
#include "query_plan_common.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "utils/join_vector.hpp"
#include "utils/logging.hpp"

#include "formatters.hpp"

namespace memgraph::query {
void PrintTo(const memgraph::query::EdgeAtom::Direction &dir, std::ostream *os) {
  switch (dir) {
    case memgraph::query::EdgeAtom::Direction::IN:
      *os << "IN";
      break;
    case memgraph::query::EdgeAtom::Direction::OUT:
      *os << "OUT";
      break;
    case memgraph::query::EdgeAtom::Direction::BOTH:
      *os << "BOTH";
      break;
  }
}
}  // namespace memgraph::query

const auto kVertexCount = 6;
// Maps vertices to workers
const std::vector<int> kVertexLocations = {0, 1, 1, 0, 2, 2};
// Edge list in form of (from, to, edge_type).
const std::vector<std::tuple<int, int, std::string>> kEdges = {{0, 1, "a"},
                                                               {1, 2, "b"},
                                                               {2, 4, "b"},
                                                               {2, 5, "a"},
                                                               {4, 1, "a"},
                                                               {4, 5, "a"},
                                                               {5, 3, "b"},
                                                               {5, 4, "a"},
                                                               {5, 5, "b"}};

// A 7-rung ladder: two rails of 7 vertices, joined at every step. It gives long paths and many
// equal-length alternatives, so an accepted path's deviation index reaches 10 here where it never
// passes 3 on the graph above. Depth is all it buys: measured over every pair, a trie node here
// holds at most 3 children against the graph above's 4, so it widens the deviation range under
// test and not the branching. No parallel edges and no self-loops, because the brute-force oracle
// keys edges by (from, to) and could not match a graph that had them.
const std::vector<int> kLadderVertexLocations = {0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1};
const std::vector<std::tuple<int, int, std::string>> kLadderEdges = {{0, 1, "a"},
                                                                     {1, 2, "a"},
                                                                     {2, 3, "a"},
                                                                     {3, 4, "a"},
                                                                     {4, 5, "a"},
                                                                     {5, 6, "a"},  // one rail
                                                                     {7, 8, "a"},
                                                                     {8, 9, "a"},
                                                                     {9, 10, "a"},
                                                                     {10, 11, "a"},
                                                                     {11, 12, "a"},
                                                                     {12, 13, "a"},  // the other
                                                                     {0, 7, "b"},
                                                                     {1, 8, "b"},
                                                                     {2, 9, "b"},
                                                                     {3, 10, "b"},
                                                                     {4, 11, "b"},
                                                                     {5, 12, "b"},
                                                                     {6, 13, "b"}};  // the rungs

// Depth is not the only thing a skipped deviation needs to show up. On the ladder every deviation
// root stays productive, so a driver that stops deviating once the index is deep still reaches
// every path. Here the roots interleave - a path accepted at a deep index still has a shorter
// sibling waiting at a shallower one - and three of the pairs below lose a path when that root is
// skipped. No parallel edges and no self-loops, for the same oracle reason as the ladder.
const std::vector<int> kInterleavedVertexLocations = {0, 0, 1, 1, 2, 2, 0, 1};
const std::vector<std::tuple<int, int, std::string>> kInterleavedEdges = {
    {0, 4, "a"}, {0, 5, "a"}, {4, 3, "a"}, {1, 6, "a"}, {5, 4, "a"}, {5, 7, "a"}, {6, 7, "a"}, {3, 5, "a"}};

// Filters input edge list by edge type and direction and returns a list of
// pairs representing valid directed edges.
std::vector<std::pair<int, int>> GetEdgeList(const std::vector<std::tuple<int, int, std::string>> &edges,
                                             memgraph::query::EdgeAtom::Direction dir,
                                             const std::vector<std::string> &edge_types) {
  std::vector<std::pair<int, int>> ret;
  for (const auto &e : edges) {
    if (edge_types.empty() || std::ranges::contains(edge_types, std::get<2>(e)))
      ret.emplace_back(std::get<0>(e), std::get<1>(e));
  }
  switch (dir) {
    case memgraph::query::EdgeAtom::Direction::OUT:
      break;
    case memgraph::query::EdgeAtom::Direction::IN:
      for (auto &e : ret) std::swap(e.first, e.second);
      break;
    case memgraph::query::EdgeAtom::Direction::BOTH:
      auto ret_copy = ret;
      for (const auto &e : ret_copy) {
        ret.emplace_back(e.second, e.first);
      }
      break;
  }
  return ret;
}

// Yen's algorithm for finding k-shortest paths. Given a graph, returns all
// shortest paths between source and target up to k paths.
std::vector<std::vector<int>> YenKShortestPaths(int num_vertices, const std::vector<std::pair<int, int>> &edges,
                                                int source, int target) {
  spdlog::info("YenKShortestPaths: source={}, target={}, edges_count={}", source, target, edges.size());

  // For simplicity, we'll use a basic implementation that finds all paths
  // and sorts them by length. In a real implementation, this would be
  // Yen's algorithm with Dijkstra's as the base.

  std::vector<std::vector<int>> all_paths;

  // Simple BFS to find all paths (this is a simplified version)
  std::queue<std::vector<int>> q;
  q.push({source});

  while (!q.empty()) {
    auto path = q.front();
    q.pop();

    int current = path.back();

    if (current == target && path.size() > 1) {
      all_paths.push_back(path);
      continue;
    }

    // Find all neighbors
    for (const auto &edge : edges) {
      if (edge.first == current) {
        int next = edge.second;
        // Avoid cycles
        if (std::find(path.begin(), path.end(), next) == path.end()) {
          auto new_path = path;
          new_path.push_back(next);
          q.push(new_path);
        }
      }
    }
  }

  // Sort by path length
  std::sort(all_paths.begin(), all_paths.end(), [](const std::vector<int> &a, const std::vector<int> &b) {
    return a.size() < b.size();
  });

  spdlog::info("YenKShortestPaths: Found {} paths total", all_paths.size());
  for (size_t i = 0; i < all_paths.size(); ++i) {
    spdlog::info("YenKShortestPaths: Path {}: length={}, vertices={}",
                 i,
                 all_paths[i].size() - 1,
                 fmt::format("{}", memgraph::utils::JoinVector(all_paths[i], "->")));
  }

  return all_paths;
}

// --- weighted KSHORTEST ---------------------------------------------------------------------------

// The weight `BuildGraph` stores on the edge between `a` and `b`. Symmetric in its two arguments so
// the oracle can read it off a traversal pair without knowing which way the edge is stored, and
// spread wide enough that the weighted order really does depart from the hop-count one - 28 times
// over the default fixture, which `KShortestWeightedTestOrderDiffersFromHopCount` insists on.
inline int64_t EdgeWeightFor(int a, int b) {
  static constexpr std::array<int64_t, 5> kWeights{11, 7, 5, 1, 9};
  return kWeights[static_cast<size_t>((std::min(a, b) * 7 + std::max(a, b) * 3) % 5)];
}

struct WeightedPath {
  std::vector<int> vertices;
  int64_t weight;

  size_t hops() const { return vertices.size() - 1; }
};

// Every simple path from source to target, in the `(total weight, hop count)` order the weighted
// expansion promises. Brute force over the edge list, so it shares nothing with Yen's or with the
// operator's own search; the vertex sequence breaks remaining ties only to make this a stable set.
inline std::vector<WeightedPath> WeightedSimplePaths(const std::vector<std::pair<int, int>> &edges, int source,
                                                     int target) {
  std::vector<WeightedPath> out;
  std::queue<WeightedPath> q;
  q.push(WeightedPath{{source}, 0});

  while (!q.empty()) {
    auto path = q.front();
    q.pop();
    const int current = path.vertices.back();
    if (current == target && path.vertices.size() > 1) {
      out.push_back(std::move(path));
      continue;
    }
    for (const auto &[from, to] : edges) {
      if (from != current) continue;
      if (std::ranges::contains(path.vertices, to)) continue;
      auto next = path;
      next.vertices.push_back(to);
      next.weight += EdgeWeightFor(from, to);
      q.push(std::move(next));
    }
  }

  std::ranges::sort(out, [](const WeightedPath &a, const WeightedPath &b) {
    return std::tuple{a.weight, a.hops(), a.vertices} < std::tuple{b.weight, b.hops(), b.vertices};
  });
  return out;
}

// A pseudo-random simple digraph: no self-loops and no parallel edges, because the oracle keys an
// edge by `(from, to)` and could not match a graph that had either.
inline std::pair<std::vector<int>, std::vector<std::tuple<int, int, std::string>>> RandomKShortestGraph(
    uint32_t seed, int vertex_count, double density) {
  std::mt19937 rng(seed);
  std::uniform_real_distribution<double> coin(0.0, 1.0);
  std::vector<std::tuple<int, int, std::string>> edges;
  for (int from = 0; from < vertex_count; ++from) {
    for (int to = 0; to < vertex_count; ++to) {
      if (from == to) continue;
      if (coin(rng) < density) edges.emplace_back(from, to, (from + to) % 2 == 0 ? "a" : "b");
    }
  }
  return {std::vector<int>(static_cast<size_t>(vertex_count), 0), edges};
}

class Yield : public memgraph::query::plan::LogicalOperator {
 public:
  Yield(const std::shared_ptr<memgraph::query::plan::LogicalOperator> &input,
        const std::vector<memgraph::query::Symbol> &modified_symbols,
        const std::vector<std::vector<memgraph::query::TypedValue>> &values)
      : input_(input ? input : std::make_shared<memgraph::query::plan::Once>()),
        modified_symbols_(modified_symbols),
        values_(values) {}

  memgraph::query::plan::UniqueCursorPtr MakeCursor(
      memgraph::utils::MemoryResource *mem, memgraph::metrics::DatabaseMetricHandles &metric_handles) const override {
    return memgraph::query::plan::MakeUniqueCursorPtr<YieldCursor>(mem, this, input_->MakeCursor(mem, metric_handles));
  }

  std::vector<memgraph::query::Symbol> ModifiedSymbols(const memgraph::query::SymbolTable &) const override {
    return modified_symbols_;
  }

  bool HasSingleInput() const override { return true; }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input() const override { return input_; }

  void set_input(std::shared_ptr<memgraph::query::plan::LogicalOperator> input) override { input_ = input; }

  bool Accept(memgraph::query::plan::HierarchicalLogicalOperatorVisitor &) override {
    LOG_FATAL("Please go away, visitor!");
  }

  std::unique_ptr<LogicalOperator> Clone(memgraph::query::AstStorage *storage) const override {
    LOG_FATAL("Don't clone Yield operator!");
  }

  std::shared_ptr<memgraph::query::plan::LogicalOperator> input_;
  std::vector<memgraph::query::Symbol> modified_symbols_;
  std::vector<std::vector<memgraph::query::TypedValue>> values_;

  class YieldCursor : public memgraph::query::plan::Cursor {
   public:
    YieldCursor(const Yield *self, memgraph::query::plan::UniqueCursorPtr input_cursor)
        : self_(self), input_cursor_(std::move(input_cursor)), pull_index_(self_->values_.size()) {}

    bool Pull(memgraph::query::Frame &frame, memgraph::query::ExecutionContext &context) override {
      if (pull_index_ == self_->values_.size()) {
        if (!input_cursor_->Pull(frame, context)) return false;
        pull_index_ = 0;
      }
      for (size_t i = 0; i < self_->values_[pull_index_].size(); ++i) {
        auto frame_writer = memgraph::query::FrameWriter(frame, nullptr, context.evaluation_context.memory);
        frame_writer.Write(self_->modified_symbols_[i], self_->values_[pull_index_][i]);
      }
      pull_index_++;
      return true;
    }

    void Reset() override {
      input_cursor_->Reset();
      pull_index_ = self_->values_.size();
    }

    void Shutdown() override {}

   private:
    const Yield *self_;
    memgraph::query::plan::UniqueCursorPtr input_cursor_;
    size_t pull_index_;
  };
};

std::vector<std::vector<memgraph::query::TypedValue>> PullResults(memgraph::query::plan::LogicalOperator *last_op,
                                                                  memgraph::query::ExecutionContext *context,
                                                                  std::vector<memgraph::query::Symbol> output_symbols) {
  memgraph::metrics::DatabaseMetricHandles handles;
  auto cursor = last_op->MakeCursor(memgraph::utils::NewDeleteResource(), handles);
  std::vector<std::vector<memgraph::query::TypedValue>> output;
  {
    memgraph::query::Frame frame(context->symbol_table.max_position());
    while (cursor->Pull(frame, *context)) {
      output.emplace_back();
      for (const auto &symbol : output_symbols) {
        output.back().push_back(frame[symbol]);
      }
    }
  }
  return output;
}

enum class FineGrainedTestType {
  ALL_GRANTED,
  ALL_DENIED,
  EDGE_TYPE_A_DENIED,
  EDGE_TYPE_B_DENIED,
  LABEL_0_DENIED,
  LABEL_3_DENIED
};

/* Various types of filter lambdas, mirroring `bfs_common.hpp`.
 * NONE           - No filter lambda used.
 * USE_FRAME      - Block a single edge or vertex read off the frame.
 * USE_FRAME_NULL - Same, but the lambda returns null instead of false.
 * USE_CTX        - Block vertex #5 by comparing its ID to a query parameter.
 * ERROR          - Lambda evaluating to an integer instead of null or boolean.
 */
enum class FilterLambdaType { NONE, USE_FRAME, USE_FRAME_NULL, USE_CTX, ERROR };

// Returns an operator that yields vertices given by their address.
std::unique_ptr<memgraph::query::plan::LogicalOperator> YieldVertices(
    memgraph::query::DbAccessor *dba, std::vector<memgraph::query::VertexAccessor> vertices,
    memgraph::query::Symbol symbol, std::shared_ptr<memgraph::query::plan::LogicalOperator> input_op) {
  std::vector<std::vector<memgraph::query::TypedValue>> frames;
  for (const auto &vertex : vertices) {
    frames.emplace_back(std::vector<memgraph::query::TypedValue>{memgraph::query::TypedValue(vertex)});
  }
  return std::make_unique<Yield>(input_op, std::vector<memgraph::query::Symbol>{symbol}, frames);
}

// Returns an operator that yields edges and vertices given by their address.
std::unique_ptr<memgraph::query::plan::LogicalOperator> YieldEntities(
    memgraph::query::DbAccessor *dba, std::vector<memgraph::query::VertexAccessor> vertices,
    std::vector<memgraph::query::EdgeAccessor> edges, memgraph::query::Symbol symbol,
    std::shared_ptr<memgraph::query::plan::LogicalOperator> input_op) {
  std::vector<std::vector<memgraph::query::TypedValue>> frames;
  for (const auto &vertex : vertices) {
    frames.emplace_back(std::vector<memgraph::query::TypedValue>{memgraph::query::TypedValue(vertex)});
  }
  for (const auto &edge : edges) {
    frames.emplace_back(std::vector<memgraph::query::TypedValue>{memgraph::query::TypedValue(edge)});
  }
  return std::make_unique<Yield>(input_op, std::vector<memgraph::query::Symbol>{symbol}, frames);
}

template <class TRecord>
auto GetProp(const TRecord &rec, std::string prop, memgraph::query::DbAccessor *dba) {
  return *rec.GetProperty(memgraph::storage::View::OLD, dba->NameToProperty(prop));
}

// Removes everything the lambda blocks, so the Yen oracle runs on the subgraph the expansion sees.
// Matches a blocked edge by `(from, to)`, not gid, so keep `kEdges` free of parallel edges.
std::vector<std::pair<int, int>> GetFilteredEdgeList(const memgraph::query::TypedValue &blocked,
                                                     memgraph::query::EdgeAtom::Direction direction,
                                                     const std::vector<std::string> &edge_types,
                                                     memgraph::query::DbAccessor *dba,
                                                     const std::vector<std::tuple<int, int, std::string>> &all_edges) {
  auto edges = all_edges;
  // An edge is blocked in both directions, so drop it before accounting for direction.
  if (blocked.IsEdge()) {
    int from = GetProp(blocked.ValueEdge(), "from", dba).ValueInt();
    int to = GetProp(blocked.ValueEdge(), "to", dba).ValueInt();
    std::erase_if(edges, [from, to](const auto &e) { return std::get<0>(e) == from && std::get<1>(e) == to; });
  }

  auto edge_list = GetEdgeList(edges, direction, edge_types);

  if (blocked.IsVertex()) {
    int id = GetProp(blocked.ValueVertex(), "id", dba).ValueInt();
    std::erase_if(edge_list, [id](const auto &e) { return e.second == id; });
  }
  return edge_list;
}

// Checks if the given path is actually a path from source to sink and if all
// of its edges exist in the given edge list. Also ensures no edge is reused and
// no vertex is revisited: KSHORTEST answers with loopless paths, and the
// inner search's root-vertex blocking is what enforces that.
template <class TPathAllocator>
void CheckPath(memgraph::query::DbAccessor *dba, const memgraph::query::VertexAccessor &source,
               const memgraph::query::VertexAccessor &sink,
               const std::vector<memgraph::query::TypedValue, TPathAllocator> &path,
               const std::vector<std::pair<int, int>> &edges) {
  if (path.empty()) return;
  memgraph::query::VertexAccessor curr = source;
  std::unordered_set<memgraph::storage::Gid> used_edges;
  std::unordered_set<memgraph::storage::Gid> visited_vertices{source.Gid()};

  for (const auto &edge_tv : path) {
    ASSERT_TRUE(edge_tv.IsEdge());
    auto edge = edge_tv.ValueEdge();

    // Check that this edge hasn't been used before
    ASSERT_TRUE(used_edges.insert(edge.Gid()).second) << "Edge " << edge.Gid() << " is reused in path";

    ASSERT_TRUE(edge.From() == curr || edge.To() == curr);
    auto next = edge.From() == curr ? edge.To() : edge.From();

    ASSERT_TRUE(visited_vertices.insert(next.Gid()).second)
        << "Vertex " << next.Gid() << " is revisited in path, which is not loopless";

    int from = GetProp(curr, "id", dba).ValueInt();
    int to = GetProp(next, "id", dba).ValueInt();
    ASSERT_TRUE(std::ranges::contains(edges, std::make_pair(from, to)))
        << "Edge " << from << "->" << to << " not found in edge list";

    curr = next;
  }
  ASSERT_EQ(curr, sink);
}

// The vertex ids a row's path visits, from the source. Comparable across separately built copies
// of the fixture, unlike gids.
std::vector<int> PathVertexIds(memgraph::query::DbAccessor *dba, const std::vector<memgraph::query::TypedValue> &row) {
  auto curr = row[0].ValueVertex();
  std::vector<int> ids{static_cast<int>(GetProp(curr, "id", dba).ValueInt())};
  for (const auto &edge_tv : row[2].ValueList()) {
    auto edge = edge_tv.ValueEdge();
    curr = edge.From() == curr ? edge.To() : edge.From();
    ids.push_back(static_cast<int>(GetProp(curr, "id", dba).ValueInt()));
  }
  return ids;
}

// Given a list of k-shortest path results of form (from, to, path),
// checks if all paths are valid and returns the path lengths.
std::vector<int> CheckPathsAndExtractLengths(memgraph::query::DbAccessor *dba,
                                             const std::vector<std::pair<int, int>> edges,
                                             const std::vector<std::vector<memgraph::query::TypedValue>> &results) {
  std::vector<int> lengths;

  for (const auto &row : results) {
    lengths.push_back(row[2].ValueList().size());
    CheckPath(dba, row[0].ValueVertex(), row[1].ValueVertex(), row[2].ValueList(), edges);
  }

  spdlog::info("lengths: {}", memgraph::utils::JoinVector(lengths, ", "));
  return lengths;
}

// Mirrors the order `YieldEntities` emits, so the oracle can enumerate the same blocked entities.
void AppendEntities(const std::vector<memgraph::query::VertexAccessor> &vertices,
                    const std::vector<memgraph::query::EdgeAccessor> &edges,
                    std::vector<memgraph::query::TypedValue> &out) {
  for (const auto &vertex : vertices) out.emplace_back(vertex);
  for (const auto &edge : edges) out.emplace_back(edge);
}

// Applies one arm's grants and denials to `user` and answers with the arcs a reader is left with,
// trimming `edge_types` where the arm denies one outright.
// The arcs an arm leaves readable to the weighted search: the edge type must be readable, and so
// must the vertex the arc reaches - the endpoint that search checks whichever way it walks the arc.
// Spelled out rather than taken from `ApplyFineGrainedArm`, whose `edges_in_result` models only the
// vertex each arm is named for while the label arms in fact leave vertex 5 ungranted as well.
inline std::vector<std::pair<int, int>> WeightedReadableArcs(FineGrainedTestType fine_grained_test_type,
                                                             memgraph::query::EdgeAtom::Direction direction) {
  auto readable = [&](const std::vector<std::string> &types, const std::vector<int> &unreadable) {
    auto arcs = GetEdgeList(kEdges, direction, types);
    std::erase_if(arcs, [&](const auto &arc) { return std::ranges::contains(unreadable, arc.second); });
    return arcs;
  };
  switch (fine_grained_test_type) {
    case FineGrainedTestType::ALL_GRANTED:
      return readable({"a", "b"}, {});
    case FineGrainedTestType::ALL_DENIED:
      return {};
    case FineGrainedTestType::EDGE_TYPE_A_DENIED:
      return readable({"b"}, {});
    case FineGrainedTestType::EDGE_TYPE_B_DENIED:
      return readable({"a"}, {});
    case FineGrainedTestType::LABEL_0_DENIED:
      return readable({"a", "b"}, {0, 5});
    case FineGrainedTestType::LABEL_3_DENIED:
      return readable({"a", "b"}, {3, 5});
  }
}

#ifdef MG_ENTERPRISE
inline std::vector<std::pair<int, int>> ApplyFineGrainedArm(memgraph::auth::User &user,
                                                            FineGrainedTestType fine_grained_test_type,
                                                            memgraph::query::EdgeAtom::Direction direction,
                                                            std::vector<std::string> &edge_types) {
  std::vector<std::pair<int, int>> edges_in_result;
  switch (fine_grained_test_type) {
    case FineGrainedTestType::ALL_GRANTED:
      user.fine_grained_access_handler().label_permissions().GrantGlobal(memgraph::auth::FineGrainedPermission::READ);
      user.fine_grained_access_handler().edge_type_permissions().GrantGlobal(
          memgraph::auth::FineGrainedPermission::READ);
      edges_in_result = GetEdgeList(kEdges, direction, {"a", "b"});
      break;
    case FineGrainedTestType::ALL_DENIED:
      break;
    case FineGrainedTestType::EDGE_TYPE_A_DENIED:
      user.fine_grained_access_handler().label_permissions().GrantGlobal(memgraph::auth::FineGrainedPermission::READ);
      user.fine_grained_access_handler().edge_type_permissions().Grant({"b"},
                                                                       memgraph::auth::FineGrainedPermission::READ);
      user.fine_grained_access_handler().edge_type_permissions().Deny({"a"}, memgraph::auth::kAllEdgeTypePermissions);

      edges_in_result = GetEdgeList(kEdges, direction, {"b"});
      edge_types.erase(std::remove(edge_types.begin(), edge_types.end(), "a"), edge_types.end());
      break;
    case FineGrainedTestType::EDGE_TYPE_B_DENIED:
      user.fine_grained_access_handler().label_permissions().GrantGlobal(memgraph::auth::FineGrainedPermission::READ);
      user.fine_grained_access_handler().edge_type_permissions().Grant({"a"},
                                                                       memgraph::auth::FineGrainedPermission::READ);
      user.fine_grained_access_handler().edge_type_permissions().Deny({"b"}, memgraph::auth::kAllEdgeTypePermissions);

      edges_in_result = GetEdgeList(kEdges, direction, {"a"});
      edge_types.erase(std::remove(edge_types.begin(), edge_types.end(), "b"), edge_types.end());
      break;
    case FineGrainedTestType::LABEL_0_DENIED:
      // Vertex 5 is not granted here either, so this arm denies two vertices rather than the one it
      // is named for. Left alone because the assertions below are built around it; the weighted arm
      // spells the real set out in `WeightedReadableArcs`.
      user.fine_grained_access_handler().edge_type_permissions().GrantGlobal(
          memgraph::auth::FineGrainedPermission::READ);
      user.fine_grained_access_handler().label_permissions().Grant({"1"}, memgraph::auth::FineGrainedPermission::READ);
      user.fine_grained_access_handler().label_permissions().Grant({"2"}, memgraph::auth::FineGrainedPermission::READ);
      user.fine_grained_access_handler().label_permissions().Grant({"3"}, memgraph::auth::FineGrainedPermission::READ);
      user.fine_grained_access_handler().label_permissions().Grant({"4"}, memgraph::auth::FineGrainedPermission::READ);
      user.fine_grained_access_handler().label_permissions().Deny({"0"}, memgraph::auth::kAllLabelPermissions);

      edges_in_result = GetEdgeList(kEdges, direction, {"a", "b"});
      edges_in_result.erase(
          std::remove_if(edges_in_result.begin(), edges_in_result.end(), [](const auto &e) { return e.second == 0; }),
          edges_in_result.end());
      break;
    case FineGrainedTestType::LABEL_3_DENIED:
      // As above: vertex 5 goes ungranted here too.
      user.fine_grained_access_handler().edge_type_permissions().GrantGlobal(
          memgraph::auth::FineGrainedPermission::READ);
      user.fine_grained_access_handler().label_permissions().Grant({"0"}, memgraph::auth::FineGrainedPermission::READ);
      user.fine_grained_access_handler().label_permissions().Grant({"1"}, memgraph::auth::FineGrainedPermission::READ);
      user.fine_grained_access_handler().label_permissions().Grant({"2"}, memgraph::auth::FineGrainedPermission::READ);
      user.fine_grained_access_handler().label_permissions().Grant({"4"}, memgraph::auth::FineGrainedPermission::READ);
      user.fine_grained_access_handler().label_permissions().Deny({"3"}, memgraph::auth::kAllLabelPermissions);

      edges_in_result = GetEdgeList(kEdges, direction, {"a", "b"});
      edges_in_result.erase(
          std::remove_if(edges_in_result.begin(), edges_in_result.end(), [](const auto &e) { return e.second == 3; }),
          edges_in_result.end());
      break;
  }

  return edges_in_result;
}

#endif

// Common interface for single-node and distributed Memgraph.
class Database {
 public:
  virtual std::unique_ptr<memgraph::storage::Storage::Accessor> Access() = 0;
  virtual std::unique_ptr<memgraph::query::plan::LogicalOperator> MakeKShortestOperator(
      memgraph::query::Symbol source_sym, memgraph::query::Symbol sink_sym, memgraph::query::Symbol edge_sym,
      memgraph::query::EdgeAtom::Direction direction, const std::vector<memgraph::storage::EdgeTypeId> &edge_types,
      const std::shared_ptr<memgraph::query::plan::LogicalOperator> &input, bool existing_node,
      memgraph::query::Expression *lower_bound, memgraph::query::Expression *upper_bound,
      const memgraph::query::plan::ExpansionLambda &filter_lambda, memgraph::query::Expression *limit = nullptr,
      std::optional<memgraph::query::plan::ExpansionLambda> weight_lambda = std::nullopt,
      std::optional<memgraph::query::Symbol> total_weight = std::nullopt) = 0;
  virtual std::pair<std::vector<memgraph::query::VertexAccessor>, std::vector<memgraph::query::EdgeAccessor>>
  BuildGraph(memgraph::query::DbAccessor *dba, const std::vector<int> &vertex_locations,
             const std::vector<std::tuple<int, int, std::string>> &edges) = 0;
  virtual ~Database() = default;

  // The fixture graph is a parameter so a case can reach deviation indices the default 6-vertex
  // graph cannot; `vertex_locations` sizes the graph, so its length is the vertex count.
  void KShortestTest(Database *db, int lower_bound, int upper_bound, memgraph::query::EdgeAtom::Direction direction,
                     std::vector<std::string> edge_types, int limit = -1,
                     FilterLambdaType filter_lambda_type = FilterLambdaType::NONE,
                     const std::vector<int> &vertex_locations = kVertexLocations,
                     const std::vector<std::tuple<int, int, std::string>> &graph_edges = kEdges) {
    const int vertex_count = static_cast<int>(vertex_locations.size());
    spdlog::info("KShortestTest: lower_bound={}, upper_bound={}, direction={}, edge_types={}",
                 lower_bound,
                 upper_bound,
                 static_cast<int>(direction),
                 edge_types.empty() ? "all" : fmt::format("{}", memgraph::utils::JoinVector(edge_types, ",")));

    auto storage_dba = db->Access();
    memgraph::query::DbAccessor dba(storage_dba.get());
    memgraph::query::ExecutionContext context{.db_accessor = &dba, .metric_handles = &TestMetricHandles()};
    memgraph::query::Symbol blocked_sym = context.symbol_table.CreateSymbol("blocked", true);
    memgraph::query::Symbol source_sym = context.symbol_table.CreateSymbol("source", true);
    memgraph::query::Symbol sink_sym = context.symbol_table.CreateSymbol("sink", true);
    memgraph::query::Symbol edges_sym = context.symbol_table.CreateSymbol("edges", true);
    memgraph::query::Symbol inner_node_sym = context.symbol_table.CreateSymbol("inner_node", true);
    memgraph::query::Symbol inner_edge_sym = context.symbol_table.CreateSymbol("inner_edge", true);
    memgraph::query::Identifier *blocked = IDENT("blocked")->MapTo(blocked_sym);
    memgraph::query::Identifier *inner_node = IDENT("inner_node")->MapTo(inner_node_sym);
    memgraph::query::Identifier *inner_edge = IDENT("inner_edge")->MapTo(inner_edge_sym);

    std::vector<memgraph::query::VertexAccessor> vertices;
    std::vector<memgraph::query::EdgeAccessor> edges;

    std::tie(vertices, edges) = db->BuildGraph(&dba, vertex_locations, graph_edges);
    spdlog::info("KShortestTest: Built graph with {} vertices and {} edges", vertices.size(), edges.size());

    dba.AdvanceCommand();

    std::shared_ptr<memgraph::query::plan::LogicalOperator> input_op;
    memgraph::query::Expression *filter_expr = nullptr;
    // The entities yielded into `blocked`, in the order the expansion will see them.
    std::vector<memgraph::query::TypedValue> blocked_values;

    switch (filter_lambda_type) {
      case FilterLambdaType::NONE:
        // No filter lambda, nothing is ever blocked.
        input_op = std::make_shared<Yield>(
            nullptr,
            std::vector<memgraph::query::Symbol>{blocked_sym},
            std::vector<std::vector<memgraph::query::TypedValue>>{{memgraph::query::TypedValue()}});
        blocked_values.emplace_back();
        break;
      case FilterLambdaType::USE_FRAME:
        // We block each entity in the graph in turn.
        input_op = YieldEntities(&dba, vertices, edges, blocked_sym, nullptr);
        filter_expr = AND(NEQ(inner_node, blocked), NEQ(inner_edge, blocked));
        AppendEntities(vertices, edges, blocked_values);
        break;
      case FilterLambdaType::USE_FRAME_NULL:
        input_op = YieldEntities(&dba, vertices, edges, blocked_sym, nullptr);
        filter_expr = IF(AND(NEQ(inner_node, blocked), NEQ(inner_edge, blocked)),
                         LITERAL(true),
                         LITERAL(memgraph::storage::ExternalPropertyValue()));
        AppendEntities(vertices, edges, blocked_values);
        break;
      case FilterLambdaType::USE_CTX:
        // We only block vertex #5, through a parameter rather than the frame.
        input_op = std::make_shared<Yield>(
            nullptr,
            std::vector<memgraph::query::Symbol>{blocked_sym},
            std::vector<std::vector<memgraph::query::TypedValue>>{{memgraph::query::TypedValue(vertices[5])}});
        filter_expr = NEQ(PROPERTY_LOOKUP(dba, inner_node, PROPERTY_PAIR(dba, "id")), PARAMETER_LOOKUP(0));
        context.evaluation_context.parameters.Add(0, memgraph::storage::ExternalPropertyValue(5));
        blocked_values.emplace_back(vertices[5]);
        break;
      case FilterLambdaType::ERROR:
        input_op = std::make_shared<Yield>(
            nullptr,
            std::vector<memgraph::query::Symbol>{blocked_sym},
            std::vector<std::vector<memgraph::query::TypedValue>>{{memgraph::query::TypedValue()}});
        filter_expr =
            IF(EQ(PROPERTY_LOOKUP(dba, inner_node, PROPERTY_PAIR(dba, "id")), LITERAL(5)), LITERAL(42), LITERAL(true));
        blocked_values.emplace_back();
        break;
    }

    // For k-shortest paths, we need both source and sink to be known
    // We run k-shortest paths for all possible source-sink pairs
    input_op = YieldVertices(&dba, vertices, source_sym, input_op);
    input_op = YieldVertices(&dba, vertices, sink_sym, input_op);

    std::vector<memgraph::storage::EdgeTypeId> storage_edge_types;
    for (const auto &t : edge_types) {
      storage_edge_types.push_back(dba.NameToEdgeType(t));
    }
    spdlog::info("KShortestTest: Using {} edge types", storage_edge_types.size());

    input_op =
        db->MakeKShortestOperator(source_sym,
                                  sink_sym,
                                  edges_sym,
                                  direction,
                                  storage_edge_types,
                                  input_op,
                                  true,
                                  lower_bound == -1 ? nullptr : LITERAL(lower_bound),
                                  upper_bound == -1 ? nullptr : LITERAL(upper_bound),
                                  memgraph::query::plan::ExpansionLambda{inner_edge_sym, inner_node_sym, filter_expr},
                                  limit == -1 ? nullptr : LITERAL(limit));

    context.evaluation_context.properties = memgraph::query::NamesToProperties(storage.properties_, &dba);
    context.evaluation_context.labels = memgraph::query::NamesToLabels(storage.labels_, &dba);
    context.evaluation_context.edgetypes = memgraph::query::NamesToEdgeTypes(storage.edge_types_, &dba);
    std::vector<std::vector<memgraph::query::TypedValue>> results;

    // A non-boolean, non-null lambda result must abort the pull.
    if (filter_lambda_type == FilterLambdaType::ERROR) {
      EXPECT_THROW(PullResults(input_op.get(),
                               &context,
                               std::vector<memgraph::query::Symbol>{source_sym, sink_sym, edges_sym, blocked_sym}),
                   memgraph::query::QueryRuntimeException);
      dba.Abort();
      return;
    }

    results = PullResults(
        input_op.get(), &context, std::vector<memgraph::query::Symbol>{source_sym, sink_sym, edges_sym, blocked_sym});

    spdlog::info("KShortestTest: Pulled {} results", results.size());
    if (limit != -1) {
      spdlog::info("KShortestTest: Limit: {}", limit);
    }

    if (upper_bound == -1) upper_bound = vertex_count;
    const int effective_lower_bound = lower_bound != -1 ? lower_bound : 1;
    const int effective_upper_bound = upper_bound;

    // The paths Yen's algorithm finds in the subgraph the lambda leaves, within the length bounds.
    auto correct_paths_for = [&](const memgraph::query::TypedValue &blocked_entity, int source_id, int sink_id) {
      auto paths = YenKShortestPaths(vertex_count,
                                     GetFilteredEdgeList(blocked_entity, direction, edge_types, &dba, graph_edges),
                                     source_id,
                                     sink_id);
      // `paths` holds vertices, not edges, hence the -1 when comparing against the bounds.
      std::erase_if(paths, [&](const std::vector<int> &path) {
        return path.size() - 1 < static_cast<size_t>(effective_lower_bound) ||
               path.size() - 1 > static_cast<size_t>(effective_upper_bound);
      });
      return paths;
    };

    // A wholly missing group is invisible to the per-group loop, so the total has to catch it. Needed
    // with a limit too: the limit caps a group, never removes one.
    {
      size_t expected_total = 0;
      for (const auto &blocked_entity : blocked_values) {
        for (int source_id = 0; source_id < vertex_count; ++source_id) {
          for (int sink_id = 0; sink_id < vertex_count; ++sink_id) {
            if (source_id == sink_id) continue;
            const auto group = correct_paths_for(blocked_entity, source_id, sink_id).size();
            expected_total += limit == -1 ? group : std::min(group, static_cast<size_t>(limit));
          }
        }
      }
      EXPECT_EQ(results.size(), expected_total);
    }

    // Group results based on blocked entity and source-sink pair and compare them to results
    // obtained by running Yen's algorithm.
    for (size_t i = 0; i < results.size();) {
      int j = i;
      auto blocked_entity = results[j][3];
      auto source = results[j][0];
      auto sink = results[j][1];

      while (j < results.size() && memgraph::query::TypedValue::BoolEqual{}(results[j][3], blocked_entity) &&
             memgraph::query::TypedValue::BoolEqual{}(results[j][0], source) &&
             memgraph::query::TypedValue::BoolEqual{}(results[j][1], sink)) {
        ++j;
      }

      SCOPED_TRACE(fmt::format("blocked = {}, source = {}, sink = {}",
                               ToString(blocked_entity, dba),
                               ToString(source, dba),
                               ToString(sink, dba)));

      auto source_id = GetProp(source.ValueVertex(), "id", &dba).ValueInt();
      auto sink_id = GetProp(sink.ValueVertex(), "id", &dba).ValueInt();
      spdlog::info("KShortestTest: Processing source_id={}, sink_id={}", source_id, sink_id);

      // Skip same vertex pairs
      if (source_id == sink_id) {
        i = j;
        continue;
      }

      auto edges_filtered = GetFilteredEdgeList(blocked_entity, direction, edge_types, &dba, graph_edges);
      auto correct_paths = correct_paths_for(blocked_entity, source_id, sink_id);
      spdlog::info("KShortestTest: Yen algorithm found {} paths", correct_paths.size());

      int expected_count = static_cast<int>(correct_paths.size());
      // The limit applies per input row, so every group is capped by it independently.
      if (limit != -1 && expected_count > limit) {
        spdlog::info("KShortestTest: Limit caps this group, expected count: {}, limit: {}", expected_count, limit);
        expected_count = limit;
        correct_paths.resize(expected_count);
      }
      EXPECT_EQ(j - i, expected_count);

      auto lengths = CheckPathsAndExtractLengths(
          &dba,
          edges_filtered,
          std::vector<std::vector<memgraph::query::TypedValue>>(results.begin() + i, results.begin() + j));

      // The path lengths should match and be in ascending order.
      ASSERT_EQ(lengths.size(), correct_paths.size());
      for (size_t idx = 0; idx < lengths.size(); ++idx) {
        EXPECT_EQ(lengths[idx],
                  static_cast<int>(correct_paths[idx].size()) - 1);  // -1 because path size includes vertices
      }

      // Check that paths are in ascending order by length
      for (size_t idx = 1; idx < lengths.size(); ++idx) {
        EXPECT_LE(lengths[idx - 1], lengths[idx]);
      }

      i = j;
    }

    dba.Abort();
  }

  // --- weighted arm --------------------------------------------------------------------------------

  // The weight lambda every weighted case here uses: `e.weight`, which is what `BuildGraph` stores
  // and what `EdgeWeightFor` replays.
  memgraph::query::plan::ExpansionLambda MakeWeightLambda(memgraph::query::DbAccessor &dba,
                                                          memgraph::query::ExecutionContext &context) {
    auto edge_sym = context.symbol_table.CreateSymbol("weight_edge", true);
    auto node_sym = context.symbol_table.CreateSymbol("weight_node", true);
    auto *inner_edge = IDENT("weight_edge")->MapTo(edge_sym);
    return memgraph::query::plan::ExpansionLambda{
        edge_sym, node_sym, PROPERTY_LOOKUP(dba, inner_edge, PROPERTY_PAIR(dba, "weight"))};
  }

  struct WeightedRun {
    // source, sink, edges, total weight
    std::vector<std::vector<memgraph::query::TypedValue>> rows;
    int64_t hops = 0;
  };

  // One weighted KSHORTEST expansion, over every ordered pair of the two vertex lists.
  WeightedRun RunWeighted(Database *db, memgraph::query::DbAccessor &dba, memgraph::query::ExecutionContext &context,
                          const std::vector<memgraph::query::VertexAccessor> &sources,
                          const std::vector<memgraph::query::VertexAccessor> &sinks,
                          memgraph::query::EdgeAtom::Direction direction,
                          const std::vector<memgraph::storage::EdgeTypeId> &edge_types, int lower_bound,
                          int upper_bound, int limit, const memgraph::query::plan::ExpansionLambda &weight_lambda,
                          memgraph::query::Expression *filter_expr = nullptr,
                          std::shared_ptr<memgraph::query::plan::LogicalOperator> input_op = nullptr,
                          std::optional<memgraph::query::plan::ExpansionLambda> filter_lambda = std::nullopt) {
    auto source_sym = context.symbol_table.CreateSymbol("source", true);
    auto sink_sym = context.symbol_table.CreateSymbol("sink", true);
    auto edges_sym = context.symbol_table.CreateSymbol("edges", true);
    auto total_sym = context.symbol_table.CreateSymbol("total", true);
    if (!filter_lambda) {
      filter_lambda = memgraph::query::plan::ExpansionLambda{context.symbol_table.CreateSymbol("filter_edge", true),
                                                             context.symbol_table.CreateSymbol("filter_node", true),
                                                             filter_expr};
    }

    input_op = YieldVertices(&dba, sources, source_sym, input_op);
    input_op = YieldVertices(&dba, sinks, sink_sym, input_op);
    input_op = db->MakeKShortestOperator(source_sym,
                                         sink_sym,
                                         edges_sym,
                                         direction,
                                         edge_types,
                                         input_op,
                                         true,
                                         lower_bound == -1 ? nullptr : LITERAL(lower_bound),
                                         upper_bound == -1 ? nullptr : LITERAL(upper_bound),
                                         *filter_lambda,
                                         limit == -1 ? nullptr : LITERAL(limit),
                                         weight_lambda,
                                         total_sym);

    context.evaluation_context.properties = memgraph::query::NamesToProperties(storage.properties_, &dba);
    context.evaluation_context.labels = memgraph::query::NamesToLabels(storage.labels_, &dba);
    context.evaluation_context.edgetypes = memgraph::query::NamesToEdgeTypes(storage.edge_types_, &dba);

    const auto hops_before = context.number_of_hops;
    WeightedRun out;
    out.rows = PullResults(
        input_op.get(), &context, std::vector<memgraph::query::Symbol>{source_sym, sink_sym, edges_sym, total_sym});
    out.hops = context.number_of_hops - hops_before;
    return out;
  }

  // Overwrites every edge's `weight`. `BuildGraph` returns the accessors in the order it was given
  // the edge list, which is how an edge is matched to its entry here - the `from`/`to` properties
  // are not readable yet, because nothing has advanced the command since they were written.
  void SetEdgeWeights(memgraph::query::DbAccessor &dba, std::vector<memgraph::query::EdgeAccessor> &edges,
                      const std::vector<std::tuple<int, int, std::string>> &graph_edges,
                      const std::map<std::pair<int, int>, memgraph::storage::PropertyValue> &weights) {
    MG_ASSERT(edges.size() == graph_edges.size(), "the accessors line up with the edge list they were built from");
    for (size_t i = 0; i < edges.size(); ++i) {
      const auto it = weights.find({std::get<0>(graph_edges[i]), std::get<1>(graph_edges[i])});
      MG_ASSERT(it != weights.end(), "every fixture edge needs a weight");
      MG_ASSERT(edges[i].SetProperty(dba.NameToProperty("weight"), it->second).has_value());
    }
  }

  // A graph whose cheapest route is also its longest: 0-1-2-3 over three cheap hops against the one
  // expensive edge 0-3. Hop count would serve them the other way round.
  static const std::vector<int> &DetourVertexLocations() {
    static const std::vector<int> v{0, 0, 0, 0};
    return v;
  }

  static const std::vector<std::tuple<int, int, std::string>> &DetourEdges() {
    static const std::vector<std::tuple<int, int, std::string>> e{
        {0, 1, "a"}, {1, 2, "a"}, {2, 3, "a"}, {0, 3, "a"}, {0, 2, "a"}};
    return e;
  }

  // The weighted matrix: every ordered pair, against a brute-force enumeration. Without a limit the
  // two must agree path for path - that is what catches a *lost* path, which a subset check cannot.
  // Also checks the served order really is ascending by `(weight, hops)`, and that the total weight
  // column equals the sum over the path the operator wrote beside it.
  void KShortestWeightedTest(Database *db, int lower_bound, int upper_bound,
                             memgraph::query::EdgeAtom::Direction direction, std::vector<std::string> edge_types,
                             int limit = -1, const std::vector<int> &vertex_locations = kVertexLocations,
                             const std::vector<std::tuple<int, int, std::string>> &graph_edges = kEdges) {
    const int vertex_count = static_cast<int>(vertex_locations.size());
    auto storage_dba = db->Access();
    memgraph::query::DbAccessor dba(storage_dba.get());
    memgraph::query::ExecutionContext context{.db_accessor = &dba, .metric_handles = &TestMetricHandles()};

    auto [vertices, edges] = db->BuildGraph(&dba, vertex_locations, graph_edges);
    dba.AdvanceCommand();

    std::vector<memgraph::storage::EdgeTypeId> storage_edge_types;
    for (const auto &t : edge_types) storage_edge_types.push_back(dba.NameToEdgeType(t));

    auto run = RunWeighted(db,
                           dba,
                           context,
                           vertices,
                           vertices,
                           direction,
                           storage_edge_types,
                           lower_bound,
                           upper_bound,
                           limit,
                           MakeWeightLambda(dba, context));

    const auto edge_list = GetEdgeList(graph_edges, direction, edge_types);
    const int effective_lower = lower_bound != -1 ? lower_bound : 1;
    const int effective_upper = upper_bound != -1 ? upper_bound : vertex_count;

    auto expected_for = [&](int source_id, int sink_id) {
      auto paths = WeightedSimplePaths(edge_list, source_id, sink_id);
      std::erase_if(paths, [&](const WeightedPath &p) {
        return p.hops() < static_cast<size_t>(effective_lower) || p.hops() > static_cast<size_t>(effective_upper);
      });
      return paths;
    };

    std::map<std::pair<int, int>, std::vector<std::pair<int64_t, std::vector<int>>>> actual;
    std::map<std::pair<int, int>, std::vector<std::pair<int64_t, size_t>>> served;
    for (const auto &row : run.rows) {
      const auto source_id = static_cast<int>(GetProp(row[0].ValueVertex(), "id", &dba).ValueInt());
      const auto sink_id = static_cast<int>(GetProp(row[1].ValueVertex(), "id", &dba).ValueInt());
      SCOPED_TRACE(fmt::format("source = {}, sink = {}", source_id, sink_id));
      CheckPath(&dba, row[0].ValueVertex(), row[1].ValueVertex(), row[2].ValueList(), edge_list);

      auto ids = PathVertexIds(&dba, row);
      int64_t summed = 0;
      for (size_t i = 1; i < ids.size(); ++i) summed += EdgeWeightFor(ids[i - 1], ids[i]);
      ASSERT_TRUE(row[3].IsInt()) << "an all-integer weight lambda must keep the total an integer";
      EXPECT_EQ(row[3].ValueInt(), summed) << "the total weight column must be the sum of the path's own edges";

      served[{source_id, sink_id}].emplace_back(summed, ids.size() - 1);
      actual[{source_id, sink_id}].emplace_back(summed, std::move(ids));
    }

    // A pair that came back empty is invisible to the per-pair loop, so the total catches it.
    size_t expected_total = 0;
    for (int source_id = 0; source_id < vertex_count; ++source_id) {
      for (int sink_id = 0; sink_id < vertex_count; ++sink_id) {
        if (source_id == sink_id) continue;
        const auto group = expected_for(source_id, sink_id).size();
        expected_total += limit == -1 ? group : std::min(group, static_cast<size_t>(limit));
      }
    }
    EXPECT_EQ(run.rows.size(), expected_total);

    for (int source_id = 0; source_id < vertex_count; ++source_id) {
      for (int sink_id = 0; sink_id < vertex_count; ++sink_id) {
        if (source_id == sink_id) continue;
        SCOPED_TRACE(fmt::format("source = {}, sink = {}", source_id, sink_id));
        const auto expected = expected_for(source_id, sink_id);
        auto &rows = actual[{source_id, sink_id}];
        const auto &order = served[{source_id, sink_id}];

        for (size_t i = 1; i < order.size(); ++i) {
          EXPECT_LE(order[i - 1], order[i]) << "paths must be served in ascending (weight, hops) order";
        }

        if (limit != -1) {
          // Which of several equally ranked paths a capped pair keeps is undetermined, so only the
          // cap is checked here; the uncapped runs are what pin the set.
          EXPECT_EQ(rows.size(), std::min(expected.size(), static_cast<size_t>(limit)));
          continue;
        }

        std::vector<std::pair<int64_t, std::vector<int>>> expected_rows;
        expected_rows.reserve(expected.size());
        for (const auto &path : expected) expected_rows.emplace_back(path.weight, path.vertices);
        std::ranges::sort(expected_rows);
        std::ranges::sort(rows);
        EXPECT_EQ(rows, expected_rows);
      }
    }

    dba.Abort();
  }

  // The weighted and the hop-count order must part company somewhere on this fixture, or the matrix
  // above would pass just as well against an expansion that ignored the lambda.
  void KShortestWeightedTestOrderDiffersFromHopCount(Database *db) {
    auto storage_dba = db->Access();
    memgraph::query::DbAccessor dba(storage_dba.get());
    memgraph::query::ExecutionContext context{.db_accessor = &dba, .metric_handles = &TestMetricHandles()};
    auto [vertices, edges] = db->BuildGraph(&dba, kVertexLocations, kEdges);
    dba.AdvanceCommand();

    auto run = RunWeighted(db,
                           dba,
                           context,
                           vertices,
                           vertices,
                           memgraph::query::EdgeAtom::Direction::BOTH,
                           {},
                           -1,
                           -1,
                           -1,
                           MakeWeightLambda(dba, context));
    ASSERT_FALSE(run.rows.empty());

    bool hops_ever_decrease = false;
    std::optional<std::pair<int, int>> previous_pair;
    size_t previous_hops = 0;
    for (const auto &row : run.rows) {
      const std::pair pair{static_cast<int>(GetProp(row[0].ValueVertex(), "id", &dba).ValueInt()),
                           static_cast<int>(GetProp(row[1].ValueVertex(), "id", &dba).ValueInt())};
      const size_t hops = row[2].ValueList().size();
      if (previous_pair == pair && hops < previous_hops) hops_ever_decrease = true;
      previous_pair = pair;
      previous_hops = hops;
    }
    EXPECT_TRUE(hops_ever_decrease) << "this fixture never serves a longer path before a shorter one, so it cannot "
                                       "tell a weighted expansion from a hop-count one";
    dba.Abort();
  }

  // One source-sink pair over the detour graph with hand-written weights: `(total, hops)` per served
  // path, in the order they came out.
  std::vector<std::pair<memgraph::query::TypedValue, size_t>> DetourResults(
      Database *db, const std::map<std::pair<int, int>, memgraph::storage::PropertyValue> &weights,
      int lower_bound = -1, int upper_bound = -1, int limit = -1) {
    auto storage_dba = db->Access();
    memgraph::query::DbAccessor dba(storage_dba.get());
    memgraph::query::ExecutionContext context{.db_accessor = &dba, .metric_handles = &TestMetricHandles()};
    auto [vertices, edges] = db->BuildGraph(&dba, DetourVertexLocations(), DetourEdges());
    SetEdgeWeights(dba, edges, DetourEdges(), weights);
    dba.AdvanceCommand();

    auto run = RunWeighted(db,
                           dba,
                           context,
                           {vertices[0]},
                           {vertices[3]},
                           memgraph::query::EdgeAtom::Direction::OUT,
                           {},
                           lower_bound,
                           upper_bound,
                           limit,
                           MakeWeightLambda(dba, context));

    std::vector<std::pair<memgraph::query::TypedValue, size_t>> out;
    for (const auto &row : run.rows) {
      CheckPath(&dba,
                row[0].ValueVertex(),
                row[1].ValueVertex(),
                row[2].ValueList(),
                GetEdgeList(DetourEdges(), memgraph::query::EdgeAtom::Direction::OUT, {}));
      out.emplace_back(row[3], row[2].ValueList().size());
    }
    dba.Abort();
    return out;
  }

  static std::map<std::pair<int, int>, memgraph::storage::PropertyValue> DetourIntWeights() {
    using PV = memgraph::storage::PropertyValue;
    return {{{0, 1}, PV(int64_t{1})},
            {{1, 2}, PV(int64_t{1})},
            {{2, 3}, PV(int64_t{1})},
            {{0, 3}, PV(int64_t{10})},
            {{0, 2}, PV(int64_t{4})}};
  }

  // The cheapest route is the longest one, and the totals are the sums written out longhand here.
  void KShortestWeightedTestHandFixture(Database *db) {
    const auto served = DetourResults(db, DetourIntWeights());
    ASSERT_EQ(served.size(), 3U);
    EXPECT_EQ(served[0].first.ValueInt(), 3);
    EXPECT_EQ(served[0].second, 3U);
    EXPECT_EQ(served[1].first.ValueInt(), 5);
    EXPECT_EQ(served[1].second, 2U);
    EXPECT_EQ(served[2].first.ValueInt(), 10);
    EXPECT_EQ(served[2].second, 1U);
  }

  // Zero weights tie every route, so hop count is all that is left to order them by - and no path
  // may wander through a vertex twice on the way, which costs nothing here.
  void KShortestWeightedTestZeroWeights(Database *db) {
    using PV = memgraph::storage::PropertyValue;
    const auto served = DetourResults(db,
                                      {{{0, 1}, PV(int64_t{0})},
                                       {{1, 2}, PV(int64_t{0})},
                                       {{2, 3}, PV(int64_t{0})},
                                       {{0, 3}, PV(int64_t{0})},
                                       {{0, 2}, PV(int64_t{0})}});
    ASSERT_EQ(served.size(), 3U);
    size_t previous_hops = 0;
    for (const auto &[total, hops] : served) {
      EXPECT_EQ(total.ValueInt(), 0);
      EXPECT_GE(hops, previous_hops) << "an all-zero total must still order by hops";
      previous_hops = hops;
    }
    // `DetourResults` runs `CheckPath`, which is what rejects a revisited vertex.
  }

  // Durations add and compare like numbers, and the total keeps the type.
  void KShortestWeightedTestDurationWeights(Database *db) {
    auto duration = [](int64_t micros) {
      return memgraph::storage::PropertyValue(
          memgraph::storage::TemporalData(memgraph::storage::TemporalType::Duration, micros));
    };
    const auto served = DetourResults(db,
                                      {{{0, 1}, duration(1)},
                                       {{1, 2}, duration(1)},
                                       {{2, 3}, duration(1)},
                                       {{0, 3}, duration(10)},
                                       {{0, 2}, duration(4)}});
    ASSERT_EQ(served.size(), 3U);
    ASSERT_TRUE(served[0].first.IsDuration()) << "a Duration weight must keep a Duration total";
    EXPECT_EQ(served[0].first.ValueDuration().microseconds, 3);
    EXPECT_EQ(served[1].first.ValueDuration().microseconds, 5);
    EXPECT_EQ(served[2].first.ValueDuration().microseconds, 10);
  }

  // An integer and a double in the same expansion add to a double; the routes that never touch the
  // double keep their integer totals.
  void KShortestWeightedTestMixedNumericWeights(Database *db) {
    using PV = memgraph::storage::PropertyValue;
    const auto served = DetourResults(db,
                                      {{{0, 1}, PV(int64_t{1})},
                                       {{1, 2}, PV(0.5)},
                                       {{2, 3}, PV(int64_t{1})},
                                       {{0, 3}, PV(int64_t{10})},
                                       {{0, 2}, PV(int64_t{4})}});
    ASSERT_EQ(served.size(), 3U);
    ASSERT_TRUE(served[0].first.IsDouble());
    EXPECT_DOUBLE_EQ(served[0].first.ValueDouble(), 2.5);
    EXPECT_TRUE(served[1].first.IsInt());
    EXPECT_EQ(served[1].first.ValueInt(), 5);
    EXPECT_EQ(served[2].first.ValueInt(), 10);
  }

  // A cheaper route needing more hops than the bound allows must lose to a pricier one that fits.
  void KShortestWeightedTestUpperBoundPrefersShorterPricierPath(Database *db) {
    const auto served = DetourResults(db, DetourIntWeights(), -1, 2);
    ASSERT_EQ(served.size(), 2U) << "the cheapest route is three hops, over the bound";
    EXPECT_EQ(served[0].first.ValueInt(), 5);
    EXPECT_EQ(served[1].first.ValueInt(), 10);
  }

  // Paths under the lower bound are not served, but they are still base paths, so the longer routes
  // that deviate from them have to come out. Here the cheapest route is also the only servable one,
  // so the bound skips from the middle of the enumeration rather than off its front.
  void KShortestWeightedTestLowerBoundKeepsDeviations(Database *db) {
    using PV = memgraph::storage::PropertyValue;
    const auto served = DetourResults(db,
                                      {{{0, 1}, PV(int64_t{1})},
                                       {{1, 2}, PV(int64_t{1})},
                                       {{2, 3}, PV(int64_t{1})},
                                       {{0, 3}, PV(int64_t{4})},
                                       {{0, 2}, PV(int64_t{2})}},
                                      3);
    ASSERT_EQ(served.size(), 1U);
    EXPECT_EQ(served[0].second, 3U);
    EXPECT_EQ(served[0].first.ValueInt(), 3);
  }

  // A `|k` under weights caps the cheapest ones, not the shortest ones.
  void KShortestWeightedTestLimitTakesTheCheapest(Database *db) {
    const auto served = DetourResults(db, DetourIntWeights(), -1, -1, 2);
    ASSERT_EQ(served.size(), 2U);
    EXPECT_EQ(served[0].first.ValueInt(), 3);
    EXPECT_EQ(served[1].first.ValueInt(), 5);
  }

  // A target nothing can reach is settled by the reverse tree alone, so the source's own component is
  // never swept. Assert on the work done, as `InvertedRangeDoesNotSearch` does.
  void KShortestWeightedTestUnreachableTargetDoesNotSearch(Database *db) {
    auto storage_dba = db->Access();
    memgraph::query::DbAccessor dba(storage_dba.get());
    memgraph::query::ExecutionContext context{.db_accessor = &dba, .metric_handles = &TestMetricHandles()};
    // Vertex 3 has no edges at all; 0, 1 and 2 form a cycle a forward sweep would walk.
    auto [vertices, edges] =
        db->BuildGraph(&dba,
                       std::vector<int>{0, 0, 0, 0},
                       std::vector<std::tuple<int, int, std::string>>{{0, 1, "a"}, {1, 2, "a"}, {2, 0, "a"}});
    dba.AdvanceCommand();

    auto run = RunWeighted(db,
                           dba,
                           context,
                           {vertices[0]},
                           {vertices[3]},
                           memgraph::query::EdgeAtom::Direction::OUT,
                           {},
                           -1,
                           -1,
                           -1,
                           MakeWeightLambda(dba, context));
    EXPECT_TRUE(run.rows.empty());
    EXPECT_EQ(run.hops, 0) << "nothing reaches the target, so nothing should have been expanded";
    dba.Abort();
  }

  // Every way a weight can be wrong, each one a runtime error rather than a silent answer.
  void KShortestWeightedTestWeightErrors(Database *db) {
    using PV = memgraph::storage::PropertyValue;
    const auto duration = PV(memgraph::storage::TemporalData(memgraph::storage::TemporalType::Duration, 1));
    const std::vector<std::pair<const char *, PV>> cases{
        {"null", PV()}, {"negative", PV(int64_t{-1})}, {"string", PV("heavy")}, {"duration among integers", duration}};

    for (const auto &[name, weight] : cases) {
      SCOPED_TRACE(name);
      auto weights = DetourIntWeights();
      weights[{0, 1}] = weight;
      EXPECT_THROW(DetourResults(db, weights), memgraph::query::QueryRuntimeException);
    }
  }

  // The filter lambda still prunes under weights. Blocking vertex 2 leaves only the single expensive
  // hop, so an expansion that ran the lambda nowhere, or only in the forward search, is visible here.
  void KShortestWeightedTestFilterLambda(Database *db) {
    auto storage_dba = db->Access();
    memgraph::query::DbAccessor dba(storage_dba.get());
    memgraph::query::ExecutionContext context{.db_accessor = &dba, .metric_handles = &TestMetricHandles()};
    auto filter_edge_sym = context.symbol_table.CreateSymbol("filter_edge", true);
    auto filter_node_sym = context.symbol_table.CreateSymbol("filter_node", true);
    auto *inner_node = IDENT("filter_node")->MapTo(filter_node_sym);

    auto [vertices, edges] = db->BuildGraph(&dba, DetourVertexLocations(), DetourEdges());
    SetEdgeWeights(dba, edges, DetourEdges(), DetourIntWeights());
    dba.AdvanceCommand();

    auto *filter_expr = NEQ(PROPERTY_LOOKUP(dba, inner_node, PROPERTY_PAIR(dba, "id")), LITERAL(2));
    auto run = RunWeighted(db,
                           dba,
                           context,
                           {vertices[0]},
                           {vertices[3]},
                           memgraph::query::EdgeAtom::Direction::OUT,
                           {},
                           -1,
                           -1,
                           -1,
                           MakeWeightLambda(dba, context),
                           nullptr,
                           nullptr,
                           memgraph::query::plan::ExpansionLambda{filter_edge_sym, filter_node_sym, filter_expr});

    ASSERT_EQ(run.rows.size(), 1U) << "both cheap routes pass through the blocked vertex";
    EXPECT_EQ(run.rows[0][3].ValueInt(), 10);
    EXPECT_EQ(run.rows[0][2].ValueList().size(), 1U);
    dba.Abort();
  }

#ifdef MG_ENTERPRISE
  // `blocked_vertex_id` also blocks every edge into that vertex with a filter lambda, so the
  // expansion must honour the access checks and the lambda at once. `limit` is `|k`, or -1 for none.
  void KShortestTestWithFineGrainedFiltering(Database *db, int upper_bound,
                                             memgraph::query::EdgeAtom::Direction direction,
                                             std::vector<std::string> edge_types, int limit,
                                             FineGrainedTestType fine_grained_test_type,
                                             std::optional<int> blocked_vertex_id = std::nullopt) {
    auto storage_dba = db->Access();
    memgraph::query::DbAccessor db_accessor(storage_dba.get());
    memgraph::query::ExecutionContext context{.db_accessor = &db_accessor, .metric_handles = &TestMetricHandles()};
    memgraph::query::Symbol source_symbol = context.symbol_table.CreateSymbol("source", true);
    memgraph::query::Symbol sink_symbol = context.symbol_table.CreateSymbol("sink", true);
    memgraph::query::Symbol edges_symbol = context.symbol_table.CreateSymbol("edges", true);
    memgraph::query::Symbol inner_node_symbol = context.symbol_table.CreateSymbol("inner_node", true);
    memgraph::query::Symbol inner_edge_symbol = context.symbol_table.CreateSymbol("inner_edge", true);
    memgraph::query::Symbol blocked_symbol = context.symbol_table.CreateSymbol("blocked", true);
    memgraph::query::Identifier *inner_node = IDENT("inner_node")->MapTo(inner_node_symbol);
    memgraph::query::Identifier *blocked = IDENT("blocked")->MapTo(blocked_symbol);

    std::vector<memgraph::query::VertexAccessor> vertices;
    std::vector<memgraph::query::EdgeAccessor> edges;

    std::tie(vertices, edges) = db->BuildGraph(&db_accessor, kVertexLocations, kEdges);

    db_accessor.AdvanceCommand();

    memgraph::auth::User user{"test"};
    const auto edges_in_result = ApplyFineGrainedArm(user, fine_grained_test_type, direction, edge_types);

    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &db_accessor};
    context.auth_checker = &auth_checker;

    // We run k-shortest paths for all possible source-sink pairs
    if (fine_grained_test_type == FineGrainedTestType::LABEL_0_DENIED) {
      vertices.erase(std::remove_if(vertices.begin(),
                                    vertices.end(),
                                    [&](const auto &v) { return GetProp(v, "id", &db_accessor).ValueInt() == 0; }),
                     vertices.end());
    }

    std::vector<memgraph::storage::EdgeTypeId> storage_edge_types;
    for (const auto &t : edge_types) {
      storage_edge_types.push_back(db_accessor.NameToEdgeType(t));
    }

    const memgraph::query::VertexAccessor *blocked_vertex = nullptr;
    auto edges_after_lambda = edges_in_result;
    if (blocked_vertex_id) {
      const auto it = std::ranges::find_if(
          vertices, [&](const auto &v) { return GetProp(v, "id", &db_accessor).ValueInt() == *blocked_vertex_id; });
      MG_ASSERT(it != vertices.end(), "The blocked vertex must be part of the fixture");
      blocked_vertex = &*it;
      std::erase_if(edges_after_lambda, [id = *blocked_vertex_id](const auto &e) { return e.second == id; });
    }

    context.evaluation_context.properties = memgraph::query::NamesToProperties(storage.properties_, &db_accessor);
    context.evaluation_context.labels = memgraph::query::NamesToLabels(storage.labels_, &db_accessor);
    context.evaluation_context.edgetypes = memgraph::query::NamesToEdgeTypes(storage.edge_types_, &db_accessor);

    // One pass over the source-sink cartesian; only the lambda and the limit vary between runs.
    auto run = [&](bool with_lambda, int path_limit) {
      std::shared_ptr<memgraph::query::plan::LogicalOperator> input_operator = nullptr;
      input_operator = YieldVertices(&db_accessor, vertices, source_symbol, input_operator);
      input_operator = YieldVertices(&db_accessor, vertices, sink_symbol, input_operator);

      memgraph::query::Expression *filter_expr = nullptr;
      if (with_lambda) {
        // Compare vertex identities against an outer frame symbol, not `inner_node.id`: a property
        // lookup built here evaluates to null, which left this whole matrix passing on zero rows.
        input_operator = std::make_shared<Yield>(
            input_operator,
            std::vector<memgraph::query::Symbol>{blocked_symbol},
            std::vector<std::vector<memgraph::query::TypedValue>>{{memgraph::query::TypedValue(*blocked_vertex)}});
        filter_expr = NEQ(inner_node, blocked);
      }

      input_operator = db->MakeKShortestOperator(
          source_symbol,
          sink_symbol,
          edges_symbol,
          direction,
          storage_edge_types,
          input_operator,
          true,
          nullptr,
          upper_bound == -1 ? nullptr : LITERAL(upper_bound),
          memgraph::query::plan::ExpansionLambda{inner_edge_symbol, inner_node_symbol, filter_expr},
          path_limit == -1 ? nullptr : LITERAL(path_limit));
      return PullResults(input_operator.get(),
                         &context,
                         std::vector<memgraph::query::Symbol>{source_symbol, sink_symbol, edges_symbol});
    };

    // The reference run: access checks only. Everything below derives from it, so nothing has to
    // model the checks - the endpoints are seeded unchecked, so a denied source or sink still yields.
    const auto baseline = run(false, -1);
    CheckPathsAndExtractLengths(&db_accessor, edges_in_result, baseline);
    if (fine_grained_test_type == FineGrainedTestType::ALL_DENIED) {
      EXPECT_EQ(baseline.size(), 0);
    } else {
      // `CheckPathsAndExtractLengths` passes vacuously on zero rows, and on the label arms the
      // assertions below degrade to a per-pair cap an empty result also satisfies.
      EXPECT_FALSE(baseline.empty());
    }

    // The lambda binds the arc head at every position but index 0, the seeded source, so it rejects
    // exactly the paths visiting the blocked vertex later. Sorted to compare as a multiset.
    std::vector<std::vector<int>> expected_paths;
    for (const auto &row : baseline) {
      auto ids = PathVertexIds(&db_accessor, row);
      if (blocked_vertex_id && std::find(ids.begin() + 1, ids.end(), *blocked_vertex_id) != ids.end()) continue;
      expected_paths.push_back(std::move(ids));
    }
    std::ranges::sort(expected_paths);

    const auto results = run(blocked_vertex_id.has_value(), limit);
    CheckPathsAndExtractLengths(&db_accessor, edges_after_lambda, results);

    std::vector<std::vector<int>> actual_paths;
    for (const auto &row : results) actual_paths.push_back(PathVertexIds(&db_accessor, row));
    std::ranges::sort(actual_paths);

    std::map<std::pair<int, int>, size_t> actual_per_pair;
    for (const auto &path : actual_paths) ++actual_per_pair[{path.front(), path.back()}];

    // Deriving from the reference run holds only while an arc's verdict is a property of the arc. It
    // is not when a *vertex* is denied: the check tests the endpoint away from the vertex being
    // expanded, so it is the arc's head on one pass and its tail on the other, and the search stops
    // as soon as one frontier empties. Denying an edge type is symmetric and stays predictable.
    const bool verdict_depends_on_search_direction = fine_grained_test_type == FineGrainedTestType::LABEL_0_DENIED ||
                                                     fine_grained_test_type == FineGrainedTestType::LABEL_3_DENIED;

    if (verdict_depends_on_search_direction) {
      if (limit != -1) {
        for (const auto &[pair, count] : actual_per_pair) {
          SCOPED_TRACE(fmt::format("source = {}, sink = {}", pair.first, pair.second));
          EXPECT_LE(count, static_cast<size_t>(limit));
        }
      }
    } else if (limit == -1) {
      // Without a limit the two runs must agree path for path, which is what pins over-blocking - a
      // subset check like `CheckPathsAndExtractLengths` cannot see a dropped path.
      EXPECT_EQ(actual_paths, expected_paths);
    } else {
      // With a limit, which of several equally long paths a pair keeps is undetermined, so check only
      // the cap, the total, and that every path also came out of the unlimited run.
      std::map<std::pair<int, int>, size_t> expected_per_pair;
      for (const auto &path : expected_paths) ++expected_per_pair[{path.front(), path.back()}];

      size_t expected_total = 0;
      for (const auto &[pair, count] : expected_per_pair) {
        expected_total += std::min(count, static_cast<size_t>(limit));
      }
      // Catches a pair the expansion dropped entirely, which the per-pair loop below cannot see.
      EXPECT_EQ(actual_paths.size(), expected_total);

      for (const auto &[pair, count] : actual_per_pair) {
        SCOPED_TRACE(fmt::format("source = {}, sink = {}", pair.first, pair.second));
        EXPECT_EQ(count, std::min(expected_per_pair[pair], static_cast<size_t>(limit)));
      }
      for (const auto &path : actual_paths) {
        EXPECT_TRUE(std::ranges::binary_search(expected_paths, path))
            << "Returned a path the unlimited run did not: " << memgraph::utils::JoinVector(path, "->");
      }
    }

    if (blocked_vertex_id && fine_grained_test_type == FineGrainedTestType::ALL_GRANTED) {
      // If the lambda pruned nothing, this arm would prove nothing about it.
      EXPECT_LT(results.size(), baseline.size());
    }

    db_accessor.Abort();
  }

  // The weighted arm under fine-grained access control. Unlike the hop-count search, this one asks
  // the same question of an arc whichever way it walks it - the check always tests the arc's head -
  // so an arc's verdict is a property of the arc and every arm, label denials included, can be
  // compared exactly against a brute-force enumeration over the arcs the permissions leave.
  void KShortestWeightedTestWithFineGrainedFiltering(Database *db, memgraph::query::EdgeAtom::Direction direction,
                                                     std::vector<std::string> edge_types,
                                                     FineGrainedTestType fine_grained_test_type) {
    auto storage_dba = db->Access();
    memgraph::query::DbAccessor dba(storage_dba.get());
    memgraph::query::ExecutionContext context{.db_accessor = &dba, .metric_handles = &TestMetricHandles()};

    auto [vertices, edges] = db->BuildGraph(&dba, kVertexLocations, kEdges);
    dba.AdvanceCommand();

    memgraph::auth::User user{"test"};
    ApplyFineGrainedArm(user, fine_grained_test_type, direction, edge_types);
    const auto readable_edges = WeightedReadableArcs(fine_grained_test_type, direction);
    // The weight lambda reads a property, and a user with no property rules at all is *restricted*,
    // not unrestricted - every lookup would come back null and this arm would prove nothing.
    // Granting them globally keeps it about the label and edge-type denials it is named for.
    user.property_access_handler().label_properties().GrantGlobal("*", memgraph::auth::kAllPropertyPermissionTypes);
    user.property_access_handler().edge_type_properties().GrantGlobal("*", memgraph::auth::kAllPropertyPermissionTypes);
    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    context.auth_checker = &auth_checker;

    // A denied vertex stays in the source and sink lists on purpose. Nothing reaches it, because
    // every arc into it is denied, but a search seeded on it still walks out of it unchecked - the
    // same pre-existing trait `*BFS` has - and `readable_edges` already says exactly that.

    std::vector<memgraph::storage::EdgeTypeId> storage_edge_types;
    for (const auto &t : edge_types) storage_edge_types.push_back(dba.NameToEdgeType(t));

    auto run = RunWeighted(db,
                           dba,
                           context,
                           vertices,
                           vertices,
                           direction,
                           storage_edge_types,
                           -1,
                           -1,
                           -1,
                           MakeWeightLambda(dba, context));

    std::map<std::pair<int, int>, std::vector<std::pair<int64_t, std::vector<int>>>> actual;
    for (const auto &row : run.rows) {
      const auto source_id = static_cast<int>(GetProp(row[0].ValueVertex(), "id", &dba).ValueInt());
      const auto sink_id = static_cast<int>(GetProp(row[1].ValueVertex(), "id", &dba).ValueInt());
      SCOPED_TRACE(fmt::format("source = {}, sink = {}", source_id, sink_id));
      CheckPath(&dba, row[0].ValueVertex(), row[1].ValueVertex(), row[2].ValueList(), readable_edges);
      actual[{source_id, sink_id}].emplace_back(row[3].ValueInt(), PathVertexIds(&dba, row));
    }

    size_t expected_total = 0;
    for (const auto &source : vertices) {
      for (const auto &sink : vertices) {
        const auto source_id = static_cast<int>(GetProp(source, "id", &dba).ValueInt());
        const auto sink_id = static_cast<int>(GetProp(sink, "id", &dba).ValueInt());
        if (source_id == sink_id) continue;
        SCOPED_TRACE(fmt::format("source = {}, sink = {}", source_id, sink_id));

        std::vector<std::pair<int64_t, std::vector<int>>> expected;
        for (const auto &path : WeightedSimplePaths(readable_edges, source_id, sink_id)) {
          expected.emplace_back(path.weight, path.vertices);
        }
        expected_total += expected.size();
        auto &rows = actual[{source_id, sink_id}];
        std::ranges::sort(expected);
        std::ranges::sort(rows);
        EXPECT_EQ(rows, expected);
      }
    }
    EXPECT_EQ(run.rows.size(), expected_total);
    if (fine_grained_test_type == FineGrainedTestType::ALL_DENIED) {
      EXPECT_TRUE(run.rows.empty());
    } else {
      // The comparison above passes vacuously on zero rows, so one arm proving nothing would be
      // invisible without this.
      EXPECT_FALSE(run.rows.empty());
    }

    dba.Abort();
  }

  // The access check must run before the lambda. Edge (5)-[:b]->(3) is the only one with `to` = 3
  // and type "b" is denied, so the lambda - which returns an integer there - must never see it.
  void KShortestTestAccessCheckBeforeFilterLambda(Database *db) {
    auto storage_dba = db->Access();
    memgraph::query::DbAccessor db_accessor(storage_dba.get());
    memgraph::query::ExecutionContext context{.db_accessor = &db_accessor, .metric_handles = &TestMetricHandles()};
    memgraph::query::Symbol source_symbol = context.symbol_table.CreateSymbol("source", true);
    memgraph::query::Symbol sink_symbol = context.symbol_table.CreateSymbol("sink", true);
    memgraph::query::Symbol edges_symbol = context.symbol_table.CreateSymbol("edges", true);
    memgraph::query::Symbol inner_node_symbol = context.symbol_table.CreateSymbol("inner_node", true);
    memgraph::query::Symbol inner_edge_symbol = context.symbol_table.CreateSymbol("inner_edge", true);
    memgraph::query::Identifier *inner_edge = IDENT("inner_edge")->MapTo(inner_edge_symbol);

    std::vector<memgraph::query::VertexAccessor> vertices;
    std::vector<memgraph::query::EdgeAccessor> edges;
    std::tie(vertices, edges) = db->BuildGraph(&db_accessor, kVertexLocations, kEdges);
    db_accessor.AdvanceCommand();

    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().GrantGlobal(memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant({"a"},
                                                                     memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Deny({"b"}, memgraph::auth::kAllEdgeTypePermissions);
    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &db_accessor};
    context.auth_checker = &auth_checker;

    std::shared_ptr<memgraph::query::plan::LogicalOperator> input_operator = nullptr;
    input_operator = YieldVertices(&db_accessor, vertices, source_symbol, input_operator);
    input_operator = YieldVertices(&db_accessor, vertices, sink_symbol, input_operator);

    auto *filter_expr = IF(EQ(PROPERTY_LOOKUP(db_accessor, inner_edge, PROPERTY_PAIR(db_accessor, "to")), LITERAL(3)),
                           LITERAL(42),
                           LITERAL(true));

    input_operator = db->MakeKShortestOperator(
        source_symbol,
        sink_symbol,
        edges_symbol,
        memgraph::query::EdgeAtom::Direction::OUT,
        {},
        input_operator,
        true,
        nullptr,
        nullptr,
        memgraph::query::plan::ExpansionLambda{inner_edge_symbol, inner_node_symbol, filter_expr});

    context.evaluation_context.properties = memgraph::query::NamesToProperties(storage.properties_, &db_accessor);
    context.evaluation_context.labels = memgraph::query::NamesToLabels(storage.labels_, &db_accessor);
    context.evaluation_context.edgetypes = memgraph::query::NamesToEdgeTypes(storage.edge_types_, &db_accessor);

    std::vector<std::vector<memgraph::query::TypedValue>> results;
    ASSERT_NO_THROW(results =
                        PullResults(input_operator.get(),
                                    &context,
                                    std::vector<memgraph::query::Symbol>{source_symbol, sink_symbol, edges_symbol}));

    EXPECT_FALSE(results.empty());
    CheckPathsAndExtractLengths(
        &db_accessor, GetEdgeList(kEdges, memgraph::query::EdgeAtom::Direction::OUT, {"a"}), results);

    db_accessor.Abort();
  }

  // The memo must not merge the two halves of the bidirectional search. Denied vertex 4 is the
  // target: the source side checks `To` = 4 and denies, the target side binds the same vertex but
  // checks `From` = 2 and allows, so a pass-blind memo replays the `false` and loses the path.
  // Relies on endpoints being seeded unchecked (pre-existing, shared with `*BFS`); if that is ever
  // fixed this returns zero rows and needs redesigning, not relaxing.
  void KShortestTestMemoDistinguishesSearchDirections(Database *db) {
    auto storage_dba = db->Access();
    memgraph::query::DbAccessor db_accessor(storage_dba.get());
    memgraph::query::ExecutionContext context{.db_accessor = &db_accessor, .metric_handles = &TestMetricHandles()};
    memgraph::query::Symbol source_symbol = context.symbol_table.CreateSymbol("source", true);
    memgraph::query::Symbol sink_symbol = context.symbol_table.CreateSymbol("sink", true);
    memgraph::query::Symbol edges_symbol = context.symbol_table.CreateSymbol("edges", true);
    memgraph::query::Symbol inner_node_symbol = context.symbol_table.CreateSymbol("inner_node", true);
    memgraph::query::Symbol inner_edge_symbol = context.symbol_table.CreateSymbol("inner_edge", true);

    std::vector<memgraph::query::VertexAccessor> vertices;
    std::vector<memgraph::query::EdgeAccessor> edges;
    std::tie(vertices, edges) = db->BuildGraph(&db_accessor, kVertexLocations, kEdges);
    db_accessor.AdvanceCommand();

    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().GrantGlobal(memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().GrantGlobal(memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Deny({"4"}, memgraph::auth::kAllLabelPermissions);
    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &db_accessor};
    context.auth_checker = &auth_checker;

    std::shared_ptr<memgraph::query::plan::LogicalOperator> input_operator = nullptr;
    input_operator = YieldVertices(&db_accessor, {vertices[2]}, source_symbol, input_operator);
    // Vertex 1 sits behind the denied vertex 4, so this also proves the denial is in effect.
    input_operator = YieldVertices(&db_accessor, {vertices[4], vertices[1]}, sink_symbol, input_operator);

    input_operator = db->MakeKShortestOperator(
        source_symbol,
        sink_symbol,
        edges_symbol,
        memgraph::query::EdgeAtom::Direction::OUT,
        {},
        input_operator,
        true,
        nullptr,
        nullptr,
        memgraph::query::plan::ExpansionLambda{inner_edge_symbol, inner_node_symbol, nullptr});

    context.evaluation_context.properties = memgraph::query::NamesToProperties(storage.properties_, &db_accessor);
    context.evaluation_context.labels = memgraph::query::NamesToLabels(storage.labels_, &db_accessor);
    context.evaluation_context.edgetypes = memgraph::query::NamesToEdgeTypes(storage.edge_types_, &db_accessor);

    auto results = PullResults(
        input_operator.get(), &context, std::vector<memgraph::query::Symbol>{source_symbol, sink_symbol, edges_symbol});

    ASSERT_FALSE(results.empty());
    // The one-hop (2)-[:b]->(4) must come out first.
    EXPECT_EQ(results[0][2].ValueList().size(), 1);
    // Nothing may reach vertex 1, which is only reachable through the denied vertex 4.
    for (const auto &row : results) EXPECT_EQ(row[1].ValueVertex(), vertices[4]);
  }
#endif

  // An inverted range is provably empty. Assert on the work done, not just the empty result:
  // unguarded, the top-up loop enumerates every simple path in the graph first.
  void KShortestTestInvertedRangeDoesNotSearch(Database *db) {
    auto storage_dba = db->Access();
    memgraph::query::DbAccessor dba(storage_dba.get());
    memgraph::query::ExecutionContext context{.db_accessor = &dba, .metric_handles = &TestMetricHandles()};
    memgraph::query::Symbol source_sym = context.symbol_table.CreateSymbol("source", true);
    memgraph::query::Symbol sink_sym = context.symbol_table.CreateSymbol("sink", true);
    memgraph::query::Symbol edges_sym = context.symbol_table.CreateSymbol("edges", true);
    memgraph::query::Symbol inner_node_sym = context.symbol_table.CreateSymbol("inner_node", true);
    memgraph::query::Symbol inner_edge_sym = context.symbol_table.CreateSymbol("inner_edge", true);

    std::vector<memgraph::query::VertexAccessor> vertices;
    std::vector<memgraph::query::EdgeAccessor> edges;
    std::tie(vertices, edges) = db->BuildGraph(&dba, kVertexLocations, kEdges);
    dba.AdvanceCommand();

    std::shared_ptr<memgraph::query::plan::LogicalOperator> input_op = nullptr;
    input_op = YieldVertices(&dba, vertices, source_sym, input_op);
    input_op = YieldVertices(&dba, vertices, sink_sym, input_op);

    auto input_operator =
        db->MakeKShortestOperator(source_sym,
                                  sink_sym,
                                  edges_sym,
                                  memgraph::query::EdgeAtom::Direction::OUT,
                                  {},
                                  input_op,
                                  true,
                                  LITERAL(kVertexCount + 1),
                                  LITERAL(kVertexCount),
                                  memgraph::query::plan::ExpansionLambda{inner_edge_sym, inner_node_sym, nullptr});

    context.evaluation_context.properties = memgraph::query::NamesToProperties(storage.properties_, &dba);
    context.evaluation_context.labels = memgraph::query::NamesToLabels(storage.labels_, &dba);
    context.evaluation_context.edgetypes = memgraph::query::NamesToEdgeTypes(storage.edge_types_, &dba);

    auto results = PullResults(
        input_operator.get(), &context, std::vector<memgraph::query::Symbol>{source_sym, sink_sym, edges_sym});

    EXPECT_TRUE(results.empty());
    EXPECT_EQ(context.number_of_hops, 0) << "An inverted range must be rejected before anything is expanded";

    dba.Abort();
  }

 protected:
  memgraph::query::AstStorage storage;
};

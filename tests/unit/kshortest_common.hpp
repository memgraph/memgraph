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
#include <limits>
#include <optional>
#include <queue>

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

// Removes everything the filter lambda blocks, so the Yen oracle runs on the subgraph the expansion
// sees: the blocked edge itself, or every edge leading into the blocked vertex.
// Matches a blocked edge by `(from, to)`, not gid, so keep `kEdges` free of parallel edges.
std::vector<std::pair<int, int>> GetFilteredEdgeList(const memgraph::query::TypedValue &blocked,
                                                     memgraph::query::EdgeAtom::Direction direction,
                                                     const std::vector<std::string> &edge_types,
                                                     memgraph::query::DbAccessor *dba) {
  auto edges = kEdges;
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
// of its edges exist in the given edge list. Also ensures no edge is reused.
template <class TPathAllocator>
void CheckPath(memgraph::query::DbAccessor *dba, const memgraph::query::VertexAccessor &source,
               const memgraph::query::VertexAccessor &sink,
               const std::vector<memgraph::query::TypedValue, TPathAllocator> &path,
               const std::vector<std::pair<int, int>> &edges) {
  if (path.empty()) return;
  memgraph::query::VertexAccessor curr = source;
  std::unordered_set<memgraph::storage::Gid> used_edges;

  for (const auto &edge_tv : path) {
    ASSERT_TRUE(edge_tv.IsEdge());
    auto edge = edge_tv.ValueEdge();

    // Check that this edge hasn't been used before
    ASSERT_TRUE(used_edges.insert(edge.Gid()).second) << "Edge " << edge.Gid() << " is reused in path";

    ASSERT_TRUE(edge.From() == curr || edge.To() == curr);
    auto next = edge.From() == curr ? edge.To() : edge.From();

    int from = GetProp(curr, "id", dba).ValueInt();
    int to = GetProp(next, "id", dba).ValueInt();
    ASSERT_TRUE(std::ranges::contains(edges, std::make_pair(from, to)))
        << "Edge " << from << "->" << to << " not found in edge list";

    curr = next;
  }
  ASSERT_EQ(curr, sink);
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

// Common interface for single-node and distributed Memgraph.
class Database {
 public:
  virtual std::unique_ptr<memgraph::storage::Storage::Accessor> Access() = 0;
  virtual std::unique_ptr<memgraph::query::plan::LogicalOperator> MakeKShortestOperator(
      memgraph::query::Symbol source_sym, memgraph::query::Symbol sink_sym, memgraph::query::Symbol edge_sym,
      memgraph::query::EdgeAtom::Direction direction, const std::vector<memgraph::storage::EdgeTypeId> &edge_types,
      const std::shared_ptr<memgraph::query::plan::LogicalOperator> &input, bool existing_node,
      memgraph::query::Expression *lower_bound, memgraph::query::Expression *upper_bound,
      const memgraph::query::plan::ExpansionLambda &filter_lambda, memgraph::query::Expression *limit = nullptr) = 0;
  virtual std::pair<std::vector<memgraph::query::VertexAccessor>, std::vector<memgraph::query::EdgeAccessor>>
  BuildGraph(memgraph::query::DbAccessor *dba, const std::vector<int> &vertex_locations,
             const std::vector<std::tuple<int, int, std::string>> &edges) = 0;
  virtual ~Database() = default;

  void KShortestTest(Database *db, int lower_bound, int upper_bound, memgraph::query::EdgeAtom::Direction direction,
                     std::vector<std::string> edge_types, int limit = -1,
                     FilterLambdaType filter_lambda_type = FilterLambdaType::NONE) {
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

    std::tie(vertices, edges) = db->BuildGraph(&dba, kVertexLocations, kEdges);
    spdlog::info("KShortestTest: Built graph with {} vertices and {} edges", vertices.size(), edges.size());

    dba.AdvanceCommand();

    std::shared_ptr<memgraph::query::plan::LogicalOperator> input_op;
    memgraph::query::Expression *filter_expr = nullptr;
    // The entities yielded into `blocked`, in the order the expansion will see them.
    std::vector<memgraph::query::TypedValue> blocked_values;

    // First build a filter lambda and an operator yielding blocked entities.
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

    if (upper_bound == -1) upper_bound = kVertexCount;
    const int effective_lower_bound = lower_bound != -1 ? lower_bound : 1;
    const int effective_upper_bound = upper_bound;

    // The paths Yen's algorithm finds in the subgraph left over by the filter lambda, respecting
    // the length bounds.
    auto correct_paths_for = [&](const memgraph::query::TypedValue &blocked_entity, int source_id, int sink_id) {
      auto paths = YenKShortestPaths(
          kVertexCount, GetFilteredEdgeList(blocked_entity, direction, edge_types, &dba), source_id, sink_id);
      // `paths` holds vertices, not edges, hence the -1 when comparing against the bounds.
      std::erase_if(paths, [&](const std::vector<int> &path) {
        return path.size() - 1 < static_cast<size_t>(effective_lower_bound) ||
               path.size() - 1 > static_cast<size_t>(effective_upper_bound);
      });
      return paths;
    };

    // A wholly missing group is invisible to the per-group loop below, so the total has to catch
    // it. Must be asserted with a limit too: the limit caps each group, never removes one.
    {
      size_t expected_total = 0;
      for (const auto &blocked_entity : blocked_values) {
        for (int source_id = 0; source_id < kVertexCount; ++source_id) {
          for (int sink_id = 0; sink_id < kVertexCount; ++sink_id) {
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

      auto edges_filtered = GetFilteredEdgeList(blocked_entity, direction, edge_types, &dba);
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

#ifdef MG_ENTERPRISE
  // `blocked_vertex_id` additionally blocks every edge leading into that vertex with a filter
  // lambda, so the expansion has to honour the access checks and the lambda at the same time.
  void KShortestTestWithFineGrainedFiltering(Database *db, int upper_bound,
                                             memgraph::query::EdgeAtom::Direction direction,
                                             std::vector<std::string> edge_types, int k,
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
        user.fine_grained_access_handler().edge_type_permissions().GrantGlobal(
            memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant({"1"},
                                                                     memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant({"2"},
                                                                     memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant({"3"},
                                                                     memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant({"4"},
                                                                     memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Deny({"0"}, memgraph::auth::kAllLabelPermissions);

        edges_in_result = GetEdgeList(kEdges, direction, {"a", "b"});
        edges_in_result.erase(
            std::remove_if(edges_in_result.begin(), edges_in_result.end(), [](const auto &e) { return e.second == 0; }),
            edges_in_result.end());
        break;
      case FineGrainedTestType::LABEL_3_DENIED:
        user.fine_grained_access_handler().edge_type_permissions().GrantGlobal(
            memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant({"0"},
                                                                     memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant({"1"},
                                                                     memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant({"2"},
                                                                     memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant({"4"},
                                                                     memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Deny({"3"}, memgraph::auth::kAllLabelPermissions);

        edges_in_result = GetEdgeList(kEdges, direction, {"a", "b"});
        edges_in_result.erase(
            std::remove_if(edges_in_result.begin(), edges_in_result.end(), [](const auto &e) { return e.second == 3; }),
            edges_in_result.end());
        break;
    }

    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &db_accessor};
    context.auth_checker = &auth_checker;

    // We run k-shortest paths for all possible source-sink pairs
    std::shared_ptr<memgraph::query::plan::LogicalOperator> input_operator = nullptr;
    if (fine_grained_test_type == FineGrainedTestType::LABEL_0_DENIED) {
      vertices.erase(std::remove_if(vertices.begin(),
                                    vertices.end(),
                                    [&](const auto &v) { return GetProp(v, "id", &db_accessor).ValueInt() == 0; }),
                     vertices.end());
    }
    input_operator = YieldVertices(&db_accessor, vertices, source_symbol, input_operator);
    input_operator = YieldVertices(&db_accessor, vertices, sink_symbol, input_operator);

    std::vector<memgraph::storage::EdgeTypeId> storage_edge_types;
    for (const auto &t : edge_types) {
      storage_edge_types.push_back(db_accessor.NameToEdgeType(t));
    }

    memgraph::query::Expression *filter_expr = nullptr;
    if (blocked_vertex_id) {
      // Compare vertex identities against an outer frame symbol, not `inner_node.id`: a property
      // lookup built here evaluates to null, which left this whole matrix passing on zero rows.
      const auto blocked_vertex = std::ranges::find_if(
          vertices, [&](const auto &v) { return GetProp(v, "id", &db_accessor).ValueInt() == *blocked_vertex_id; });
      MG_ASSERT(blocked_vertex != vertices.end(), "The blocked vertex must be part of the fixture");
      input_operator = std::make_shared<Yield>(
          input_operator,
          std::vector<memgraph::query::Symbol>{blocked_symbol},
          std::vector<std::vector<memgraph::query::TypedValue>>{{memgraph::query::TypedValue(*blocked_vertex)}});
      filter_expr = NEQ(inner_node, blocked);
      std::erase_if(edges_in_result, [id = *blocked_vertex_id](const auto &e) { return e.second == id; });
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
        memgraph::query::plan::ExpansionLambda{inner_edge_symbol, inner_node_symbol, filter_expr});

    context.evaluation_context.properties = memgraph::query::NamesToProperties(storage.properties_, &db_accessor);
    context.evaluation_context.labels = memgraph::query::NamesToLabels(storage.labels_, &db_accessor);
    context.evaluation_context.edgetypes = memgraph::query::NamesToEdgeTypes(storage.edge_types_, &db_accessor);
    std::vector<std::vector<memgraph::query::TypedValue>> results;

    results = PullResults(
        input_operator.get(), &context, std::vector<memgraph::query::Symbol>{source_symbol, sink_symbol, edges_symbol});

    switch (fine_grained_test_type) {
      case FineGrainedTestType::ALL_GRANTED:
        // `CheckPathsAndExtractLengths` only validates the paths that came back, so it passes
        // vacuously on zero rows. With everything granted a path always survives, so pin that.
        // The denied arms below can legitimately be empty.
        EXPECT_FALSE(results.empty());
        CheckPathsAndExtractLengths(&db_accessor, edges_in_result, results);
        break;
      case FineGrainedTestType::ALL_DENIED:
        EXPECT_EQ(results.size(), 0);
        break;
      case FineGrainedTestType::EDGE_TYPE_A_DENIED:
      case FineGrainedTestType::EDGE_TYPE_B_DENIED:
      case FineGrainedTestType::LABEL_0_DENIED:
      case FineGrainedTestType::LABEL_3_DENIED:
        CheckPathsAndExtractLengths(&db_accessor, edges_in_result, results);
        break;
    }

    db_accessor.Abort();
  }

  // The access check must run before the lambda. Edge (5)-[:b]->(3) is the only one with `to` = 3
  // and type "b" is denied, so the lambda - which returns an integer exactly there - must never
  // run on it. The operator gets no edge type filter, so storage really does hand it that edge.
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

  // The memo must not merge the two halves of the bidirectional search: they check access on
  // opposite endpoints of the same edge while binding the same vertex as the lambda's node.
  // Denied vertex 4 is the target and (2)-[:b]->(4) runs source to target. The source side is
  // denied on `To` = 4; the target side binds the same vertex 4 but checks `From` = 2 and allows.
  // A memo blind to the pass replays the `false` and loses the one-hop path.
  // Depends on endpoints being seeded without an access check (pre-existing, shared with `*BFS`).
  // If that gap is closed this returns zero rows and needs redesigning, not relaxing.
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
    // Vertex 1 is only reachable from vertex 2 through the denied vertex 4, so it doubles as a
    // check that the denial is in effect at all.
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

 protected:
  memgraph::query::AstStorage storage;
};

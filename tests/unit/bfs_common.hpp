// Copyright 2024 Memgraph Ltd.
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

#include "gtest/gtest.h"

#include "auth/models.hpp"
#include "glue/auth_checker.hpp"
#include "query/context.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"
#include "query_common.hpp"

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
const std::vector<std::tuple<int, int, std::string>> kEdges = {{0, 1, "a"}, {1, 2, "b"}, {2, 4, "b"},
                                                               {2, 5, "a"}, {4, 1, "a"}, {4, 5, "a"},
                                                               {5, 3, "b"}, {5, 4, "a"}, {5, 5, "b"}};

// Filters input edge list by edge type and direction and returns a list of
// pairs representing valid directed edges.
std::vector<std::pair<int, int>> GetEdgeList(const std::vector<std::tuple<int, int, std::string>> &edges,
                                             memgraph::query::EdgeAtom::Direction dir,
                                             const std::vector<std::string> &edge_types) {
  std::vector<std::pair<int, int>> ret;
  for (const auto &e : edges) {
    if (edge_types.empty() || memgraph::utils::Contains(edge_types, std::get<2>(e)))
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

// Floyd-Warshall algorithm. Given a graph, returns its distance matrix. If
// there is no path between two vertices, corresponding matrix entry will be
// -1.
std::vector<std::vector<int>> FloydWarshall(int num_vertices, const std::vector<std::pair<int, int>> &edges) {
  int inf = std::numeric_limits<int>::max();
  std::vector<std::vector<int>> dist(num_vertices, std::vector<int>(num_vertices, inf));

  for (const auto &e : edges) dist[e.first][e.second] = 1;
  for (int i = 0; i < num_vertices; ++i) dist[i][i] = 0;

  for (int k = 0; k < num_vertices; ++k) {
    for (int i = 0; i < num_vertices; ++i) {
      for (int j = 0; j < num_vertices; ++j) {
        if (dist[i][k] == inf || dist[k][j] == inf) continue;
        dist[i][j] = std::min(dist[i][j], dist[i][k] + dist[k][j]);
      }
    }
  }

  for (int i = 0; i < num_vertices; ++i)
    for (int j = 0; j < num_vertices; ++j)
      if (dist[i][j] == inf) dist[i][j] = -1;

  return dist;
}

class Yield : public memgraph::query::plan::LogicalOperator {
 public:
  Yield(const std::shared_ptr<memgraph::query::plan::LogicalOperator> &input,
        const std::vector<memgraph::query::Symbol> &modified_symbols,
        const std::vector<std::vector<memgraph::query::TypedValue>> &values)
      : input_(input ? input : std::make_shared<memgraph::query::plan::Once>()),
        modified_symbols_(modified_symbols),
        values_(values) {}

  memgraph::query::plan::UniqueCursorPtr MakeCursor(memgraph::utils::MemoryResource *mem) const override {
    return memgraph::query::plan::MakeUniqueCursorPtr<YieldCursor>(mem, this, input_->MakeCursor(mem));
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
        frame[self_->modified_symbols_[i]] = self_->values_[pull_index_][i];
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
  auto cursor = last_op->MakeCursor(memgraph::utils::NewDeleteResource());
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

/* Various types of lambdas.
 * NONE           - No filter lambda used.
 * USE_FRAME      - Block a single edge or vertex. Tests if frame is sent over
 *                  the network properly in distributed BFS.
 * USE_FRAME_NULL - Block a single node or vertex, but lambda returns null
 *                  instead of false.
 * USE_CTX -        Block a vertex by checking if its ID is equal to a
 *                  parameter. Tests if evaluation context is sent over the
 *                  network properly in distributed BFS.
 * ERROR -          Lambda that evaluates to an integer instead of null or
 *                  boolean.In distributed BFS, it will fail on worker other
 *                  than master, to test if errors are propagated correctly.
 */

enum class FilterLambdaType { NONE, USE_FRAME, USE_FRAME_NULL, USE_CTX, ERROR };

enum class FineGrainedTestType {
  ALL_GRANTED,
  ALL_DENIED,
  EDGE_TYPE_A_DENIED,
  EDGE_TYPE_B_DENIED,
  LABEL_0_DENIED,
  LABEL_3_DENIED
};

// Returns an operator that yields vertices given by their address. We will also
// include memgraph::query::TypedValue() to account for the optional match case.
std::unique_ptr<memgraph::query::plan::LogicalOperator> YieldVertices(
    memgraph::query::DbAccessor *dba, std::vector<memgraph::query::VertexAccessor> vertices,
    memgraph::query::Symbol symbol, std::shared_ptr<memgraph::query::plan::LogicalOperator> input_op) {
  std::vector<std::vector<memgraph::query::TypedValue>> frames;
  frames.push_back(std::vector<memgraph::query::TypedValue>{memgraph::query::TypedValue()});
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

// Checks if the given path is actually a path from source to sink and if all
// of its edges exist in the given edge list.
template <class TPathAllocator>
void CheckPath(memgraph::query::DbAccessor *dba, const memgraph::query::VertexAccessor &source,
               const memgraph::query::VertexAccessor &sink,
               const std::vector<memgraph::query::TypedValue, TPathAllocator> &path,
               const std::vector<std::pair<int, int>> &edges) {
  auto curr = source;
  for (const auto &edge_tv : path) {
    ASSERT_TRUE(edge_tv.IsEdge());
    auto edge = edge_tv.ValueEdge();

    ASSERT_TRUE(edge.From() == curr || edge.To() == curr);
    auto next = edge.From() == curr ? edge.To() : edge.From();

    int from = GetProp(curr, "id", dba).ValueInt();
    int to = GetProp(next, "id", dba).ValueInt();
    ASSERT_TRUE(memgraph::utils::Contains(edges, std::make_pair(from, to)));

    curr = next;
  }
  ASSERT_EQ(curr, sink);
}

// Given a list of BFS results of form (from, to, path, blocked entity),
// checks if all paths are valid and returns the distance matrix.
std::vector<std::vector<int>> CheckPathsAndExtractDistances(
    memgraph::query::DbAccessor *dba, const std::vector<std::pair<int, int>> edges,
    const std::vector<std::vector<memgraph::query::TypedValue>> &results) {
  std::vector<std::vector<int>> distances(kVertexCount, std::vector<int>(kVertexCount, -1));

  for (size_t i = 0; i < kVertexCount; ++i) distances[i][i] = 0;

  for (const auto &row : results) {
    auto source = GetProp(row[0].ValueVertex(), "id", dba).ValueInt();
    auto sink = GetProp(row[1].ValueVertex(), "id", dba).ValueInt();
    distances[source][sink] = row[2].ValueList().size();
    CheckPath(dba, row[0].ValueVertex(), row[1].ValueVertex(), row[2].ValueList(), edges);
  }
  return distances;
}

// Common interface for single-node and distributed Memgraph.
class Database {
 public:
  virtual std::unique_ptr<memgraph::storage::Storage::Accessor> Access() = 0;
  virtual std::unique_ptr<memgraph::query::plan::LogicalOperator> MakeBfsOperator(
      memgraph::query::Symbol source_sym, memgraph::query::Symbol sink_sym, memgraph::query::Symbol edge_sym,
      memgraph::query::EdgeAtom::Direction direction, const std::vector<memgraph::storage::EdgeTypeId> &edge_types,
      const std::shared_ptr<memgraph::query::plan::LogicalOperator> &input, bool existing_node,
      memgraph::query::Expression *lower_bound, memgraph::query::Expression *upper_bound,
      const memgraph::query::plan::ExpansionLambda &filter_lambda) = 0;
  virtual std::pair<std::vector<memgraph::query::VertexAccessor>, std::vector<memgraph::query::EdgeAccessor>>
  BuildGraph(memgraph::query::DbAccessor *dba, const std::vector<int> &vertex_locations,
             const std::vector<std::tuple<int, int, std::string>> &edges) = 0;
  virtual ~Database() = default;

  void BfsTest(Database *db, int lower_bound, int upper_bound, memgraph::query::EdgeAtom::Direction direction,
               std::vector<std::string> edge_types, bool known_sink, FilterLambdaType filter_lambda_type) {
    auto storage_dba = db->Access();
    memgraph::query::DbAccessor dba(storage_dba.get());
    memgraph::query::ExecutionContext context{.db_accessor = &dba};
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

    dba.AdvanceCommand();

    std::shared_ptr<memgraph::query::plan::LogicalOperator> input_op;

    memgraph::query::Expression *filter_expr;

    // First build a filter lambda and an operator yielding blocked entities.
    switch (filter_lambda_type) {
      case FilterLambdaType::NONE:
        // No filter lambda, nothing is ever blocked.
        input_op = std::make_shared<Yield>(
            nullptr, std::vector<memgraph::query::Symbol>{blocked_sym},
            std::vector<std::vector<memgraph::query::TypedValue>>{{memgraph::query::TypedValue()}});
        filter_expr = nullptr;
        break;
      case FilterLambdaType::USE_FRAME:
        // We block each entity in the graph and run BFS.
        input_op = YieldEntities(&dba, vertices, edges, blocked_sym, nullptr);
        filter_expr = AND(NEQ(inner_node, blocked), NEQ(inner_edge, blocked));
        break;
      case FilterLambdaType::USE_FRAME_NULL:
        // We block each entity in the graph and run BFS.
        input_op = YieldEntities(&dba, vertices, edges, blocked_sym, nullptr);
        filter_expr = IF(AND(NEQ(inner_node, blocked), NEQ(inner_edge, blocked)), LITERAL(true),
                         LITERAL(memgraph::storage::PropertyValue()));
        break;
      case FilterLambdaType::USE_CTX:
        // We only block vertex #5 and run BFS.
        input_op = std::make_shared<Yield>(
            nullptr, std::vector<memgraph::query::Symbol>{blocked_sym},
            std::vector<std::vector<memgraph::query::TypedValue>>{{memgraph::query::TypedValue(vertices[5])}});
        filter_expr = NEQ(PROPERTY_LOOKUP(dba, inner_node, PROPERTY_PAIR(dba, "id")), PARAMETER_LOOKUP(0));
        context.evaluation_context.parameters.Add(0, memgraph::storage::PropertyValue(5));
        break;
      case FilterLambdaType::ERROR:
        // Evaluate to 42 for vertex #5 which is on worker 1.
        filter_expr =
            IF(EQ(PROPERTY_LOOKUP(dba, inner_node, PROPERTY_PAIR(dba, "id")), LITERAL(5)), LITERAL(42), LITERAL(true));
    }

    // We run BFS once from each vertex for each blocked entity.
    input_op = YieldVertices(&dba, vertices, source_sym, input_op);

    // If the sink is known, we run BFS for all posible combinations of source,
    // sink and blocked entity.
    if (known_sink) {
      input_op = YieldVertices(&dba, vertices, sink_sym, input_op);
    }

    std::vector<memgraph::storage::EdgeTypeId> storage_edge_types;
    for (const auto &t : edge_types) {
      storage_edge_types.push_back(dba.NameToEdgeType(t));
    }

    input_op = db->MakeBfsOperator(source_sym, sink_sym, edges_sym, direction, storage_edge_types, input_op, known_sink,
                                   lower_bound == -1 ? nullptr : LITERAL(lower_bound),
                                   upper_bound == -1 ? nullptr : LITERAL(upper_bound),
                                   memgraph::query::plan::ExpansionLambda{inner_edge_sym, inner_node_sym, filter_expr});

    context.evaluation_context.properties = memgraph::query::NamesToProperties(storage.properties_, &dba);
    context.evaluation_context.labels = memgraph::query::NamesToLabels(storage.labels_, &dba);
    std::vector<std::vector<memgraph::query::TypedValue>> results;

    // An exception should be thrown on one of the pulls.
    if (filter_lambda_type == FilterLambdaType::ERROR) {
      EXPECT_THROW(PullResults(input_op.get(), &context,
                               std::vector<memgraph::query::Symbol>{source_sym, sink_sym, edges_sym, blocked_sym}),
                   memgraph::query::QueryRuntimeException);
      return;
    }

    results = PullResults(input_op.get(), &context,
                          std::vector<memgraph::query::Symbol>{source_sym, sink_sym, edges_sym, blocked_sym});

    // Group results based on blocked entity and compare them to results
    // obtained by running Floyd-Warshall.
    for (size_t i = 0; i < results.size();) {
      int j = i;
      auto blocked = results[j][3];
      while (j < results.size() && memgraph::query::TypedValue::BoolEqual{}(results[j][3], blocked)) ++j;

      SCOPED_TRACE(fmt::format("blocked entity = {}", ToString(blocked, dba)));

      // When an edge is blocked, it is blocked in both directions so we remove
      // it before modifying edge list to account for direction and edge types;
      auto edges = kEdges;
      if (blocked.IsEdge()) {
        int from = GetProp(blocked.ValueEdge(), "from", &dba).ValueInt();
        int to = GetProp(blocked.ValueEdge(), "to", &dba).ValueInt();
        edges.erase(
            std::remove_if(edges.begin(), edges.end(),
                           [from, to](const auto &e) { return std::get<0>(e) == from && std::get<1>(e) == to; }),
            edges.end());
      }

      // Now add edges in opposite direction if necessary.
      auto edges_blocked = GetEdgeList(edges, direction, edge_types);

      // When a vertex is blocked, we remove all edges that lead into it.
      if (blocked.IsVertex()) {
        int id = GetProp(blocked.ValueVertex(), "id", &dba).ValueInt();
        edges_blocked.erase(
            std::remove_if(edges_blocked.begin(), edges_blocked.end(), [id](const auto &e) { return e.second == id; }),
            edges_blocked.end());
      }

      auto correct_with_bounds = FloydWarshall(kVertexCount, edges_blocked);

      if (lower_bound == -1) lower_bound = 0;
      if (upper_bound == -1) upper_bound = kVertexCount;

      // Remove paths whose length doesn't satisfy given length bounds.
      for (int a = 0; a < kVertexCount; ++a) {
        for (int b = 0; b < kVertexCount; ++b) {
          if (a != b && (correct_with_bounds[a][b] < lower_bound || correct_with_bounds[a][b] > upper_bound))
            correct_with_bounds[a][b] = -1;
        }
      }

      int num_results = 0;
      for (int a = 0; a < kVertexCount; ++a)
        for (int b = 0; b < kVertexCount; ++b)
          if (a != b && correct_with_bounds[a][b] != -1) {
            ++num_results;
          }
      // There should be exactly 1 successful pull for each existing path.
      EXPECT_EQ(j - i, num_results);

      auto distances = CheckPathsAndExtractDistances(
          &dba, edges_blocked,
          std::vector<std::vector<memgraph::query::TypedValue>>(results.begin() + i, results.begin() + j));

      // The distances should also match.
      EXPECT_EQ(distances, correct_with_bounds);

      i = j;
    }

    dba.Abort();
  }

#ifdef MG_ENTERPRISE
  void BfsTestWithFineGrainedFiltering(Database *db, int lower_bound, int upper_bound,
                                       memgraph::query::EdgeAtom::Direction direction,
                                       std::vector<std::string> edge_types, bool known_sink,
                                       FineGrainedTestType fine_grained_test_type) {
    auto storage_dba = db->Access();
    memgraph::query::DbAccessor db_accessor(storage_dba.get());
    memgraph::query::ExecutionContext context{.db_accessor = &db_accessor};
    memgraph::query::Symbol blocked_symbol = context.symbol_table.CreateSymbol("blocked", true);
    memgraph::query::Symbol source_symbol = context.symbol_table.CreateSymbol("source", true);
    memgraph::query::Symbol sink_symbol = context.symbol_table.CreateSymbol("sink", true);
    memgraph::query::Symbol edges_symbol = context.symbol_table.CreateSymbol("edges", true);
    memgraph::query::Symbol inner_node_symbol = context.symbol_table.CreateSymbol("inner_node", true);
    memgraph::query::Symbol inner_edge_symbol = context.symbol_table.CreateSymbol("inner_edge", true);

    std::vector<memgraph::query::VertexAccessor> vertices;
    std::vector<memgraph::query::EdgeAccessor> edges;

    std::tie(vertices, edges) = db->BuildGraph(&db_accessor, kVertexLocations, kEdges);

    db_accessor.AdvanceCommand();

    std::shared_ptr<memgraph::query::plan::LogicalOperator> input_operator;

    memgraph::query::Expression *filter_expr = nullptr;

    input_operator =
        std::make_shared<Yield>(nullptr, std::vector<memgraph::query::Symbol>{blocked_symbol},
                                std::vector<std::vector<memgraph::query::TypedValue>>{{memgraph::query::TypedValue()}});

    memgraph::auth::User user{"test"};
    std::vector<std::pair<int, int>> edges_in_result;
    switch (fine_grained_test_type) {
      case FineGrainedTestType::ALL_GRANTED:
        user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                         memgraph::auth::FineGrainedPermission::READ);
        edges_in_result = GetEdgeList(kEdges, direction, {"a", "b"});
        break;
      case FineGrainedTestType::ALL_DENIED:
        break;
      case FineGrainedTestType::EDGE_TYPE_A_DENIED:
        user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().edge_type_permissions().Grant("b",
                                                                         memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().edge_type_permissions().Grant(
            "a", memgraph::auth::FineGrainedPermission::NOTHING);

        edges_in_result = GetEdgeList(kEdges, direction, {"b"});
        break;
      case FineGrainedTestType::EDGE_TYPE_B_DENIED:
        user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().edge_type_permissions().Grant("a",
                                                                         memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().edge_type_permissions().Grant(
            "b", memgraph::auth::FineGrainedPermission::NOTHING);

        edges_in_result = GetEdgeList(kEdges, direction, {"a"});
        break;
      case FineGrainedTestType::LABEL_0_DENIED:
        user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                         memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant("1", memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant("2", memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant("3", memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant("4", memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant("0",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);

        edges_in_result = GetEdgeList(kEdges, direction, {"a", "b"});
        edges_in_result.erase(
            std::remove_if(edges_in_result.begin(), edges_in_result.end(), [](const auto &e) { return e.second == 0; }),
            edges_in_result.end());
        break;
      case FineGrainedTestType::LABEL_3_DENIED:
        user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                         memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant("0", memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant("1", memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant("2", memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant("4", memgraph::auth::FineGrainedPermission::READ);
        user.fine_grained_access_handler().label_permissions().Grant("3",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);

        edges_in_result = GetEdgeList(kEdges, direction, {"a", "b"});
        edges_in_result.erase(
            std::remove_if(edges_in_result.begin(), edges_in_result.end(), [](const auto &e) { return e.second == 3; }),
            edges_in_result.end());
        break;
    }

    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &db_accessor};
    context.auth_checker = std::make_unique<memgraph::glue::FineGrainedAuthChecker>(std::move(auth_checker));
    // We run BFS once from each vertex for each blocked entity.
    input_operator = YieldVertices(&db_accessor, vertices, source_symbol, input_operator);

    // If the sink is known, we run BFS for all posible combinations of source,
    // sink and blocked entity.
    if (known_sink) {
      input_operator = YieldVertices(&db_accessor, vertices, sink_symbol, input_operator);
    }

    std::vector<memgraph::storage::EdgeTypeId> storage_edge_types;
    for (const auto &t : edge_types) {
      storage_edge_types.push_back(db_accessor.NameToEdgeType(t));
    }

    input_operator = db->MakeBfsOperator(
        source_symbol, sink_symbol, edges_symbol, direction, storage_edge_types, input_operator, known_sink,
        lower_bound == -1 ? nullptr : LITERAL(lower_bound), upper_bound == -1 ? nullptr : LITERAL(upper_bound),
        memgraph::query::plan::ExpansionLambda{inner_edge_symbol, inner_node_symbol, filter_expr});

    context.evaluation_context.properties = memgraph::query::NamesToProperties(storage.properties_, &db_accessor);
    context.evaluation_context.labels = memgraph::query::NamesToLabels(storage.labels_, &db_accessor);
    std::vector<std::vector<memgraph::query::TypedValue>> results;

    results =
        PullResults(input_operator.get(), &context,
                    std::vector<memgraph::query::Symbol>{source_symbol, sink_symbol, edges_symbol, blocked_symbol});

    switch (fine_grained_test_type) {
      case FineGrainedTestType::ALL_GRANTED:
        switch (direction) {
          case memgraph::query::EdgeAtom::Direction::IN:
            CheckPathsAndExtractDistances(
                &db_accessor, edges_in_result,
                std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            break;
          case memgraph::query::EdgeAtom::Direction::OUT:
            CheckPathsAndExtractDistances(
                &db_accessor, edges_in_result,
                std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            break;
          case memgraph::query::EdgeAtom::Direction::BOTH:
            CheckPathsAndExtractDistances(
                &db_accessor, edges_in_result,
                std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            break;
        }
        break;
      case FineGrainedTestType::ALL_DENIED:
        switch (direction) {
          case memgraph::query::EdgeAtom::Direction::IN:
            EXPECT_EQ(results.size(), 0);
            break;
          case memgraph::query::EdgeAtom::Direction::OUT:
            EXPECT_EQ(results.size(), 0);
            break;
          case memgraph::query::EdgeAtom::Direction::BOTH:
            EXPECT_EQ(results.size(), 0);
            break;
        }
        break;
      case FineGrainedTestType::EDGE_TYPE_A_DENIED:
        switch (direction) {
          case memgraph::query::EdgeAtom::Direction::IN:
            CheckPathsAndExtractDistances(
                &db_accessor, edges_in_result,
                std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            break;
          case memgraph::query::EdgeAtom::Direction::OUT:
            CheckPathsAndExtractDistances(
                &db_accessor, edges_in_result,
                std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            break;
          case memgraph::query::EdgeAtom::Direction::BOTH:
            CheckPathsAndExtractDistances(
                &db_accessor, edges_in_result,
                std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            break;
        }
        break;
      case FineGrainedTestType::EDGE_TYPE_B_DENIED:
        switch (direction) {
          case memgraph::query::EdgeAtom::Direction::IN:
            CheckPathsAndExtractDistances(
                &db_accessor, edges_in_result,
                std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            break;
          case memgraph::query::EdgeAtom::Direction::OUT:
            CheckPathsAndExtractDistances(
                &db_accessor, edges_in_result,
                std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            break;
          case memgraph::query::EdgeAtom::Direction::BOTH:
            CheckPathsAndExtractDistances(
                &db_accessor, edges_in_result,
                std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            break;
        }
        break;
      case FineGrainedTestType::LABEL_0_DENIED:
        switch (direction) {
          case memgraph::query::EdgeAtom::Direction::IN:
            if (known_sink) {
              CheckPathsAndExtractDistances(
                  &db_accessor, edges_in_result,
                  std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            } else {
              CheckPathsAndExtractDistances(
                  &db_accessor, edges_in_result,
                  std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            }
            break;
          case memgraph::query::EdgeAtom::Direction::OUT:
            if (known_sink) {
              CheckPathsAndExtractDistances(
                  &db_accessor, edges_in_result,
                  std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            } else {
              CheckPathsAndExtractDistances(
                  &db_accessor, edges_in_result,
                  std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            }
            break;
          case memgraph::query::EdgeAtom::Direction::BOTH:
            if (known_sink) {
              CheckPathsAndExtractDistances(
                  &db_accessor, edges_in_result,
                  std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            } else {
              CheckPathsAndExtractDistances(
                  &db_accessor, edges_in_result,
                  std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            }
            break;
        }
        break;
      case FineGrainedTestType::LABEL_3_DENIED:
        switch (direction) {
          case memgraph::query::EdgeAtom::Direction::IN:
            if (known_sink) {
              CheckPathsAndExtractDistances(
                  &db_accessor, edges_in_result,
                  std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            } else {
              CheckPathsAndExtractDistances(
                  &db_accessor, edges_in_result,
                  std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            }
            break;
          case memgraph::query::EdgeAtom::Direction::OUT:
            if (known_sink) {
              CheckPathsAndExtractDistances(
                  &db_accessor, edges_in_result,
                  std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            } else {
              CheckPathsAndExtractDistances(
                  &db_accessor, edges_in_result,
                  std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            }
            break;
          case memgraph::query::EdgeAtom::Direction::BOTH:
            if (known_sink) {
              CheckPathsAndExtractDistances(
                  &db_accessor, edges_in_result,
                  std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            } else {
              CheckPathsAndExtractDistances(
                  &db_accessor, edges_in_result,
                  std::vector<std::vector<memgraph::query::TypedValue>>(results.begin(), results.begin()));
            }
            break;
        }
        break;
    }

    db_accessor.Abort();
  }
#endif

 protected:
  memgraph::query::AstStorage storage;
};

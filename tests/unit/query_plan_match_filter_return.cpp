#include <experimental/optional>
#include <iterator>
#include <memory>
#include <unordered_map>
#include <vector>

#include <fmt/format.h>

#include "cppitertools/enumerate.hpp"
#include "cppitertools/product.hpp"
#include "cppitertools/range.hpp"
#include "cppitertools/repeat.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "communication/result_stream_faker.hpp"
#include "database/graph_db.hpp"
#include "distributed/data_manager.hpp"
#include "distributed/updates_rpc_server.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/plan/distributed_ops.hpp"
#include "query/plan/operator.hpp"

#include "distributed_common.hpp"
#include "query_plan_common.hpp"

using namespace query;
using namespace query::plan;

class MatchReturnFixture : public testing::Test {
 protected:
  database::SingleNode db_;
  std::unique_ptr<database::GraphDbAccessor> dba_{db_.Access()};
  AstStorage storage;
  SymbolTable symbol_table;

  void AddVertices(int count) {
    for (int i = 0; i < count; i++) dba_->InsertVertex();
  }

  template <typename TResult>
  std::vector<TResult> Results(std::shared_ptr<Produce> &op) {
    std::vector<TResult> res;
    for (const auto &row : CollectProduce(op.get(), symbol_table, *dba_))
      res.emplace_back(row[0].Value<TResult>());
    return res;
  }
};

TEST_F(MatchReturnFixture, MatchReturn) {
  AddVertices(2);
  dba_->AdvanceCommand();

  auto test_pull_count = [&](GraphView graph_view) {
    auto scan_all =
        MakeScanAll(storage, symbol_table, "n", nullptr, graph_view);
    auto output = NEXPR("n", IDENT("n"));
    auto produce = MakeProduce(scan_all.op_, output);
    symbol_table[*output->expression_] = scan_all.sym_;
    symbol_table[*output] =
        symbol_table.CreateSymbol("named_expression_1", true);
    return PullAll(produce, *dba_, symbol_table);
  };

  EXPECT_EQ(2, test_pull_count(GraphView::NEW));
  EXPECT_EQ(2, test_pull_count(GraphView::OLD));
  dba_->InsertVertex();
  EXPECT_EQ(3, test_pull_count(GraphView::NEW));
  EXPECT_EQ(2, test_pull_count(GraphView::OLD));
  dba_->AdvanceCommand();
  EXPECT_EQ(3, test_pull_count(GraphView::OLD));
}

TEST_F(MatchReturnFixture, MatchReturnPath) {
  AddVertices(2);
  dba_->AdvanceCommand();

  auto scan_all = MakeScanAll(storage, symbol_table, "n", nullptr);
  Symbol path_sym = symbol_table.CreateSymbol("path", true);
  auto make_path = std::make_shared<ConstructNamedPath>(
      scan_all.op_, path_sym, std::vector<Symbol>{scan_all.sym_});
  auto output = NEXPR("path", IDENT("path"));
  symbol_table[*output->expression_] = path_sym;
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1", true);
  auto produce = MakeProduce(make_path, output);
  auto results = Results<query::Path>(produce);
  ASSERT_EQ(results.size(), 2);
  std::vector<query::Path> expected_paths;
  for (const auto &v : dba_->Vertices(false)) expected_paths.emplace_back(v);
  ASSERT_EQ(expected_paths.size(), 2);
  EXPECT_TRUE(std::is_permutation(expected_paths.begin(), expected_paths.end(),
                                  results.begin()));
}

TEST(QueryPlan, MatchReturnCartesian) {
  database::SingleNode db;
  auto dba = db.Access();

  dba->InsertVertex().add_label(dba->Label("l1"));
  dba->InsertVertex().add_label(dba->Label("l2"));
  dba->AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto m = MakeScanAll(storage, symbol_table, "m", n.op_);
  auto return_n = NEXPR("n", IDENT("n"));
  symbol_table[*return_n->expression_] = n.sym_;
  symbol_table[*return_n] =
      symbol_table.CreateSymbol("named_expression_1", true);
  auto return_m = NEXPR("m", IDENT("m"));
  symbol_table[*return_m->expression_] = m.sym_;
  symbol_table[*return_m] =
      symbol_table.CreateSymbol("named_expression_2", true);
  auto produce = MakeProduce(m.op_, return_n, return_m);

  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(results.size(), 4);
  // ensure the result ordering is OK:
  // "n" from the results is the same for the first two rows, while "m" isn't
  EXPECT_EQ(results[0][0].Value<VertexAccessor>(),
            results[1][0].Value<VertexAccessor>());
  EXPECT_NE(results[0][1].Value<VertexAccessor>(),
            results[1][1].Value<VertexAccessor>());
}

TEST(QueryPlan, StandaloneReturn) {
  database::SingleNode db;
  auto dba = db.Access();

  // add a few nodes to the database
  dba->InsertVertex();
  dba->InsertVertex();
  dba->AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto output = NEXPR("n", LITERAL(42));
  auto produce = MakeProduce(std::shared_ptr<LogicalOperator>(nullptr), output);
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1", true);

  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].size(), 1);
  EXPECT_EQ(results[0][0].Value<int64_t>(), 42);
}

TEST(QueryPlan, NodeFilterLabelsAndProperties) {
  database::SingleNode db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;

  // add a few nodes to the database
  storage::Label label = dba.Label("Label");
  auto property = PROPERTY_PAIR("Property");
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto v3 = dba.InsertVertex();
  auto v4 = dba.InsertVertex();
  auto v5 = dba.InsertVertex();
  dba.InsertVertex();
  // test all combination of (label | no_label) * (no_prop | wrong_prop |
  // right_prop)
  // only v1-v3 will have the right labels
  v1.add_label(label);
  v2.add_label(label);
  v3.add_label(label);
  // v1 and v4 will have the right properties
  v1.PropsSet(property.second, 42);
  v2.PropsSet(property.second, 1);
  v4.PropsSet(property.second, 42);
  v5.PropsSet(property.second, 1);
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  // make a scan all
  auto n = MakeScanAll(storage, symbol_table, "n");
  n.node_->labels_.emplace_back(label);
  n.node_->properties_[property] = LITERAL(42);

  // node filtering
  auto *filter_expr =
      AND(storage.Create<LabelsTest>(n.node_->identifier_, n.node_->labels_),
          EQ(PROPERTY_LOOKUP(n.node_->identifier_, property), LITERAL(42)));
  auto node_filter = std::make_shared<Filter>(n.op_, filter_expr);

  // make a named expression and a produce
  auto output = NEXPR("x", IDENT("n"));
  symbol_table[*output->expression_] = n.sym_;
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1", true);
  auto produce = MakeProduce(node_filter, output);

  EXPECT_EQ(1, PullAll(produce, dba, symbol_table));

  //  test that filtering works with old records
  v4.Reconstruct();
  v4.add_label(label);
  EXPECT_EQ(1, PullAll(produce, dba, symbol_table));
  dba.AdvanceCommand();
  EXPECT_EQ(2, PullAll(produce, dba, symbol_table));
}

TEST(QueryPlan, NodeFilterMultipleLabels) {
  database::SingleNode db;
  auto dba = db.Access();

  // add a few nodes to the database
  storage::Label label1 = dba->Label("label1");
  storage::Label label2 = dba->Label("label2");
  storage::Label label3 = dba->Label("label3");
  // the test will look for nodes that have label1 and label2
  dba->InsertVertex();                    // NOT accepted
  dba->InsertVertex().add_label(label1);  // NOT accepted
  dba->InsertVertex().add_label(label2);  // NOT accepted
  dba->InsertVertex().add_label(label3);  // NOT accepted
  auto v1 = dba->InsertVertex();          // YES accepted
  v1.add_label(label1);
  v1.add_label(label2);
  auto v2 = dba->InsertVertex();  // NOT accepted
  v2.add_label(label1);
  v2.add_label(label3);
  auto v3 = dba->InsertVertex();  // YES accepted
  v3.add_label(label1);
  v3.add_label(label2);
  v3.add_label(label3);
  dba->AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  // make a scan all
  auto n = MakeScanAll(storage, symbol_table, "n");
  n.node_->labels_.emplace_back(label1);
  n.node_->labels_.emplace_back(label2);

  // node filtering
  auto *filter_expr =
      storage.Create<LabelsTest>(n.node_->identifier_, n.node_->labels_);
  auto node_filter = std::make_shared<Filter>(n.op_, filter_expr);

  // make a named expression and a produce
  auto output = NEXPR("n", IDENT("n"));
  auto produce = MakeProduce(node_filter, output);

  // fill up the symbol table
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1", true);
  symbol_table[*output->expression_] = n.sym_;

  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(results.size(), 2);
}

TEST(QueryPlan, Cartesian) {
  database::SingleNode db;
  auto dba = db.Access();

  auto add_vertex = [&dba](std::string label) {
    auto vertex = dba->InsertVertex();
    vertex.add_label(dba->Label(label));
    return vertex;
  };

  std::vector<VertexAccessor> vertices{add_vertex("v1"), add_vertex("v2"),
                                       add_vertex("v3")};
  dba->AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto m = MakeScanAll(storage, symbol_table, "m");
  auto return_n = NEXPR("n", IDENT("n"));
  symbol_table[*return_n->expression_] = n.sym_;
  symbol_table[*return_n] =
      symbol_table.CreateSymbol("named_expression_1", true);
  auto return_m = NEXPR("m", IDENT("m"));
  symbol_table[*return_m->expression_] = m.sym_;
  symbol_table[*return_m] =
      symbol_table.CreateSymbol("named_expression_2", true);

  std::vector<Symbol> left_symbols{n.sym_};
  std::vector<Symbol> right_symbols{m.sym_};
  auto cartesian_op =
      std::make_shared<Cartesian>(n.op_, left_symbols, m.op_, right_symbols);

  auto produce = MakeProduce(cartesian_op, return_n, return_m);

  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(results.size(), 9);
  for (int i = 0; i < 3; ++i) {
    for (int j = 0; j < 3; ++j) {
      EXPECT_EQ(results[3 * i + j][0].Value<VertexAccessor>(), vertices[j]);
      EXPECT_EQ(results[3 * i + j][1].Value<VertexAccessor>(), vertices[i]);
    }
  }
}

TEST(QueryPlan, CartesianEmptySet) {
  database::SingleNode db;
  auto dba = db.Access();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto m = MakeScanAll(storage, symbol_table, "m");
  auto return_n = NEXPR("n", IDENT("n"));
  symbol_table[*return_n->expression_] = n.sym_;
  symbol_table[*return_n] =
      symbol_table.CreateSymbol("named_expression_1", true);
  auto return_m = NEXPR("m", IDENT("m"));
  symbol_table[*return_m->expression_] = m.sym_;
  symbol_table[*return_m] =
      symbol_table.CreateSymbol("named_expression_2", true);

  std::vector<Symbol> left_symbols{n.sym_};
  std::vector<Symbol> right_symbols{m.sym_};
  auto cartesian_op =
      std::make_shared<Cartesian>(n.op_, left_symbols, m.op_, right_symbols);

  auto produce = MakeProduce(cartesian_op, return_n, return_m);

  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(results.size(), 0);
}

TEST(QueryPlan, CartesianThreeWay) {
  database::SingleNode db;
  auto dba = db.Access();
  auto add_vertex = [&dba](std::string label) {
    auto vertex = dba->InsertVertex();
    vertex.add_label(dba->Label(label));
    return vertex;
  };

  std::vector<VertexAccessor> vertices{add_vertex("v1"), add_vertex("v2"),
                                       add_vertex("v3")};
  dba->AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto m = MakeScanAll(storage, symbol_table, "m");
  auto l = MakeScanAll(storage, symbol_table, "l");
  auto return_n = NEXPR("n", IDENT("n"));
  symbol_table[*return_n->expression_] = n.sym_;
  symbol_table[*return_n] =
      symbol_table.CreateSymbol("named_expression_1", true);
  auto return_m = NEXPR("m", IDENT("m"));
  symbol_table[*return_m->expression_] = m.sym_;
  symbol_table[*return_m] =
      symbol_table.CreateSymbol("named_expression_2", true);
  auto return_l = NEXPR("l", IDENT("l"));
  symbol_table[*return_l->expression_] = l.sym_;
  symbol_table[*return_l] =
      symbol_table.CreateSymbol("named_expression_3", true);

  std::vector<Symbol> n_symbols{n.sym_};
  std::vector<Symbol> m_symbols{m.sym_};
  std::vector<Symbol> n_m_symbols{n.sym_, m.sym_};
  std::vector<Symbol> l_symbols{l.sym_};
  auto cartesian_op_1 =
      std::make_shared<Cartesian>(n.op_, n_symbols, m.op_, m_symbols);

  auto cartesian_op_2 = std::make_shared<Cartesian>(cartesian_op_1, n_m_symbols,
                                                    l.op_, l_symbols);

  auto produce = MakeProduce(cartesian_op_2, return_n, return_m, return_l);

  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(results.size(), 27);
  int id = 0;
  for (int i = 0; i < 3; ++i) {
    for (int j = 0; j < 3; ++j) {
      for (int k = 0; k < 3; ++k) {
        EXPECT_EQ(results[id][0].Value<VertexAccessor>(), vertices[k]);
        EXPECT_EQ(results[id][1].Value<VertexAccessor>(), vertices[j]);
        EXPECT_EQ(results[id][2].Value<VertexAccessor>(), vertices[i]);
        ++id;
      }
    }
  }
}

class ExpandFixture : public testing::Test {
 protected:
  database::SingleNode db_;
  std::unique_ptr<database::GraphDbAccessor> dba_{db_.Access()};
  AstStorage storage;
  SymbolTable symbol_table;

  // make a V-graph (v3)<-[r2]-(v1)-[r1]->(v2)
  VertexAccessor v1 = dba_->InsertVertex();
  VertexAccessor v2 = dba_->InsertVertex();
  VertexAccessor v3 = dba_->InsertVertex();
  storage::EdgeType edge_type = dba_->EdgeType("Edge");
  EdgeAccessor r1 = dba_->InsertEdge(v1, v2, edge_type);
  EdgeAccessor r2 = dba_->InsertEdge(v1, v3, edge_type);

  void SetUp() override {
    v1.add_label(dba_->Label("l1"));
    v2.add_label(dba_->Label("l2"));
    v3.add_label(dba_->Label("l3"));
    dba_->AdvanceCommand();
  }
};

TEST_F(ExpandFixture, Expand) {
  auto test_expand = [&](EdgeAtom::Direction direction, GraphView graph_view) {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", direction,
                          {}, "m", false, graph_view);

    // make a named expression and a produce
    auto output = NEXPR("m", IDENT("m"));
    symbol_table[*output->expression_] = r_m.node_sym_;
    symbol_table[*output] =
        symbol_table.CreateSymbol("named_expression_1", true);
    auto produce = MakeProduce(r_m.op_, output);

    return PullAll(produce, *dba_, symbol_table);
  };

  // test that expand works well for both old and new graph state
  v1.Reconstruct();
  v2.Reconstruct();
  v3.Reconstruct();
  dba_->InsertEdge(v1, v2, edge_type);
  dba_->InsertEdge(v1, v3, edge_type);
  EXPECT_EQ(2, test_expand(EdgeAtom::Direction::OUT, GraphView::OLD));
  EXPECT_EQ(2, test_expand(EdgeAtom::Direction::IN, GraphView::OLD));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::BOTH, GraphView::OLD));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::OUT, GraphView::NEW));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::IN, GraphView::NEW));
  EXPECT_EQ(8, test_expand(EdgeAtom::Direction::BOTH, GraphView::NEW));
  dba_->AdvanceCommand();
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::OUT, GraphView::OLD));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::IN, GraphView::OLD));
  EXPECT_EQ(8, test_expand(EdgeAtom::Direction::BOTH, GraphView::OLD));
}

TEST_F(ExpandFixture, ExpandPath) {
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m =
      MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                 EdgeAtom::Direction::OUT, {}, "m", false, GraphView::OLD);
  Symbol path_sym = symbol_table.CreateSymbol("path", true);
  auto path = std::make_shared<ConstructNamedPath>(
      r_m.op_, path_sym,
      std::vector<Symbol>{n.sym_, r_m.edge_sym_, r_m.node_sym_});
  auto output = NEXPR("m", IDENT("m"));
  symbol_table[*output->expression_] = path_sym;
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1", true);
  auto produce = MakeProduce(path, output);

  std::vector<query::Path> expected_paths{{v1, r2, v3}, {v1, r1, v2}};
  auto results = CollectProduce(produce.get(), symbol_table, *dba_);
  ASSERT_EQ(results.size(), 2);
  std::vector<query::Path> results_paths;
  for (const auto &result : results)
    results_paths.emplace_back(result[0].ValuePath());
  EXPECT_TRUE(std::is_permutation(expected_paths.begin(), expected_paths.end(),
                                  results_paths.begin()));
}

/**
 * A fixture that sets a graph up and provides some functions.
 *
 * The graph is a double chain:
 *    (v:0)-(v:1)-(v:2)
 *         X     X
 *    (v:0)-(v:1)-(v:2)
 *
 * Each vertex is labeled (the labels are available as a
 * member in this class). Edges have properties set that
 * indicate origin and destination vertex for debugging.
 */
class QueryPlanExpandVariable : public testing::Test {
 protected:
  // type returned by the GetEdgeListSizes function, used
  // a lot below in test declaration
  using map_int = std::unordered_map<int, int>;

  database::SingleNode db_;
  std::unique_ptr<database::GraphDbAccessor> dba_{db_.Access()};
  // labels for layers in the double chain
  std::vector<storage::Label> labels;
  // for all the edges
  storage::EdgeType edge_type = dba_->EdgeType("edge_type");

  AstStorage storage;
  SymbolTable symbol_table;

  // using std::experimental::nullopt
  std::experimental::nullopt_t nullopt = std::experimental::nullopt;

  void SetUp() {
    // create the graph
    int chain_length = 3;
    std::vector<VertexAccessor> layer;
    for (int from_layer_ind = -1; from_layer_ind < chain_length - 1;
         from_layer_ind++) {
      std::vector<VertexAccessor> new_layer{dba_->InsertVertex(),
                                            dba_->InsertVertex()};
      auto label = dba_->Label(std::to_string(from_layer_ind + 1));
      labels.push_back(label);
      for (size_t v_to_ind = 0; v_to_ind < new_layer.size(); v_to_ind++) {
        auto &v_to = new_layer[v_to_ind];
        v_to.add_label(label);
        for (size_t v_from_ind = 0; v_from_ind < layer.size(); v_from_ind++) {
          auto &v_from = layer[v_from_ind];
          auto edge = dba_->InsertEdge(v_from, v_to, edge_type);
          edge.PropsSet(dba_->Property("p"),
                        fmt::format("V{}{}->V{}{}", from_layer_ind, v_from_ind,
                                    from_layer_ind + 1, v_to_ind));
        }
      }
      layer = new_layer;
    }
    dba_->AdvanceCommand();
    ASSERT_EQ(CountIterable(dba_->Vertices(false)), 2 * chain_length);
    ASSERT_EQ(CountIterable(dba_->Edges(false)), 4 * (chain_length - 1));
  }

  /**
   * Expands the given LogicalOperator input with a match
   * (ScanAll->Filter(label)->Expand). Can create both VariableExpand
   * ops and plain Expand (depending on template param).
   * When creating plain Expand the bound arguments (lower, upper) are ignored.
   *
   * @param is_reverse Set to true if ExpandVariable should produce the list of
   * edges in reverse order. As if ExpandVariable starts from `node_to` and ends
   * with `node_from`.
   * @return the last created logical op.
   */
  template <typename TExpansionOperator>
  std::shared_ptr<LogicalOperator> AddMatch(
      std::shared_ptr<LogicalOperator> input_op, const std::string &node_from,
      int layer, EdgeAtom::Direction direction,
      const std::vector<storage::EdgeType> &edge_types,
      std::experimental::optional<size_t> lower,
      std::experimental::optional<size_t> upper, Symbol edge_sym,
      const std::string &node_to, GraphView graph_view,
      bool is_reverse = false) {
    auto n_from = MakeScanAll(storage, symbol_table, node_from, input_op);
    auto filter_op = std::make_shared<Filter>(
        n_from.op_, storage.Create<query::LabelsTest>(
                        n_from.node_->identifier_,
                        std::vector<storage::Label>{labels[layer]}));

    auto n_to = NODE(node_to);
    auto n_to_sym = symbol_table.CreateSymbol(node_to, true);
    symbol_table[*n_to->identifier_] = n_to_sym;

    if (std::is_same<TExpansionOperator, ExpandVariable>::value) {
      // convert optional ints to optional expressions
      auto convert = [this](std::experimental::optional<size_t> bound) {
        return bound ? LITERAL(static_cast<int64_t>(bound.value())) : nullptr;
      };

      return std::make_shared<ExpandVariable>(
          n_to_sym, edge_sym, EdgeAtom::Type::DEPTH_FIRST, direction,
          edge_types, is_reverse, convert(lower), convert(upper), filter_op,
          n_from.sym_, false,
          ExpandVariable::Lambda{symbol_table.CreateSymbol("inner_edge", false),
                                 symbol_table.CreateSymbol("inner_node", false),
                                 nullptr},
          std::experimental::nullopt, std::experimental::nullopt, graph_view);
    } else
      return std::make_shared<Expand>(n_to_sym, edge_sym, direction, edge_types,
                                      filter_op, n_from.sym_, false,
                                      graph_view);
  }

  /* Creates an edge (in the frame and symbol table). Returns the symbol. */
  auto Edge(const std::string &identifier, EdgeAtom::Direction direction) {
    auto edge = EDGE(identifier, direction);
    auto edge_sym = symbol_table.CreateSymbol(identifier, true);
    symbol_table[*edge->identifier_] = edge_sym;
    return edge_sym;
  }

  /**
   * Pulls from the given input and returns the results under the given symbol.
   *
   * @return a vector of values of the given type.
   * @tparam TResult type of the result that is sought.
   */
  template <typename TResult>
  auto GetResults(std::shared_ptr<LogicalOperator> input_op, Symbol symbol) {
    Frame frame(symbol_table.max_position());
    auto cursor = input_op->MakeCursor(*dba_);
    Context context(*dba_);
    context.symbol_table_ = symbol_table;
    std::vector<TResult> results;
    while (cursor->Pull(frame, context))
      results.emplace_back(frame[symbol].Value<TResult>());
    return results;
  }

  /**
   * Pulls from the given input and analyses the edge-list (result of variable
   * length expansion) found in the results under the given symbol.
   *
   * @return a map {edge_list_length -> number_of_results}
   */
  auto GetEdgeListSizes(std::shared_ptr<LogicalOperator> input_op,
                        Symbol symbol) {
    map_int count_per_length;
    for (const auto &edge_list :
         GetResults<std::vector<TypedValue>>(input_op, symbol)) {
      auto length = edge_list.size();
      auto found = count_per_length.find(length);
      if (found == count_per_length.end())
        count_per_length[length] = 1;
      else
        found->second++;
    }
    return count_per_length;
  }
};

TEST_F(QueryPlanExpandVariable, OneVariableExpansion) {
  auto test_expand = [&](int layer, EdgeAtom::Direction direction,
                         std::experimental::optional<size_t> lower,
                         std::experimental::optional<size_t> upper,
                         bool reverse) {
    auto e = Edge("r", direction);
    return GetEdgeListSizes(
        AddMatch<ExpandVariable>(nullptr, "n", layer, direction, {}, lower,
                                 upper, e, "m", GraphView::OLD, reverse),
        e);
  };

  for (int reverse = 0; reverse < 2; ++reverse) {
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 0, 0, reverse),
              (map_int{{0, 2}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, 0, reverse),
              (map_int{{0, 2}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 0, 0, reverse),
              (map_int{{0, 2}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 1, 1, reverse),
              (map_int{}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, 1, reverse),
              (map_int{{1, 4}}));
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 1, 1, reverse),
              (map_int{{1, 4}}));
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 1, 1, reverse),
              (map_int{{1, 4}}));
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 1, 1, reverse),
              (map_int{{1, 8}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2, reverse),
              (map_int{{2, 8}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 3, reverse),
              (map_int{{2, 8}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, 2, reverse),
              (map_int{{1, 4}, {2, 8}}));

    // the following tests also check edge-uniqueness (cyphermorphisim)
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 1, 2, reverse),
              (map_int{{1, 4}, {2, 12}}));
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, 4, reverse),
              (map_int{{4, 24}}));

    // default bound values (lower default is 1, upper default is inf)
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, nullopt, 0, reverse),
              (map_int{}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, nullopt, 1, reverse),
              (map_int{{1, 4}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, nullopt, 2, reverse),
              (map_int{{1, 4}, {2, 8}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 7, nullopt, reverse),
              (map_int{{7, 24}, {8, 24}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 8, nullopt, reverse),
              (map_int{{8, 24}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 9, nullopt, reverse),
              (map_int{}));
  }
}

TEST_F(QueryPlanExpandVariable, EdgeUniquenessSingleAndVariableExpansion) {
  auto test_expand = [&](int layer, EdgeAtom::Direction direction,
                         std::experimental::optional<size_t> lower,
                         std::experimental::optional<size_t> upper,
                         bool single_expansion_before,
                         bool add_uniqueness_check) {
    std::shared_ptr<LogicalOperator> last_op{nullptr};
    std::vector<Symbol> symbols;

    if (single_expansion_before) {
      symbols.push_back(Edge("r0", direction));
      last_op = AddMatch<Expand>(last_op, "n0", layer, direction, {}, lower,
                                 upper, symbols.back(), "m0", GraphView::OLD);
    }

    auto var_length_sym = Edge("r1", direction);
    symbols.push_back(var_length_sym);
    last_op =
        AddMatch<ExpandVariable>(last_op, "n1", layer, direction, {}, lower,
                                 upper, var_length_sym, "m1", GraphView::OLD);

    if (!single_expansion_before) {
      symbols.push_back(Edge("r2", direction));
      last_op = AddMatch<Expand>(last_op, "n2", layer, direction, {}, lower,
                                 upper, symbols.back(), "m2", GraphView::OLD);
    }

    if (add_uniqueness_check) {
      auto last_symbol = symbols.back();
      symbols.pop_back();
      last_op = std::make_shared<ExpandUniquenessFilter<EdgeAccessor>>(
          last_op, last_symbol, symbols);
    }

    return GetEdgeListSizes(last_op, var_length_sym);
  };

  // no uniqueness between variable and single expansion
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 3, true, false),
            (map_int{{2, 4 * 8}}));
  // with uniqueness test, different ordering of (variable, single) expansion
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 3, true, true),
            (map_int{{2, 3 * 8}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 3, false, true),
            (map_int{{2, 3 * 8}}));
}

TEST_F(QueryPlanExpandVariable, EdgeUniquenessTwoVariableExpansions) {
  auto test_expand = [&](int layer, EdgeAtom::Direction direction,
                         std::experimental::optional<size_t> lower,
                         std::experimental::optional<size_t> upper,
                         bool add_uniqueness_check) {
    auto e1 = Edge("r1", direction);
    auto first =
        AddMatch<ExpandVariable>(nullptr, "n1", layer, direction, {}, lower,
                                 upper, e1, "m1", GraphView::OLD);
    auto e2 = Edge("r2", direction);
    auto last_op =
        AddMatch<ExpandVariable>(first, "n2", layer, direction, {}, lower,
                                 upper, e2, "m2", GraphView::OLD);
    if (add_uniqueness_check) {
      last_op = std::make_shared<ExpandUniquenessFilter<EdgeAccessor>>(
          last_op, e2, std::vector<Symbol>{e1});
    }

    return GetEdgeListSizes(last_op, e2);
  };

  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2, false),
            (map_int{{2, 8 * 8}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2, true),
            (map_int{{2, 5 * 8}}));
}

TEST_F(QueryPlanExpandVariable, GraphState) {
  auto test_expand = [&](GraphView graph_view,
                         const std::vector<storage::EdgeType> &edge_types) {
    auto e = Edge("r", EdgeAtom::Direction::OUT);
    return GetEdgeListSizes(
        AddMatch<ExpandVariable>(nullptr, "n", 0, EdgeAtom::Direction::OUT,
                                 edge_types, 2, 2, e, "m", graph_view),
        e);
  };

  auto new_edge_type = dba_->EdgeType("some_type");
  // add two vertices branching out from the second layer
  for (VertexAccessor &vertex : dba_->Vertices(true))
    if (vertex.has_label(labels[1])) {
      auto new_vertex = dba_->InsertVertex();
      dba_->InsertEdge(vertex, new_vertex, new_edge_type);
    }
  ASSERT_EQ(CountIterable(dba_->Vertices(false)), 6);
  ASSERT_EQ(CountIterable(dba_->Vertices(true)), 8);

  EXPECT_EQ(test_expand(GraphView::OLD, {}), (map_int{{2, 8}}));
  EXPECT_EQ(test_expand(GraphView::OLD, {new_edge_type}), (map_int{}));
  EXPECT_EQ(test_expand(GraphView::NEW, {}), (map_int{{2, 12}}));
  EXPECT_EQ(test_expand(GraphView::NEW, {edge_type}), (map_int{{2, 8}}));
  EXPECT_EQ(test_expand(GraphView::NEW, {new_edge_type}), (map_int{}));
  dba_->AdvanceCommand();
  for (const auto graph_view : {GraphView::OLD, GraphView::NEW}) {
    EXPECT_EQ(test_expand(graph_view, {}), (map_int{{2, 12}}));
    EXPECT_EQ(test_expand(graph_view, {edge_type}), (map_int{{2, 8}}));
    EXPECT_EQ(test_expand(graph_view, {new_edge_type}), (map_int{}));
  }
}

TEST_F(QueryPlanExpandVariable, NamedPath) {
  auto e = Edge("r", EdgeAtom::Direction::OUT);
  auto expand =
      AddMatch<ExpandVariable>(nullptr, "n", 0, EdgeAtom::Direction::OUT, {}, 2,
                               2, e, "m", GraphView::OLD);
  auto find_symbol = [this](const std::string &name) {
    for (const auto &pos_sym : symbol_table.table())
      if (pos_sym.second.name() == name) return pos_sym.second;
    throw std::runtime_error("Symbol not found");
  };

  auto path_symbol =
      symbol_table.CreateSymbol("path", true, Symbol::Type::Path);
  auto create_path = std::make_shared<ConstructNamedPath>(
      expand, path_symbol,
      std::vector<Symbol>{find_symbol("n"), e, find_symbol("m")});

  std::vector<query::Path> expected_paths;
  for (const auto &v : dba_->Vertices(labels[0], false))
    for (const auto &e1 : v.out())
      for (const auto &e2 : e1.to().out())
        expected_paths.emplace_back(v, e1, e1.to(), e2, e2.to());
  ASSERT_EQ(expected_paths.size(), 8);

  auto results = GetResults<query::Path>(create_path, path_symbol);
  ASSERT_EQ(results.size(), 8);
  EXPECT_TRUE(std::is_permutation(results.begin(), results.end(),
                                  expected_paths.begin(),
                                  TypedValue::BoolEqual{}));
}

namespace std {
template <>
struct hash<std::pair<int, int>> {
  size_t operator()(const std::pair<int, int> &p) const {
    return p.first + 31 * p.second;
  }
};
}  // namespace std

std::vector<std::vector<int>> FloydWarshall(
    int num_vertices, const std::vector<std::pair<int, int>> &edges,
    EdgeAtom::Direction dir) {
  auto has_edge = [&](int u, int v) -> bool {
    bool res = false;
    if (dir != EdgeAtom::Direction::IN)
      res |= utils::Contains(edges, std::make_pair(u, v));
    if (dir != EdgeAtom::Direction::OUT)
      res |= utils::Contains(edges, std::make_pair(v, u));
    return res;
  };

  int inf = std::numeric_limits<int>::max();
  std::vector<std::vector<int>> dist(num_vertices,
                                     std::vector<int>(num_vertices, inf));

  for (int i = 0; i < num_vertices; ++i)
    for (int j = 0; j < num_vertices; ++j)
      if (has_edge(i, j)) dist[i][j] = 1;
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

class STShortestPathTest : public ::testing::Test {
 protected:
  STShortestPathTest() : db(), dba_ptr(db.Access()), dba(*dba_ptr) {}

  void SetUp() {
    for (int i = 0; i < NUM_VERTICES; ++i) {
      vertices.emplace_back(dba.InsertVertex());
      vertices[i].PropsSet(dba.Property("id"), i);
    }
    for (auto edge : EDGES) {
      edges.emplace_back(dba.InsertEdge(
          vertices[edge.first], vertices[edge.second], dba.EdgeType("Edge")));
      edges.back().PropsSet(dba.Property("id"),
                            fmt::format("{}-{}", edge.first, edge.second));
    }

    dba.AdvanceCommand();

    ASSERT_EQ(dba.VerticesCount(), NUM_VERTICES);
    ASSERT_EQ(dba.EdgesCount(), EDGES.size());
  }

  std::vector<std::vector<TypedValue>> ShortestPaths(
      std::shared_ptr<query::plan::LogicalOperator> input_cursor,
      Symbol source_symbol, Symbol sink_symbol, EdgeAtom::Direction dir,
      Expression *lower_bound = nullptr, Expression *upper_bound = nullptr,
      std::experimental::optional<ExpandVariable::Lambda> expand_lambda =
          std::experimental::nullopt) {
    if (!expand_lambda) {
      expand_lambda = ExpandVariable::Lambda{
          symbol_table.CreateSymbol("inner_edge", true),
          symbol_table.CreateSymbol("inner_node", true), nullptr};
    }

    auto edges_symbol = symbol_table.CreateSymbol("edges_symbol", true);

    auto expand_variable = std::make_shared<ExpandVariable>(
        sink_symbol, edges_symbol, EdgeAtom::Type::BREADTH_FIRST, dir,
        std::vector<storage::EdgeType>{dba.EdgeType("Edge")}, false,
        lower_bound, upper_bound, input_cursor, source_symbol, true,
        *expand_lambda, std::experimental::nullopt, std::experimental::nullopt,
        GraphView::OLD);

    auto source_output_symbol =
        symbol_table.CreateSymbol("s", true, Symbol::Type::Vertex);
    auto sink_output_symbol =
        symbol_table.CreateSymbol("t", true, Symbol::Type::Vertex);
    auto edges_output_symbol =
        symbol_table.CreateSymbol("edge", true, Symbol::Type::EdgeList);

    auto source_id = IDENT("s");
    auto sink_id = IDENT("t");
    auto edges_id = IDENT("e");

    symbol_table[*source_id] = source_symbol;
    symbol_table[*sink_id] = sink_symbol;
    symbol_table[*edges_id] = edges_symbol;

    auto source_ne = NEXPR("s", source_id);
    auto sink_ne = NEXPR("s", sink_id);
    auto edges_ne = NEXPR("e", edges_id);

    symbol_table[*source_ne] = source_output_symbol;
    symbol_table[*sink_ne] = sink_output_symbol;
    symbol_table[*edges_ne] = edges_output_symbol;

    auto produce = MakeProduce(expand_variable, source_ne, sink_ne, edges_ne);
    return CollectProduce(produce.get(), symbol_table, dba);
  }

  void CheckPath(const VertexAccessor &source, const VertexAccessor &sink,
                 EdgeAtom::Direction dir, const std::vector<TypedValue> &path) {
    // Check that the given path is actually a path from source to sink, that
    // expansion direction is correct and that given edges actually exist in the
    // test graph
    VertexAccessor curr = source;
    for (const auto &edge_tv : path) {
      EXPECT_TRUE(edge_tv.IsEdge());
      auto edge = edge_tv.ValueEdge();
      EXPECT_TRUE(edge.from() == curr || edge.to() == curr);
      EXPECT_TRUE(curr == edge.from() || dir != EdgeAtom::Direction::OUT);
      EXPECT_TRUE(curr == edge.to() || dir != EdgeAtom::Direction::IN);
      int from = edge.from().PropsAt(dba.Property("id")).Value<int64_t>();
      int to = edge.to().PropsAt(dba.Property("id")).Value<int64_t>();
      EXPECT_TRUE(utils::Contains(EDGES, std::make_pair(from, to)));
      curr = curr == edge.from() ? edge.to() : edge.from();
    }
    EXPECT_EQ(curr, sink);
  }

  database::SingleNode db;
  std::unique_ptr<database::GraphDbAccessor> dba_ptr;
  database::GraphDbAccessor &dba;
  std::vector<VertexAccessor> vertices;
  std::vector<EdgeAccessor> edges;

  AstStorage storage;
  SymbolTable symbol_table;

  const int NUM_VERTICES = 6;
  const std::vector<std::pair<int, int>> EDGES = {
      {0, 1}, {1, 2}, {2, 4}, {2, 5}, {4, 1}, {4, 5}, {5, 4}, {5, 5}, {5, 3}};
};

TEST_F(STShortestPathTest, DirectionAndExpansionDepth) {
  auto lower_bounds = iter::range(-1, NUM_VERTICES + 1);
  auto upper_bounds = iter::range(-1, NUM_VERTICES + 1);
  auto directions = std::vector<EdgeAtom::Direction>{EdgeAtom::Direction::IN,
                                                     EdgeAtom::Direction::OUT,
                                                     EdgeAtom::Direction::BOTH};

  for (const auto &test :
       iter::product(lower_bounds, upper_bounds, directions)) {
    int lower_bound;
    int upper_bound;
    EdgeAtom::Direction dir;
    std::tie(lower_bound, upper_bound, dir) = test;

    auto dist = FloydWarshall(NUM_VERTICES, EDGES, dir);

    auto source = MakeScanAll(storage, symbol_table, "s");
    auto sink = MakeScanAll(storage, symbol_table, "t", source.op_);

    auto results =
        ShortestPaths(sink.op_, source.sym_, sink.sym_, dir,
                      lower_bound == -1 ? nullptr : LITERAL(lower_bound),
                      upper_bound == -1 ? nullptr : LITERAL(upper_bound));

    if (lower_bound == -1) lower_bound = 0;
    if (upper_bound == -1) upper_bound = NUM_VERTICES;
    size_t output_count = 0;
    for (int i = 0; i < NUM_VERTICES; ++i) {
      for (int j = 0; j < NUM_VERTICES; ++j) {
        if (i != j && dist[i][j] != -1 && dist[i][j] >= lower_bound &&
            dist[i][j] <= upper_bound)
          ++output_count;
      }
    }

    EXPECT_EQ(results.size(), output_count);

    for (const auto &result : results) {
      int s =
          result[0].ValueVertex().PropsAt(dba.Property("id")).Value<int64_t>();
      int t =
          result[1].ValueVertex().PropsAt(dba.Property("id")).Value<int64_t>();
      EXPECT_EQ(dist[s][t], (int)result[2].ValueList().size());
      CheckPath(result[0].ValueVertex(), result[1].ValueVertex(), dir,
                result[2].ValueList());
    }
  }
}

TEST_F(STShortestPathTest, ExpandLambda) {
  Symbol inner_node_symbol = symbol_table.CreateSymbol("inner_node", true);
  Symbol inner_edge_symbol = symbol_table.CreateSymbol("inner_edge", true);
  auto inner_node = IDENT("inner_node");
  auto inner_edge = IDENT("inner_edge");

  symbol_table[*inner_node] = inner_node_symbol;
  symbol_table[*inner_edge] = inner_edge_symbol;

  // (filter expression, expected shortest path length)
  std::vector<std::pair<Expression *, int>> tests = {
      // Block vertex 1 (this stops expansion from source side)
      {NEQ(PROPERTY_LOOKUP(inner_node, dba.Property("id")), LITERAL(1)), -1},
      // Block vertex 5 (this stops expansion from sink side)
      {NEQ(PROPERTY_LOOKUP(inner_node, dba.Property("id")), LITERAL(5)), -1},
      // Block source vertex
      {NEQ(PROPERTY_LOOKUP(inner_node, dba.Property("id")), LITERAL(0)), 4},
      // Block sink vertex
      {NEQ(PROPERTY_LOOKUP(inner_node, dba.Property("id")), LITERAL(3)), -1},
      // Block edge 0-1 (this stops expansion from source side)
      {NEQ(PROPERTY_LOOKUP(inner_edge, dba.Property("id")), LITERAL("0-1")),
       -1},
      // Block edge 5-3 (this stops expansion from sink side)
      {NEQ(PROPERTY_LOOKUP(inner_edge, dba.Property("id")), LITERAL("5-3")),
       -1},
      // Block edges 2-5 and 4-1
      {AND(NEQ(PROPERTY_LOOKUP(inner_edge, dba.Property("id")), LITERAL("2-5")),
           NEQ(PROPERTY_LOOKUP(inner_edge, dba.Property("id")),
               LITERAL("4-1"))),
       5}};

  for (auto test : tests) {
    Expression *expression;
    int length;

    std::tie(expression, length) = test;

    auto source =
        MakeUnwind(symbol_table, "s", nullptr, LIST(LITERAL(vertices[0])));
    auto sink =
        MakeUnwind(symbol_table, "t", source.op_, LIST(LITERAL(vertices[3])));
    auto results =
        ShortestPaths(sink.op_, source.sym_, sink.sym_,
                      EdgeAtom::Direction::BOTH, nullptr, nullptr,
                      ExpandVariable::Lambda{inner_edge_symbol,
                                             inner_node_symbol, expression});

    if (length == -1) {
      EXPECT_EQ(results.size(), 0);
    } else {
      ASSERT_EQ(results.size(), 1);
      EXPECT_EQ(results[0][2].ValueList().size(), length);
    }
  }
}

TEST_F(STShortestPathTest, OptionalMatch) {
  for (int i = 0; i <= 2; ++i) {
    auto source = MakeUnwind(
        symbol_table, "s", nullptr,
        LIST(i == 0 ? LITERAL(vertices[0]) : LITERAL(TypedValue::Null)));
    auto sink = MakeUnwind(
        symbol_table, "t", source.op_,
        LIST(i == 1 ? LITERAL(vertices[3]) : LITERAL(TypedValue::Null)));
    auto results = ShortestPaths(sink.op_, source.sym_, sink.sym_,
                                 EdgeAtom::Direction::BOTH);
    EXPECT_EQ(results.size(), 0);
  }
}

enum class TestType { SINGLE_NODE, DISTRIBUTED };

/** A test fixture for breadth first expansion */
class QueryPlanExpandBfs
    : public testing::TestWithParam<std::pair<TestType, int>> {
 private:
  std::unique_ptr<Cluster> cluster_;
  std::unique_ptr<database::SingleNode> single_node_;
  database::GraphDb *db_{nullptr};
  std::unordered_map<std::pair<int, int>, storage::EdgeAddress> e_;

 protected:
  QueryPlanExpandBfs()
      : cluster_(GetParam().first == TestType::DISTRIBUTED
                     ? new Cluster(GetParam().second)
                     : nullptr),
        single_node_(GetParam().first == TestType::DISTRIBUTED
                         ? nullptr
                         : new database::SingleNode()),
        db_([&]() -> database::GraphDb * {
          if (cluster_) return cluster_->master();
          return single_node_.get();
        }()),
        dba_ptr(db_->Access()),
        dba(*dba_ptr) {}

  // Worker IDs where vertices are located.
  const std::vector<int> vertices = {0, 1, 1, 0, 1, 2};
  // Edges in graph.
  const std::vector<std::pair<int, int>> edges = {
      {0, 1}, {1, 2}, {2, 4}, {2, 5}, {4, 1}, {4, 5}, {5, 4}, {5, 5}, {5, 3}};

  // Style-guide non-conformant name due to PROPERTY_PAIR and PROPERTY_LOOKUP
  // macro requirements.
  std::unique_ptr<database::GraphDbAccessor> dba_ptr;
  database::GraphDbAccessor &dba;
  std::vector<storage::VertexAddress> v;

  AstStorage storage;
  SymbolTable symbol_table;

  std::pair<std::string, storage::Property> prop = PROPERTY_PAIR("property");
  storage::EdgeType edge_type = dba.EdgeType("edge_type");
  // Inner edge and vertex symbols.
  // Edge from a to b has `prop` with the value ab (all ints).
  Symbol inner_edge = symbol_table.CreateSymbol("inner_edge", true);
  Symbol inner_node = symbol_table.CreateSymbol("inner_node", true);

  void SetUp() {
    for (auto p : iter::enumerate(vertices)) {
      int id, worker;
      std::tie(id, worker) = p;
      if (GetParam().first == TestType::SINGLE_NODE || worker == 0) {
        auto vertex = dba.InsertVertex();
        vertex.PropsSet(prop.second, id);
        v.push_back(vertex.GlobalAddress());
      } else {
        auto vertex = database::InsertVertexIntoRemote(
            &dba, worker, {}, {{prop.second, id}}, std::experimental::nullopt);
        v.push_back(vertex.GlobalAddress());
      }
    }

    for (auto p : edges) {
      VertexAccessor from(v[p.first], dba);
      VertexAccessor to(v[p.second], dba);
      auto edge = dba.InsertEdge(from, to, edge_type);
      edge.PropsSet(prop.second, p.first * 10 + p.second);
      e_.emplace(p, edge.GlobalAddress());
    }

    AdvanceCommand(dba.transaction_id());
  }

  // Defines and performs a breadth-first expansion with the given parameters.
  // Returns a vector of pairs. Each pair is (vector-of-edges, vertex).
  auto ExpandBF(EdgeAtom::Direction direction, int min_depth, int max_depth,
                Expression *where, const std::vector<TypedValue> &sources = {},
                std::experimental::optional<TypedValue> existing_node =
                    std::experimental::nullopt) {
    auto source_sym = symbol_table.CreateSymbol("source", true);

    // Wrap all sources in a list and unwind it.
    std::vector<Expression *> source_literals;
    for (auto &source : sources) {
      source_literals.emplace_back(LITERAL(source));
    }
    auto source_expr = storage.Create<ListLiteral>(source_literals);

    std::shared_ptr<LogicalOperator> last_op =
        std::make_shared<query::plan::Unwind>(nullptr, source_expr, source_sym);

    auto node_sym = symbol_table.CreateSymbol("node", true);
    auto edge_list_sym = symbol_table.CreateSymbol("edgelist_", true);

    if (GetParam().first == TestType::DISTRIBUTED) {
      last_op = std::make_shared<DistributedExpandBfs>(
          node_sym, edge_list_sym, direction, std::vector<storage::EdgeType>{},
          last_op, source_sym, !!existing_node, GraphView::OLD,
          LITERAL(min_depth), LITERAL(max_depth),
          ExpandVariable::Lambda{inner_edge, inner_node, where});
    } else {
      last_op = std::make_shared<ExpandVariable>(
          node_sym, edge_list_sym, EdgeAtom::Type::BREADTH_FIRST, direction,
          std::vector<storage::EdgeType>{}, false, LITERAL(min_depth),
          LITERAL(max_depth), last_op, source_sym,
          static_cast<bool>(existing_node),
          ExpandVariable::Lambda{inner_edge, inner_node, where},
          std::experimental::nullopt, std::experimental::nullopt,
          GraphView::OLD);
    }

    Frame frame(symbol_table.max_position());

    if (existing_node) {
      frame[node_sym] = *existing_node;
    }

    auto cursor = last_op->MakeCursor(dba);
    std::vector<std::pair<std::vector<EdgeAccessor>, VertexAccessor>> results;
    Context context(dba);
    context.symbol_table_ = symbol_table;
    while (cursor->Pull(frame, context)) {
      results.emplace_back(std::vector<EdgeAccessor>(),
                           frame[node_sym].Value<VertexAccessor>());
      for (const TypedValue &edge : frame[edge_list_sym].ValueList())
        results.back().first.emplace_back(edge.Value<EdgeAccessor>());
    }

    return results;
  }

  template <typename TAccessor>
  auto GetProp(const TAccessor &accessor) {
    return accessor.PropsAt(prop.second).template Value<int64_t>();
  }

  bool EdgesEqual(const std::vector<EdgeAccessor> &edges,
                  const std::vector<int> &ids) {
    if (edges.size() != ids.size()) return false;
    for (size_t i = 0; i < edges.size(); ++i) {
      if (GetProp(edges[i]) != ids[i]) return false;
    }
    return true;
  }

  void ApplyUpdates(tx::TransactionId tx_id) {
    if (GetParam().first == TestType::DISTRIBUTED)
      cluster_->ApplyUpdates(tx_id);
  }

  void AdvanceCommand(tx::TransactionId tx_id) {
    if (GetParam().first == TestType::DISTRIBUTED)
      cluster_->AdvanceCommand(tx_id);
    else
      db_->Access(tx_id)->AdvanceCommand();
  }
};

TEST_P(QueryPlanExpandBfs, Basic) {
  auto results = ExpandBF(EdgeAtom::Direction::BOTH, 1, 1000, nullptr,
                          {VertexAccessor(v[0], dba)});

  ASSERT_EQ(results.size(), 5);

  EXPECT_EQ(GetProp(results[0].second), 1);
  EXPECT_TRUE(EdgesEqual(results[0].first, {1}));

  if (GetProp(results[1].second) == 4) {
    std::swap(results[1], results[2]);
  }

  EXPECT_EQ(GetProp(results[1].second), 2);
  EXPECT_TRUE(EdgesEqual(results[1].first, {1, 12}));

  EXPECT_EQ(GetProp(results[2].second), 4);
  EXPECT_TRUE(EdgesEqual(results[2].first, {1, 41}));

  EXPECT_EQ(GetProp(results[3].second), 5);
  EXPECT_TRUE(EdgesEqual(results[3].first, {1, 41, 45}) ||
              EdgesEqual(results[3].first, {1, 41, 54}) ||
              EdgesEqual(results[3].first, {1, 12, 25}));

  EXPECT_EQ(GetProp(results[4].second), 3);
  EXPECT_TRUE(EdgesEqual(results[4].first, {1, 41, 45, 53}) ||
              EdgesEqual(results[4].first, {1, 41, 54, 53}) ||
              EdgesEqual(results[4].first, {1, 12, 25, 53}));
}

TEST_P(QueryPlanExpandBfs, EdgeDirection) {
  {
    auto results = ExpandBF(EdgeAtom::Direction::OUT, 1, 1000, nullptr,
                            {VertexAccessor(v[4], dba)});
    ASSERT_EQ(results.size(), 4);

    if (GetProp(results[0].second) == 5) {
      std::swap(results[0], results[1]);
    }

    EXPECT_EQ(GetProp(results[0].second), 1);
    EXPECT_TRUE(EdgesEqual(results[0].first, {41}));

    EXPECT_EQ(GetProp(results[1].second), 5);
    EXPECT_TRUE(EdgesEqual(results[1].first, {45}));

    if (GetProp(results[2].second) == 3) {
      std::swap(results[2], results[3]);
    }

    EXPECT_EQ(GetProp(results[2].second), 2);
    EXPECT_TRUE(EdgesEqual(results[2].first, {41, 12}));

    EXPECT_EQ(GetProp(results[3].second), 3);
    EXPECT_TRUE(EdgesEqual(results[3].first, {45, 53}));
  }

  {
    auto results = ExpandBF(EdgeAtom::Direction::IN, 1, 1000, nullptr,
                            {VertexAccessor(v[4], dba)});
    ASSERT_EQ(results.size(), 4);

    if (GetProp(results[0].second) == 5) {
      std::swap(results[0], results[1]);
    }

    EXPECT_EQ(GetProp(results[0].second), 2);
    EXPECT_TRUE(EdgesEqual(results[0].first, {24}));

    EXPECT_EQ(GetProp(results[1].second), 5);
    EXPECT_TRUE(EdgesEqual(results[1].first, {54}));

    EXPECT_EQ(GetProp(results[2].second), 1);
    EXPECT_TRUE(EdgesEqual(results[2].first, {24, 12}));

    EXPECT_EQ(GetProp(results[3].second), 0);
    EXPECT_TRUE(EdgesEqual(results[3].first, {24, 12, 1}));
  }
}

TEST_P(QueryPlanExpandBfs, Where) {
  // TODO(mtomic): lambda filtering in distributed
  if (GetParam().first == TestType::DISTRIBUTED) {
    return;
  }

  auto ident = IDENT("inner_element");
  {
    symbol_table[*ident] = inner_node;
    auto filter_expr = LESS(PROPERTY_LOOKUP(ident, prop), LITERAL(4));
    auto results = ExpandBF(EdgeAtom::Direction::BOTH, 1, 1000, filter_expr,
                            {VertexAccessor(v[0], dba)});
    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(GetProp(results[0].second), 1);
    EXPECT_EQ(GetProp(results[1].second), 2);
  }
  {
    symbol_table[*ident] = inner_edge;
    auto filter_expr = AND(LESS(PROPERTY_LOOKUP(ident, prop), LITERAL(50)),
                           NEQ(PROPERTY_LOOKUP(ident, prop), LITERAL(12)));
    auto results = ExpandBF(EdgeAtom::Direction::BOTH, 1, 1000, filter_expr,
                            {VertexAccessor(v[0], dba)});
    ASSERT_EQ(results.size(), 4);
    EXPECT_EQ(GetProp(results[0].second), 1);
    EXPECT_EQ(GetProp(results[1].second), 4);

    if (GetProp(results[2].second) == 5) {
      std::swap(results[2], results[3]);
    }
    EXPECT_EQ(GetProp(results[2].second), 2);
    EXPECT_EQ(GetProp(results[3].second), 5);
    EXPECT_TRUE(EdgesEqual(results[3].first, {1, 41, 45}));
  }
}

TEST_P(QueryPlanExpandBfs, MultipleInputs) {
  auto results =
      ExpandBF(EdgeAtom::Direction::BOTH, 1, 1000, nullptr,
               {VertexAccessor(v[0], dba), VertexAccessor(v[3], dba)});
  // Expect that each vertex has been returned 2 times.
  EXPECT_EQ(results.size(), 10);
  std::vector<int> found(5, 0);
  for (const auto &row : results) found[GetProp(row.second)]++;
  EXPECT_EQ(found, (std::vector<int>{1, 2, 2, 1, 2}));
}

TEST_P(QueryPlanExpandBfs, ExistingNode) {
  // In single-node, this is handled by STShortestPath cursor instead of
  // SingleSourceShortestPath cursor.
  using testing::ElementsAre;
  using testing::WhenSorted;

  std::vector<TypedValue> sources;
  for (int i = 0; i < 5; ++i) {
    sources.push_back(VertexAccessor(v[i], dba));
  }

  {
    auto results =
        ExpandBF(EdgeAtom::Direction::BOTH, 1, 1000, nullptr,
                 {VertexAccessor(v[0], dba)}, VertexAccessor(v[3], dba));
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(GetProp(results[0].second), 3);
  }
  {
    auto results = ExpandBF(EdgeAtom::Direction::IN, 1, 1000, nullptr, sources,
                            VertexAccessor(v[5], dba));

    std::vector<int> nodes;
    for (auto &row : results) {
      EXPECT_EQ(GetProp(row.second), 5);
      nodes.push_back(GetProp(row.first[0]) % 10);
    }
    EXPECT_THAT(nodes, WhenSorted(ElementsAre(1, 2, 3, 4)));
  }
  {
    auto results = ExpandBF(EdgeAtom::Direction::OUT, 1, 1000, nullptr, sources,
                            VertexAccessor(v[5], dba));

    std::vector<int> nodes;
    for (auto &row : results) {
      EXPECT_EQ(GetProp(row.second), 5);
      nodes.push_back(GetProp(row.first[0]) / 10);
    }
    EXPECT_THAT(nodes, WhenSorted(ElementsAre(0, 1, 2, 4)));
  }
}

TEST_P(QueryPlanExpandBfs, OptionalMatch) {
  {
    auto results = ExpandBF(EdgeAtom::Direction::BOTH, 1, 1000, nullptr,
                            {TypedValue::Null});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = ExpandBF(EdgeAtom::Direction::BOTH, 1, 1000, nullptr,
                            {VertexAccessor(v[0], dba)}, TypedValue::Null);
    EXPECT_EQ(results.size(), 0);
  }
}

TEST_P(QueryPlanExpandBfs, ExpansionDepth) {
  {
    auto results = ExpandBF(EdgeAtom::Direction::BOTH, 2, 3, nullptr,
                            {VertexAccessor(v[0], dba)});
    EXPECT_EQ(results.size(), 3);
    if (GetProp(results[0].second) == 4) {
      std::swap(results[0], results[1]);
    }

    EXPECT_EQ(GetProp(results[0].second), 2);
    EXPECT_EQ(GetProp(results[1].second), 4);
    EXPECT_EQ(GetProp(results[2].second), 5);
  }
}

INSTANTIATE_TEST_CASE_P(SingleNode, QueryPlanExpandBfs,
                        ::testing::Values(std::make_pair(TestType::SINGLE_NODE,
                                                         0)));

INSTANTIATE_TEST_CASE_P(Distributed, QueryPlanExpandBfs,
                        ::testing::Values(std::make_pair(TestType::DISTRIBUTED,
                                                         2)));

/** A test fixture for weighted shortest path expansion */
class QueryPlanExpandWeightedShortestPath : public testing::Test {
 public:
  struct ResultType {
    std::vector<EdgeAccessor> path;
    VertexAccessor vertex;
    double total_weight;
  };

 protected:
  // style-guide non-conformant name due to PROPERTY_PAIR and
  // PROPERTY_LOOKUP macro requirements
  database::SingleNode db;
  std::unique_ptr<database::GraphDbAccessor> dba_ptr{db.Access()};
  database::GraphDbAccessor &dba{*dba_ptr};
  std::pair<std::string, storage::Property> prop = PROPERTY_PAIR("property");
  storage::EdgeType edge_type = dba.EdgeType("edge_type");

  // make 5 vertices because we'll need to compare against them exactly
  // v[0] has `prop` with the value 0
  std::vector<VertexAccessor> v;

  // make some edges too, in a map (from, to) vertex indices
  std::unordered_map<std::pair<int, int>, EdgeAccessor> e;

  AstStorage storage;
  SymbolTable symbol_table;

  // inner edge and vertex symbols
  Symbol filter_edge = symbol_table.CreateSymbol("f_edge", true);
  Symbol filter_node = symbol_table.CreateSymbol("f_node", true);

  Symbol weight_edge = symbol_table.CreateSymbol("w_edge", true);
  Symbol weight_node = symbol_table.CreateSymbol("w_node", true);

  Symbol total_weight = symbol_table.CreateSymbol("total_weight", true);

  void SetUp() {
    for (int i = 0; i < 5; i++) {
      v.push_back(dba.InsertVertex());
      v.back().PropsSet(prop.second, i);
    }

    auto add_edge = [&](int from, int to, double weight) {
      EdgeAccessor edge = dba.InsertEdge(v[from], v[to], edge_type);
      edge.PropsSet(prop.second, weight);
      e.emplace(std::make_pair(from, to), edge);
    };

    add_edge(0, 1, 5);
    add_edge(1, 4, 5);
    add_edge(0, 2, 3);
    add_edge(2, 3, 3);
    add_edge(3, 4, 3);
    add_edge(4, 0, 12);

    dba.AdvanceCommand();
    for (auto &vertex : v) vertex.Reconstruct();
    for (auto &edge : e) edge.second.Reconstruct();
  }

  // defines and performs a weighted shortest expansion with the given
  // params returns a vector of pairs. each pair is (vector-of-edges,
  // vertex)
  auto ExpandWShortest(EdgeAtom::Direction direction,
                       std::experimental::optional<int> max_depth,
                       Expression *where, GraphView graph_view = GraphView::OLD,
                       std::experimental::optional<int> node_id = 0,
                       ScanAllTuple *existing_node_input = nullptr) {
    // scan the nodes optionally filtering on property value
    auto n =
        MakeScanAll(storage, symbol_table, "n",
                    existing_node_input ? existing_node_input->op_ : nullptr);
    auto last_op = n.op_;
    if (node_id) {
      last_op = std::make_shared<Filter>(
          last_op,
          EQ(PROPERTY_LOOKUP(n.node_->identifier_, prop), LITERAL(*node_id)));
    }

    auto ident_e = IDENT("e");
    symbol_table[*ident_e] = weight_edge;

    // expand wshortest
    auto node_sym = existing_node_input
                        ? existing_node_input->sym_
                        : symbol_table.CreateSymbol("node", true);
    auto edge_list_sym = symbol_table.CreateSymbol("edgelist_", true);
    auto filter_lambda = last_op = std::make_shared<ExpandVariable>(
        node_sym, edge_list_sym, EdgeAtom::Type::WEIGHTED_SHORTEST_PATH,
        direction, std::vector<storage::EdgeType>{}, false, nullptr,
        max_depth ? LITERAL(max_depth.value()) : nullptr, last_op, n.sym_,
        existing_node_input != nullptr,
        ExpandVariable::Lambda{filter_edge, filter_node, where},
        ExpandVariable::Lambda{weight_edge, weight_node,
                               PROPERTY_LOOKUP(ident_e, prop)},
        total_weight, graph_view);

    Frame frame(symbol_table.max_position());
    auto cursor = last_op->MakeCursor(dba);
    std::vector<ResultType> results;
    Context context(dba);
    context.symbol_table_ = symbol_table;
    while (cursor->Pull(frame, context)) {
      results.push_back(ResultType{std::vector<EdgeAccessor>(),
                                   frame[node_sym].Value<VertexAccessor>(),
                                   frame[total_weight].Value<double>()});
      for (const TypedValue &edge : frame[edge_list_sym].ValueList())
        results.back().path.emplace_back(edge.Value<EdgeAccessor>());
    }

    return results;
  }

  template <typename TAccessor>
  auto GetProp(const TAccessor &accessor) {
    return accessor.PropsAt(prop.second).template Value<int64_t>();
  }

  template <typename TAccessor>
  auto GetDoubleProp(const TAccessor &accessor) {
    return accessor.PropsAt(prop.second).template Value<double>();
  }

  Expression *PropNe(Symbol symbol, int value) {
    auto ident = IDENT("inner_element");
    symbol_table[*ident] = symbol;
    return NEQ(PROPERTY_LOOKUP(ident, prop), LITERAL(value));
  }
};

// Testing weighted shortest path on this graph:
//
//      5            5
//      /-->--[1]-->--\
//     /               \
//    /        12       \         2
//  [0]--------<--------[4]------->-------[5]
//    \                 /         (on some tests only)
//     \               /
//      \->[2]->-[3]->/
//      3      3     3

TEST_F(QueryPlanExpandWeightedShortestPath, Basic) {
  auto results =
      ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true));

  ASSERT_EQ(results.size(), 4);

  // check end nodes
  EXPECT_EQ(GetProp(results[0].vertex), 2);
  EXPECT_EQ(GetProp(results[1].vertex), 1);
  EXPECT_EQ(GetProp(results[2].vertex), 3);
  EXPECT_EQ(GetProp(results[3].vertex), 4);

  // check paths and total weights
  EXPECT_EQ(results[0].path.size(), 1);
  EXPECT_EQ(GetDoubleProp(results[0].path[0]), 3);
  EXPECT_EQ(results[0].total_weight, 3);

  EXPECT_EQ(results[1].path.size(), 1);
  EXPECT_EQ(GetDoubleProp(results[1].path[0]), 5);
  EXPECT_EQ(results[1].total_weight, 5);

  EXPECT_EQ(results[2].path.size(), 2);
  EXPECT_EQ(GetDoubleProp(results[2].path[0]), 3);
  EXPECT_EQ(GetDoubleProp(results[2].path[1]), 3);
  EXPECT_EQ(results[2].total_weight, 6);

  EXPECT_EQ(results[3].path.size(), 3);
  EXPECT_EQ(GetDoubleProp(results[3].path[0]), 3);
  EXPECT_EQ(GetDoubleProp(results[3].path[1]), 3);
  EXPECT_EQ(GetDoubleProp(results[3].path[2]), 3);
  EXPECT_EQ(results[3].total_weight, 9);
}

TEST_F(QueryPlanExpandWeightedShortestPath, EdgeDirection) {
  {
    auto results =
        ExpandWShortest(EdgeAtom::Direction::OUT, 1000, LITERAL(true));
    ASSERT_EQ(results.size(), 4);
    EXPECT_EQ(GetProp(results[0].vertex), 2);
    EXPECT_EQ(results[0].total_weight, 3);
    EXPECT_EQ(GetProp(results[1].vertex), 1);
    EXPECT_EQ(results[1].total_weight, 5);
    EXPECT_EQ(GetProp(results[2].vertex), 3);
    EXPECT_EQ(results[2].total_weight, 6);
    EXPECT_EQ(GetProp(results[3].vertex), 4);
    EXPECT_EQ(results[3].total_weight, 9);
  }
  {
    auto results =
        ExpandWShortest(EdgeAtom::Direction::IN, 1000, LITERAL(true));
    ASSERT_EQ(results.size(), 4);
    EXPECT_EQ(GetProp(results[0].vertex), 4);
    EXPECT_EQ(results[0].total_weight, 12);
    EXPECT_EQ(GetProp(results[1].vertex), 3);
    EXPECT_EQ(results[1].total_weight, 15);
    EXPECT_EQ(GetProp(results[2].vertex), 1);
    EXPECT_EQ(results[2].total_weight, 17);
    EXPECT_EQ(GetProp(results[3].vertex), 2);
    EXPECT_EQ(results[3].total_weight, 18);
  }
}

TEST_F(QueryPlanExpandWeightedShortestPath, Where) {
  {
    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, 1000,
                                   PropNe(filter_node, 2));
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(GetProp(results[0].vertex), 1);
    EXPECT_EQ(results[0].total_weight, 5);
    EXPECT_EQ(GetProp(results[1].vertex), 4);
    EXPECT_EQ(results[1].total_weight, 10);
    EXPECT_EQ(GetProp(results[2].vertex), 3);
    EXPECT_EQ(results[2].total_weight, 13);
  }
  {
    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, 1000,
                                   PropNe(filter_node, 1));
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(GetProp(results[0].vertex), 2);
    EXPECT_EQ(results[0].total_weight, 3);
    EXPECT_EQ(GetProp(results[1].vertex), 3);
    EXPECT_EQ(results[1].total_weight, 6);
    EXPECT_EQ(GetProp(results[2].vertex), 4);
    EXPECT_EQ(results[2].total_weight, 9);
  }
}

TEST_F(QueryPlanExpandWeightedShortestPath, GraphState) {
  auto ExpandSize = [this](GraphView graph_view) {
    return ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true),
                           graph_view)
        .size();
  };
  EXPECT_EQ(ExpandSize(GraphView::OLD), 4);
  EXPECT_EQ(ExpandSize(GraphView::NEW), 4);
  auto new_vertex = dba.InsertVertex();
  new_vertex.PropsSet(prop.second, 5);
  auto edge = dba.InsertEdge(v[4], new_vertex, edge_type);
  edge.PropsSet(prop.second, 2);
  EXPECT_EQ(CountIterable(dba.Vertices(false)), 5);
  EXPECT_EQ(CountIterable(dba.Vertices(true)), 6);
  EXPECT_EQ(ExpandSize(GraphView::OLD), 4);
  EXPECT_EQ(ExpandSize(GraphView::NEW), 5);
  dba.AdvanceCommand();
  EXPECT_EQ(ExpandSize(GraphView::OLD), 5);
  EXPECT_EQ(ExpandSize(GraphView::NEW), 5);
}

TEST_F(QueryPlanExpandWeightedShortestPath, ExistingNode) {
  auto ExpandPreceeding =
      [this](std::experimental::optional<int> preceeding_node_id) {
        // scan the nodes optionally filtering on property value
        auto n0 = MakeScanAll(storage, symbol_table, "n0");
        if (preceeding_node_id) {
          auto filter = std::make_shared<Filter>(
              n0.op_, EQ(PROPERTY_LOOKUP(n0.node_->identifier_, prop),
                         LITERAL(*preceeding_node_id)));
          // inject the filter op into the ScanAllTuple. that way the filter
          // op can be passed into the ExpandWShortest function without too
          // much refactor
          n0.op_ = filter;
        }

        return ExpandWShortest(EdgeAtom::Direction::OUT, 1000, LITERAL(true),
                               GraphView::OLD, std::experimental::nullopt, &n0);
      };

  EXPECT_EQ(ExpandPreceeding(std::experimental::nullopt).size(), 20);
  {
    auto results = ExpandPreceeding(3);
    ASSERT_EQ(results.size(), 4);
    for (int i = 0; i < 4; i++) EXPECT_EQ(GetProp(results[i].vertex), 3);
  }
}

TEST_F(QueryPlanExpandWeightedShortestPath, UpperBound) {
  {
    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH,
                                   std::experimental::nullopt, LITERAL(true));
    ASSERT_EQ(results.size(), 4);
    EXPECT_EQ(GetProp(results[0].vertex), 2);
    EXPECT_EQ(results[0].total_weight, 3);
    EXPECT_EQ(GetProp(results[1].vertex), 1);
    EXPECT_EQ(results[1].total_weight, 5);
    EXPECT_EQ(GetProp(results[2].vertex), 3);
    EXPECT_EQ(results[2].total_weight, 6);
    EXPECT_EQ(GetProp(results[3].vertex), 4);
    EXPECT_EQ(results[3].total_weight, 9);
  }
  {
    auto new_vertex = dba.InsertVertex();
    new_vertex.PropsSet(prop.second, 5);
    auto edge = dba.InsertEdge(v[4], new_vertex, edge_type);
    edge.PropsSet(prop.second, 2);

    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, 3, LITERAL(true),
                                   GraphView::NEW);

    ASSERT_EQ(results.size(), 5);
    EXPECT_EQ(GetProp(results[0].vertex), 2);
    EXPECT_EQ(results[0].total_weight, 3);
    EXPECT_EQ(GetProp(results[1].vertex), 1);
    EXPECT_EQ(results[1].total_weight, 5);
    EXPECT_EQ(GetProp(results[2].vertex), 3);
    EXPECT_EQ(results[2].total_weight, 6);
    EXPECT_EQ(GetProp(results[3].vertex), 4);
    EXPECT_EQ(results[3].total_weight, 9);
    EXPECT_EQ(GetProp(results[4].vertex), 5);
    EXPECT_EQ(results[4].total_weight, 12);
  }
  {
    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, 2, LITERAL(true));
    ASSERT_EQ(results.size(), 4);
    EXPECT_EQ(GetProp(results[0].vertex), 2);
    EXPECT_EQ(results[0].total_weight, 3);
    EXPECT_EQ(GetProp(results[1].vertex), 1);
    EXPECT_EQ(results[1].total_weight, 5);
    EXPECT_EQ(GetProp(results[2].vertex), 3);
    EXPECT_EQ(results[2].total_weight, 6);
    EXPECT_EQ(GetProp(results[3].vertex), 4);
    EXPECT_EQ(results[3].total_weight, 10);
  }
  {
    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, 1, LITERAL(true));
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(GetProp(results[0].vertex), 2);
    EXPECT_EQ(results[0].total_weight, 3);
    EXPECT_EQ(GetProp(results[1].vertex), 1);
    EXPECT_EQ(results[1].total_weight, 5);
    EXPECT_EQ(GetProp(results[2].vertex), 4);
    EXPECT_EQ(results[2].total_weight, 12);
  }
}

TEST_F(QueryPlanExpandWeightedShortestPath, Exceptions) {
  {
    auto new_vertex = dba.InsertVertex();
    new_vertex.PropsSet(prop.second, 5);
    auto edge = dba.InsertEdge(v[4], new_vertex, edge_type);
    edge.PropsSet(prop.second, "not a number");
    EXPECT_THROW(ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true),
                                 GraphView::NEW),
                 QueryRuntimeException);
  }
  {
    auto new_vertex = dba.InsertVertex();
    new_vertex.PropsSet(prop.second, 5);
    auto edge = dba.InsertEdge(v[4], new_vertex, edge_type);
    edge.PropsSet(prop.second, -10);  // negative weight
    EXPECT_THROW(ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true),
                                 GraphView::NEW),
                 QueryRuntimeException);
  }
  {
    // negative upper bound
    EXPECT_THROW(ExpandWShortest(EdgeAtom::Direction::BOTH, -1, LITERAL(true),
                                 GraphView::NEW),
                 QueryRuntimeException);
  }
}

TEST(QueryPlan, ExpandOptional) {
  database::SingleNode db;
  auto dba = db.Access();

  AstStorage storage;
  SymbolTable symbol_table;

  // graph (v2 {p: 2})<-[:T]-(v1 {p: 1})-[:T]->(v3 {p: 2})
  auto prop = dba->Property("p");
  auto edge_type = dba->EdgeType("T");
  auto v1 = dba->InsertVertex();
  v1.PropsSet(prop, 1);
  auto v2 = dba->InsertVertex();
  v2.PropsSet(prop, 2);
  dba->InsertEdge(v1, v2, edge_type);
  auto v3 = dba->InsertVertex();
  v3.PropsSet(prop, 2);
  dba->InsertEdge(v1, v3, edge_type);
  dba->AdvanceCommand();

  // MATCH (n) OPTIONAL MATCH (n)-[r]->(m)
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m =
      MakeExpand(storage, symbol_table, nullptr, n.sym_, "r",
                 EdgeAtom::Direction::OUT, {}, "m", false, GraphView::OLD);
  auto optional = std::make_shared<plan::Optional>(
      n.op_, r_m.op_, std::vector<Symbol>{r_m.edge_sym_, r_m.node_sym_});

  // RETURN n, r, m
  auto n_ne = NEXPR("n", IDENT("n"));
  symbol_table[*n_ne->expression_] = n.sym_;
  symbol_table[*n_ne] = symbol_table.CreateSymbol("n", true);
  auto r_ne = NEXPR("r", IDENT("r"));
  symbol_table[*r_ne->expression_] = r_m.edge_sym_;
  symbol_table[*r_ne] = symbol_table.CreateSymbol("r", true);
  auto m_ne = NEXPR("m", IDENT("m"));
  symbol_table[*m_ne->expression_] = r_m.node_sym_;
  symbol_table[*m_ne] = symbol_table.CreateSymbol("m", true);
  auto produce = MakeProduce(optional, n_ne, r_ne, m_ne);

  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  ASSERT_EQ(4, results.size());
  int v1_is_n_count = 0;
  for (auto &row : results) {
    ASSERT_EQ(row[0].type(), TypedValue::Type::Vertex);
    VertexAccessor &va = row[0].Value<VertexAccessor>();
    auto va_p = va.PropsAt(prop);
    ASSERT_EQ(va_p.type(), PropertyValue::Type::Int);
    if (va_p.Value<int64_t>() == 1) {
      v1_is_n_count++;
      EXPECT_EQ(row[1].type(), TypedValue::Type::Edge);
      EXPECT_EQ(row[2].type(), TypedValue::Type::Vertex);
    } else {
      EXPECT_EQ(row[1].type(), TypedValue::Type::Null);
      EXPECT_EQ(row[2].type(), TypedValue::Type::Null);
    }
  }
  EXPECT_EQ(2, v1_is_n_count);
}

TEST(QueryPlan, OptionalMatchEmptyDB) {
  database::SingleNode db;
  auto dba = db.Access();

  AstStorage storage;
  SymbolTable symbol_table;

  // OPTIONAL MATCH (n)
  auto n = MakeScanAll(storage, symbol_table, "n");
  // RETURN n
  auto n_ne = NEXPR("n", IDENT("n"));
  symbol_table[*n_ne->expression_] = n.sym_;
  symbol_table[*n_ne] = symbol_table.CreateSymbol("n", true);
  auto optional = std::make_shared<plan::Optional>(nullptr, n.op_,
                                                   std::vector<Symbol>{n.sym_});
  auto produce = MakeProduce(optional, n_ne);

  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  ASSERT_EQ(1, results.size());
  EXPECT_EQ(results[0][0].type(), TypedValue::Type::Null);
}

TEST(QueryPlan, OptionalMatchEmptyDBExpandFromNode) {
  database::SingleNode db;
  auto dba = db.Access();
  AstStorage storage;
  SymbolTable symbol_table;
  // OPTIONAL MATCH (n)
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto optional = std::make_shared<plan::Optional>(nullptr, n.op_,
                                                   std::vector<Symbol>{n.sym_});
  // WITH n
  auto n_ne = NEXPR("n", IDENT("n"));
  symbol_table[*n_ne->expression_] = n.sym_;
  auto with_n_sym = symbol_table.CreateSymbol("n", true);
  symbol_table[*n_ne] = with_n_sym;
  auto with = MakeProduce(optional, n_ne);
  // MATCH (n) -[r]-> (m)
  auto r_m =
      MakeExpand(storage, symbol_table, with, with_n_sym, "r",
                 EdgeAtom::Direction::OUT, {}, "m", false, GraphView::OLD);
  // RETURN m
  auto m_ne = NEXPR("m", IDENT("m"));
  symbol_table[*m_ne->expression_] = r_m.node_sym_;
  symbol_table[*m_ne] = symbol_table.CreateSymbol("m", true);
  auto produce = MakeProduce(r_m.op_, m_ne);
  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(0, results.size());
}

TEST(QueryPlan, OptionalMatchThenExpandToMissingNode) {
  database::SingleNode db;
  auto dba = db.Access();
  // Make a graph with 2 connected, unlabeled nodes.
  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();
  auto edge_type = dba->EdgeType("edge_type");
  dba->InsertEdge(v1, v2, edge_type);
  dba->AdvanceCommand();
  EXPECT_EQ(2, CountIterable(dba->Vertices(false)));
  EXPECT_EQ(1, CountIterable(dba->Edges(false)));
  AstStorage storage;
  SymbolTable symbol_table;
  // OPTIONAL MATCH (n :missing)
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto label_missing = dba->Label("missing");
  n.node_->labels_.emplace_back(label_missing);

  auto *filter_expr =
      storage.Create<LabelsTest>(n.node_->identifier_, n.node_->labels_);
  auto node_filter = std::make_shared<Filter>(n.op_, filter_expr);
  auto optional = std::make_shared<plan::Optional>(nullptr, node_filter,
                                                   std::vector<Symbol>{n.sym_});
  // WITH n
  auto n_ne = NEXPR("n", IDENT("n"));
  symbol_table[*n_ne->expression_] = n.sym_;
  auto with_n_sym = symbol_table.CreateSymbol("n", true);
  symbol_table[*n_ne] = with_n_sym;
  auto with = MakeProduce(optional, n_ne);
  // MATCH (m) -[r]-> (n)
  auto m = MakeScanAll(storage, symbol_table, "m", with);
  auto edge_direction = EdgeAtom::Direction::OUT;
  auto edge = EDGE("r", edge_direction);
  auto edge_sym = symbol_table.CreateSymbol("r", true);
  symbol_table[*edge->identifier_] = edge_sym;
  auto node = NODE("n");
  symbol_table[*node->identifier_] = with_n_sym;
  auto expand = std::make_shared<plan::Expand>(
      with_n_sym, edge_sym, edge_direction, std::vector<storage::EdgeType>{},
      m.op_, m.sym_, true, GraphView::OLD);
  // RETURN m
  auto m_ne = NEXPR("m", IDENT("m"));
  symbol_table[*m_ne->expression_] = m.sym_;
  symbol_table[*m_ne] = symbol_table.CreateSymbol("m", true);
  auto produce = MakeProduce(expand, m_ne);
  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(0, results.size());
}

TEST(QueryPlan, ExpandExistingNode) {
  database::SingleNode db;
  auto dba = db.Access();

  // make a graph (v1)->(v2) that
  // has a recursive edge (v1)->(v1)
  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();
  auto edge_type = dba->EdgeType("Edge");
  dba->InsertEdge(v1, v1, edge_type);
  dba->InsertEdge(v1, v2, edge_type);
  dba->AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto test_existing = [&](bool with_existing, int expected_result_count) {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_n = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                          EdgeAtom::Direction::OUT, {}, "n", with_existing,
                          GraphView::OLD);
    if (with_existing)
      r_n.op_ =
          std::make_shared<Expand>(n.sym_, r_n.edge_sym_, r_n.edge_->direction_,
                                   std::vector<storage::EdgeType>{}, n.op_,
                                   n.sym_, with_existing, GraphView::OLD);

    // make a named expression and a produce
    auto output = NEXPR("n", IDENT("n"));
    symbol_table[*output->expression_] = n.sym_;
    symbol_table[*output] =
        symbol_table.CreateSymbol("named_expression_1", true);
    auto produce = MakeProduce(r_n.op_, output);

    auto results = CollectProduce(produce.get(), symbol_table, *dba);
    EXPECT_EQ(results.size(), expected_result_count);
  };

  test_existing(true, 1);
  test_existing(false, 2);
}

TEST(QueryPlan, ExpandBothCycleEdgeCase) {
  // we're testing that expanding on BOTH
  // does only one expansion for a cycle
  database::SingleNode db;
  auto dba = db.Access();

  auto v = dba->InsertVertex();
  dba->InsertEdge(v, v, dba->EdgeType("et"));
  dba->AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_ =
      MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                 EdgeAtom::Direction::BOTH, {}, "_", false, GraphView::OLD);
  EXPECT_EQ(1, PullAll(r_.op_, *dba, symbol_table));
}

TEST(QueryPlan, EdgeFilter) {
  database::SingleNode db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;

  // make an N-star expanding from (v1)
  // where only one edge will qualify
  // and there are all combinations of
  // (edge_type yes|no) * (property yes|absent|no)
  std::vector<storage::EdgeType> edge_types;
  for (int j = 0; j < 2; ++j)
    edge_types.push_back(dba.EdgeType("et" + std::to_string(j)));
  std::vector<VertexAccessor> vertices;
  for (int i = 0; i < 7; ++i) vertices.push_back(dba.InsertVertex());
  auto prop = PROPERTY_PAIR("property");
  std::vector<EdgeAccessor> edges;
  for (int i = 0; i < 6; ++i) {
    edges.push_back(
        dba.InsertEdge(vertices[0], vertices[i + 1], edge_types[i % 2]));
    switch (i % 3) {
      case 0:
        edges.back().PropsSet(prop.second, 42);
        break;
      case 1:
        edges.back().PropsSet(prop.second, 100);
        break;
      default:
        break;
    }
  }
  dba.AdvanceCommand();
  for (auto &vertex : vertices) vertex.Reconstruct();
  for (auto &edge : edges) edge.Reconstruct();

  AstStorage storage;
  SymbolTable symbol_table;

  auto test_filter = [&]() {
    // define an operator tree for query
    // MATCH (n)-[r :et0 {property: 42}]->(m) RETURN m

    auto n = MakeScanAll(storage, symbol_table, "n");
    const auto &edge_type = edge_types[0];
    auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                          EdgeAtom::Direction::OUT, {edge_type}, "m", false,
                          GraphView::OLD);
    r_m.edge_->edge_types_.push_back(edge_type);
    r_m.edge_->properties_[prop] = LITERAL(42);
    auto *filter_expr =
        EQ(PROPERTY_LOOKUP(r_m.edge_->identifier_, prop), LITERAL(42));
    auto edge_filter = std::make_shared<Filter>(r_m.op_, filter_expr);

    // make a named expression and a produce
    auto output = NEXPR("m", IDENT("m"));
    symbol_table[*output->expression_] = r_m.node_sym_;
    symbol_table[*output] =
        symbol_table.CreateSymbol("named_expression_1", true);
    auto produce = MakeProduce(edge_filter, output);

    return PullAll(produce, dba, symbol_table);
  };

  EXPECT_EQ(1, test_filter());
  // test that edge filtering always filters on old state
  for (auto &edge : edges) edge.PropsSet(prop.second, 42);
  EXPECT_EQ(1, test_filter());
  dba.AdvanceCommand();
  EXPECT_EQ(3, test_filter());
}

TEST(QueryPlan, EdgeFilterMultipleTypes) {
  database::SingleNode db;
  auto dba = db.Access();

  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();
  auto type_1 = dba->EdgeType("type_1");
  auto type_2 = dba->EdgeType("type_2");
  auto type_3 = dba->EdgeType("type_3");
  dba->InsertEdge(v1, v2, type_1);
  dba->InsertEdge(v1, v2, type_2);
  dba->InsertEdge(v1, v2, type_3);
  dba->AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  // make a scan all
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                        EdgeAtom::Direction::OUT, {type_1, type_2}, "m", false,
                        GraphView::OLD);

  // make a named expression and a produce
  auto output = NEXPR("m", IDENT("m"));
  auto produce = MakeProduce(r_m.op_, output);

  // fill up the symbol table
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1", true);
  symbol_table[*output->expression_] = r_m.node_sym_;

  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(results.size(), 2);
}

TEST(QueryPlan, Filter) {
  database::SingleNode db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;

  // add a 6 nodes with property 'prop', 2 have true as value
  auto property = PROPERTY_PAIR("property");
  for (int i = 0; i < 6; ++i)
    dba.InsertVertex().PropsSet(property.second, i % 3 == 0);
  dba.InsertVertex();  // prop not set, gives NULL
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto e =
      storage.Create<PropertyLookup>(storage.Create<Identifier>("n"), property);
  symbol_table[*e->expression_] = n.sym_;
  auto f = std::make_shared<Filter>(n.op_, e);

  auto output =
      storage.Create<NamedExpression>("x", storage.Create<Identifier>("n"));
  symbol_table[*output->expression_] = n.sym_;
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1", true);
  auto produce = MakeProduce(f, output);

  EXPECT_EQ(CollectProduce(produce.get(), symbol_table, dba).size(), 2);
}

TEST(QueryPlan, ExpandUniquenessFilter) {
  database::SingleNode db;
  auto dba = db.Access();

  // make a graph that has (v1)->(v2) and a recursive edge (v1)->(v1)
  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();
  auto edge_type = dba->EdgeType("edge_type");
  dba->InsertEdge(v1, v2, edge_type);
  dba->InsertEdge(v1, v1, edge_type);
  dba->AdvanceCommand();

  auto check_expand_results = [&](bool vertex_uniqueness,
                                  bool edge_uniqueness) {
    AstStorage storage;
    SymbolTable symbol_table;

    auto n1 = MakeScanAll(storage, symbol_table, "n1");
    auto r1_n2 =
        MakeExpand(storage, symbol_table, n1.op_, n1.sym_, "r1",
                   EdgeAtom::Direction::OUT, {}, "n2", false, GraphView::OLD);
    std::shared_ptr<LogicalOperator> last_op = r1_n2.op_;
    if (vertex_uniqueness)
      last_op = std::make_shared<ExpandUniquenessFilter<VertexAccessor>>(
          last_op, r1_n2.node_sym_, std::vector<Symbol>{n1.sym_});
    auto r2_n3 =
        MakeExpand(storage, symbol_table, last_op, r1_n2.node_sym_, "r2",
                   EdgeAtom::Direction::OUT, {}, "n3", false, GraphView::OLD);
    last_op = r2_n3.op_;
    if (edge_uniqueness)
      last_op = std::make_shared<ExpandUniquenessFilter<EdgeAccessor>>(
          last_op, r2_n3.edge_sym_, std::vector<Symbol>{r1_n2.edge_sym_});
    if (vertex_uniqueness)
      last_op = std::make_shared<ExpandUniquenessFilter<VertexAccessor>>(
          last_op, r2_n3.node_sym_,
          std::vector<Symbol>{n1.sym_, r1_n2.node_sym_});

    return PullAll(last_op, *dba, symbol_table);
  };

  EXPECT_EQ(2, check_expand_results(false, false));
  EXPECT_EQ(0, check_expand_results(true, false));
  EXPECT_EQ(1, check_expand_results(false, true));
}

TEST(QueryPlan, Distinct) {
  // test queries like
  // UNWIND [1, 2, 3, 3] AS x RETURN DISTINCT x

  database::SingleNode db;
  auto dba = db.Access();
  AstStorage storage;
  SymbolTable symbol_table;

  auto check_distinct = [&](const std::vector<TypedValue> input,
                            const std::vector<TypedValue> output,
                            bool assume_int_value) {
    auto input_expr = LITERAL(TypedValue(input));

    auto x = symbol_table.CreateSymbol("x", true);
    auto unwind = std::make_shared<plan::Unwind>(nullptr, input_expr, x);
    auto x_expr = IDENT("x");
    symbol_table[*x_expr] = x;

    auto distinct =
        std::make_shared<plan::Distinct>(unwind, std::vector<Symbol>{x});

    auto x_ne = NEXPR("x", x_expr);
    symbol_table[*x_ne] = symbol_table.CreateSymbol("x_ne", true);
    auto produce = MakeProduce(distinct, x_ne);

    auto results = CollectProduce(produce.get(), symbol_table, *dba);
    ASSERT_EQ(output.size(), results.size());
    auto output_it = output.begin();
    for (const auto &row : results) {
      ASSERT_EQ(1, row.size());
      ASSERT_EQ(row[0].type(), output_it->type());
      if (assume_int_value)
        EXPECT_EQ(output_it->Value<int64_t>(), row[0].Value<int64_t>());
      output_it++;
    }
  };

  check_distinct({1, 1, 2, 3, 3, 3}, {1, 2, 3}, true);
  check_distinct({3, 2, 3, 5, 3, 5, 2, 1, 2}, {3, 2, 5, 1}, true);
  check_distinct(
      {3, "two", TypedValue::Null, 3, true, false, "TWO", TypedValue::Null},
      {3, "two", TypedValue::Null, true, false, "TWO"}, false);
}

TEST(QueryPlan, ScanAllByLabel) {
  database::SingleNode db;
  auto dba = db.Access();
  // Add a vertex with a label and one without.
  auto label = dba->Label("label");
  auto labeled_vertex = dba->InsertVertex();
  labeled_vertex.add_label(label);
  dba->InsertVertex();
  dba->AdvanceCommand();
  EXPECT_EQ(2, CountIterable(dba->Vertices(false)));
  // MATCH (n :label)
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all_by_label =
      MakeScanAllByLabel(storage, symbol_table, "n", label);
  // RETURN n
  auto output = NEXPR("n", IDENT("n"));
  auto produce = MakeProduce(scan_all_by_label.op_, output);
  symbol_table[*output->expression_] = scan_all_by_label.sym_;
  symbol_table[*output] = symbol_table.CreateSymbol("n", true);
  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  ASSERT_EQ(results.size(), 1);
  auto result_row = results[0];
  ASSERT_EQ(result_row.size(), 1);
  EXPECT_EQ(result_row[0].Value<VertexAccessor>(), labeled_vertex);
}

TEST(QueryPlan, ScanAllByLabelProperty) {
  database::SingleNode db;
  // Add 5 vertices with same label, but with different property values.
  auto label = db.Access()->Label("label");
  auto prop = db.Access()->Property("prop");
  // vertex property values that will be stored into the DB
  // clang-format off
  std::vector<TypedValue> values{
      true, false, "a", "b", "c", 0, 1, 2, 0.5, 1.5, 2.5,
      std::vector<TypedValue>{0}, std::vector<TypedValue>{1},
      std::vector<TypedValue>{2}};
  // clang-format on
  {
    auto dba = db.Access();
    for (const auto &value : values) {
      auto vertex = dba->InsertVertex();
      vertex.add_label(label);
      vertex.PropsSet(prop, value);
    }
    dba->Commit();
    db.Access()->BuildIndex(label, prop);
  }
  auto dba = db.Access();
  ASSERT_EQ(14, CountIterable(dba->Vertices(false)));

  auto check = [&dba, label, prop](TypedValue lower, Bound::Type lower_type,
                                   TypedValue upper, Bound::Type upper_type,
                                   const std::vector<TypedValue> &expected) {
    AstStorage storage;
    SymbolTable symbol_table;
    auto scan_all = MakeScanAllByLabelPropertyRange(
        storage, symbol_table, "n", label, prop,
        Bound{LITERAL(lower), lower_type}, Bound{LITERAL(upper), upper_type});
    // RETURN n
    auto output = NEXPR("n", IDENT("n"));
    auto produce = MakeProduce(scan_all.op_, output);
    symbol_table[*output->expression_] = scan_all.sym_;
    symbol_table[*output] = symbol_table.CreateSymbol("n", true);
    auto results = CollectProduce(produce.get(), symbol_table, *dba);
    ASSERT_EQ(results.size(), expected.size());
    for (size_t i = 0; i < expected.size(); i++) {
      TypedValue equal =
          results[i][0].Value<VertexAccessor>().PropsAt(prop) == expected[i];
      ASSERT_EQ(equal.type(), TypedValue::Type::Bool);
      EXPECT_TRUE(equal.Value<bool>());
    }
  };

  // normal ranges that return something
  check(false, Bound::Type::INCLUSIVE, true, Bound::Type::EXCLUSIVE, {false});
  check(false, Bound::Type::EXCLUSIVE, true, Bound::Type::INCLUSIVE, {true});
  check("a", Bound::Type::EXCLUSIVE, "c", Bound::Type::EXCLUSIVE, {"b"});
  check(0, Bound::Type::EXCLUSIVE, 2, Bound::Type::INCLUSIVE, {0.5, 1, 1.5, 2});
  check(1.5, Bound::Type::EXCLUSIVE, 2.5, Bound::Type::INCLUSIVE, {2, 2.5});
  check(std::vector<TypedValue>{0.5}, Bound::Type::EXCLUSIVE,
        std::vector<TypedValue>{1.5}, Bound::Type::INCLUSIVE,
        {TypedValue(std::vector<TypedValue>{1})});

  // when a range contains different types, nothing should get returned
  for (const auto &value_a : values)
    for (const auto &value_b : values) {
      if (PropertyValue::AreComparableTypes(
              static_cast<PropertyValue>(value_a).type(),
              static_cast<PropertyValue>(value_b).type()))
        continue;
      check(value_a, Bound::Type::INCLUSIVE, value_b, Bound::Type::INCLUSIVE,
            {});
    }
}

TEST(QueryPlan, ScanAllByLabelPropertyEqualityNoError) {
  database::SingleNode db;
  // Add 2 vertices with same label, but with property values that cannot be
  // compared. On the other hand, equality works fine.
  auto label = db.Access()->Label("label");
  auto prop = db.Access()->Property("prop");
  {
    auto dba = db.Access();
    auto number_vertex = dba->InsertVertex();
    number_vertex.add_label(label);
    number_vertex.PropsSet(prop, 42);
    auto string_vertex = dba->InsertVertex();
    string_vertex.add_label(label);
    string_vertex.PropsSet(prop, "string");
    dba->Commit();
    db.Access()->BuildIndex(label, prop);
  }
  auto dba = db.Access();
  EXPECT_EQ(2, CountIterable(dba->Vertices(false)));
  // MATCH (n :label {prop: 42})
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all = MakeScanAllByLabelPropertyValue(storage, symbol_table, "n",
                                                  label, prop, LITERAL(42));
  // RETURN n
  auto output = NEXPR("n", IDENT("n"));
  auto produce = MakeProduce(scan_all.op_, output);
  symbol_table[*output->expression_] = scan_all.sym_;
  symbol_table[*output] = symbol_table.CreateSymbol("n", true);
  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  ASSERT_EQ(results.size(), 1);
  const auto &row = results[0];
  ASSERT_EQ(row.size(), 1);
  auto vertex = row[0].Value<VertexAccessor>();
  auto value = vertex.PropsAt(prop);
  TypedValue::BoolEqual eq;
  EXPECT_TRUE(eq(value, 42));
}

TEST(QueryPlan, ScanAllByLabelPropertyValueError) {
  database::SingleNode db;
  auto label = db.Access()->Label("label");
  auto prop = db.Access()->Property("prop");
  {
    auto dba = db.Access();
    for (int i = 0; i < 2; ++i) {
      auto vertex = dba->InsertVertex();
      vertex.add_label(label);
      vertex.PropsSet(prop, i);
    }
    dba->Commit();
  }
  db.Access()->BuildIndex(label, prop);
  auto dba = db.Access();
  EXPECT_EQ(2, CountIterable(dba->Vertices(false)));
  // MATCH (m), (n :label {prop: m})
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all = MakeScanAll(storage, symbol_table, "m");
  auto *ident_m = IDENT("m");
  symbol_table[*ident_m] = scan_all.sym_;
  auto scan_index = MakeScanAllByLabelPropertyValue(
      storage, symbol_table, "n", label, prop, ident_m, scan_all.op_);
  EXPECT_THROW(PullAll(scan_index.op_, *dba, symbol_table),
               QueryRuntimeException);
}

TEST(QueryPlan, ScanAllByLabelPropertyRangeError) {
  database::SingleNode db;
  auto label = db.Access()->Label("label");
  auto prop = db.Access()->Property("prop");
  {
    auto dba = db.Access();
    for (int i = 0; i < 2; ++i) {
      auto vertex = dba->InsertVertex();
      vertex.add_label(label);
      vertex.PropsSet(prop, i);
    }
    dba->Commit();
  }
  db.Access()->BuildIndex(label, prop);
  auto dba = db.Access();
  EXPECT_EQ(2, CountIterable(dba->Vertices(false)));
  // MATCH (m), (n :label {prop: m})
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all = MakeScanAll(storage, symbol_table, "m");
  auto *ident_m = IDENT("m");
  symbol_table[*ident_m] = scan_all.sym_;
  {
    // Lower bound isn't property value
    auto scan_index = MakeScanAllByLabelPropertyRange(
        storage, symbol_table, "n", label, prop,
        Bound{ident_m, Bound::Type::INCLUSIVE}, std::experimental::nullopt,
        scan_all.op_);
    EXPECT_THROW(PullAll(scan_index.op_, *dba, symbol_table),
                 QueryRuntimeException);
  }
  {
    // Upper bound isn't property value
    auto scan_index = MakeScanAllByLabelPropertyRange(
        storage, symbol_table, "n", label, prop, std::experimental::nullopt,
        Bound{ident_m, Bound::Type::INCLUSIVE}, scan_all.op_);
    EXPECT_THROW(PullAll(scan_index.op_, *dba, symbol_table),
                 QueryRuntimeException);
  }
  {
    // Both bounds aren't property value
    auto scan_index = MakeScanAllByLabelPropertyRange(
        storage, symbol_table, "n", label, prop,
        Bound{ident_m, Bound::Type::INCLUSIVE},
        Bound{ident_m, Bound::Type::INCLUSIVE}, scan_all.op_);
    EXPECT_THROW(PullAll(scan_index.op_, *dba, symbol_table),
                 QueryRuntimeException);
  }
}

TEST(QueryPlan, ScanAllByLabelPropertyEqualNull) {
  database::SingleNode db;
  // Add 2 vertices with the same label, but one has a property value while
  // the other does not. Checking if the value is equal to null, should
  // yield no results.
  auto label = db.Access()->Label("label");
  auto prop = db.Access()->Property("prop");
  {
    auto dba = db.Access();
    auto vertex = dba->InsertVertex();
    vertex.add_label(label);
    auto vertex_with_prop = dba->InsertVertex();
    vertex_with_prop.add_label(label);
    vertex_with_prop.PropsSet(prop, 42);
    dba->Commit();
    db.Access()->BuildIndex(label, prop);
  }
  auto dba = db.Access();
  EXPECT_EQ(2, CountIterable(dba->Vertices(false)));
  // MATCH (n :label {prop: 42})
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all = MakeScanAllByLabelPropertyValue(
      storage, symbol_table, "n", label, prop, LITERAL(TypedValue::Null));
  // RETURN n
  auto output = NEXPR("n", IDENT("n"));
  auto produce = MakeProduce(scan_all.op_, output);
  symbol_table[*output->expression_] = scan_all.sym_;
  symbol_table[*output] = symbol_table.CreateSymbol("n", true);
  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(results.size(), 0);
}

TEST(QueryPlan, ScanAllByLabelPropertyRangeNull) {
  database::SingleNode db;
  // Add 2 vertices with the same label, but one has a property value while
  // the other does not. Checking if the value is between nulls, should
  // yield no results.
  auto label = db.Access()->Label("label");
  auto prop = db.Access()->Property("prop");
  {
    auto dba = db.Access();
    auto vertex = dba->InsertVertex();
    vertex.add_label(label);
    auto vertex_with_prop = dba->InsertVertex();
    vertex_with_prop.add_label(label);
    vertex_with_prop.PropsSet(prop, 42);
    dba->Commit();
    db.Access()->BuildIndex(label, prop);
  }
  auto dba = db.Access();
  EXPECT_EQ(2, CountIterable(dba->Vertices(false)));
  // MATCH (n :label) WHERE null <= n.prop < null
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all = MakeScanAllByLabelPropertyRange(
      storage, symbol_table, "n", label, prop,
      Bound{LITERAL(TypedValue::Null), Bound::Type::INCLUSIVE},
      Bound{LITERAL(TypedValue::Null), Bound::Type::EXCLUSIVE});
  // RETURN n
  auto output = NEXPR("n", IDENT("n"));
  auto produce = MakeProduce(scan_all.op_, output);
  symbol_table[*output->expression_] = scan_all.sym_;
  symbol_table[*output] = symbol_table.CreateSymbol("n", true);
  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(results.size(), 0);
}

TEST(QueryPlan, ScanAllByLabelPropertyNoValueInIndexContinuation) {
  database::SingleNode db;
  auto label = db.Access()->Label("label");
  auto prop = db.Access()->Property("prop");
  {
    auto dba = db.Access();
    auto v = dba->InsertVertex();
    v.add_label(label);
    v.PropsSet(prop, 2);
    dba->Commit();
    db.Access()->BuildIndex(label, prop);
  }
  auto dba = db.Access();
  EXPECT_EQ(1, CountIterable(dba->Vertices(false)));

  AstStorage storage;
  SymbolTable symbol_table;

  // UNWIND [1, 2, 3] as x
  auto input_expr = LIST(LITERAL(1), LITERAL(2), LITERAL(3));
  auto x = symbol_table.CreateSymbol("x", true);
  auto unwind = std::make_shared<plan::Unwind>(nullptr, input_expr, x);
  auto x_expr = IDENT("x");
  symbol_table[*x_expr] = x;

  // MATCH (n :label {prop: x})
  auto scan_all = MakeScanAllByLabelPropertyValue(storage, symbol_table, "n",
                                                  label, prop, x_expr, unwind);

  EXPECT_EQ(PullAll(scan_all.op_, *dba, symbol_table), 1);
}

TEST(QueryPlan, ScanAllEqualsScanAllByLabelProperty) {
  database::SingleNode db;
  auto label = db.Access()->Label("label");
  auto prop = db.Access()->Property("prop");

  // Insert vertices
  const int vertex_count = 300, vertex_prop_count = 50;
  const int prop_value1 = 42, prop_value2 = 69;

  for (int i = 0; i < vertex_count; ++i) {
    auto dba = db.Access();
    auto v = dba->InsertVertex();
    v.add_label(label);
    v.PropsSet(prop, i < vertex_prop_count ? prop_value1 : prop_value2);
    dba->Commit();
  }

  db.Access()->BuildIndex(label, prop);

  // Make sure there are `vertex_count` vertices
  {
    auto dba = db.Access();
    EXPECT_EQ(vertex_count, CountIterable(dba->Vertices(false)));
  }

  // Make sure there are `vertex_prop_count` results when using index
  auto count_with_index = [&db, &label, &prop](int prop_value, int prop_count) {
    AstStorage storage;
    SymbolTable symbol_table;
    auto dba = db.Access();
    auto scan_all_by_label_property_value = MakeScanAllByLabelPropertyValue(
        storage, symbol_table, "n", label, prop, LITERAL(prop_value));
    auto output = NEXPR("n", IDENT("n"));
    auto produce = MakeProduce(scan_all_by_label_property_value.op_, output);
    symbol_table[*output->expression_] = scan_all_by_label_property_value.sym_;
    symbol_table[*output] =
        symbol_table.CreateSymbol("named_expression_1", true);
    EXPECT_EQ(PullAll(produce, *dba, symbol_table), prop_count);
  };

  // Make sure there are `vertex_count` results when using scan all
  auto count_with_scan_all = [&db, &prop](int prop_value, int prop_count) {
    AstStorage storage;
    SymbolTable symbol_table;
    auto dba_ptr = db.Access();
    auto &dba = *dba_ptr;
    auto scan_all = MakeScanAll(storage, symbol_table, "n");
    auto e = storage.Create<PropertyLookup>(storage.Create<Identifier>("n"),
                                            std::make_pair("prop", prop));
    symbol_table[*e->expression_] = scan_all.sym_;
    auto filter =
        std::make_shared<Filter>(scan_all.op_, EQ(e, LITERAL(prop_value)));
    auto output = NEXPR("n", IDENT("n"));
    auto produce = MakeProduce(filter, output);
    symbol_table[*output->expression_] = scan_all.sym_;
    symbol_table[*output] =
        symbol_table.CreateSymbol("named_expression_1", true);
    EXPECT_EQ(PullAll(produce, dba, symbol_table), prop_count);
  };

  count_with_index(prop_value1, vertex_prop_count);
  count_with_scan_all(prop_value1, vertex_prop_count);

  count_with_index(prop_value2, vertex_count - vertex_prop_count);
  count_with_scan_all(prop_value2, vertex_count - vertex_prop_count);
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

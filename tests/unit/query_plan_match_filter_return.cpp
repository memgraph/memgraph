#include <iterator>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include <fmt/format.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cppitertools/enumerate.hpp>
#include <cppitertools/product.hpp>
#include <cppitertools/range.hpp>
#include <cppitertools/repeat.hpp>

#include "communication/result_stream_faker.hpp"
#include "database/single_node/graph_db.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/plan/operator.hpp"

#include "query_plan_common.hpp"

using namespace query;
using namespace query::plan;

class MatchReturnFixture : public testing::Test {
 protected:
  database::GraphDb db_;
  database::GraphDbAccessor dba_{db_.Access()};
  AstStorage storage;
  SymbolTable symbol_table;

  void AddVertices(int count) {
    for (int i = 0; i < count; i++) dba_.InsertVertex();
  }

  std::vector<Path> PathResults(std::shared_ptr<Produce> &op) {
    std::vector<Path> res;
    auto context = MakeContext(storage, symbol_table, &dba_);
    for (const auto &row : CollectProduce(*op, &context))
      res.emplace_back(row[0].ValuePath());
    return res;
  }
};

TEST_F(MatchReturnFixture, MatchReturn) {
  AddVertices(2);
  dba_.AdvanceCommand();

  auto test_pull_count = [&](GraphView graph_view) {
    auto scan_all =
        MakeScanAll(storage, symbol_table, "n", nullptr, graph_view);
    auto output =
        NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))
            ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(scan_all.op_, output);
    auto context = MakeContext(storage, symbol_table, &dba_);
    return PullAll(*produce, &context);
  };

  EXPECT_EQ(2, test_pull_count(GraphView::NEW));
  EXPECT_EQ(2, test_pull_count(GraphView::OLD));
  dba_.InsertVertex();
  EXPECT_EQ(3, test_pull_count(GraphView::NEW));
  EXPECT_EQ(2, test_pull_count(GraphView::OLD));
  dba_.AdvanceCommand();
  EXPECT_EQ(3, test_pull_count(GraphView::OLD));
}

TEST_F(MatchReturnFixture, MatchReturnPath) {
  AddVertices(2);
  dba_.AdvanceCommand();

  auto scan_all = MakeScanAll(storage, symbol_table, "n", nullptr);
  Symbol path_sym = symbol_table.CreateSymbol("path", true);
  auto make_path = std::make_shared<ConstructNamedPath>(
      scan_all.op_, path_sym, std::vector<Symbol>{scan_all.sym_});
  auto output =
      NEXPR("path", IDENT("path")->MapTo(path_sym))
          ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(make_path, output);
  auto results = PathResults(produce);
  ASSERT_EQ(results.size(), 2);
  std::vector<query::Path> expected_paths;
  for (const auto &v : dba_.Vertices(false)) expected_paths.emplace_back(v);
  ASSERT_EQ(expected_paths.size(), 2);
  EXPECT_TRUE(std::is_permutation(expected_paths.begin(), expected_paths.end(),
                                  results.begin()));
}

TEST(QueryPlan, MatchReturnCartesian) {
  database::GraphDb db;
  auto dba = db.Access();

  dba.InsertVertex().add_label(dba.Label("l1"));
  dba.InsertVertex().add_label(dba.Label("l2"));
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto m = MakeScanAll(storage, symbol_table, "m", n.op_);
  auto return_n =
      NEXPR("n", IDENT("n")->MapTo(n.sym_))
          ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto return_m =
      NEXPR("m", IDENT("m")->MapTo(m.sym_))
          ->MapTo(symbol_table.CreateSymbol("named_expression_2", true));
  auto produce = MakeProduce(m.op_, return_n, return_m);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 4);
  // ensure the result ordering is OK:
  // "n" from the results is the same for the first two rows, while "m" isn't
  EXPECT_EQ(results[0][0].ValueVertex(), results[1][0].ValueVertex());
  EXPECT_NE(results[0][1].ValueVertex(), results[1][1].ValueVertex());
}

TEST(QueryPlan, StandaloneReturn) {
  database::GraphDb db;
  auto dba = db.Access();

  // add a few nodes to the database
  dba.InsertVertex();
  dba.InsertVertex();
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto output = NEXPR("n", LITERAL(42));
  auto produce = MakeProduce(std::shared_ptr<LogicalOperator>(nullptr), output);
  output->MapTo(symbol_table.CreateSymbol("named_expression_1", true));

  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 1);
  EXPECT_EQ(results[0].size(), 1);
  EXPECT_EQ(results[0][0].ValueInt(), 42);
}

TEST(QueryPlan, NodeFilterLabelsAndProperties) {
  database::GraphDb db;
  auto dba = db.Access();

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
  v1.PropsSet(property.second, PropertyValue(42));
  v2.PropsSet(property.second, PropertyValue(1));
  v4.PropsSet(property.second, PropertyValue(42));
  v5.PropsSet(property.second, PropertyValue(1));
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  // make a scan all
  auto n = MakeScanAll(storage, symbol_table, "n");
  n.node_->labels_.emplace_back(storage.GetLabelIx(dba.LabelName(label)));
  n.node_->properties_[storage.GetPropertyIx(property.first)] = LITERAL(42);

  // node filtering
  auto *filter_expr =
      AND(storage.Create<LabelsTest>(n.node_->identifier_, n.node_->labels_),
          EQ(PROPERTY_LOOKUP(n.node_->identifier_, property), LITERAL(42)));
  auto node_filter = std::make_shared<Filter>(n.op_, filter_expr);

  // make a named expression and a produce
  auto output =
      NEXPR("x", IDENT("n")->MapTo(n.sym_))
          ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(node_filter, output);

  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*produce, &context));

  //  test that filtering works with old records
  v4.Reconstruct();
  v4.add_label(label);
  EXPECT_EQ(1, PullAll(*produce, &context));
  dba.AdvanceCommand();
  EXPECT_EQ(2, PullAll(*produce, &context));
}

TEST(QueryPlan, NodeFilterMultipleLabels) {
  database::GraphDb db;
  auto dba = db.Access();

  // add a few nodes to the database
  storage::Label label1 = dba.Label("label1");
  storage::Label label2 = dba.Label("label2");
  storage::Label label3 = dba.Label("label3");
  // the test will look for nodes that have label1 and label2
  dba.InsertVertex();                    // NOT accepted
  dba.InsertVertex().add_label(label1);  // NOT accepted
  dba.InsertVertex().add_label(label2);  // NOT accepted
  dba.InsertVertex().add_label(label3);  // NOT accepted
  auto v1 = dba.InsertVertex();          // YES accepted
  v1.add_label(label1);
  v1.add_label(label2);
  auto v2 = dba.InsertVertex();  // NOT accepted
  v2.add_label(label1);
  v2.add_label(label3);
  auto v3 = dba.InsertVertex();  // YES accepted
  v3.add_label(label1);
  v3.add_label(label2);
  v3.add_label(label3);
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  // make a scan all
  auto n = MakeScanAll(storage, symbol_table, "n");
  n.node_->labels_.emplace_back(storage.GetLabelIx(dba.LabelName(label1)));
  n.node_->labels_.emplace_back(storage.GetLabelIx(dba.LabelName(label2)));

  // node filtering
  auto *filter_expr =
      storage.Create<LabelsTest>(n.node_->identifier_, n.node_->labels_);
  auto node_filter = std::make_shared<Filter>(n.op_, filter_expr);

  // make a named expression and a produce
  auto output =
      NEXPR("n", IDENT("n")->MapTo(n.sym_))
          ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(node_filter, output);

  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 2);
}

TEST(QueryPlan, Cartesian) {
  database::GraphDb db;
  auto dba = db.Access();

  auto add_vertex = [&dba](std::string label) {
    auto vertex = dba.InsertVertex();
    vertex.add_label(dba.Label(label));
    return vertex;
  };

  std::vector<VertexAccessor> vertices{add_vertex("v1"), add_vertex("v2"),
                                       add_vertex("v3")};
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto m = MakeScanAll(storage, symbol_table, "m");
  auto return_n =
      NEXPR("n", IDENT("n")->MapTo(n.sym_))
          ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto return_m =
      NEXPR("m", IDENT("m")->MapTo(m.sym_))
          ->MapTo(symbol_table.CreateSymbol("named_expression_2", true));

  std::vector<Symbol> left_symbols{n.sym_};
  std::vector<Symbol> right_symbols{m.sym_};
  auto cartesian_op =
      std::make_shared<Cartesian>(n.op_, left_symbols, m.op_, right_symbols);

  auto produce = MakeProduce(cartesian_op, return_n, return_m);

  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 9);
  for (int i = 0; i < 3; ++i) {
    for (int j = 0; j < 3; ++j) {
      EXPECT_EQ(results[3 * i + j][0].ValueVertex(), vertices[j]);
      EXPECT_EQ(results[3 * i + j][1].ValueVertex(), vertices[i]);
    }
  }
}

TEST(QueryPlan, CartesianEmptySet) {
  database::GraphDb db;
  auto dba = db.Access();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto m = MakeScanAll(storage, symbol_table, "m");
  auto return_n =
      NEXPR("n", IDENT("n")->MapTo(n.sym_))
          ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto return_m =
      NEXPR("m", IDENT("m")->MapTo(m.sym_))
          ->MapTo(symbol_table.CreateSymbol("named_expression_2", true));

  std::vector<Symbol> left_symbols{n.sym_};
  std::vector<Symbol> right_symbols{m.sym_};
  auto cartesian_op =
      std::make_shared<Cartesian>(n.op_, left_symbols, m.op_, right_symbols);

  auto produce = MakeProduce(cartesian_op, return_n, return_m);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 0);
}

TEST(QueryPlan, CartesianThreeWay) {
  database::GraphDb db;
  auto dba = db.Access();
  auto add_vertex = [&dba](std::string label) {
    auto vertex = dba.InsertVertex();
    vertex.add_label(dba.Label(label));
    return vertex;
  };

  std::vector<VertexAccessor> vertices{add_vertex("v1"), add_vertex("v2"),
                                       add_vertex("v3")};
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto m = MakeScanAll(storage, symbol_table, "m");
  auto l = MakeScanAll(storage, symbol_table, "l");
  auto return_n =
      NEXPR("n", IDENT("n")->MapTo(n.sym_))
          ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto return_m =
      NEXPR("m", IDENT("m")->MapTo(m.sym_))
          ->MapTo(symbol_table.CreateSymbol("named_expression_2", true));
  auto return_l =
      NEXPR("l", IDENT("l")->MapTo(l.sym_))
          ->MapTo(symbol_table.CreateSymbol("named_expression_3", true));

  std::vector<Symbol> n_symbols{n.sym_};
  std::vector<Symbol> m_symbols{m.sym_};
  std::vector<Symbol> n_m_symbols{n.sym_, m.sym_};
  std::vector<Symbol> l_symbols{l.sym_};
  auto cartesian_op_1 =
      std::make_shared<Cartesian>(n.op_, n_symbols, m.op_, m_symbols);

  auto cartesian_op_2 = std::make_shared<Cartesian>(cartesian_op_1, n_m_symbols,
                                                    l.op_, l_symbols);

  auto produce = MakeProduce(cartesian_op_2, return_n, return_m, return_l);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 27);
  int id = 0;
  for (int i = 0; i < 3; ++i) {
    for (int j = 0; j < 3; ++j) {
      for (int k = 0; k < 3; ++k) {
        EXPECT_EQ(results[id][0].ValueVertex(), vertices[k]);
        EXPECT_EQ(results[id][1].ValueVertex(), vertices[j]);
        EXPECT_EQ(results[id][2].ValueVertex(), vertices[i]);
        ++id;
      }
    }
  }
}

class ExpandFixture : public testing::Test {
 protected:
  database::GraphDb db_;
  database::GraphDbAccessor dba_{db_.Access()};
  AstStorage storage;
  SymbolTable symbol_table;

  // make a V-graph (v3)<-[r2]-(v1)-[r1]->(v2)
  VertexAccessor v1 = dba_.InsertVertex();
  VertexAccessor v2 = dba_.InsertVertex();
  VertexAccessor v3 = dba_.InsertVertex();
  storage::EdgeType edge_type = dba_.EdgeType("Edge");
  EdgeAccessor r1 = dba_.InsertEdge(v1, v2, edge_type);
  EdgeAccessor r2 = dba_.InsertEdge(v1, v3, edge_type);

  void SetUp() override {
    v1.add_label(dba_.Label("l1"));
    v2.add_label(dba_.Label("l2"));
    v3.add_label(dba_.Label("l3"));
    dba_.AdvanceCommand();
  }
};

TEST_F(ExpandFixture, Expand) {
  auto test_expand = [&](EdgeAtom::Direction direction, GraphView graph_view) {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", direction,
                          {}, "m", false, graph_view);

    // make a named expression and a produce
    auto output =
        NEXPR("m", IDENT("m")->MapTo(r_m.node_sym_))
            ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(r_m.op_, output);
    auto context = MakeContext(storage, symbol_table, &dba_);
    return PullAll(*produce, &context);
  };

  // test that expand works well for both old and new graph state
  v1.Reconstruct();
  v2.Reconstruct();
  v3.Reconstruct();
  dba_.InsertEdge(v1, v2, edge_type);
  dba_.InsertEdge(v1, v3, edge_type);
  EXPECT_EQ(2, test_expand(EdgeAtom::Direction::OUT, GraphView::OLD));
  EXPECT_EQ(2, test_expand(EdgeAtom::Direction::IN, GraphView::OLD));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::BOTH, GraphView::OLD));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::OUT, GraphView::NEW));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::IN, GraphView::NEW));
  EXPECT_EQ(8, test_expand(EdgeAtom::Direction::BOTH, GraphView::NEW));
  dba_.AdvanceCommand();
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
  auto output =
      NEXPR("path", IDENT("path")->MapTo(path_sym))
          ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(path, output);

  std::vector<query::Path> expected_paths{query::Path(v1, r2, v3),
                                          query::Path(v1, r1, v2)};
  auto context = MakeContext(storage, symbol_table, &dba_);
  auto results = CollectProduce(*produce, &context);
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

  database::GraphDb db_;
  database::GraphDbAccessor dba_{db_.Access()};
  // labels for layers in the double chain
  std::vector<storage::Label> labels;
  // for all the edges
  storage::EdgeType edge_type = dba_.EdgeType("edge_type");

  AstStorage storage;
  SymbolTable symbol_table;

  // using std::nullopt
  std::nullopt_t nullopt = std::nullopt;

  void SetUp() {
    // create the graph
    int chain_length = 3;
    std::vector<VertexAccessor> layer;
    for (int from_layer_ind = -1; from_layer_ind < chain_length - 1;
         from_layer_ind++) {
      std::vector<VertexAccessor> new_layer{dba_.InsertVertex(),
                                            dba_.InsertVertex()};
      auto label = dba_.Label(std::to_string(from_layer_ind + 1));
      labels.push_back(label);
      for (size_t v_to_ind = 0; v_to_ind < new_layer.size(); v_to_ind++) {
        auto &v_to = new_layer[v_to_ind];
        v_to.add_label(label);
        for (size_t v_from_ind = 0; v_from_ind < layer.size(); v_from_ind++) {
          auto &v_from = layer[v_from_ind];
          auto edge = dba_.InsertEdge(v_from, v_to, edge_type);
          edge.PropsSet(dba_.Property("p"),
                        PropertyValue(fmt::format(
                            "V{}{}->V{}{}", from_layer_ind, v_from_ind,
                            from_layer_ind + 1, v_to_ind)));
        }
      }
      layer = new_layer;
    }
    dba_.AdvanceCommand();
    ASSERT_EQ(CountIterable(dba_.Vertices(false)), 2 * chain_length);
    ASSERT_EQ(CountIterable(dba_.Edges(false)), 4 * (chain_length - 1));
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
      std::optional<size_t> lower, std::optional<size_t> upper, Symbol edge_sym,
      const std::string &node_to, GraphView graph_view,
      bool is_reverse = false) {
    auto n_from = MakeScanAll(storage, symbol_table, node_from, input_op);
    auto filter_op = std::make_shared<Filter>(
        n_from.op_,
        storage.Create<query::LabelsTest>(
            n_from.node_->identifier_, std::vector<LabelIx>{storage.GetLabelIx(
                                           dba_.LabelName(labels[layer]))}));

    auto n_to = NODE(node_to);
    auto n_to_sym = symbol_table.CreateSymbol(node_to, true);
    n_to->identifier_->MapTo(n_to_sym);

    if (std::is_same<TExpansionOperator, ExpandVariable>::value) {
      // convert optional ints to optional expressions
      auto convert = [this](std::optional<size_t> bound) {
        return bound ? LITERAL(static_cast<int64_t>(bound.value())) : nullptr;
      };
      CHECK(graph_view == GraphView::OLD)
          << "ExpandVariable should only be planned with GraphView::OLD";

      return std::make_shared<ExpandVariable>(
          filter_op, n_from.sym_, n_to_sym, edge_sym,
          EdgeAtom::Type::DEPTH_FIRST, direction, edge_types, is_reverse,
          convert(lower), convert(upper), false,
          ExpansionLambda{symbol_table.CreateSymbol("inner_edge", false),
                          symbol_table.CreateSymbol("inner_node", false),
                          nullptr},
          std::nullopt, std::nullopt);
    } else
      return std::make_shared<Expand>(filter_op, n_from.sym_, n_to_sym,
                                      edge_sym, direction, edge_types, false,
                                      graph_view);
  }

  /* Creates an edge (in the frame and symbol table). Returns the symbol. */
  auto Edge(const std::string &identifier, EdgeAtom::Direction direction) {
    auto edge = EDGE(identifier, direction);
    auto edge_sym = symbol_table.CreateSymbol(identifier, true);
    edge->identifier_->MapTo(edge_sym);
    return edge_sym;
  }

  /**
   * Pulls from the given input and returns the results under the given symbol.
   */
  auto GetListResults(std::shared_ptr<LogicalOperator> input_op, Symbol symbol) {
    Frame frame(symbol_table.max_position());
    auto cursor = input_op->MakeCursor(utils::NewDeleteResource());
    auto context = MakeContext(storage, symbol_table, &dba_);
    std::vector<utils::AVector<TypedValue>> results;
    while (cursor->Pull(frame, context))
      results.emplace_back(frame[symbol].ValueList());
    return results;
  }

  /**
   * Pulls from the given input and returns the results under the given symbol.
   */
  auto GetPathResults(std::shared_ptr<LogicalOperator> input_op, Symbol symbol) {
    Frame frame(symbol_table.max_position());
    auto cursor = input_op->MakeCursor(utils::NewDeleteResource());
    auto context = MakeContext(storage, symbol_table, &dba_);
    std::vector<Path> results;
    while (cursor->Pull(frame, context))
      results.emplace_back(frame[symbol].ValuePath());
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
    for (const auto &edge_list : GetListResults(input_op, symbol)) {
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
                         std::optional<size_t> lower,
                         std::optional<size_t> upper, bool reverse) {
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
                         std::optional<size_t> lower,
                         std::optional<size_t> upper,
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
      last_op =
          std::make_shared<EdgeUniquenessFilter>(last_op, last_symbol, symbols);
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
  auto test_expand =
      [&](int layer, EdgeAtom::Direction direction, std::optional<size_t> lower,
          std::optional<size_t> upper, bool add_uniqueness_check) {
        auto e1 = Edge("r1", direction);
        auto first =
            AddMatch<ExpandVariable>(nullptr, "n1", layer, direction, {}, lower,
                                     upper, e1, "m1", GraphView::OLD);
        auto e2 = Edge("r2", direction);
        auto last_op =
            AddMatch<ExpandVariable>(first, "n2", layer, direction, {}, lower,
                                     upper, e2, "m2", GraphView::OLD);
        if (add_uniqueness_check) {
          last_op = std::make_shared<EdgeUniquenessFilter>(
              last_op, e2, std::vector<Symbol>{e1});
        }

        return GetEdgeListSizes(last_op, e2);
      };

  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2, false),
            (map_int{{2, 8 * 8}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2, true),
            (map_int{{2, 5 * 8}}));
}

TEST_F(QueryPlanExpandVariable, NamedPath) {
  auto e = Edge("r", EdgeAtom::Direction::OUT);
  auto expand =
      AddMatch<ExpandVariable>(nullptr, "n", 0, EdgeAtom::Direction::OUT, {}, 2,
                               2, e, "m", GraphView::OLD);
  auto find_symbol = [this](const std::string &name) {
    for (const auto &sym : symbol_table.table())
      if (sym.second.name() == name) return sym.second;
    throw std::runtime_error("Symbol not found");
  };

  auto path_symbol =
      symbol_table.CreateSymbol("path", true, Symbol::Type::PATH);
  auto create_path = std::make_shared<ConstructNamedPath>(
      expand, path_symbol,
      std::vector<Symbol>{find_symbol("n"), e, find_symbol("m")});

  std::vector<query::Path> expected_paths;
  for (const auto &v : dba_.Vertices(labels[0], false))
    for (const auto &e1 : v.out())
      for (const auto &e2 : e1.to().out())
        expected_paths.emplace_back(v, e1, e1.to(), e2, e2.to());
  ASSERT_EQ(expected_paths.size(), 8);

  auto results = GetPathResults(create_path, path_symbol);
  ASSERT_EQ(results.size(), 8);
  EXPECT_TRUE(std::is_permutation(results.begin(), results.end(),
                                  expected_paths.begin()));
}

namespace std {
template <>
struct hash<std::pair<int, int>> {
  size_t operator()(const std::pair<int, int> &p) const {
    return p.first + 31 * p.second;
  }
};
}  // namespace std

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
  database::GraphDb db;
  database::GraphDbAccessor dba{db.Access()};
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
      v.back().PropsSet(prop.second, PropertyValue(i));
    }

    auto add_edge = [&](int from, int to, double weight) {
      EdgeAccessor edge = dba.InsertEdge(v[from], v[to], edge_type);
      edge.PropsSet(prop.second, PropertyValue(weight));
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
                       std::optional<int> max_depth, Expression *where,
                       std::optional<int> node_id = 0,
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
    ident_e->MapTo(weight_edge);

    // expand wshortest
    auto node_sym = existing_node_input
                        ? existing_node_input->sym_
                        : symbol_table.CreateSymbol("node", true);
    auto edge_list_sym = symbol_table.CreateSymbol("edgelist_", true);
    auto filter_lambda = last_op = std::make_shared<ExpandVariable>(
        last_op, n.sym_, node_sym, edge_list_sym,
        EdgeAtom::Type::WEIGHTED_SHORTEST_PATH, direction,
        std::vector<storage::EdgeType>{}, false, nullptr,
        max_depth ? LITERAL(max_depth.value()) : nullptr,
        existing_node_input != nullptr,
        ExpansionLambda{filter_edge, filter_node, where},
        ExpansionLambda{weight_edge, weight_node,
                        PROPERTY_LOOKUP(ident_e, prop)},
        total_weight);

    Frame frame(symbol_table.max_position());
    auto cursor = last_op->MakeCursor(utils::NewDeleteResource());
    std::vector<ResultType> results;
    auto context = MakeContext(storage, symbol_table, &dba);
    while (cursor->Pull(frame, context)) {
      results.push_back(ResultType{std::vector<EdgeAccessor>(),
                                   frame[node_sym].ValueVertex(),
                                   frame[total_weight].ValueDouble()});
      for (const TypedValue &edge : frame[edge_list_sym].ValueList())
        results.back().path.emplace_back(edge.ValueEdge());
    }

    return results;
  }

  template <typename TAccessor>
  auto GetProp(const TAccessor &accessor) {
    return accessor.PropsAt(prop.second).ValueInt();
  }

  template <typename TAccessor>
  auto GetDoubleProp(const TAccessor &accessor) {
    return accessor.PropsAt(prop.second).ValueDouble();
  }

  Expression *PropNe(Symbol symbol, int value) {
    auto ident = IDENT("inner_element");
    ident->MapTo(symbol);
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

TEST_F(QueryPlanExpandWeightedShortestPath, ExistingNode) {
  auto ExpandPreceeding = [this](std::optional<int> preceeding_node_id) {
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
                           std::nullopt, &n0);
  };

  EXPECT_EQ(ExpandPreceeding(std::nullopt).size(), 20);
  {
    auto results = ExpandPreceeding(3);
    ASSERT_EQ(results.size(), 4);
    for (int i = 0; i < 4; i++) EXPECT_EQ(GetProp(results[i].vertex), 3);
  }
}

TEST_F(QueryPlanExpandWeightedShortestPath, UpperBound) {
  {
    auto results =
        ExpandWShortest(EdgeAtom::Direction::BOTH, std::nullopt, LITERAL(true));
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
  {
    auto new_vertex = dba.InsertVertex();
    new_vertex.PropsSet(prop.second, PropertyValue(5));
    auto edge = dba.InsertEdge(v[4], new_vertex, edge_type);
    edge.PropsSet(prop.second, PropertyValue(2));
    dba.AdvanceCommand();

    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, 3, LITERAL(true));

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
}

TEST_F(QueryPlanExpandWeightedShortestPath, NonNumericWeight) {
  auto new_vertex = dba.InsertVertex();
  new_vertex.PropsSet(prop.second, PropertyValue(5));
  auto edge = dba.InsertEdge(v[4], new_vertex, edge_type);
  edge.PropsSet(prop.second, PropertyValue("not a number"));
  dba.AdvanceCommand();
  EXPECT_THROW(ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true)),
               QueryRuntimeException);
}

TEST_F(QueryPlanExpandWeightedShortestPath, NegativeWeight) {
  auto new_vertex = dba.InsertVertex();
  new_vertex.PropsSet(prop.second, PropertyValue(5));
  auto edge = dba.InsertEdge(v[4], new_vertex, edge_type);
  edge.PropsSet(prop.second, PropertyValue(-10));  // negative weight
  dba.AdvanceCommand();
  EXPECT_THROW(ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true)),
               QueryRuntimeException);
}

TEST_F(QueryPlanExpandWeightedShortestPath, NegativeUpperBound) {
  EXPECT_THROW(ExpandWShortest(EdgeAtom::Direction::BOTH, -1, LITERAL(true)),
               QueryRuntimeException);
}

TEST(QueryPlan, ExpandOptional) {
  database::GraphDb db;
  auto dba = db.Access();

  AstStorage storage;
  SymbolTable symbol_table;

  // graph (v2 {p: 2})<-[:T]-(v1 {p: 1})-[:T]->(v3 {p: 2})
  auto prop = dba.Property("p");
  auto edge_type = dba.EdgeType("T");
  auto v1 = dba.InsertVertex();
  v1.PropsSet(prop, PropertyValue(1));
  auto v2 = dba.InsertVertex();
  v2.PropsSet(prop, PropertyValue(2));
  dba.InsertEdge(v1, v2, edge_type);
  auto v3 = dba.InsertVertex();
  v3.PropsSet(prop, PropertyValue(2));
  dba.InsertEdge(v1, v3, edge_type);
  dba.AdvanceCommand();

  // MATCH (n) OPTIONAL MATCH (n)-[r]->(m)
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m =
      MakeExpand(storage, symbol_table, nullptr, n.sym_, "r",
                 EdgeAtom::Direction::OUT, {}, "m", false, GraphView::OLD);
  auto optional = std::make_shared<plan::Optional>(
      n.op_, r_m.op_, std::vector<Symbol>{r_m.edge_sym_, r_m.node_sym_});

  // RETURN n, r, m
  auto n_ne = NEXPR("n", IDENT("n")->MapTo(n.sym_))
                  ->MapTo(symbol_table.CreateSymbol("n", true));
  auto r_ne = NEXPR("r", IDENT("r")->MapTo(r_m.edge_sym_))
                  ->MapTo(symbol_table.CreateSymbol("r", true));
  auto m_ne = NEXPR("m", IDENT("m")->MapTo(r_m.node_sym_))
                  ->MapTo(symbol_table.CreateSymbol("m", true));
  auto produce = MakeProduce(optional, n_ne, r_ne, m_ne);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(4, results.size());
  int v1_is_n_count = 0;
  for (auto &row : results) {
    ASSERT_EQ(row[0].type(), TypedValue::Type::Vertex);
    VertexAccessor &va = row[0].ValueVertex();
    auto va_p = va.PropsAt(prop);
    ASSERT_EQ(va_p.type(), PropertyValue::Type::Int);
    if (va_p.ValueInt() == 1) {
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
  database::GraphDb db;
  auto dba = db.Access();

  AstStorage storage;
  SymbolTable symbol_table;

  // OPTIONAL MATCH (n)
  auto n = MakeScanAll(storage, symbol_table, "n");
  // RETURN n
  auto n_ne = NEXPR("n", IDENT("n")->MapTo(n.sym_))
                  ->MapTo(symbol_table.CreateSymbol("n", true));
  auto optional = std::make_shared<plan::Optional>(nullptr, n.op_,
                                                   std::vector<Symbol>{n.sym_});
  auto produce = MakeProduce(optional, n_ne);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(1, results.size());
  EXPECT_EQ(results[0][0].type(), TypedValue::Type::Null);
}

TEST(QueryPlan, OptionalMatchEmptyDBExpandFromNode) {
  database::GraphDb db;
  auto dba = db.Access();
  AstStorage storage;
  SymbolTable symbol_table;
  // OPTIONAL MATCH (n)
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto optional = std::make_shared<plan::Optional>(nullptr, n.op_,
                                                   std::vector<Symbol>{n.sym_});
  // WITH n
  auto n_ne = NEXPR("n", IDENT("n")->MapTo(n.sym_));
  auto with_n_sym = symbol_table.CreateSymbol("n", true);
  n_ne->MapTo(with_n_sym);
  auto with = MakeProduce(optional, n_ne);
  // MATCH (n) -[r]-> (m)
  auto r_m =
      MakeExpand(storage, symbol_table, with, with_n_sym, "r",
                 EdgeAtom::Direction::OUT, {}, "m", false, GraphView::OLD);
  // RETURN m
  auto m_ne = NEXPR("m", IDENT("m")->MapTo(r_m.node_sym_))
                  ->MapTo(symbol_table.CreateSymbol("m", true));
  auto produce = MakeProduce(r_m.op_, m_ne);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(0, results.size());
}

TEST(QueryPlan, OptionalMatchThenExpandToMissingNode) {
  database::GraphDb db;
  auto dba = db.Access();
  // Make a graph with 2 connected, unlabeled nodes.
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto edge_type = dba.EdgeType("edge_type");
  dba.InsertEdge(v1, v2, edge_type);
  dba.AdvanceCommand();
  EXPECT_EQ(2, CountIterable(dba.Vertices(false)));
  EXPECT_EQ(1, CountIterable(dba.Edges(false)));
  AstStorage storage;
  SymbolTable symbol_table;
  // OPTIONAL MATCH (n :missing)
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto label_missing = "missing";
  n.node_->labels_.emplace_back(storage.GetLabelIx(label_missing));

  auto *filter_expr =
      storage.Create<LabelsTest>(n.node_->identifier_, n.node_->labels_);
  auto node_filter = std::make_shared<Filter>(n.op_, filter_expr);
  auto optional = std::make_shared<plan::Optional>(nullptr, node_filter,
                                                   std::vector<Symbol>{n.sym_});
  // WITH n
  auto n_ne = NEXPR("n", IDENT("n")->MapTo(n.sym_));
  auto with_n_sym = symbol_table.CreateSymbol("n", true);
  n_ne->MapTo(with_n_sym);
  auto with = MakeProduce(optional, n_ne);
  // MATCH (m) -[r]-> (n)
  auto m = MakeScanAll(storage, symbol_table, "m", with);
  auto edge_direction = EdgeAtom::Direction::OUT;
  auto edge = EDGE("r", edge_direction);
  auto edge_sym = symbol_table.CreateSymbol("r", true);
  edge->identifier_->MapTo(edge_sym);
  auto node = NODE("n");
  node->identifier_->MapTo(with_n_sym);
  auto expand = std::make_shared<plan::Expand>(
      m.op_, m.sym_, with_n_sym, edge_sym, edge_direction,
      std::vector<storage::EdgeType>{}, true, GraphView::OLD);
  // RETURN m
  auto m_ne = NEXPR("m", IDENT("m")->MapTo(m.sym_))
                  ->MapTo(symbol_table.CreateSymbol("m", true));
  auto produce = MakeProduce(expand, m_ne);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(0, results.size());
}

TEST(QueryPlan, ExpandExistingNode) {
  database::GraphDb db;
  auto dba = db.Access();

  // make a graph (v1)->(v2) that
  // has a recursive edge (v1)->(v1)
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto edge_type = dba.EdgeType("Edge");
  dba.InsertEdge(v1, v1, edge_type);
  dba.InsertEdge(v1, v2, edge_type);
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto test_existing = [&](bool with_existing, int expected_result_count) {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_n = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                          EdgeAtom::Direction::OUT, {}, "n", with_existing,
                          GraphView::OLD);
    if (with_existing)
      r_n.op_ = std::make_shared<Expand>(
          n.op_, n.sym_, n.sym_, r_n.edge_sym_, r_n.edge_->direction_,
          std::vector<storage::EdgeType>{}, with_existing, GraphView::OLD);

    // make a named expression and a produce
    auto output =
        NEXPR("n", IDENT("n")->MapTo(n.sym_))
            ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(r_n.op_, output);
    auto context = MakeContext(storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);
    EXPECT_EQ(results.size(), expected_result_count);
  };

  test_existing(true, 1);
  test_existing(false, 2);
}

TEST(QueryPlan, ExpandBothCycleEdgeCase) {
  // we're testing that expanding on BOTH
  // does only one expansion for a cycle
  database::GraphDb db;
  auto dba = db.Access();

  auto v = dba.InsertVertex();
  dba.InsertEdge(v, v, dba.EdgeType("et"));
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_ =
      MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                 EdgeAtom::Direction::BOTH, {}, "_", false, GraphView::OLD);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*r_.op_, &context));
}

TEST(QueryPlan, EdgeFilter) {
  database::GraphDb db;
  auto dba = db.Access();

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
        edges.back().PropsSet(prop.second, PropertyValue(42));
        break;
      case 1:
        edges.back().PropsSet(prop.second, PropertyValue(100));
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
    r_m.edge_->edge_types_.push_back(
        storage.GetEdgeTypeIx(dba.EdgeTypeName(edge_type)));
    r_m.edge_->properties_[storage.GetPropertyIx(prop.first)] = LITERAL(42);
    auto *filter_expr =
        EQ(PROPERTY_LOOKUP(r_m.edge_->identifier_, prop), LITERAL(42));
    auto edge_filter = std::make_shared<Filter>(r_m.op_, filter_expr);

    // make a named expression and a produce
    auto output =
        NEXPR("m", IDENT("m")->MapTo(r_m.node_sym_))
            ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(edge_filter, output);
    auto context = MakeContext(storage, symbol_table, &dba);
    return PullAll(*produce, &context);
  };

  EXPECT_EQ(1, test_filter());
  // test that edge filtering always filters on old state
  for (auto &edge : edges) edge.PropsSet(prop.second, PropertyValue(42));
  EXPECT_EQ(1, test_filter());
  dba.AdvanceCommand();
  EXPECT_EQ(3, test_filter());
}

TEST(QueryPlan, EdgeFilterMultipleTypes) {
  database::GraphDb db;
  auto dba = db.Access();

  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto type_1 = dba.EdgeType("type_1");
  auto type_2 = dba.EdgeType("type_2");
  auto type_3 = dba.EdgeType("type_3");
  dba.InsertEdge(v1, v2, type_1);
  dba.InsertEdge(v1, v2, type_2);
  dba.InsertEdge(v1, v2, type_3);
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  // make a scan all
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                        EdgeAtom::Direction::OUT, {type_1, type_2}, "m", false,
                        GraphView::OLD);

  // make a named expression and a produce
  auto output =
      NEXPR("m", IDENT("m")->MapTo(r_m.node_sym_))
          ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(r_m.op_, output);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 2);
}

TEST(QueryPlan, Filter) {
  database::GraphDb db;
  auto dba = db.Access();

  // add a 6 nodes with property 'prop', 2 have true as value
  auto property = PROPERTY_PAIR("property");
  for (int i = 0; i < 6; ++i)
    dba.InsertVertex().PropsSet(property.second, PropertyValue(i % 3 == 0));
  dba.InsertVertex();  // prop not set, gives NULL
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto e = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), property);
  auto f = std::make_shared<Filter>(n.op_, e);

  auto output =
      NEXPR("x", IDENT("n")->MapTo(n.sym_))
          ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(f, output);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(CollectProduce(*produce, &context).size(), 2);
}

TEST(QueryPlan, EdgeUniquenessFilter) {
  database::GraphDb db;
  auto dba = db.Access();

  // make a graph that has (v1)->(v2) and a recursive edge (v1)->(v1)
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto edge_type = dba.EdgeType("edge_type");
  dba.InsertEdge(v1, v2, edge_type);
  dba.InsertEdge(v1, v1, edge_type);
  dba.AdvanceCommand();

  auto check_expand_results = [&](bool edge_uniqueness) {
    AstStorage storage;
    SymbolTable symbol_table;

    auto n1 = MakeScanAll(storage, symbol_table, "n1");
    auto r1_n2 =
        MakeExpand(storage, symbol_table, n1.op_, n1.sym_, "r1",
                   EdgeAtom::Direction::OUT, {}, "n2", false, GraphView::OLD);
    std::shared_ptr<LogicalOperator> last_op = r1_n2.op_;
    auto r2_n3 =
        MakeExpand(storage, symbol_table, last_op, r1_n2.node_sym_, "r2",
                   EdgeAtom::Direction::OUT, {}, "n3", false, GraphView::OLD);
    last_op = r2_n3.op_;
    if (edge_uniqueness)
      last_op = std::make_shared<EdgeUniquenessFilter>(
          last_op, r2_n3.edge_sym_, std::vector<Symbol>{r1_n2.edge_sym_});
    auto context = MakeContext(storage, symbol_table, &dba);
    return PullAll(*last_op, &context);
  };

  EXPECT_EQ(2, check_expand_results(false));
  EXPECT_EQ(1, check_expand_results(true));
}

TEST(QueryPlan, Distinct) {
  // test queries like
  // UNWIND [1, 2, 3, 3] AS x RETURN DISTINCT x

  database::GraphDb db;
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
    x_expr->MapTo(x);

    auto distinct =
        std::make_shared<plan::Distinct>(unwind, std::vector<Symbol>{x});

    auto x_ne = NEXPR("x", x_expr);
    x_ne->MapTo(symbol_table.CreateSymbol("x_ne", true));
    auto produce = MakeProduce(distinct, x_ne);
    auto context = MakeContext(storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);
    ASSERT_EQ(output.size(), results.size());
    auto output_it = output.begin();
    for (const auto &row : results) {
      ASSERT_EQ(1, row.size());
      ASSERT_EQ(row[0].type(), output_it->type());
      if (assume_int_value) EXPECT_EQ(output_it->ValueInt(), row[0].ValueInt());
      output_it++;
    }
  };

  check_distinct({TypedValue(1), TypedValue(1), TypedValue(2), TypedValue(3),
                  TypedValue(3), TypedValue(3)},
                 {TypedValue(1), TypedValue(2), TypedValue(3)}, true);
  check_distinct({TypedValue(3), TypedValue(2), TypedValue(3), TypedValue(5),
                  TypedValue(3), TypedValue(5), TypedValue(2), TypedValue(1),
                  TypedValue(2)},
                 {TypedValue(3), TypedValue(2), TypedValue(5), TypedValue(1)},
                 true);
  check_distinct(
      {TypedValue(3), TypedValue("two"), TypedValue(), TypedValue(3),
       TypedValue(true), TypedValue(false), TypedValue("TWO"), TypedValue()},
      {TypedValue(3), TypedValue("two"), TypedValue(), TypedValue(true),
       TypedValue(false), TypedValue("TWO")},
      false);
}

TEST(QueryPlan, ScanAllByLabel) {
  database::GraphDb db;
  auto dba = db.Access();
  // Add a vertex with a label and one without.
  auto label = dba.Label("label");
  auto labeled_vertex = dba.InsertVertex();
  labeled_vertex.add_label(label);
  dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(2, CountIterable(dba.Vertices(false)));
  // MATCH (n :label)
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all_by_label =
      MakeScanAllByLabel(storage, symbol_table, "n", label);
  // RETURN n
  auto output = NEXPR("n", IDENT("n")->MapTo(scan_all_by_label.sym_))
                    ->MapTo(symbol_table.CreateSymbol("n", true));
  auto produce = MakeProduce(scan_all_by_label.op_, output);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), 1);
  auto result_row = results[0];
  ASSERT_EQ(result_row.size(), 1);
  EXPECT_EQ(result_row[0].ValueVertex(), labeled_vertex);
}

TEST(QueryPlan, ScanAllByLabelProperty) {
  database::GraphDb db;
  // Add 5 vertices with same label, but with different property values.
  auto label = db.Access().Label("label");
  auto prop = db.Access().Property("prop");
  // vertex property values that will be stored into the DB
  std::vector<PropertyValue> values{
      PropertyValue(true),
      PropertyValue(false),
      PropertyValue("a"),
      PropertyValue("b"),
      PropertyValue("c"),
      PropertyValue(0),
      PropertyValue(1),
      PropertyValue(2),
      PropertyValue(0.5),
      PropertyValue(1.5),
      PropertyValue(2.5),
      PropertyValue(std::vector<PropertyValue>{PropertyValue(0)}),
      PropertyValue(std::vector<PropertyValue>{PropertyValue(1)}),
      PropertyValue(std::vector<PropertyValue>{PropertyValue(2)})};
  {
    auto dba = db.Access();
    for (const auto &value : values) {
      auto vertex = dba.InsertVertex();
      vertex.add_label(label);
      vertex.PropsSet(prop, value);
    }
    dba.Commit();
    db.Access().BuildIndex(label, prop);
  }
  auto dba = db.Access();
  ASSERT_EQ(14, CountIterable(dba.Vertices(false)));

  auto check = [&dba, label, prop](TypedValue lower, Bound::Type lower_type,
                                   TypedValue upper, Bound::Type upper_type,
                                   const std::vector<TypedValue> &expected) {
    AstStorage storage;
    SymbolTable symbol_table;
    auto scan_all = MakeScanAllByLabelPropertyRange(
        storage, symbol_table, "n", label, prop, "prop",
        Bound{LITERAL(lower), lower_type}, Bound{LITERAL(upper), upper_type});
    // RETURN n
    auto output = NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))
                      ->MapTo(symbol_table.CreateSymbol("n", true));
    auto produce = MakeProduce(scan_all.op_, output);
    auto context = MakeContext(storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);
    ASSERT_EQ(results.size(), expected.size());
    for (size_t i = 0; i < expected.size(); i++) {
      TypedValue equal =
          TypedValue(results[i][0].ValueVertex().PropsAt(prop)) == expected[i];
      ASSERT_EQ(equal.type(), TypedValue::Type::Bool);
      EXPECT_TRUE(equal.ValueBool());
    }
  };

  // normal ranges that return something
  check(TypedValue(false), Bound::Type::INCLUSIVE, TypedValue(true),
        Bound::Type::EXCLUSIVE, {TypedValue(false)});
  check(TypedValue(false), Bound::Type::EXCLUSIVE, TypedValue(true),
        Bound::Type::INCLUSIVE, {TypedValue(true)});
  check(TypedValue("a"), Bound::Type::EXCLUSIVE, TypedValue("c"),
        Bound::Type::EXCLUSIVE, {TypedValue("b")});
  check(TypedValue(0), Bound::Type::EXCLUSIVE, TypedValue(2),
        Bound::Type::INCLUSIVE,
        {TypedValue(0.5), TypedValue(1), TypedValue(1.5), TypedValue(2)});
  check(TypedValue(1.5), Bound::Type::EXCLUSIVE, TypedValue(2.5),
        Bound::Type::INCLUSIVE, {TypedValue(2), TypedValue(2.5)});
  check(TypedValue(std::vector<TypedValue>{TypedValue(0.5)}),
        Bound::Type::EXCLUSIVE,
        TypedValue(std::vector<TypedValue>{TypedValue(1.5)}),
        Bound::Type::INCLUSIVE,
        {TypedValue(std::vector<TypedValue>{TypedValue(1)})});

  // when a range contains different types, nothing should get returned
  for (const auto &value_a : values)
    for (const auto &value_b : values) {
      if (PropertyValue::AreComparableTypes(
              static_cast<PropertyValue>(value_a).type(),
              static_cast<PropertyValue>(value_b).type()))
        continue;
      check(TypedValue(value_a), Bound::Type::INCLUSIVE, TypedValue(value_b),
            Bound::Type::INCLUSIVE, {});
    }
}

TEST(QueryPlan, ScanAllByLabelPropertyEqualityNoError) {
  database::GraphDb db;
  // Add 2 vertices with same label, but with property values that cannot be
  // compared. On the other hand, equality works fine.
  auto label = db.Access().Label("label");
  auto prop = db.Access().Property("prop");
  {
    auto dba = db.Access();
    auto number_vertex = dba.InsertVertex();
    number_vertex.add_label(label);
    number_vertex.PropsSet(prop, PropertyValue(42));
    auto string_vertex = dba.InsertVertex();
    string_vertex.add_label(label);
    string_vertex.PropsSet(prop, PropertyValue("string"));
    dba.Commit();
    db.Access().BuildIndex(label, prop);
  }
  auto dba = db.Access();
  EXPECT_EQ(2, CountIterable(dba.Vertices(false)));
  // MATCH (n :label {prop: 42})
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all = MakeScanAllByLabelPropertyValue(
      storage, symbol_table, "n", label, prop, "prop", LITERAL(42));
  // RETURN n
  auto output = NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))
                    ->MapTo(symbol_table.CreateSymbol("n", true));
  auto produce = MakeProduce(scan_all.op_, output);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), 1);
  const auto &row = results[0];
  ASSERT_EQ(row.size(), 1);
  auto vertex = row[0].ValueVertex();
  TypedValue value(vertex.PropsAt(prop));
  TypedValue::BoolEqual eq;
  EXPECT_TRUE(eq(value, TypedValue(42)));
}

TEST(QueryPlan, ScanAllByLabelPropertyValueError) {
  database::GraphDb db;
  auto label = db.Access().Label("label");
  auto prop = db.Access().Property("prop");
  {
    auto dba = db.Access();
    for (int i = 0; i < 2; ++i) {
      auto vertex = dba.InsertVertex();
      vertex.add_label(label);
      vertex.PropsSet(prop, PropertyValue(i));
    }
    dba.Commit();
  }
  db.Access().BuildIndex(label, prop);
  auto dba = db.Access();
  EXPECT_EQ(2, CountIterable(dba.Vertices(false)));
  // MATCH (m), (n :label {prop: m})
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all = MakeScanAll(storage, symbol_table, "m");
  auto *ident_m = IDENT("m");
  ident_m->MapTo(scan_all.sym_);
  auto scan_index = MakeScanAllByLabelPropertyValue(
      storage, symbol_table, "n", label, prop, "prop", ident_m, scan_all.op_);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_THROW(PullAll(*scan_index.op_, &context), QueryRuntimeException);
}

TEST(QueryPlan, ScanAllByLabelPropertyRangeError) {
  database::GraphDb db;
  auto label = db.Access().Label("label");
  auto prop = db.Access().Property("prop");
  {
    auto dba = db.Access();
    for (int i = 0; i < 2; ++i) {
      auto vertex = dba.InsertVertex();
      vertex.add_label(label);
      vertex.PropsSet(prop, PropertyValue(i));
    }
    dba.Commit();
  }
  db.Access().BuildIndex(label, prop);
  auto dba = db.Access();
  EXPECT_EQ(2, CountIterable(dba.Vertices(false)));
  // MATCH (m), (n :label {prop: m})
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all = MakeScanAll(storage, symbol_table, "m");
  auto *ident_m = IDENT("m");
  ident_m->MapTo(scan_all.sym_);
  {
    // Lower bound isn't property value
    auto scan_index = MakeScanAllByLabelPropertyRange(
        storage, symbol_table, "n", label, prop, "prop",
        Bound{ident_m, Bound::Type::INCLUSIVE}, std::nullopt, scan_all.op_);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_THROW(PullAll(*scan_index.op_, &context), QueryRuntimeException);
  }
  {
    // Upper bound isn't property value
    auto scan_index = MakeScanAllByLabelPropertyRange(
        storage, symbol_table, "n", label, prop, "prop", std::nullopt,
        Bound{ident_m, Bound::Type::INCLUSIVE}, scan_all.op_);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_THROW(PullAll(*scan_index.op_, &context), QueryRuntimeException);
  }
  {
    // Both bounds aren't property value
    auto scan_index = MakeScanAllByLabelPropertyRange(
        storage, symbol_table, "n", label, prop, "prop",
        Bound{ident_m, Bound::Type::INCLUSIVE},
        Bound{ident_m, Bound::Type::INCLUSIVE}, scan_all.op_);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_THROW(PullAll(*scan_index.op_, &context), QueryRuntimeException);
  }
}

TEST(QueryPlan, ScanAllByLabelPropertyEqualNull) {
  database::GraphDb db;
  // Add 2 vertices with the same label, but one has a property value while
  // the other does not. Checking if the value is equal to null, should
  // yield no results.
  auto label = db.Access().Label("label");
  auto prop = db.Access().Property("prop");
  {
    auto dba = db.Access();
    auto vertex = dba.InsertVertex();
    vertex.add_label(label);
    auto vertex_with_prop = dba.InsertVertex();
    vertex_with_prop.add_label(label);
    vertex_with_prop.PropsSet(prop, PropertyValue(42));
    dba.Commit();
    db.Access().BuildIndex(label, prop);
  }
  auto dba = db.Access();
  EXPECT_EQ(2, CountIterable(dba.Vertices(false)));
  // MATCH (n :label {prop: 42})
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all =
      MakeScanAllByLabelPropertyValue(storage, symbol_table, "n", label, prop,
                                      "prop", LITERAL(TypedValue()));
  // RETURN n
  auto output = NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))
                    ->MapTo(symbol_table.CreateSymbol("n", true));
  auto produce = MakeProduce(scan_all.op_, output);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 0);
}

TEST(QueryPlan, ScanAllByLabelPropertyRangeNull) {
  database::GraphDb db;
  // Add 2 vertices with the same label, but one has a property value while
  // the other does not. Checking if the value is between nulls, should
  // yield no results.
  auto label = db.Access().Label("label");
  auto prop = db.Access().Property("prop");
  {
    auto dba = db.Access();
    auto vertex = dba.InsertVertex();
    vertex.add_label(label);
    auto vertex_with_prop = dba.InsertVertex();
    vertex_with_prop.add_label(label);
    vertex_with_prop.PropsSet(prop, PropertyValue(42));
    dba.Commit();
    db.Access().BuildIndex(label, prop);
  }
  auto dba = db.Access();
  EXPECT_EQ(2, CountIterable(dba.Vertices(false)));
  // MATCH (n :label) WHERE null <= n.prop < null
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all = MakeScanAllByLabelPropertyRange(
      storage, symbol_table, "n", label, prop, "prop",
      Bound{LITERAL(TypedValue()), Bound::Type::INCLUSIVE},
      Bound{LITERAL(TypedValue()), Bound::Type::EXCLUSIVE});
  // RETURN n
  auto output = NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))
                    ->MapTo(symbol_table.CreateSymbol("n", true));
  auto produce = MakeProduce(scan_all.op_, output);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 0);
}

TEST(QueryPlan, ScanAllByLabelPropertyNoValueInIndexContinuation) {
  database::GraphDb db;
  auto label = db.Access().Label("label");
  auto prop = db.Access().Property("prop");
  {
    auto dba = db.Access();
    auto v = dba.InsertVertex();
    v.add_label(label);
    v.PropsSet(prop, PropertyValue(2));
    dba.Commit();
    db.Access().BuildIndex(label, prop);
  }
  auto dba = db.Access();
  EXPECT_EQ(1, CountIterable(dba.Vertices(false)));

  AstStorage storage;
  SymbolTable symbol_table;

  // UNWIND [1, 2, 3] as x
  auto input_expr = LIST(LITERAL(1), LITERAL(2), LITERAL(3));
  auto x = symbol_table.CreateSymbol("x", true);
  auto unwind = std::make_shared<plan::Unwind>(nullptr, input_expr, x);
  auto x_expr = IDENT("x");
  x_expr->MapTo(x);

  // MATCH (n :label {prop: x})
  auto scan_all = MakeScanAllByLabelPropertyValue(
      storage, symbol_table, "n", label, prop, "prop", x_expr, unwind);

  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(PullAll(*scan_all.op_, &context), 1);
}

TEST(QueryPlan, ScanAllEqualsScanAllByLabelProperty) {
  database::GraphDb db;
  auto label = db.Access().Label("label");
  auto prop = db.Access().Property("prop");

  // Insert vertices
  const int vertex_count = 300, vertex_prop_count = 50;
  const int prop_value1 = 42, prop_value2 = 69;

  for (int i = 0; i < vertex_count; ++i) {
    auto dba = db.Access();
    auto v = dba.InsertVertex();
    v.add_label(label);
    v.PropsSet(
        prop, PropertyValue(i < vertex_prop_count ? prop_value1 : prop_value2));
    dba.Commit();
  }

  db.Access().BuildIndex(label, prop);

  // Make sure there are `vertex_count` vertices
  {
    auto dba = db.Access();
    EXPECT_EQ(vertex_count, CountIterable(dba.Vertices(false)));
  }

  // Make sure there are `vertex_prop_count` results when using index
  auto count_with_index = [&db, &label, &prop](int prop_value, int prop_count) {
    AstStorage storage;
    SymbolTable symbol_table;
    auto dba = db.Access();
    auto scan_all_by_label_property_value = MakeScanAllByLabelPropertyValue(
        storage, symbol_table, "n", label, prop, "prop", LITERAL(prop_value));
    auto output =
        NEXPR("n", IDENT("n")->MapTo(scan_all_by_label_property_value.sym_))
            ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(scan_all_by_label_property_value.op_, output);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_EQ(PullAll(*produce, &context), prop_count);
  };

  // Make sure there are `vertex_count` results when using scan all
  auto count_with_scan_all = [&db, &prop](int prop_value, int prop_count) {
    AstStorage storage;
    SymbolTable symbol_table;
    auto dba = db.Access();
    auto scan_all = MakeScanAll(storage, symbol_table, "n");
    auto e = PROPERTY_LOOKUP(IDENT("n")->MapTo(scan_all.sym_),
                             std::make_pair("prop", prop));
    auto filter =
        std::make_shared<Filter>(scan_all.op_, EQ(e, LITERAL(prop_value)));
    auto output =
        NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))
            ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(filter, output);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_EQ(PullAll(*produce, &context), prop_count);
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

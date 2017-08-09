//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 14.03.17.
//

#include <experimental/optional>
#include <iterator>
#include <memory>
#include <unordered_map>
#include <vector>

#include <fmt/format.h>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "communication/result_stream_faker.hpp"
#include "database/dbms.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/plan/operator.hpp"

#include "query_plan_common.hpp"

using namespace query;
using namespace query::plan;

TEST(QueryPlan, MatchReturn) {
  Dbms dbms;
  auto dba = dbms.active();

  // add a few nodes to the database
  dba->InsertVertex();
  dba->InsertVertex();
  dba->AdvanceCommand();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto test_pull_count = [&](GraphView graph_view) {
    auto scan_all =
        MakeScanAll(storage, symbol_table, "n", nullptr, graph_view);
    auto output = NEXPR("n", IDENT("n"));
    auto produce = MakeProduce(scan_all.op_, output);
    symbol_table[*output->expression_] = scan_all.sym_;
    symbol_table[*output] =
        symbol_table.CreateSymbol("named_expression_1", true);
    return PullAll(produce, *dba, symbol_table);
  };

  EXPECT_EQ(2, test_pull_count(GraphView::NEW));
  EXPECT_EQ(2, test_pull_count(GraphView::OLD));
  dba->InsertVertex();
  EXPECT_EQ(3, test_pull_count(GraphView::NEW));
  EXPECT_EQ(2, test_pull_count(GraphView::OLD));
  dba->AdvanceCommand();
  EXPECT_EQ(3, test_pull_count(GraphView::OLD));
}

TEST(QueryPlan, MatchReturnCartesian) {
  Dbms dbms;
  auto dba = dbms.active();

  dba->InsertVertex().add_label(dba->Label("l1"));
  dba->InsertVertex().add_label(dba->Label("l2"));
  dba->AdvanceCommand();

  AstTreeStorage storage;
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
  Dbms dbms;
  auto dba = dbms.active();

  // add a few nodes to the database
  dba->InsertVertex();
  dba->InsertVertex();
  dba->AdvanceCommand();

  AstTreeStorage storage;
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
  Dbms dbms;
  auto dba = dbms.active();

  // add a few nodes to the database
  GraphDbTypes::Label label = dba->Label("Label");
  auto property = PROPERTY_PAIR("Property");
  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();
  auto v3 = dba->InsertVertex();
  auto v4 = dba->InsertVertex();
  auto v5 = dba->InsertVertex();
  dba->InsertVertex();
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
  dba->AdvanceCommand();

  AstTreeStorage storage;
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

  EXPECT_EQ(1, PullAll(produce, *dba, symbol_table));

  //  test that filtering works with old records
  v4.Reconstruct();
  v4.add_label(label);
  EXPECT_EQ(1, PullAll(produce, *dba, symbol_table));
  dba->AdvanceCommand();
  EXPECT_EQ(2, PullAll(produce, *dba, symbol_table));
}

TEST(QueryPlan, NodeFilterMultipleLabels) {
  Dbms dbms;
  auto dba = dbms.active();

  // add a few nodes to the database
  GraphDbTypes::Label label1 = dba->Label("label1");
  GraphDbTypes::Label label2 = dba->Label("label2");
  GraphDbTypes::Label label3 = dba->Label("label3");
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

  AstTreeStorage storage;
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

TEST(QueryPlan, Expand) {
  Dbms dbms;
  auto dba = dbms.active();

  // make a V-graph (v3)<-[r2]-(v1)-[r1]->(v2)
  auto v1 = dba->InsertVertex();
  v1.add_label((GraphDbTypes::Label)1);
  auto v2 = dba->InsertVertex();
  v2.add_label((GraphDbTypes::Label)2);
  auto v3 = dba->InsertVertex();
  v3.add_label((GraphDbTypes::Label)3);
  auto edge_type = dba->EdgeType("Edge");
  dba->InsertEdge(v1, v2, edge_type);
  dba->InsertEdge(v1, v3, edge_type);
  dba->AdvanceCommand();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto test_expand = [&](EdgeAtom::Direction direction, GraphView graph_view) {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", direction,
                          false, "m", false, graph_view);

    // make a named expression and a produce
    auto output = NEXPR("m", IDENT("m"));
    symbol_table[*output->expression_] = r_m.node_sym_;
    symbol_table[*output] =
        symbol_table.CreateSymbol("named_expression_1", true);
    auto produce = MakeProduce(r_m.op_, output);

    return PullAll(produce, *dba, symbol_table);
  };

  EXPECT_EQ(2, test_expand(EdgeAtom::Direction::OUT, GraphView::AS_IS));
  EXPECT_EQ(2, test_expand(EdgeAtom::Direction::IN, GraphView::AS_IS));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::BOTH, GraphView::AS_IS));
  //
  // test that expand works well for both old and new graph state
  v1.Reconstruct();
  v2.Reconstruct();
  v3.Reconstruct();
  dba->InsertEdge(v1, v2, edge_type);
  dba->InsertEdge(v1, v3, edge_type);
  EXPECT_EQ(2, test_expand(EdgeAtom::Direction::OUT, GraphView::OLD));
  EXPECT_EQ(2, test_expand(EdgeAtom::Direction::IN, GraphView::OLD));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::BOTH, GraphView::OLD));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::OUT, GraphView::NEW));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::IN, GraphView::NEW));
  EXPECT_EQ(8, test_expand(EdgeAtom::Direction::BOTH, GraphView::NEW));
  dba->AdvanceCommand();
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::OUT, GraphView::OLD));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::IN, GraphView::OLD));
  EXPECT_EQ(8, test_expand(EdgeAtom::Direction::BOTH, GraphView::OLD));
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
  // type returned by the GetResults function, used
  // a lot below in test declaration
  using map_int = std::unordered_map<int, int>;

  Dbms dbms;
  std::unique_ptr<GraphDbAccessor> dba = dbms.active();
  // labels for layers in the double chain
  std::vector<GraphDbTypes::Label> labels;

  AstTreeStorage storage;
  SymbolTable symbol_table;

  // using std::experimental::nullopt
  std::experimental::nullopt_t nullopt = std::experimental::nullopt;

  void SetUp() {
    // create the graph
    int chain_length = 3;
    std::vector<VertexAccessor> layer;
    for (int from_layer_ind = -1; from_layer_ind < chain_length - 1;
         from_layer_ind++) {
      std::vector<VertexAccessor> new_layer{dba->InsertVertex(),
                                            dba->InsertVertex()};
      auto label = dba->Label(std::to_string(from_layer_ind + 1));
      labels.push_back(label);
      for (size_t v_to_ind = 0; v_to_ind < new_layer.size(); v_to_ind++) {
        auto &v_to = new_layer[v_to_ind];
        v_to.add_label(label);
        for (size_t v_from_ind = 0; v_from_ind < layer.size(); v_from_ind++) {
          auto &v_from = layer[v_from_ind];
          auto edge = dba->InsertEdge(v_from, v_to, dba->EdgeType("edge_type"));
          edge.PropsSet(dba->Property("p"),
                        fmt::format("V{}{}->V{}{}", from_layer_ind, v_from_ind,
                                    from_layer_ind + 1, v_to_ind));
        }
      }
      layer = new_layer;
    }
    dba->AdvanceCommand();
    ASSERT_EQ(CountIterable(dba->Vertices(false)), 2 * chain_length);
    ASSERT_EQ(CountIterable(dba->Edges(false)), 4 * (chain_length - 1));
  }

  /**
   * Expands the given LogicalOperator input with a match
   * (ScanAll->Filter(label)->Expand). Can create both VariableExpand
   * ops and plain Expand (depending on template param).
   * When creating plain Expand the bound arguments (lower, upper) are ignored.
   *
   * @return the last created logical op.
   */
  template <typename TExpansionOperator>
  std::shared_ptr<LogicalOperator> AddMatch(
      std::shared_ptr<LogicalOperator> input_op, const std::string &node_from,
      int layer, EdgeAtom::Direction direction,
      std::experimental::optional<size_t> lower,
      std::experimental::optional<size_t> upper, Symbol edge_sym,
      bool existing_edge, const std::string &node_to,
      GraphView graph_view = GraphView::AS_IS) {
    auto n_from = MakeScanAll(storage, symbol_table, node_from, input_op);
    auto filter_op = std::make_shared<Filter>(
        n_from.op_, storage.Create<query::LabelsTest>(
                        n_from.node_->identifier_,
                        std::vector<GraphDbTypes::Label>{labels[layer]}));

    auto n_to = NODE(node_to);
    auto n_to_sym = symbol_table.CreateSymbol(node_to, true);
    symbol_table[*n_to->identifier_] = n_to_sym;

    if (std::is_same<TExpansionOperator, ExpandVariable>::value) {
      // convert optional ints to optional expressions
      auto convert = [this](std::experimental::optional<size_t> bound) {
        return bound ? LITERAL(static_cast<int64_t>(bound.value())) : nullptr;
      };
      return std::make_shared<ExpandVariable>(
          n_to_sym, edge_sym, direction, convert(lower), convert(upper),
          filter_op, n_from.sym_, false, existing_edge, graph_view);
    } else
      return std::make_shared<Expand>(n_to_sym, edge_sym, direction, filter_op,
                                      n_from.sym_, false, existing_edge,
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
   * Pulls from the given input and analyses the edge-list (result of variable
   * length expansion) found in the results under the given symbol.
   *
   * @return a map {path_lenth -> number_of_results}
   */
  auto GetResults(std::shared_ptr<LogicalOperator> input_op, Symbol symbol) {
    map_int count_per_length;
    Frame frame(symbol_table.max_position());
    auto cursor = input_op->MakeCursor(*dba);
    while (cursor->Pull(frame, symbol_table)) {
      auto length = frame[symbol].Value<std::vector<TypedValue>>().size();
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
                         std::experimental::optional<size_t> upper) {
    auto e = Edge("r", direction);
    return GetResults(AddMatch<ExpandVariable>(nullptr, "n", layer, direction,
                                               lower, upper, e, false, "m"),
                      e);
  };

  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 0, 0), (map_int{{0, 2}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, 0), (map_int{{0, 2}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 0, 0), (map_int{{0, 2}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 1, 1), (map_int{}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, 1), (map_int{{1, 4}}));
  EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 1, 1), (map_int{{1, 4}}));
  EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 1, 1), (map_int{{1, 4}}));
  EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 1, 1), (map_int{{1, 8}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2), (map_int{{2, 8}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 3), (map_int{{2, 8}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, 2),
            (map_int{{1, 4}, {2, 8}}));

  // the following tests also check edge-uniqueness (cyphermorphisim)
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 1, 2),
            (map_int{{1, 4}, {2, 12}}));
  EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, 4),
            (map_int{{4, 24}}));

  // default bound values (lower default is 1, upper default is inf)
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, nullopt, 0), (map_int{}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, nullopt, 1),
            (map_int{{1, 4}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, nullopt, 2),
            (map_int{{1, 4}, {2, 8}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 7, nullopt),
            (map_int{{7, 24}, {8, 24}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 8, nullopt),
            (map_int{{8, 24}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 9, nullopt), (map_int{}));
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
      last_op = AddMatch<Expand>(last_op, "n0", layer, direction, lower, upper,
                                 symbols.back(), false, "m0");
    }

    auto var_length_sym = Edge("r1", direction);
    symbols.push_back(var_length_sym);
    last_op = AddMatch<ExpandVariable>(last_op, "n1", layer, direction, lower,
                                       upper, var_length_sym, false, "m1");

    if (!single_expansion_before) {
      symbols.push_back(Edge("r2", direction));
      last_op = AddMatch<Expand>(last_op, "n2", layer, direction, lower, upper,
                                 symbols.back(), false, "m2");
    }

    if (add_uniqueness_check) {
      auto last_symbol = symbols.back();
      symbols.pop_back();
      last_op = std::make_shared<ExpandUniquenessFilter<EdgeAccessor>>(
          last_op, last_symbol, symbols);
    }

    return GetResults(last_op, var_length_sym);
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
    auto first = AddMatch<ExpandVariable>(nullptr, "n1", layer, direction,
                                          lower, upper, e1, false, "m1");
    auto e2 = Edge("r2", direction);
    auto last_op = AddMatch<ExpandVariable>(first, "n2", layer, direction,
                                            lower, upper, e2, false, "m2");
    if (add_uniqueness_check) {
      last_op = std::make_shared<ExpandUniquenessFilter<EdgeAccessor>>(
          last_op, e2, std::vector<Symbol>{e1});
    }

    return GetResults(last_op, e2);
  };

  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2, false),
            (map_int{{2, 8 * 8}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2, true),
            (map_int{{2, 5 * 8}}));
}

TEST_F(QueryPlanExpandVariable, ExistingEdges) {
  auto test_expand = [&](int layer, EdgeAtom::Direction direction,
                         std::experimental::optional<size_t> lower,
                         std::experimental::optional<size_t> upper,
                         bool same_edge_symbol) {
    auto e1 = Edge("r1", direction);
    auto first = AddMatch<ExpandVariable>(nullptr, "n1", layer, direction,
                                          lower, upper, e1, false, "m1");
    auto e2 = same_edge_symbol ? e1 : Edge("r2", direction);
    auto second = AddMatch<ExpandVariable>(first, "n2", layer, direction, lower,
                                           upper, e2, same_edge_symbol, "m2");
    return GetResults(second, e2);
  };

  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, 1, false),
            (map_int{{1, 4 * 4}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, 1, true),
            (map_int{{1, 4}}));

  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, 1, false),
            (map_int{{0, 2 * 6}, {1, 4 * 6}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, 1, true),
            (map_int{{0, 4}, {1, 4}}));

  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 3, false),
            (map_int{{2, 8 * 8}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 3, true),
            (map_int{{2, 8}}));

  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 1, 2, false),
            (map_int{{1, 4 * 16}, {2, 12 * 16}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 1, 2, true),
            (map_int{{1, 4}, {2, 12}}));
}

TEST_F(QueryPlanExpandVariable, GraphState) {
  auto test_expand = [&](GraphView graph_view) {
    auto e = Edge("r", EdgeAtom::Direction::OUT);
    return GetResults(
        AddMatch<ExpandVariable>(nullptr, "n", 0, EdgeAtom::Direction::OUT, 2,
                                 2, e, false, "m", graph_view),
        e);
  };

  EXPECT_EQ(test_expand(GraphView::OLD), (map_int{{2, 8}}));

  // add two vertices branching out from the second layer
  for (VertexAccessor &vertex : dba->Vertices(true))
    if (vertex.has_label(labels[1])) {
      auto new_vertex = dba->InsertVertex();
      dba->InsertEdge(vertex, new_vertex, dba->EdgeType("some_type"));
    }
  ASSERT_EQ(CountIterable(dba->Vertices(false)), 6);
  ASSERT_EQ(CountIterable(dba->Vertices(true)), 8);

  EXPECT_EQ(test_expand(GraphView::OLD), (map_int{{2, 8}}));
  EXPECT_EQ(test_expand(GraphView::NEW), (map_int{{2, 12}}));
  dba->AdvanceCommand();
  EXPECT_EQ(test_expand(GraphView::OLD), (map_int{{2, 12}}));
  EXPECT_EQ(test_expand(GraphView::NEW), (map_int{{2, 12}}));
}

namespace std {
template <>
struct hash<std::pair<int, int>> {
  size_t operator()(const std::pair<int, int> &p) const {
    return p.first + 31 * p.second;
  }
};
}

// TODO test optional + variable length

/** A test fixture for breadth first expansion */
class QueryPlanExpandBreadthFirst : public testing::Test {
 protected:
  Dbms dbms_;
  // style-guide non-conformant name due to PROPERTY_PAIR and PROPERTY_LOOKUP
  // macro requirements
  std::unique_ptr<GraphDbAccessor> dba = dbms_.active();
  std::pair<std::string, GraphDbTypes::Property> prop =
      PROPERTY_PAIR("property");
  GraphDbTypes::EdgeType edge_type = dba->EdgeType("edge_type");

  // make 4 vertices because we'll need to compare against them exactly
  // v[0] has `prop` with the value 0
  std::vector<VertexAccessor> v;

  // make some edges too, in a map (from, to) vertex indices
  std::unordered_map<std::pair<int, int>, EdgeAccessor> e;

  AstTreeStorage storage;
  SymbolTable symbol_table;

  // inner edge and vertex symbols
  // edge from a to b has `prop` with the value ab (all ints)
  Symbol inner_edge = symbol_table.CreateSymbol("inner_edge", true);
  Symbol inner_node = symbol_table.CreateSymbol("inner_node", true);

  void SetUp() {
    for (int i = 0; i < 4; i++) {
      v.push_back(dba->InsertVertex());
      v.back().PropsSet(prop.second, i);
    }

    auto add_edge = [&](int from, int to) {
      EdgeAccessor edge = dba->InsertEdge(v[from], v[to], edge_type);
      edge.PropsSet(prop.second, from * 10 + to);
      e.emplace(std::make_pair(from, to), edge);
    };

    add_edge(0, 1);
    add_edge(2, 0);
    add_edge(2, 1);
    add_edge(1, 3);
    add_edge(3, 2);
    add_edge(2, 2);

    dba->AdvanceCommand();
    for (auto &vertex : v) vertex.Reconstruct();
    for (auto &edge : e) edge.second.Reconstruct();
  }

  // defines and performs a breadth-first expansion with the given params
  // returns a vector of pairs. each pair is (vector-of-edges, vertex)
  auto ExpandBF(EdgeAtom::Direction direction, int max_depth, Expression *where,
                GraphView graph_view = GraphView::AS_IS,
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

    // expand bf
    auto node_sym = existing_node_input
                        ? existing_node_input->sym_
                        : symbol_table.CreateSymbol("node", true);
    auto edge_list_sym = symbol_table.CreateSymbol("edgelist_", true);
    last_op = std::make_shared<ExpandBreadthFirst>(
        node_sym, edge_list_sym, direction, LITERAL(max_depth), inner_node,
        inner_edge, where, last_op, n.sym_, existing_node_input != nullptr,
        graph_view);

    Frame frame(symbol_table.max_position());
    auto cursor = last_op->MakeCursor(*dba);
    std::vector<std::pair<std::vector<EdgeAccessor>, VertexAccessor>> results;
    while (cursor->Pull(frame, symbol_table)) {
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

  Expression *PropNe(Symbol symbol, int value) {
    auto ident = IDENT("inner_element");
    symbol_table[*ident] = symbol;
    return NEQ(PROPERTY_LOOKUP(ident, prop), LITERAL(value));
  }
};

#define EXPECT_EITHER(value, a, b) EXPECT_TRUE(value == a || value == b)

TEST_F(QueryPlanExpandBreadthFirst, Basic) {
  auto results = ExpandBF(EdgeAtom::Direction::BOTH, 1000, LITERAL(true));

  ASSERT_EQ(results.size(), 3);

  // check end nodes
  EXPECT_EITHER(GetProp(results[0].second), 1, 2);
  EXPECT_EITHER(GetProp(results[1].second), 1, 2);
  EXPECT_NE(GetProp(results[0].second), GetProp(results[1].second));
  EXPECT_EQ(GetProp(results[2].second), 3);

  // check edges
  ASSERT_EQ(results[0].first.size(), 1);
  EXPECT_EITHER(GetProp(results[0].first[0]), 1, 20);
  ASSERT_EQ(results[1].first.size(), 1);
  EXPECT_EITHER(GetProp(results[1].first[0]), 1, 20);
  ASSERT_EQ(results[2].first.size(), 2);
  EXPECT_EITHER(GetProp(results[2].first[1]), 13, 32);
}

TEST_F(QueryPlanExpandBreadthFirst, EdgeDirection) {
  {
    auto results = ExpandBF(EdgeAtom::Direction::IN, 1000, LITERAL(true));
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(GetProp(results[0].second), 2);
    EXPECT_EQ(GetProp(results[1].second), 3);
    EXPECT_EQ(GetProp(results[2].second), 1);
    for (int i = 0; i < 3; i++) EXPECT_EQ(results[i].first.size(), i + 1);
    // assume edges are OK because vertices are (tested before)
  }
  {
    auto results = ExpandBF(EdgeAtom::Direction::OUT, 1000, LITERAL(true));
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(GetProp(results[0].second), 1);
    EXPECT_EQ(GetProp(results[1].second), 3);
    EXPECT_EQ(GetProp(results[2].second), 2);
    for (int i = 0; i < 3; i++) EXPECT_EQ(results[i].first.size(), i + 1);
    // assume edges are OK because vertices are (tested before)
  }
}

TEST_F(QueryPlanExpandBreadthFirst, Where) {
  {
    auto results =
        ExpandBF(EdgeAtom::Direction::BOTH, 1000, PropNe(inner_node, 2));
    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(GetProp(results[0].second), 1);
    EXPECT_EQ(GetProp(results[1].second), 3);
  }
  {
    auto results =
        ExpandBF(EdgeAtom::Direction::BOTH, 1000, PropNe(inner_edge, 20));
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(GetProp(results[0].second), 1);
    EXPECT_EITHER(GetProp(results[1].second), 3, 2);
    EXPECT_EITHER(GetProp(results[2].second), 3, 2);
    EXPECT_NE(GetProp(results[1].second), GetProp(results[2].second));
  }
}

TEST_F(QueryPlanExpandBreadthFirst, GraphState) {
  auto ExpandSize = [this](GraphView graph_view) {
    return ExpandBF(EdgeAtom::Direction::BOTH, 1000, LITERAL(true), graph_view)
        .size();
  };
  EXPECT_EQ(ExpandSize(GraphView::OLD), 3);
  EXPECT_EQ(ExpandSize(GraphView::NEW), 3);
  auto new_vertex = dba->InsertVertex();
  new_vertex.PropsSet(prop.second, 4);
  dba->InsertEdge(v[3], new_vertex, edge_type);
  EXPECT_EQ(CountIterable(dba->Vertices(false)), 4);
  EXPECT_EQ(CountIterable(dba->Vertices(true)), 5);
  EXPECT_EQ(ExpandSize(GraphView::OLD), 3);
  EXPECT_EQ(ExpandSize(GraphView::NEW), 4);
  dba->AdvanceCommand();
  EXPECT_EQ(ExpandSize(GraphView::OLD), 4);
  EXPECT_EQ(ExpandSize(GraphView::NEW), 4);
}

TEST_F(QueryPlanExpandBreadthFirst, MultipleInputs) {
  auto results = ExpandBF(EdgeAtom::Direction::OUT, 1000, LITERAL(true),
                          GraphView::AS_IS, std::experimental::nullopt);
  // expect that each vertex has been returned 3 times
  EXPECT_EQ(results.size(), 12);
  std::vector<int> found(4, 0);
  for (const auto &row : results) found[GetProp(row.second)]++;
  EXPECT_EQ(found, std::vector<int>(4, 3));
}

TEST_F(QueryPlanExpandBreadthFirst, ExistingNode) {
  auto ExpandPreceeding =
      [this](std::experimental::optional<int> preceeding_node_id) {
        // scan the nodes optionally filtering on property value
        auto n0 = MakeScanAll(storage, symbol_table, "n0");
        if (preceeding_node_id) {
          auto filter = std::make_shared<Filter>(
              n0.op_, EQ(PROPERTY_LOOKUP(n0.node_->identifier_, prop),
                         LITERAL(*preceeding_node_id)));
          // inject the filter op into the ScanAllTuple. that way the filter op
          // can be passed into the ExpandBF function without too much refactor
          n0.op_ = filter;
        }

        return ExpandBF(EdgeAtom::Direction::OUT, 1000, LITERAL(true),
                        GraphView::AS_IS, std::experimental::nullopt, &n0);
      };

  EXPECT_EQ(ExpandPreceeding(std::experimental::nullopt).size(), 12);
  {
    auto results = ExpandPreceeding(0);
    ASSERT_EQ(results.size(), 3);
    for (int i = 0; i < 3; i++) EXPECT_EQ(GetProp(results[i].second), 0);
  }
}

TEST(QueryPlan, ExpandOptional) {
  Dbms dbms;
  auto dba = dbms.active();

  AstTreeStorage storage;
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
  auto r_m = MakeExpand(storage, symbol_table, nullptr, n.sym_, "r",
                        EdgeAtom::Direction::OUT, false, "m", false);
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
  Dbms dbms;
  auto dba = dbms.active();

  AstTreeStorage storage;
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
  Dbms dbms;
  auto dba = dbms.active();
  AstTreeStorage storage;
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
  auto r_m = MakeExpand(storage, symbol_table, with, with_n_sym, "r",
                        EdgeAtom::Direction::OUT, false, "m", false);
  // RETURN m
  auto m_ne = NEXPR("m", IDENT("m"));
  symbol_table[*m_ne->expression_] = r_m.node_sym_;
  symbol_table[*m_ne] = symbol_table.CreateSymbol("m", true);
  auto produce = MakeProduce(r_m.op_, m_ne);
  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(0, results.size());
}

TEST(QueryPlan, OptionalMatchThenExpandToMissingNode) {
  Dbms dbms;
  auto dba = dbms.active();
  // Make a graph with 2 connected, unlabeled nodes.
  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();
  auto edge_type = dba->EdgeType("edge_type");
  dba->InsertEdge(v1, v2, edge_type);
  dba->AdvanceCommand();
  EXPECT_EQ(2, CountIterable(dba->Vertices(false)));
  EXPECT_EQ(1, CountIterable(dba->Edges(false)));
  AstTreeStorage storage;
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
      with_n_sym, edge_sym, edge_direction, m.op_, m.sym_, true, false);
  // RETURN m
  auto m_ne = NEXPR("m", IDENT("m"));
  symbol_table[*m_ne->expression_] = m.sym_;
  symbol_table[*m_ne] = symbol_table.CreateSymbol("m", true);
  auto produce = MakeProduce(expand, m_ne);
  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(0, results.size());
}

TEST(QueryPlan, OptionalMatchThenExpandToMissingEdge) {
  Dbms dbms;
  auto dba = dbms.active();
  // Make a graph with 2 connected, unlabeled nodes.
  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();
  auto edge_type = dba->EdgeType("edge_type");
  dba->InsertEdge(v1, v2, edge_type);
  dba->AdvanceCommand();
  EXPECT_EQ(2, CountIterable(dba->Vertices(false)));
  EXPECT_EQ(1, CountIterable(dba->Edges(false)));
  AstTreeStorage storage;
  SymbolTable symbol_table;
  // OPTIONAL MATCH (n :missing) -[r]- (m)
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto label_missing = dba->Label("missing");
  n.node_->labels_.emplace_back(label_missing);
  auto *filter_expr =
      storage.Create<LabelsTest>(n.node_->identifier_, n.node_->labels_);
  auto node_filter = std::make_shared<Filter>(n.op_, filter_expr);
  auto r_m = MakeExpand(storage, symbol_table, node_filter, n.sym_, "r",
                        EdgeAtom::Direction::BOTH, false, "m", false);
  auto optional = std::make_shared<plan::Optional>(
      nullptr, r_m.op_,
      std::vector<Symbol>{n.sym_, r_m.edge_sym_, r_m.node_sym_});
  // WITH r
  auto r_ne = NEXPR("r", IDENT("r"));
  symbol_table[*r_ne->expression_] = r_m.edge_sym_;
  auto with_r_sym = symbol_table.CreateSymbol("r", true);
  symbol_table[*r_ne] = with_r_sym;
  auto with = MakeProduce(optional, r_ne);
  // MATCH (a) -[r]- (b)
  auto a = MakeScanAll(storage, symbol_table, "a", with);
  auto edge_direction = EdgeAtom::Direction::BOTH;
  auto edge = EDGE("r", edge_direction);
  symbol_table[*edge->identifier_] = with_r_sym;
  auto node = NODE("n");
  auto node_sym = symbol_table.CreateSymbol("b", true);
  symbol_table[*node->identifier_] = node_sym;
  auto expand = std::make_shared<plan::Expand>(
      node_sym, with_r_sym, edge_direction, a.op_, a.sym_, false, true);
  // RETURN a
  auto a_ne = NEXPR("a", IDENT("a"));
  symbol_table[*a_ne->expression_] = a.sym_;
  symbol_table[*a_ne] = symbol_table.CreateSymbol("a", true);
  auto produce = MakeProduce(expand, a_ne);
  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(0, results.size());
}

TEST(QueryPlan, ExpandExistingNode) {
  Dbms dbms;
  auto dba = dbms.active();

  // make a graph (v1)->(v2) that
  // has a recursive edge (v1)->(v1)
  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();
  auto edge_type = dba->EdgeType("Edge");
  dba->InsertEdge(v1, v1, edge_type);
  dba->InsertEdge(v1, v2, edge_type);
  dba->AdvanceCommand();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto test_existing = [&](bool with_existing, int expected_result_count) {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_n = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                          EdgeAtom::Direction::OUT, false, "n", with_existing);
    if (with_existing)
      r_n.op_ =
          std::make_shared<Expand>(n.sym_, r_n.edge_sym_, r_n.edge_->direction_,
                                   n.op_, n.sym_, with_existing, false);

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

TEST(QueryPlan, ExpandExistingEdge) {
  Dbms dbms;
  auto dba = dbms.active();

  // make a V-graph (v3)<-[r2]-(v1)-[r1]->(v2)
  auto v1 = dba->InsertVertex();
  v1.add_label((GraphDbTypes::Label)1);
  auto v2 = dba->InsertVertex();
  v2.add_label((GraphDbTypes::Label)2);
  auto v3 = dba->InsertVertex();
  v3.add_label((GraphDbTypes::Label)3);
  auto edge_type = dba->EdgeType("Edge");
  dba->InsertEdge(v1, v2, edge_type);
  dba->InsertEdge(v1, v3, edge_type);
  dba->AdvanceCommand();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto test_existing = [&](bool with_existing, int expected_result_count) {
    auto i = MakeScanAll(storage, symbol_table, "i");
    auto r_j = MakeExpand(storage, symbol_table, i.op_, i.sym_, "r",
                          EdgeAtom::Direction::BOTH, false, "j", false);
    auto r_k = MakeExpand(storage, symbol_table, r_j.op_, r_j.node_sym_, "r",
                          EdgeAtom::Direction::BOTH, with_existing, "k", false);
    if (with_existing)
      r_k.op_ = std::make_shared<Expand>(r_k.node_sym_, r_j.edge_sym_,
                                         r_k.edge_->direction_, r_j.op_,
                                         r_j.node_sym_, false, with_existing);

    // make a named expression and a produce
    auto output = NEXPR("r", IDENT("r"));
    symbol_table[*output->expression_] = r_j.edge_sym_;
    symbol_table[*output] =
        symbol_table.CreateSymbol("named_expression_1", true);
    auto produce = MakeProduce(r_k.op_, output);

    auto results = CollectProduce(produce.get(), symbol_table, *dba);
    EXPECT_EQ(results.size(), expected_result_count);

  };

  test_existing(true, 4);
  test_existing(false, 6);
}

TEST(QueryPlan, ExpandBothCycleEdgeCase) {
  // we're testing that expanding on BOTH
  // does only one expansion for a cycle
  Dbms dbms;
  auto dba = dbms.active();

  auto v = dba->InsertVertex();
  dba->InsertEdge(v, v, dba->EdgeType("et"));
  dba->AdvanceCommand();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_ = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                       EdgeAtom::Direction::BOTH, false, "_", false);
  EXPECT_EQ(1, PullAll(r_.op_, *dba, symbol_table));
}

TEST(QueryPlan, EdgeFilter) {
  Dbms dbms;
  auto dba = dbms.active();

  // make an N-star expanding from (v1)
  // where only one edge will qualify
  // and there are all combinations of
  // (edge_type yes|no) * (property yes|absent|no)
  std::vector<GraphDbTypes::EdgeType> edge_types;
  for (int j = 0; j < 2; ++j)
    edge_types.push_back(dba->EdgeType("et" + std::to_string(j)));
  std::vector<VertexAccessor> vertices;
  for (int i = 0; i < 7; ++i) vertices.push_back(dba->InsertVertex());
  auto prop = PROPERTY_PAIR("property");
  std::vector<EdgeAccessor> edges;
  for (int i = 0; i < 6; ++i) {
    edges.push_back(
        dba->InsertEdge(vertices[0], vertices[i + 1], edge_types[i % 2]));
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
  dba->AdvanceCommand();
  for (auto &vertex : vertices) vertex.Reconstruct();
  for (auto &edge : edges) edge.Reconstruct();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto test_filter = [&]() {
    // define an operator tree for query
    // MATCH (n)-[r]->(m) RETURN m

    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                          EdgeAtom::Direction::OUT, false, "m", false);
    r_m.edge_->edge_types_.push_back(edge_types[0]);
    r_m.edge_->properties_[prop] = LITERAL(42);
    auto *filter_expr =
        AND(storage.Create<EdgeTypeTest>(r_m.edge_->identifier_,
                                         r_m.edge_->edge_types_),
            EQ(PROPERTY_LOOKUP(r_m.edge_->identifier_, prop), LITERAL(42)));
    auto edge_filter = std::make_shared<Filter>(r_m.op_, filter_expr);

    // make a named expression and a produce
    auto output = NEXPR("m", IDENT("m"));
    symbol_table[*output->expression_] = r_m.node_sym_;
    symbol_table[*output] =
        symbol_table.CreateSymbol("named_expression_1", true);
    auto produce = MakeProduce(edge_filter, output);

    return PullAll(produce, *dba, symbol_table);
  };

  EXPECT_EQ(1, test_filter());
  // test that edge filtering always filters on old state
  for (auto &edge : edges) edge.PropsSet(prop.second, 42);
  EXPECT_EQ(1, test_filter());
  dba->AdvanceCommand();
  EXPECT_EQ(3, test_filter());
}

TEST(QueryPlan, EdgeFilterMultipleTypes) {
  Dbms dbms;
  auto dba = dbms.active();

  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();
  auto type_1 = dba->EdgeType("type_1");
  auto type_2 = dba->EdgeType("type_2");
  auto type_3 = dba->EdgeType("type_3");
  dba->InsertEdge(v1, v2, type_1);
  dba->InsertEdge(v1, v2, type_2);
  dba->InsertEdge(v1, v2, type_3);
  dba->AdvanceCommand();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  // make a scan all
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                        EdgeAtom::Direction::OUT, false, "m", false);
  // add an edge type filter
  r_m.edge_->edge_types_.push_back(type_1);
  r_m.edge_->edge_types_.push_back(type_2);
  auto *filter_expr = storage.Create<EdgeTypeTest>(r_m.edge_->identifier_,
                                                   r_m.edge_->edge_types_);
  auto edge_filter = std::make_shared<Filter>(r_m.op_, filter_expr);

  // make a named expression and a produce
  auto output = NEXPR("m", IDENT("m"));
  auto produce = MakeProduce(edge_filter, output);

  // fill up the symbol table
  symbol_table[*output] = symbol_table.CreateSymbol("named_expression_1", true);
  symbol_table[*output->expression_] = r_m.node_sym_;

  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(results.size(), 2);
}

TEST(QueryPlan, Filter) {
  Dbms dbms;
  auto dba = dbms.active();

  // add a 6 nodes with property 'prop', 2 have true as value
  auto property = PROPERTY_PAIR("property");
  for (int i = 0; i < 6; ++i)
    dba->InsertVertex().PropsSet(property.second, i % 3 == 0);
  dba->InsertVertex();  // prop not set, gives NULL
  dba->AdvanceCommand();

  AstTreeStorage storage;
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

  EXPECT_EQ(CollectProduce(produce.get(), symbol_table, *dba).size(), 2);
}

TEST(QueryPlan, ExpandUniquenessFilter) {
  Dbms dbms;
  auto dba = dbms.active();

  // make a graph that has (v1)->(v2) and a recursive edge (v1)->(v1)
  auto v1 = dba->InsertVertex();
  auto v2 = dba->InsertVertex();
  auto edge_type = dba->EdgeType("edge_type");
  dba->InsertEdge(v1, v2, edge_type);
  dba->InsertEdge(v1, v1, edge_type);
  dba->AdvanceCommand();

  auto check_expand_results = [&](bool vertex_uniqueness,
                                  bool edge_uniqueness) {
    AstTreeStorage storage;
    SymbolTable symbol_table;

    auto n1 = MakeScanAll(storage, symbol_table, "n1");
    auto r1_n2 = MakeExpand(storage, symbol_table, n1.op_, n1.sym_, "r1",
                            EdgeAtom::Direction::OUT, false, "n2", false);
    std::shared_ptr<LogicalOperator> last_op = r1_n2.op_;
    if (vertex_uniqueness)
      last_op = std::make_shared<ExpandUniquenessFilter<VertexAccessor>>(
          last_op, r1_n2.node_sym_, std::vector<Symbol>{n1.sym_});
    auto r2_n3 = MakeExpand(storage, symbol_table, last_op, r1_n2.node_sym_,
                            "r2", EdgeAtom::Direction::OUT, false, "n3", false);
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

  Dbms dbms;
  auto dba = dbms.active();
  AstTreeStorage storage;
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
  Dbms dbms;
  auto dba = dbms.active();
  // Add a vertex with a label and one without.
  auto label = dba->Label("label");
  auto labeled_vertex = dba->InsertVertex();
  labeled_vertex.add_label(label);
  dba->InsertVertex();
  dba->AdvanceCommand();
  EXPECT_EQ(2, CountIterable(dba->Vertices(false)));
  // MATCH (n :label)
  AstTreeStorage storage;
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
  Dbms dbms;
  auto dba = dbms.active();
  // Add 5 vertices with same label, but with different property values.
  auto label = dba->Label("label");
  auto prop = dba->Property("prop");

  // vertex property values that will be stored into the DB
  // clang-format off
  std::vector<TypedValue> values{
      true, false, "a", "b", "c", 0, 1, 2, 0.5, 1.5, 2.5,
      std::vector<TypedValue>{0}, std::vector<TypedValue>{1},
      std::vector<TypedValue>{2}};
  // clang-format on

  for (const auto &value : values) {
    auto vertex = dba->InsertVertex();
    vertex.add_label(label);
    vertex.PropsSet(prop, value);
  }
  dba->Commit();
  dba = dbms.active();
  dba->BuildIndex(label, prop);
  dba->Commit();
  dba = dbms.active();
  ASSERT_EQ(14, CountIterable(dba->Vertices(false)));

  auto check = [&dba, label, prop](TypedValue lower, Bound::Type lower_type,
                                   TypedValue upper, Bound::Type upper_type,
                                   const std::vector<TypedValue> &expected) {
    AstTreeStorage storage;
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
  Dbms dbms;
  auto dba = dbms.active();
  // Add 2 vertices with same label, but with property values that cannot be
  // compared. On the other hand, equality works fine.
  auto label = dba->Label("label");
  auto prop = dba->Property("prop");
  auto number_vertex = dba->InsertVertex();
  number_vertex.add_label(label);
  number_vertex.PropsSet(prop, 42);
  auto string_vertex = dba->InsertVertex();
  string_vertex.add_label(label);
  string_vertex.PropsSet(prop, "string");
  dba->Commit();
  dba = dbms.active();
  dba->BuildIndex(label, prop);
  dba = dbms.active();
  EXPECT_EQ(2, CountIterable(dba->Vertices(false)));
  // MATCH (n :label {prop: 42})
  AstTreeStorage storage;
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

TEST(QueryPlan, ScanAllByLabelPropertyEqualNull) {
  Dbms dbms;
  auto dba = dbms.active();
  // Add 2 vertices with the same label, but one has a property value while
  // the
  // other does not. Checking if the value is equal to null, should yield no
  // results.
  auto label = dba->Label("label");
  auto prop = dba->Property("prop");
  auto vertex = dba->InsertVertex();
  vertex.add_label(label);
  auto vertex_with_prop = dba->InsertVertex();
  vertex_with_prop.add_label(label);
  vertex_with_prop.PropsSet(prop, 42);
  dba->Commit();
  dba = dbms.active();
  dba->BuildIndex(label, prop);
  dba = dbms.active();
  EXPECT_EQ(2, CountIterable(dba->Vertices(false)));
  // MATCH (n :label {prop: 42})
  AstTreeStorage storage;
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

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

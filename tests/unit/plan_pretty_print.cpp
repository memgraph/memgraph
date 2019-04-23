#include <gtest/gtest.h>

#include "database/graph_db.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/pretty_print.hpp"

#include "query_common.hpp"

using namespace query;
using namespace query::plan;

// The JSON formatted plan is consumed (or will be) by Memgraph Lab, and
// therefore should not be changed before synchronizing with whoever is
// maintaining Memgraph Lab. Hopefully, one day integration tests will exist and
// there will be no need to be super careful.

// This is a hack to prevent Googletest from crashing when outputing JSONs.
namespace nlohmann {
void PrintTo(const json &json, std::ostream *os) {
  *os << std::endl << json.dump(1);
}
}  // namespace nlohmann

using namespace nlohmann;

class PrintToJsonTest : public ::testing::Test {
 protected:
  PrintToJsonTest() : db(), dba(db.Access()) {}

  AstStorage storage;
  SymbolTable symbol_table;

  database::GraphDb db;
  database::GraphDbAccessor dba;

  Symbol GetSymbol(std::string name) {
    return symbol_table.CreateSymbol(name, true);
  }

  void Check(LogicalOperator *root, std::string expected) {
    EXPECT_EQ(PlanToJson(dba, root), json::parse(expected));
  }
};

TEST_F(PrintToJsonTest, Once) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<Once>();

  Check(last_op.get(), R"(
        {
          "name" : "Once"
        })");
}

TEST_F(PrintToJsonTest, ScanAll) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<ScanAll>(nullptr, GetSymbol("node"));

  Check(last_op.get(), R"(
        {
          "name" : "ScanAll",
          "output_symbol" : "node",
          "input" : { "name" : "Once" }
        })");
}

TEST_F(PrintToJsonTest, ScanAllByLabel) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<ScanAllByLabel>(nullptr, GetSymbol("node"),
                                             dba.Label("Label"));

  Check(last_op.get(), R"(
        {
          "name" : "ScanAllByLabel",
          "label" : "Label",
          "output_symbol" : "node",
          "input" : { "name" : "Once" }
        })");
}

TEST_F(PrintToJsonTest, ScanAllByLabelPropertyRange) {
  {
    std::shared_ptr<LogicalOperator> last_op;
    last_op = std::make_shared<ScanAllByLabelPropertyRange>(
        nullptr, GetSymbol("node"), dba.Label("Label"), dba.Property("prop"),
        "prop", utils::MakeBoundInclusive<Expression *>(LITERAL(1)),
        utils::MakeBoundExclusive<Expression *>(LITERAL(20)));

    Check(last_op.get(), R"(
        {
          "name" : "ScanAllByLabelPropertyRange",
          "label" : "Label",
          "property" : "prop",
          "lower_bound" : {
            "value" : "1",
            "type" : "inclusive"
          },
          "upper_bound" : {
            "value" : "20",
            "type" : "exclusive"
          },
          "output_symbol" : "node",
          "input" : { "name" : "Once" }
        })");
  }
  {
    std::shared_ptr<LogicalOperator> last_op;
    last_op = std::make_shared<ScanAllByLabelPropertyRange>(
        nullptr, GetSymbol("node"), dba.Label("Label"), dba.Property("prop"),
        "prop", std::nullopt,
        utils::MakeBoundExclusive<Expression *>(LITERAL(20)));

    Check(last_op.get(), R"(
        {
          "name" : "ScanAllByLabelPropertyRange",
          "label" : "Label",
          "property" : "prop",
          "lower_bound" : null,
          "upper_bound" : {
            "value" : "20",
            "type" : "exclusive"
          },
          "output_symbol" : "node",
          "input" : { "name" : "Once" }
        })");
  }
  {
    std::shared_ptr<LogicalOperator> last_op;
    last_op = std::make_shared<ScanAllByLabelPropertyRange>(
        nullptr, GetSymbol("node"), dba.Label("Label"), dba.Property("prop"),
        "prop", utils::MakeBoundInclusive<Expression *>(LITERAL(1)),
        std::nullopt);

    Check(last_op.get(), R"(
        {
          "name" : "ScanAllByLabelPropertyRange",
          "label" : "Label",
          "property" : "prop",
          "lower_bound" : {
            "value" : "1",
            "type" : "inclusive"
          },
          "upper_bound" : null,
          "output_symbol" : "node",
          "input" : { "name" : "Once" }
        })");
  }
}

TEST_F(PrintToJsonTest, ScanAllByLabelPropertyValue) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<ScanAllByLabelPropertyValue>(
      nullptr, GetSymbol("node"), dba.Label("Label"), dba.Property("prop"),
      "prop", ADD(LITERAL(21), LITERAL(21)));

  Check(last_op.get(), R"sep(
        {
          "name" : "ScanAllByLabelPropertyValue",
          "label" : "Label",
          "property" : "prop",
          "expression" : "(+ 21 21)",
          "output_symbol" : "node",
          "input" : { "name" : "Once" }
        })sep");
}

TEST_F(PrintToJsonTest, CreateNode) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<CreateNode>(
      nullptr,
      NodeCreationInfo{GetSymbol("node"),
                       {dba.Label("Label1"), dba.Label("Label2")},
                       {{dba.Property("prop1"), LITERAL(5)},
                        {dba.Property("prop2"), LITERAL("some cool stuff")}}});

  Check(last_op.get(), R"(
          {
            "name" : "CreateNode",
            "node_info" : {
              "symbol" : "node",
              "labels" : ["Label1", "Label2"],
              "properties" : {
                "prop1" : "5",
                "prop2" : "\"some cool stuff\""
              }
            },
            "input" : { "name" : "Once" }
          })");
}

TEST_F(PrintToJsonTest, CreateExpand) {
  Symbol node1_sym = GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, GetSymbol("node1"));
  last_op = std::make_shared<CreateExpand>(
      NodeCreationInfo{GetSymbol("node2"),
                       {dba.Label("Label1"), dba.Label("Label2")},
                       {{dba.Property("prop1"), LITERAL(5)},
                        {dba.Property("prop2"), LITERAL("some cool stuff")}}},
      EdgeCreationInfo{GetSymbol("edge"),
                       {{dba.Property("weight"), LITERAL(5.32)}},
                       dba.EdgeType("edge_type"),
                       EdgeAtom::Direction::OUT},
      last_op, node1_sym, false);

  Check(last_op.get(), R"(
          {
            "name" : "CreateExpand",
            "node_info" : {
              "symbol" : "node2",
              "labels" : ["Label1", "Label2"],
              "properties" : {
                "prop1" : "5",
                "prop2" : "\"some cool stuff\""
              }
            },
            "edge_info" : {
              "symbol" : "edge",
              "properties" : {
                "weight" : "5.32"
               },
               "edge_type" : "edge_type",
               "direction" : "out"
            },
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node1",
              "input" : { "name" : "Once" }
            },
            "input_symbol" : "node1",
            "existing_node" : false
          })");
}

TEST_F(PrintToJsonTest, Expand) {
  auto node1_sym = GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<Expand>(
      last_op, node1_sym, GetSymbol("node2"), GetSymbol("edge"),
      EdgeAtom::Direction::BOTH,
      std::vector<storage::EdgeType>{dba.EdgeType("EdgeType1"),
                                     dba.EdgeType("EdgeType2")},
      false, GraphView::OLD);

  Check(last_op.get(), R"(
          {
            "name" : "Expand",
            "input_symbol" : "node1",
            "node_symbol" : "node2",
            "edge_symbol" : "edge",
            "direction" : "both",
            "edge_types" : ["EdgeType1", "EdgeType2"],
            "existing_node" : false,
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node1",
              "input" : { "name" : "Once" }
            }
          })");
}

TEST_F(PrintToJsonTest, ExpandVariable) {
  auto node1_sym = GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<ExpandVariable>(
      last_op, node1_sym, GetSymbol("node2"), GetSymbol("edge"),
      EdgeAtom::Type::BREADTH_FIRST, EdgeAtom::Direction::OUT,
      std::vector<storage::EdgeType>{dba.EdgeType("EdgeType1"),
                                     dba.EdgeType("EdgeType2")},
      false, LITERAL(2), LITERAL(5), false,
      ExpansionLambda{GetSymbol("inner_node"), GetSymbol("inner_edge"),
                      PROPERTY_LOOKUP("inner_node", dba.Property("unblocked"))},
      std::nullopt, std::nullopt);

  Check(last_op.get(), R"sep(
          {
            "name" : "ExpandVariable",
            "input_symbol" : "node1",
            "node_symbol" : "node2",
            "edge_symbol" : "edge",
            "direction" : "out",
            "edge_types" : ["EdgeType1", "EdgeType2"],
            "existing_node" : false,
            "type" : "bfs",
            "is_reverse" : false,
            "lower_bound" : "2",
            "upper_bound" : "5",
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node1",
              "input" : { "name" : "Once" }
            },
            "filter_lambda" : "(PropertyLookup (Identifier \"inner_node\") \"unblocked\")"
          })sep");
}

TEST_F(PrintToJsonTest, ExpandVariableWsp) {
  auto node1_sym = GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<ExpandVariable>(
      last_op, node1_sym, GetSymbol("node2"), GetSymbol("edge"),
      EdgeAtom::Type::WEIGHTED_SHORTEST_PATH, EdgeAtom::Direction::OUT,
      std::vector<storage::EdgeType>{dba.EdgeType("EdgeType1"),
                                     dba.EdgeType("EdgeType2")},
      false, LITERAL(2), LITERAL(5), false,
      ExpansionLambda{GetSymbol("inner_node"), GetSymbol("inner_edge"),
                      nullptr},
      ExpansionLambda{GetSymbol("inner_node"), GetSymbol("inner_edge"),
                      PROPERTY_LOOKUP("inner_edge", dba.Property("weight"))},
      GetSymbol("total"));

  Check(last_op.get(), R"sep(
          {
            "name" : "ExpandVariable",
            "input_symbol" : "node1",
            "node_symbol" : "node2",
            "edge_symbol" : "edge",
            "direction" : "out",
            "edge_types" : ["EdgeType1", "EdgeType2"],
            "existing_node" : false,
            "type" : "wsp",
            "is_reverse" : false,
            "lower_bound" : "2",
            "upper_bound" : "5",
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node1",
              "input" : { "name" : "Once" }
            },
            "filter_lambda" : null,
            "weight_lambda" : "(PropertyLookup (Identifier \"inner_edge\") \"weight\")",
            "total_weight_symbol" : "total"
          })sep");
}

TEST_F(PrintToJsonTest, ConstructNamedPath) {
  auto node1_sym = GetSymbol("node1");
  auto edge1_sym = GetSymbol("edge1");
  auto node2_sym = GetSymbol("node2");
  auto edge2_sym = GetSymbol("edge2");
  auto node3_sym = GetSymbol("node3");

  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<Expand>(
      last_op, node1_sym, node2_sym, edge1_sym, EdgeAtom::Direction::OUT,
      std::vector<storage::EdgeType>{}, false, GraphView::OLD);
  last_op = std::make_shared<Expand>(
      last_op, node2_sym, node3_sym, edge2_sym, EdgeAtom::Direction::OUT,
      std::vector<storage::EdgeType>{}, false, GraphView::OLD);
  last_op = std::make_shared<ConstructNamedPath>(
      last_op, GetSymbol("path"),
      std::vector<Symbol>{node1_sym, edge1_sym, node2_sym, edge2_sym,
                          node3_sym});

  Check(last_op.get(), R"(
          {
            "name" : "ConstructNamedPath",
            "path_symbol" : "path",
            "path_elements" : ["node1", "edge1", "node2", "edge2", "node3"],
            "input" : {
              "name" : "Expand",
              "input_symbol" : "node2",
              "node_symbol" : "node3",
              "edge_symbol" : "edge2",
              "direction" : "out",
              "edge_types" : null,
              "existing_node" : false,
              "input" : {
                "name" : "Expand",
                "input_symbol" : "node1",
                "node_symbol" : "node2",
                "edge_symbol": "edge1",
                "direction" : "out",
                "edge_types" : null,
                "existing_node" : false,
                "input" : {
                  "name" : "ScanAll",
                  "output_symbol" : "node1",
                  "input" : { "name" : "Once" }
                }
              }
            }
          })");
}

TEST_F(PrintToJsonTest, Filter) {
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, GetSymbol("node1"));
  last_op = std::make_shared<Filter>(
      last_op, EQ(PROPERTY_LOOKUP("node1", dba.Property("prop")), LITERAL(5)));

  Check(last_op.get(), R"sep(
          {
            "name" : "Filter",
            "expression" : "(== (PropertyLookup (Identifier \"node1\") \"prop\") 5)",
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node1",
              "input" : { "name" : "Once" }
            }
          })sep");
}

TEST_F(PrintToJsonTest, Produce) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<Produce>(
      nullptr,
      std::vector<NamedExpression *>{NEXPR("pet", LITERAL(5)),
                                     NEXPR("string", LITERAL("string"))});

  Check(last_op.get(), R"sep(
          {
            "name" : "Produce",
            "named_expressions" : [
              {
                "expression" : "5",
                "name" : "pet"
              },
              {
                "expression" : "\"string\"",
                "name" : "string"
              }
            ],
            "input" : { "name" : "Once" }
          })sep");
}

TEST_F(PrintToJsonTest, Delete) {
  auto node_sym = GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<Expand>(
      last_op, node_sym, GetSymbol("node2"), GetSymbol("edge"),
      EdgeAtom::Direction::BOTH, std::vector<storage::EdgeType>{}, false,
      GraphView::OLD);
  last_op = std::make_shared<plan::Delete>(
      last_op, std::vector<Expression *>{IDENT("node2")}, true);

  Check(last_op.get(), R"sep(
          {
            "name" : "Delete",
            "expressions" : [ "(Identifier \"node2\")" ],
            "detach" : true,
            "input" : {
              "name" : "Expand",
              "input_symbol" : "node1",
              "node_symbol" : "node2",
              "edge_symbol" : "edge",
              "direction" : "both",
              "edge_types" : null,
              "existing_node" : false,
              "input" : {
                "name" : "ScanAll",
                "output_symbol" : "node1",
                "input" : { "name" : "Once" }
              }
            }
          })sep");
}

TEST_F(PrintToJsonTest, SetProperty) {
  storage::Property prop = dba.Property("prop");

  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, GetSymbol("node"));
  last_op = std::make_shared<plan::SetProperty>(
      last_op, prop, PROPERTY_LOOKUP("node", prop),
      ADD(PROPERTY_LOOKUP("node", prop), LITERAL(1)));

  Check(last_op.get(), R"sep(
          {
            "name" : "SetProperty",
            "property" : "prop",
            "lhs" : "(PropertyLookup (Identifier \"node\") \"prop\")",
            "rhs" : "(+ (PropertyLookup (Identifier \"node\") \"prop\") 1)",
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node",
              "input" : { "name" : "Once" }
            }
          })sep");
}

TEST_F(PrintToJsonTest, SetProperties) {
  auto node_sym = GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::SetProperties>(
      last_op, node_sym,
      MAP({{storage.GetPropertyIx("prop1"), LITERAL(1)},
           {storage.GetPropertyIx("prop2"), LITERAL("propko")}}),
      plan::SetProperties::Op::REPLACE);

  Check(last_op.get(), R"sep(
          {
            "name" : "SetProperties",
            "input_symbol" : "node",
            "rhs" : "{\"prop1\": 1, \"prop2\": \"propko\"}",
            "op" : "replace",
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node",
              "input" : { "name" : "Once" }
            }
          })sep");
}

TEST_F(PrintToJsonTest, SetLabels) {
  auto node_sym = GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::SetLabels>(
      last_op, node_sym,
      std::vector<storage::Label>{dba.Label("label1"), dba.Label("label2")});

  Check(last_op.get(), R"(
          {
            "name" : "SetLabels",
            "input_symbol" : "node",
            "labels" : ["label1", "label2"],
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node",
              "input" : { "name" : "Once" }
            }
          })");
}

TEST_F(PrintToJsonTest, RemoveProperty) {
  auto node_sym = GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::RemoveProperty>(
      last_op, dba.Property("prop"),
      PROPERTY_LOOKUP("node", dba.Property("prop")));

  Check(last_op.get(), R"sep(
          {
            "name" : "RemoveProperty",
            "lhs" : "(PropertyLookup (Identifier \"node\") \"prop\")",
            "property" : "prop",
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node",
              "input" : { "name" : "Once" }
            }
          })sep");
}

TEST_F(PrintToJsonTest, RemoveLabels) {
  auto node_sym = GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::RemoveLabels>(
      last_op, node_sym,
      std::vector<storage::Label>{dba.Label("label1"), dba.Label("label2")});

  Check(last_op.get(), R"(
          {
            "name" : "RemoveLabels",
            "input_symbol" : "node",
            "labels" : ["label1", "label2"],
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node",
              "input" : { "name" : "Once" }
            }
          })");
}

TEST_F(PrintToJsonTest, EdgeUniquenessFilter) {
  auto node1_sym = GetSymbol("node1");
  auto node2_sym = GetSymbol("node2");
  auto node3_sym = GetSymbol("node3");
  auto node4_sym = GetSymbol("node4");

  auto edge1_sym = GetSymbol("edge1");
  auto edge2_sym = GetSymbol("edge2");

  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<Expand>(
      last_op, node1_sym, node2_sym, edge1_sym, EdgeAtom::Direction::IN,
      std::vector<storage::EdgeType>{}, false, GraphView::OLD);
  last_op = std::make_shared<ScanAll>(last_op, node3_sym);
  last_op = std::make_shared<Expand>(
      last_op, node3_sym, node4_sym, edge2_sym, EdgeAtom::Direction::OUT,
      std::vector<storage::EdgeType>{}, false, GraphView::OLD);
  last_op = std::make_shared<EdgeUniquenessFilter>(
      last_op, edge2_sym, std::vector<Symbol>{edge1_sym});

  Check(last_op.get(), R"(
          {
            "name" : "EdgeUniquenessFilter",
            "expand_symbol" : "edge2",
            "previous_symbols" : ["edge1"],
            "input" : {
              "name" : "Expand",
              "input_symbol" : "node3",
              "node_symbol" : "node4",
              "edge_symbol" : "edge2",
              "direction" : "out",
              "edge_types" : null,
              "existing_node" : false,
              "input" : {
                "name" : "ScanAll",
                "output_symbol" : "node3",
                "input" : {
                  "name" : "Expand",
                  "input_symbol" : "node1",
                  "node_symbol" : "node2",
                  "edge_symbol" : "edge1",
                  "direction" : "in",
                  "edge_types" : null,
                  "existing_node" : false,
                  "input" : {
                    "name" : "ScanAll",
                    "output_symbol" : "node1",
                    "input" : { "name" : "Once" }
                  }
                }
              }
            }
          })");
}

TEST_F(PrintToJsonTest, Accumulate) {
  storage::Property prop = dba.Property("prop");
  auto node_sym = GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::SetProperty>(
      last_op, prop, PROPERTY_LOOKUP("node", prop),
      ADD(PROPERTY_LOOKUP("node", prop), LITERAL(1)));
  last_op = std::make_shared<plan::Accumulate>(
      last_op, std::vector<Symbol>{node_sym}, true);

  Check(last_op.get(), R"sep(
          {
            "name" : "Accumulate",
            "symbols" : ["node"],
            "advance_command" : true,
            "input" : {
              "name" : "SetProperty",
              "property" : "prop",
              "lhs" : "(PropertyLookup (Identifier \"node\") \"prop\")",
              "rhs" : "(+ (PropertyLookup (Identifier \"node\") \"prop\") 1)",
              "input" : {
                "name" : "ScanAll",
                "output_symbol" : "node",
                "input" : { "name" : "Once" }
              }
            }
          })sep");
}

TEST_F(PrintToJsonTest, Aggregate) {
  storage::Property value = dba.Property("value");
  storage::Property color = dba.Property("color");
  storage::Property type = dba.Property("type");
  auto node_sym = GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::Aggregate>(
      last_op,
      std::vector<Aggregate::Element>{
          {PROPERTY_LOOKUP("node", value), nullptr, Aggregation::Op::SUM,
           GetSymbol("sum")},
          {PROPERTY_LOOKUP("node", value), PROPERTY_LOOKUP("node", color),
           Aggregation::Op::COLLECT_MAP, GetSymbol("map")}},
      std::vector<Expression *>{PROPERTY_LOOKUP("node", type)},
      std::vector<Symbol>{node_sym});

  Check(last_op.get(), R"sep(
          {
            "name" : "Aggregate",
            "aggregations" : [
              {
                "value" : "(PropertyLookup (Identifier \"node\") \"value\")",
                "op" : "sum",
                "output_symbol" : "sum"
              },
              {
                "value" : "(PropertyLookup (Identifier \"node\") \"value\")",
                "key" : "(PropertyLookup (Identifier \"node\") \"color\")",
                "op" : "collect",
                "output_symbol" : "map"
              }
            ],
            "group_by" : [
              "(PropertyLookup (Identifier \"node\") \"type\")"
            ],
            "remember" : ["node"],
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node",
              "input" : { "name" : "Once" }
            }
          })sep");
}

TEST_F(PrintToJsonTest, Skip) {
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, GetSymbol("node"));
  last_op = std::make_shared<Skip>(last_op, LITERAL(42));

  Check(last_op.get(), R"sep(
          {
            "name" : "Skip",
            "expression" : "42",
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node",
              "input" : { "name" : "Once" }
            }
          })sep");
}

TEST_F(PrintToJsonTest, Limit) {
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, GetSymbol("node"));
  last_op = std::make_shared<Limit>(last_op, LITERAL(42));

  Check(last_op.get(), R"sep(
          {
            "name" : "Limit",
            "expression" : "42",
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node",
              "input" : { "name" : "Once" }
            }
          })sep");
}

TEST_F(PrintToJsonTest, OrderBy) {
  Symbol node_sym = GetSymbol("node");
  storage::Property value = dba.Property("value");
  storage::Property color = dba.Property("color");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<OrderBy>(
      last_op,
      std::vector<SortItem>{{Ordering::ASC, PROPERTY_LOOKUP("node", value)},
                            {Ordering::DESC, PROPERTY_LOOKUP("node", color)}},
      std::vector<Symbol>{node_sym});

  Check(last_op.get(), R"sep(
          {
            "name" : "OrderBy",
            "order_by" : [
              {
                "ordering" : "asc",
                "expression" : "(PropertyLookup (Identifier \"node\") \"value\")"
              },
              {
                "ordering" : "desc",
                "expression" : "(PropertyLookup (Identifier \"node\") \"color\")"
              }
            ],
            "output_symbols" : ["node"],
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node",
              "input" : { "name" : "Once" }
            }
          })sep");
}

TEST_F(PrintToJsonTest, Merge) {
  Symbol node_sym = GetSymbol("node");
  storage::Label label = dba.Label("label");

  std::shared_ptr<LogicalOperator> match =
      std::make_shared<ScanAllByLabel>(nullptr, node_sym, label);

  std::shared_ptr<LogicalOperator> create = std::make_shared<CreateNode>(
      nullptr, NodeCreationInfo{node_sym, {label}, {}});

  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<plan::Merge>(nullptr, match, create);

  Check(last_op.get(), R"sep(
          {
            "name" : "Merge",
            "input" : { "name" : "Once" },
            "merge_match" : {
              "name" : "ScanAllByLabel",
              "label" : "label",
              "output_symbol" : "node",
              "input" : { "name" : "Once" }
            },
            "merge_create" : {
              "name" : "CreateNode",
              "node_info" : {
                "symbol" : "node",
                "labels" : ["label"],
                "properties"  : null
              },
              "input" : { "name" : "Once" }
            }
          })sep");
}

TEST_F(PrintToJsonTest, Optional) {
  Symbol node1_sym = GetSymbol("node1");
  Symbol node2_sym = GetSymbol("node2");
  Symbol edge_sym = GetSymbol("edge");

  std::shared_ptr<LogicalOperator> input =
      std::make_shared<ScanAll>(nullptr, node1_sym);

  std::shared_ptr<LogicalOperator> expand = std::make_shared<Expand>(
      nullptr, node1_sym, node2_sym, edge_sym, EdgeAtom::Direction::OUT,
      std::vector<storage::EdgeType>{}, false, GraphView::OLD);

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<Optional>(
      input, expand, std::vector<Symbol>{node2_sym, edge_sym});

  Check(last_op.get(), R"sep(
          {
            "name" : "Optional",
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node1",
              "input" : { "name" : "Once" }
            },
            "optional" : {
              "name" : "Expand",
              "input_symbol" : "node1",
              "node_symbol" : "node2",
              "edge_symbol" : "edge",
              "direction" : "out",
              "edge_types" : null,
              "existing_node" : false,
              "input" : { "name" : "Once" }
            },
            "optional_symbols" : ["node2", "edge"]
          })sep");
}

TEST_F(PrintToJsonTest, Unwind) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<plan::Unwind>(
      nullptr, LIST(LITERAL(1), LITERAL(2), LITERAL(3)), GetSymbol("x"));

  Check(last_op.get(), R"sep(
          {
            "name" : "Unwind",
            "output_symbol" : "x",
            "input_expression" : "(ListLiteral [1, 2, 3])",
            "input" : { "name" : "Once" }
          })sep");
}

TEST_F(PrintToJsonTest, Distinct) {
  Symbol x = GetSymbol("x");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<plan::Unwind>(
      nullptr, LIST(LITERAL(2), LITERAL(3), LITERAL(2)), x);
  last_op = std::make_shared<Distinct>(last_op, std::vector<Symbol>{x});

  Check(last_op.get(), R"sep(
          {
            "name" : "Distinct",
            "value_symbols" : ["x"],
            "input" : {
              "name" : "Unwind",
              "output_symbol" : "x",
              "input_expression" : "(ListLiteral [2, 3, 2])",
              "input" : { "name" : "Once" }
            }
          })sep");
}

TEST_F(PrintToJsonTest, Union) {
  Symbol x = GetSymbol("x");
  std::shared_ptr<LogicalOperator> lhs = std::make_shared<plan::Unwind>(
      nullptr, LIST(LITERAL(2), LITERAL(3), LITERAL(2)), x);

  Symbol node = GetSymbol("x");
  std::shared_ptr<LogicalOperator> rhs =
      std::make_shared<ScanAll>(nullptr, node);

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<Union>(
      lhs, rhs, std::vector<Symbol>{GetSymbol("x")}, std::vector<Symbol>{x},
      std::vector<Symbol>{node});

  Check(last_op.get(), R"sep(
          {
            "name" : "Union",
            "union_symbols" : ["x"],
            "left_symbols" : ["x"],
            "right_symbols" : ["x"],
            "left_op" : {
              "name" : "Unwind",
              "output_symbol" : "x",
              "input_expression" : "(ListLiteral [2, 3, 2])",
              "input" : { "name" : "Once" }
            },
            "right_op" : {
              "name" : "ScanAll",
              "output_symbol" : "x",
              "input" : { "name" : "Once" }
            }
          })sep");
}

TEST_F(PrintToJsonTest, Cartesian) {
  Symbol x = GetSymbol("x");
  std::shared_ptr<LogicalOperator> lhs = std::make_shared<plan::Unwind>(
      nullptr, LIST(LITERAL(2), LITERAL(3), LITERAL(2)), x);

  Symbol node = GetSymbol("node");
  std::shared_ptr<LogicalOperator> rhs =
      std::make_shared<ScanAll>(nullptr, node);

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<Cartesian>(
      lhs, std::vector<Symbol>{x}, rhs, std::vector<Symbol>{node});

  Check(last_op.get(), R"sep(
          {
            "name" : "Cartesian",
            "left_symbols" : ["x"],
            "right_symbols" : ["node"],
            "left_op" : {
              "name" : "Unwind",
              "output_symbol" : "x",
              "input_expression" : "(ListLiteral [2, 3, 2])",
              "input" : { "name" : "Once" }
            },
            "right_op" : {
              "name" : "ScanAll",
              "output_symbol" : "node",
              "input" : { "name" : "Once" }
            }
          })sep");
}

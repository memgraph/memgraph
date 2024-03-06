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

#include <gtest/gtest.h>

#include "disk_test_utils.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"
#include "query/plan/pretty_print.hpp"

#include "query_common.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;

// The JSON formatted plan is consumed (or will be) by Memgraph Lab, and
// therefore should not be changed before synchronizing with whoever is
// maintaining Memgraph Lab. Hopefully, one day integration tests will exist and
// there will be no need to be super careful.

// This is a hack to prevent Googletest from crashing when outputing JSONs.
namespace nlohmann {
void PrintTo(const json &json, std::ostream *os) { *os << std::endl << json.dump(1); }
}  // namespace nlohmann

using namespace nlohmann;

template <typename StorageType>
class PrintToJsonTest : public ::testing::Test {
 protected:
  const std::string testSuite = "plan_pretty_print";

  PrintToJsonTest()
      : config(disk_test_utils::GenerateOnDiskConfig(testSuite)),
        db(new StorageType(config)),
        dba_storage(db->Access(memgraph::replication_coordination_glue::ReplicationRole::MAIN)),
        dba(dba_storage.get()) {}

  ~PrintToJsonTest() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }

  AstStorage storage;
  SymbolTable symbol_table;

  memgraph::storage::Config config;
  std::unique_ptr<memgraph::storage::Storage> db;
  std::unique_ptr<memgraph::storage::Storage::Accessor> dba_storage;
  memgraph::query::DbAccessor dba;

  Symbol GetSymbol(std::string name) { return symbol_table.CreateSymbol(name, true); }

  void Check(LogicalOperator *root, std::string expected) { EXPECT_EQ(PlanToJson(dba, root), json::parse(expected)); }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(PrintToJsonTest, StorageTypes);

TYPED_TEST(PrintToJsonTest, Once) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<Once>();

  this->Check(last_op.get(), R"(
        {
          "name" : "Once"
        })");
}

TYPED_TEST(PrintToJsonTest, ScanAll) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<ScanAll>(nullptr, this->GetSymbol("node"));

  this->Check(last_op.get(), R"(
        {
          "name" : "ScanAll",
          "output_symbol" : "node",
          "input" : { "name" : "Once" }
        })");
}

TYPED_TEST(PrintToJsonTest, ScanAllByLabel) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<ScanAllByLabel>(nullptr, this->GetSymbol("node"), this->dba.NameToLabel("Label"));

  this->Check(last_op.get(), R"(
        {
          "name" : "ScanAllByLabel",
          "label" : "Label",
          "output_symbol" : "node",
          "input" : { "name" : "Once" }
        })");
}

TYPED_TEST(PrintToJsonTest, ScanAllByLabelPropertyRange) {
  {
    std::shared_ptr<LogicalOperator> last_op;
    last_op = std::make_shared<ScanAllByLabelPropertyRange>(
        nullptr, this->GetSymbol("node"), this->dba.NameToLabel("Label"), this->dba.NameToProperty("prop"), "prop",
        memgraph::utils::MakeBoundInclusive<Expression *>(LITERAL(1)),
        memgraph::utils::MakeBoundExclusive<Expression *>(LITERAL(20)));

    this->Check(last_op.get(), R"(
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
        nullptr, this->GetSymbol("node"), this->dba.NameToLabel("Label"), this->dba.NameToProperty("prop"), "prop",
        std::nullopt, memgraph::utils::MakeBoundExclusive<Expression *>(LITERAL(20)));

    this->Check(last_op.get(), R"(
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
        nullptr, this->GetSymbol("node"), this->dba.NameToLabel("Label"), this->dba.NameToProperty("prop"), "prop",
        memgraph::utils::MakeBoundInclusive<Expression *>(LITERAL(1)), std::nullopt);

    this->Check(last_op.get(), R"(
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

TYPED_TEST(PrintToJsonTest, ScanAllByLabelPropertyValue) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<ScanAllByLabelPropertyValue>(
      nullptr, this->GetSymbol("node"), this->dba.NameToLabel("Label"), this->dba.NameToProperty("prop"), "prop",
      ADD(LITERAL(21), LITERAL(21)));

  this->Check(last_op.get(), R"sep(
        {
          "name" : "ScanAllByLabelPropertyValue",
          "label" : "Label",
          "property" : "prop",
          "expression" : "(+ 21 21)",
          "output_symbol" : "node",
          "input" : { "name" : "Once" }
        })sep");
}

TYPED_TEST(PrintToJsonTest, CreateNode) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<CreateNode>(
      nullptr, NodeCreationInfo{this->GetSymbol("node"),
                                {this->dba.NameToLabel("Label1"), this->dba.NameToLabel("Label2")},
                                {{this->dba.NameToProperty("prop1"), LITERAL(5)},
                                 {this->dba.NameToProperty("prop2"), LITERAL("some cool stuff")}}});

  this->Check(last_op.get(), R"(
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

TYPED_TEST(PrintToJsonTest, CreateExpand) {
  Symbol node1_sym = this->GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, this->GetSymbol("node1"));
  last_op = std::make_shared<CreateExpand>(
      NodeCreationInfo{this->GetSymbol("node2"),
                       {this->dba.NameToLabel("Label1"), this->dba.NameToLabel("Label2")},
                       {{this->dba.NameToProperty("prop1"), LITERAL(5)},
                        {this->dba.NameToProperty("prop2"), LITERAL("some cool stuff")}}},
      EdgeCreationInfo{this->GetSymbol("edge"),
                       {{this->dba.NameToProperty("weight"), LITERAL(5.32)}},
                       this->dba.NameToEdgeType("edge_type"),
                       EdgeAtom::Direction::OUT},
      last_op, node1_sym, false);

  this->Check(last_op.get(), R"(
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

TYPED_TEST(PrintToJsonTest, Expand) {
  auto node1_sym = this->GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<Expand>(last_op, node1_sym, this->GetSymbol("node2"), this->GetSymbol("edge"),
                                     EdgeAtom::Direction::BOTH,
                                     std::vector<memgraph::storage::EdgeTypeId>{this->dba.NameToEdgeType("EdgeType1"),
                                                                                this->dba.NameToEdgeType("EdgeType2")},
                                     false, memgraph::storage::View::OLD);

  this->Check(last_op.get(), R"(
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

TYPED_TEST(PrintToJsonTest, ExpandVariable) {
  auto node1_sym = this->GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<ExpandVariable>(
      last_op, node1_sym, this->GetSymbol("node2"), this->GetSymbol("edge"), EdgeAtom::Type::BREADTH_FIRST,
      EdgeAtom::Direction::OUT,
      std::vector<memgraph::storage::EdgeTypeId>{this->dba.NameToEdgeType("EdgeType1"),
                                                 this->dba.NameToEdgeType("EdgeType2")},
      false, LITERAL(2), LITERAL(5), false,
      ExpansionLambda{this->GetSymbol("inner_node"), this->GetSymbol("inner_edge"),
                      PROPERTY_LOOKUP(this->dba, "inner_node", this->dba.NameToProperty("unblocked"))},
      std::nullopt, std::nullopt);

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, ExpandVariableWsp) {
  auto node1_sym = this->GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<ExpandVariable>(
      last_op, node1_sym, this->GetSymbol("node2"), this->GetSymbol("edge"), EdgeAtom::Type::WEIGHTED_SHORTEST_PATH,
      EdgeAtom::Direction::OUT,
      std::vector<memgraph::storage::EdgeTypeId>{this->dba.NameToEdgeType("EdgeType1"),
                                                 this->dba.NameToEdgeType("EdgeType2")},
      false, LITERAL(2), LITERAL(5), false,
      ExpansionLambda{this->GetSymbol("inner_node"), this->GetSymbol("inner_edge"), nullptr},
      ExpansionLambda{this->GetSymbol("inner_node"), this->GetSymbol("inner_edge"),
                      PROPERTY_LOOKUP(this->dba, "inner_edge", this->dba.NameToProperty("weight"))},
      this->GetSymbol("total"));

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, ConstructNamedPath) {
  auto node1_sym = this->GetSymbol("node1");
  auto edge1_sym = this->GetSymbol("edge1");
  auto node2_sym = this->GetSymbol("node2");
  auto edge2_sym = this->GetSymbol("edge2");
  auto node3_sym = this->GetSymbol("node3");

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<Expand>(last_op, node1_sym, node2_sym, edge1_sym, EdgeAtom::Direction::OUT,
                                     std::vector<memgraph::storage::EdgeTypeId>{}, false, memgraph::storage::View::OLD);
  last_op = std::make_shared<Expand>(last_op, node2_sym, node3_sym, edge2_sym, EdgeAtom::Direction::OUT,
                                     std::vector<memgraph::storage::EdgeTypeId>{}, false, memgraph::storage::View::OLD);
  last_op = std::make_shared<ConstructNamedPath>(
      last_op, this->GetSymbol("path"), std::vector<Symbol>{node1_sym, edge1_sym, node2_sym, edge2_sym, node3_sym});

  this->Check(last_op.get(), R"(
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

TYPED_TEST(PrintToJsonTest, Filter) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, this->GetSymbol("node1"));
  last_op =
      std::make_shared<Filter>(last_op, std::vector<std::shared_ptr<LogicalOperator>>{},
                               EQ(PROPERTY_LOOKUP(this->dba, "node1", this->dba.NameToProperty("prop")), LITERAL(5)));

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, Produce) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<Produce>(
      nullptr, std::vector<NamedExpression *>{NEXPR("pet", LITERAL(5)), NEXPR("string", LITERAL("string"))});

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, Delete) {
  auto node_sym = this->GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<Expand>(last_op, node_sym, this->GetSymbol("node2"), this->GetSymbol("edge"),
                                     EdgeAtom::Direction::BOTH, std::vector<memgraph::storage::EdgeTypeId>{}, false,
                                     memgraph::storage::View::OLD);
  last_op = std::make_shared<plan::Delete>(last_op, std::vector<Expression *>{IDENT("node2")}, true);

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, SetProperty) {
  memgraph::storage::PropertyId prop = this->dba.NameToProperty("prop");

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, this->GetSymbol("node"));
  last_op = std::make_shared<plan::SetProperty>(last_op, prop, PROPERTY_LOOKUP(this->dba, "node", prop),
                                                ADD(PROPERTY_LOOKUP(this->dba, "node", prop), LITERAL(1)));

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, SetProperties) {
  auto node_sym = this->GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::SetProperties>(last_op, node_sym,
                                                  MAP({{this->storage.GetPropertyIx("prop1"), LITERAL(1)},
                                                       {this->storage.GetPropertyIx("prop2"), LITERAL("propko")}}),
                                                  plan::SetProperties::Op::REPLACE);

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, SetLabels) {
  auto node_sym = this->GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::SetLabels>(
      last_op, node_sym,
      std::vector<std::variant<memgraph::storage::LabelId, memgraph::query::Expression *>>{
          this->dba.NameToLabel("label1"), this->dba.NameToLabel("label2")});

  this->Check(last_op.get(), R"(
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

TYPED_TEST(PrintToJsonTest, RemoveProperty) {
  auto node_sym = this->GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::RemoveProperty>(
      last_op, this->dba.NameToProperty("prop"), PROPERTY_LOOKUP(this->dba, "node", this->dba.NameToProperty("prop")));

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, RemoveLabels) {
  auto node_sym = this->GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::RemoveLabels>(
      last_op, node_sym,
      std::vector<std::variant<memgraph::storage::LabelId, memgraph::query::Expression *>>{
          this->dba.NameToLabel("label1"), this->dba.NameToLabel("label2")});

  this->Check(last_op.get(), R"(
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

TYPED_TEST(PrintToJsonTest, EdgeUniquenessFilter) {
  auto node1_sym = this->GetSymbol("node1");
  auto node2_sym = this->GetSymbol("node2");
  auto node3_sym = this->GetSymbol("node3");
  auto node4_sym = this->GetSymbol("node4");

  auto edge1_sym = this->GetSymbol("edge1");
  auto edge2_sym = this->GetSymbol("edge2");

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<Expand>(last_op, node1_sym, node2_sym, edge1_sym, EdgeAtom::Direction::IN,
                                     std::vector<memgraph::storage::EdgeTypeId>{}, false, memgraph::storage::View::OLD);
  last_op = std::make_shared<ScanAll>(last_op, node3_sym);
  last_op = std::make_shared<Expand>(last_op, node3_sym, node4_sym, edge2_sym, EdgeAtom::Direction::OUT,
                                     std::vector<memgraph::storage::EdgeTypeId>{}, false, memgraph::storage::View::OLD);
  last_op = std::make_shared<EdgeUniquenessFilter>(last_op, edge2_sym, std::vector<Symbol>{edge1_sym});

  this->Check(last_op.get(), R"(
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

TYPED_TEST(PrintToJsonTest, Accumulate) {
  memgraph::storage::PropertyId prop = this->dba.NameToProperty("prop");
  auto node_sym = this->GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::SetProperty>(last_op, prop, PROPERTY_LOOKUP(this->dba, "node", prop),
                                                ADD(PROPERTY_LOOKUP(this->dba, "node", prop), LITERAL(1)));
  last_op = std::make_shared<plan::Accumulate>(last_op, std::vector<Symbol>{node_sym}, true);

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, Aggregate) {
  memgraph::storage::PropertyId value = this->dba.NameToProperty("value");
  memgraph::storage::PropertyId color = this->dba.NameToProperty("color");
  memgraph::storage::PropertyId type = this->dba.NameToProperty("type");
  auto node_sym = this->GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::Aggregate>(
      last_op,
      std::vector<Aggregate::Element>{
          {PROPERTY_LOOKUP(this->dba, "node", value), nullptr, Aggregation::Op::SUM, this->GetSymbol("sum")},
          {PROPERTY_LOOKUP(this->dba, "node", value), PROPERTY_LOOKUP(this->dba, "node", color),
           Aggregation::Op::COLLECT_MAP, this->GetSymbol("map")},
          {nullptr, nullptr, Aggregation::Op::COUNT, this->GetSymbol("count")}},
      std::vector<Expression *>{PROPERTY_LOOKUP(this->dba, "node", type)}, std::vector<Symbol>{node_sym});

  this->Check(last_op.get(), R"sep(
          {
            "name" : "Aggregate",
            "aggregations" : [
              {
                "value" : "(PropertyLookup (Identifier \"node\") \"value\")",
                "op" : "sum",
                "output_symbol" : "sum",
                "distinct" : false
              },
              {
                "value" : "(PropertyLookup (Identifier \"node\") \"value\")",
                "key" : "(PropertyLookup (Identifier \"node\") \"color\")",
                "op" : "collect",
                "output_symbol" : "map",
                "distinct" : false
              },
              {
                "op": "count",
                "output_symbol": "count",
                "distinct" : false
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

TYPED_TEST(PrintToJsonTest, AggregateWithDistinct) {
  memgraph::storage::PropertyId value = this->dba.NameToProperty("value");
  memgraph::storage::PropertyId color = this->dba.NameToProperty("color");
  memgraph::storage::PropertyId type = this->dba.NameToProperty("type");
  auto node_sym = this->GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::Aggregate>(
      last_op,
      std::vector<Aggregate::Element>{
          {PROPERTY_LOOKUP(this->dba, "node", value), nullptr, Aggregation::Op::SUM, this->GetSymbol("sum"), true},
          {PROPERTY_LOOKUP(this->dba, "node", value), PROPERTY_LOOKUP(this->dba, "node", color),
           Aggregation::Op::COLLECT_MAP, this->GetSymbol("map"), true},
          {nullptr, nullptr, Aggregation::Op::COUNT, this->GetSymbol("count"), true}},
      std::vector<Expression *>{PROPERTY_LOOKUP(this->dba, "node", type)}, std::vector<Symbol>{node_sym});

  this->Check(last_op.get(), R"sep(
          {
            "name" : "Aggregate",
            "aggregations" : [
              {
                "value" : "(PropertyLookup (Identifier \"node\") \"value\")",
                "op" : "sum",
                "output_symbol" : "sum",
                "distinct" : true
              },
              {
                "value" : "(PropertyLookup (Identifier \"node\") \"value\")",
                "key" : "(PropertyLookup (Identifier \"node\") \"color\")",
                "op" : "collect",
                "output_symbol" : "map",
                "distinct" : true
              },
              {
                "op": "count",
                "output_symbol": "count",
                "distinct" : true
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

TYPED_TEST(PrintToJsonTest, Skip) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, this->GetSymbol("node"));
  last_op = std::make_shared<Skip>(last_op, LITERAL(42));

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, Limit) {
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, this->GetSymbol("node"));
  last_op = std::make_shared<Limit>(last_op, LITERAL(42));

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, OrderBy) {
  Symbol node_sym = this->GetSymbol("node");
  memgraph::storage::PropertyId value = this->dba.NameToProperty("value");
  memgraph::storage::PropertyId color = this->dba.NameToProperty("color");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, node_sym);
  last_op =
      std::make_shared<OrderBy>(last_op,
                                std::vector<SortItem>{{Ordering::ASC, PROPERTY_LOOKUP(this->dba, "node", value)},
                                                      {Ordering::DESC, PROPERTY_LOOKUP(this->dba, "node", color)}},
                                std::vector<Symbol>{node_sym});

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, Merge) {
  Symbol node_sym = this->GetSymbol("node");
  memgraph::storage::LabelId label = this->dba.NameToLabel("label");

  std::shared_ptr<LogicalOperator> match = std::make_shared<ScanAllByLabel>(nullptr, node_sym, label);

  std::shared_ptr<LogicalOperator> create =
      std::make_shared<CreateNode>(nullptr, NodeCreationInfo{node_sym, {label}, {}});

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<plan::Merge>(nullptr, match, create);

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, Optional) {
  Symbol node1_sym = this->GetSymbol("node1");
  Symbol node2_sym = this->GetSymbol("node2");
  Symbol edge_sym = this->GetSymbol("edge");

  std::shared_ptr<LogicalOperator> input = std::make_shared<ScanAll>(nullptr, node1_sym);

  std::shared_ptr<LogicalOperator> expand =
      std::make_shared<Expand>(nullptr, node1_sym, node2_sym, edge_sym, EdgeAtom::Direction::OUT,
                               std::vector<memgraph::storage::EdgeTypeId>{}, false, memgraph::storage::View::OLD);

  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<Optional>(input, expand, std::vector<Symbol>{node2_sym, edge_sym});

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, Unwind) {
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<plan::Unwind>(nullptr, LIST(LITERAL(1), LITERAL(2), LITERAL(3)), this->GetSymbol("x"));

  this->Check(last_op.get(), R"sep(
          {
            "name" : "Unwind",
            "output_symbol" : "x",
            "input_expression" : "(ListLiteral [1, 2, 3])",
            "input" : { "name" : "Once" }
          })sep");
}

TYPED_TEST(PrintToJsonTest, Distinct) {
  Symbol x = this->GetSymbol("x");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<plan::Unwind>(nullptr, LIST(LITERAL(2), LITERAL(3), LITERAL(2)), x);
  last_op = std::make_shared<Distinct>(last_op, std::vector<Symbol>{x});

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, Union) {
  Symbol x = this->GetSymbol("x");
  std::shared_ptr<LogicalOperator> lhs =
      std::make_shared<plan::Unwind>(nullptr, LIST(LITERAL(2), LITERAL(3), LITERAL(2)), x);

  Symbol node = this->GetSymbol("x");
  std::shared_ptr<LogicalOperator> rhs = std::make_shared<ScanAll>(nullptr, node);

  std::shared_ptr<LogicalOperator> last_op = std::make_shared<Union>(
      lhs, rhs, std::vector<Symbol>{this->GetSymbol("x")}, std::vector<Symbol>{x}, std::vector<Symbol>{node});

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, Cartesian) {
  Symbol x = this->GetSymbol("x");
  std::shared_ptr<LogicalOperator> lhs =
      std::make_shared<plan::Unwind>(nullptr, LIST(LITERAL(2), LITERAL(3), LITERAL(2)), x);

  Symbol node = this->GetSymbol("node");
  std::shared_ptr<LogicalOperator> rhs = std::make_shared<ScanAll>(nullptr, node);

  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<Cartesian>(lhs, std::vector<Symbol>{x}, rhs, std::vector<Symbol>{node});

  this->Check(last_op.get(), R"sep(
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

TYPED_TEST(PrintToJsonTest, CallProcedure) {
  memgraph::query::plan::CallProcedure call_op;
  call_op.input_ = std::make_shared<Once>();
  call_op.procedure_name_ = "mg.reload";
  call_op.arguments_ = {LITERAL("example")};
  call_op.result_fields_ = {"name", "signature"};
  call_op.result_symbols_ = {this->GetSymbol("name_alias"), this->GetSymbol("signature_alias")};
  this->Check(&call_op, R"sep(
          {
            "arguments" : ["\"example\""],
            "input" : { "name" : "Once" },
            "name" : "CallProcedure",
            "procedure_name" : "mg.reload",
            "result_fields" : ["name", "signature"],
            "result_symbols" : ["name_alias", "signature_alias"]
          })sep");
}

TYPED_TEST(PrintToJsonTest, Foreach) {
  Symbol x = this->GetSymbol("x");
  std::shared_ptr<LogicalOperator> create = std::make_shared<CreateNode>(
      nullptr, NodeCreationInfo{this->GetSymbol("node"), {this->dba.NameToLabel("Label1")}, {}});
  std::shared_ptr<LogicalOperator> foreach =
      std::make_shared<plan::Foreach>(nullptr, std::move(create), LIST(LITERAL(1)), x);

  this->Check(foreach.get(), R"sep(
          {
           "expression": "(ListLiteral [1])",
           "input": {
            "name": "Once"
           },
           "name": "Foreach",
           "loop_variable_symbol": "x",
           "update_clauses": {
            "input": {
             "name": "Once"
            },
            "name": "CreateNode",
            "node_info": {
             "labels": [
              "Label1"
             ],
             "properties": null,
             "symbol": "node"
            }
           }
          })sep");
}

TYPED_TEST(PrintToJsonTest, Exists) {
  Symbol x = this->GetSymbol("x");
  Symbol e = this->GetSymbol("edge");
  Symbol n = this->GetSymbol("node");
  Symbol output = this->GetSymbol("output_symbol");
  std::shared_ptr<LogicalOperator> last_op = std::make_shared<ScanAll>(nullptr, x);
  std::shared_ptr<LogicalOperator> expand =
      std::make_shared<Expand>(nullptr, x, n, e, memgraph::query::EdgeAtom::Direction::BOTH,
                               std::vector<memgraph::storage::EdgeTypeId>{this->dba.NameToEdgeType("EdgeType1")}, false,
                               memgraph::storage::View::OLD);
  std::shared_ptr<LogicalOperator> limit = std::make_shared<Limit>(expand, LITERAL(1));
  std::shared_ptr<LogicalOperator> evaluate_pattern_filter = std::make_shared<EvaluatePatternFilter>(limit, output);
  last_op = std::make_shared<Filter>(
      last_op, std::vector<std::shared_ptr<LogicalOperator>>{evaluate_pattern_filter},
      EXISTS(PATTERN(NODE("x"), EDGE("edge", memgraph::query::EdgeAtom::Direction::BOTH, {}, false),
                     NODE("node", std::nullopt, false))));

  this->Check(last_op.get(), R"sep(
          {
            "expression": "(Exists expression)",
            "input": {
              "input": {
                "name": "Once"
              },
              "name": "ScanAll",
              "output_symbol": "x"
            },
            "name": "Filter",
            "pattern_filter1": {
              "input": {
                "expression": "1",
                "input": {
                  "direction": "both",
                  "edge_symbol": "edge",
                  "edge_types": [
                    "EdgeType1"
                  ],
                  "existing_node": false,
                  "input": {
                    "name": "Once"
                  },
                  "input_symbol": "x",
                  "name": "Expand",
                  "node_symbol": "node"
                },
                "name": "Limit"
              },
              "name": "EvaluatePatternFilter",
              "output_symbol": "output_symbol"
            }
          })sep");
}

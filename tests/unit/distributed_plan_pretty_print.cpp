#include <gtest/gtest.h>

#include "database/distributed/graph_db.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/distributed_ops.hpp"
#include "query/plan/distributed_pretty_print.hpp"

#include "distributed_common.hpp"
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
 public:
  static void SetUpTestCase() {
    cluster_ = std::make_unique<Cluster>(0, "distributed_plan_to_json");
  }
  static void TearDownTestCase() { cluster_ = nullptr; }

 protected:
  PrintToJsonTest() : dba_ptr(cluster_->master()->Access()), dba(*dba_ptr) {}

  AstStorage storage;
  SymbolTable symbol_table;

  std::unique_ptr<database::GraphDbAccessor> dba_ptr;
  database::GraphDbAccessor &dba;

  Symbol GetSymbol(std::string name) {
    return symbol_table.CreateSymbol(name, true);
  }

  void Check(LogicalOperator *root, std::string expected) {
    EXPECT_EQ(DistributedPlanToJson(dba, root), json::parse(expected));
  }

  static std::unique_ptr<Cluster> cluster_;
};

std::unique_ptr<Cluster> PrintToJsonTest::cluster_{nullptr};

TEST_F(PrintToJsonTest, PullRemote) {
  Symbol node_sym = GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<ScanAll>(nullptr, GetSymbol("node"));
  last_op =
      std::make_shared<PullRemote>(last_op, 1, std::vector<Symbol>{node_sym});

  Check(last_op.get(), R"(
        {
          "name" : "PullRemote",
          "symbols" : ["node"],
          "input" : {
            "name" : "ScanAll",
            "output_symbol" : "node",
            "input" : { "name" : "Once" }
          }
        })");
}

TEST_F(PrintToJsonTest, Synchronize) {
  storage::Property prop = dba.Property("prop");
  auto node_sym = GetSymbol("node");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<plan::SetProperty>(
      last_op, prop, PROPERTY_LOOKUP("node", prop),
      ADD(PROPERTY_LOOKUP("node", prop), LITERAL(1)));
  auto pull_remote =
      std::make_shared<PullRemote>(last_op, 1, std::vector<Symbol>{node_sym});
  last_op = std::make_shared<Synchronize>(std::make_shared<Once>(), pull_remote,
                                          true);

  Check(last_op.get(), R"sep(
          {
            "name" : "Synchronize",
            "advance_command" : true,
            "input" : { "name" : "Once" },
            "pull_remote" : {
              "name" : "PullRemote",
              "symbols" : ["node"],
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
            }
          })sep");
}

TEST_F(PrintToJsonTest, SynchronizeNoPullRemote) {
  auto last_op =
      std::make_shared<Synchronize>(std::make_shared<Once>(), nullptr, false);

  Check(last_op.get(), R"sep(
          {
            "name" : "Synchronize",
            "advance_command" : false,
            "input" : { "name" : "Once" },
            "pull_remote" : null
          })sep");
}

TEST_F(PrintToJsonTest, PullRemoteOrderBy) {
  Symbol node_sym = GetSymbol("node");
  storage::Property value = dba.Property("value");
  storage::Property color = dba.Property("color");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node_sym);
  last_op = std::make_shared<PullRemoteOrderBy>(
      last_op, 1,
      std::vector<SortItem>{{Ordering::ASC, PROPERTY_LOOKUP("node", value)},
                            {Ordering::DESC, PROPERTY_LOOKUP("node", color)}},
      std::vector<Symbol>{node_sym});

  Check(last_op.get(), R"sep(
          {
            "name" : "PullRemoteOrderBy",
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
            "symbols" : ["node"],
            "input" : {
              "name" : "ScanAll",
              "output_symbol" : "node",
              "input" : { "name" : "Once" }
            }
          })sep");
}

TEST_F(PrintToJsonTest, Expand) {
  auto node1_sym = GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<DistributedExpand>(
      last_op, node1_sym, GetSymbol("node2"), GetSymbol("edge"),
      EdgeAtom::Direction::BOTH,
      std::vector<storage::EdgeType>{dba.EdgeType("EdgeType1"),
                                     dba.EdgeType("EdgeType2")},
      false, GraphView::OLD);

  Check(last_op.get(), R"(
          {
            "name" : "DistributedExpand",
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

TEST_F(PrintToJsonTest, DistributedExpandBfs) {
  auto node1_sym = GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, node1_sym);
  last_op = std::make_shared<DistributedExpandBfs>(
      last_op, node1_sym, GetSymbol("node2"), GetSymbol("edge"),
      EdgeAtom::Direction::OUT,
      std::vector<storage::EdgeType>{dba.EdgeType("EdgeType1"),
                                     dba.EdgeType("EdgeType2")},
      false, LITERAL(2), LITERAL(5),
      ExpansionLambda{
          GetSymbol("inner_node"), GetSymbol("inner_edge"),
          PROPERTY_LOOKUP("inner_node", dba.Property("unblocked"))});

  Check(last_op.get(), R"sep(
          {
            "name" : "DistributedExpandBfs",
            "input_symbol" : "node1",
            "node_symbol" : "node2",
            "edge_symbol" : "edge",
            "direction" : "out",
            "edge_types" : ["EdgeType1", "EdgeType2"],
            "existing_node" : false,
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

TEST_F(PrintToJsonTest, DistributedCreateNode) {
  std::shared_ptr<LogicalOperator> last_op;
  last_op = std::make_shared<DistributedCreateNode>(
      std::make_shared<Once>(),
      NodeCreationInfo{GetSymbol("node"),
                       {dba.Label("Label1"), dba.Label("Label2")},
                       {{dba.Property("prop1"), LITERAL(5)},
                        {dba.Property("prop2"), LITERAL("some cool stuff")}}},
      true);

  Check(last_op.get(), R"(
          {
            "name" : "DistributedCreateNode",
            "on_random_worker" : true,
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

TEST_F(PrintToJsonTest, DistributedCreateExpand) {
  Symbol node1_sym = GetSymbol("node1");
  std::shared_ptr<LogicalOperator> last_op =
      std::make_shared<ScanAll>(nullptr, GetSymbol("node1"));
  last_op = std::make_shared<DistributedCreateExpand>(
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
            "name" : "DistributedCreateExpand",
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

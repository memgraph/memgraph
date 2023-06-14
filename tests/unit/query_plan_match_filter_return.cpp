// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query_plan_common.hpp"

#include <iterator>
#include <memory>
#include <optional>
#include <unordered_map>
#include <variant>
#include <vector>

#include <fmt/format.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cppitertools/enumerate.hpp>
#include <cppitertools/product.hpp>
#include <cppitertools/range.hpp>
#include <cppitertools/repeat.hpp>

#include "auth/auth.hpp"
#include "auth/models.hpp"
#include "glue/auth_checker.hpp"
#include "license/license.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/plan/operator.hpp"
#include "utils/synchronized.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;

class MatchReturnFixture : public testing::Test {
 protected:
  memgraph::storage::Storage db;
  memgraph::storage::Storage::Accessor storage_dba{db.Access()};
  memgraph::query::DbAccessor dba{&storage_dba};
  AstStorage storage;
  SymbolTable symbol_table;

  void AddVertices(int count) {
    for (int i = 0; i < count; i++) dba.InsertVertex();
  }
  void SetUp() override { memgraph::license::global_license_checker.EnableTesting(); }

  std::vector<Path> PathResults(std::shared_ptr<Produce> &op) {
    std::vector<Path> res;
    auto context = MakeContext(storage, symbol_table, &dba);
    for (const auto &row : CollectProduce(*op, &context)) res.emplace_back(row[0].ValuePath());
    return res;
  }

  int PullCountAuthorized(ScanAllTuple scan_all, memgraph::auth::User user) {
#ifdef MG_ENTERPRISE
    auto output =
        NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(scan_all.op_, output);
    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(storage, symbol_table, &dba, &auth_checker);
    return PullAll(*produce, &context);
#endif
    return 0;
  }
};

TEST_F(MatchReturnFixture, MatchReturn) {
  AddVertices(2);
  dba.AdvanceCommand();

  auto test_pull_count = [&](memgraph::storage::View view) {
    auto scan_all = MakeScanAll(storage, symbol_table, "n", nullptr, view);
    auto output =
        NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(scan_all.op_, output);
    auto context = MakeContext(storage, symbol_table, &dba);
    return PullAll(*produce, &context);
  };

  EXPECT_EQ(2, test_pull_count(memgraph::storage::View::NEW));
  EXPECT_EQ(2, test_pull_count(memgraph::storage::View::OLD));
  dba.InsertVertex();
  EXPECT_EQ(3, test_pull_count(memgraph::storage::View::NEW));
  EXPECT_EQ(2, test_pull_count(memgraph::storage::View::OLD));
  dba.AdvanceCommand();
  EXPECT_EQ(3, test_pull_count(memgraph::storage::View::OLD));
}

TEST_F(MatchReturnFixture, MatchReturnPath) {
  AddVertices(2);
  dba.AdvanceCommand();

  auto scan_all = MakeScanAll(storage, symbol_table, "n", nullptr);
  Symbol path_sym = symbol_table.CreateSymbol("path", true);
  auto make_path = std::make_shared<ConstructNamedPath>(scan_all.op_, path_sym, std::vector<Symbol>{scan_all.sym_});
  auto output =
      NEXPR("path", IDENT("path")->MapTo(path_sym))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(make_path, output);
  auto results = PathResults(produce);
  ASSERT_EQ(results.size(), 2);
  std::vector<memgraph::query::Path> expected_paths;
  for (const auto &v : dba.Vertices(memgraph::storage::View::OLD)) expected_paths.emplace_back(v);
  ASSERT_EQ(expected_paths.size(), 2);
  EXPECT_TRUE(std::is_permutation(expected_paths.begin(), expected_paths.end(), results.begin()));
}

#ifdef MG_ENTERPRISE
TEST_F(MatchReturnFixture, ScanAllWithAuthChecker) {
  std::string labelName = "l1";
  const auto label = dba.NameToLabel(labelName);

  ASSERT_TRUE(dba.InsertVertex().AddLabel(label).HasValue());
  dba.AdvanceCommand();

  auto test_hypothesis = [&](memgraph::auth::User user, memgraph::storage::View view, int expected_pull_count) {
    auto scan_all = MakeScanAll(storage, symbol_table, "n", nullptr, view);
    ASSERT_EQ(expected_pull_count, PullCountAuthorized(scan_all, user));

    scan_all = MakeScanAll(storage, symbol_table, "n", nullptr, view);
    ASSERT_EQ(expected_pull_count, PullCountAuthorized(scan_all, user));
  };

  {
    auto user = memgraph::auth::User{"grant_global"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

    test_hypothesis(user, memgraph::storage::View::OLD, 1);
    test_hypothesis(user, memgraph::storage::View::NEW, 1);
  }

  {
    auto user = memgraph::auth::User{"deny_global"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::NOTHING);

    test_hypothesis(user, memgraph::storage::View::OLD, 0);
    test_hypothesis(user, memgraph::storage::View::NEW, 0);
  }

  {
    auto user = memgraph::auth::User{"grant_label_read"};
    user.fine_grained_access_handler().label_permissions().Grant(labelName,
                                                                 memgraph::auth::FineGrainedPermission::READ);
    test_hypothesis(user, memgraph::storage::View::OLD, 1);
    test_hypothesis(user, memgraph::storage::View::NEW, 1);
  }

  {
    auto user = memgraph::auth::User{"deny_label_read"};
    user.fine_grained_access_handler().label_permissions().Grant(labelName,
                                                                 memgraph::auth::FineGrainedPermission::NOTHING);

    test_hypothesis(user, memgraph::storage::View::OLD, 0);
    test_hypothesis(user, memgraph::storage::View::NEW, 0);
  }

  {
    auto user = memgraph::auth::User{"grant_global_deny_label"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant(labelName,
                                                                 memgraph::auth::FineGrainedPermission::NOTHING);

    test_hypothesis(user, memgraph::storage::View::OLD, 0);
    test_hypothesis(user, memgraph::storage::View::NEW, 0);
  }

  {
    auto user = memgraph::auth::User{"deny_global_grant_label"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().label_permissions().Grant(labelName,
                                                                 memgraph::auth::FineGrainedPermission::READ);

    test_hypothesis(user, memgraph::storage::View::OLD, 1);
    test_hypothesis(user, memgraph::storage::View::NEW, 1);
  }

  {
    auto user = memgraph::auth::User{"global_update_deny_label"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);
    user.fine_grained_access_handler().label_permissions().Grant(labelName,
                                                                 memgraph::auth::FineGrainedPermission::NOTHING);

    test_hypothesis(user, memgraph::storage::View::OLD, 0);
    test_hypothesis(user, memgraph::storage::View::NEW, 0);
  }

  {
    auto user = memgraph::auth::User{"global_create_delete_deny_label"};
    user.fine_grained_access_handler().label_permissions().Grant("*",
                                                                 memgraph::auth::FineGrainedPermission::CREATE_DELETE);
    user.fine_grained_access_handler().label_permissions().Grant(labelName,
                                                                 memgraph::auth::FineGrainedPermission::NOTHING);

    test_hypothesis(user, memgraph::storage::View::OLD, 0);
    test_hypothesis(user, memgraph::storage::View::NEW, 0);
  }
}
#endif

TEST(QueryPlan, MatchReturnCartesian) {
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

  ASSERT_TRUE(dba.InsertVertex().AddLabel(dba.NameToLabel("l1")).HasValue());
  ASSERT_TRUE(dba.InsertVertex().AddLabel(dba.NameToLabel("l2")).HasValue());
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto m = MakeScanAll(storage, symbol_table, "m", n.op_);
  auto return_n = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto return_m = NEXPR("m", IDENT("m")->MapTo(m.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_2", true));
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
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

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
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

  // add a few nodes to the database
  memgraph::storage::LabelId label = dba.NameToLabel("Label");
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
  ASSERT_TRUE(v1.AddLabel(label).HasValue());
  ASSERT_TRUE(v2.AddLabel(label).HasValue());
  ASSERT_TRUE(v3.AddLabel(label).HasValue());
  // v1 and v4 will have the right properties
  ASSERT_TRUE(v1.SetProperty(property.second, memgraph::storage::PropertyValue(42)).HasValue());
  ASSERT_TRUE(v2.SetProperty(property.second, memgraph::storage::PropertyValue(1)).HasValue());
  ASSERT_TRUE(v4.SetProperty(property.second, memgraph::storage::PropertyValue(42)).HasValue());
  ASSERT_TRUE(v5.SetProperty(property.second, memgraph::storage::PropertyValue(1)).HasValue());
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  // make a scan all
  auto n = MakeScanAll(storage, symbol_table, "n");
  n.node_->labels_.emplace_back(storage.GetLabelIx(dba.LabelToName(label)));
  std::get<0>(n.node_->properties_)[storage.GetPropertyIx(property.first)] = LITERAL(42);

  // node filtering
  auto *filter_expr = AND(storage.Create<LabelsTest>(n.node_->identifier_, n.node_->labels_),
                          EQ(PROPERTY_LOOKUP(n.node_->identifier_, property), LITERAL(42)));
  auto node_filter = std::make_shared<Filter>(n.op_, std::vector<std::shared_ptr<LogicalOperator>>{}, filter_expr);

  // make a named expression and a produce
  auto output = NEXPR("x", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(node_filter, output);

  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*produce, &context));

  //  test that filtering works with old records
  ASSERT_TRUE(v4.AddLabel(label).HasValue());
  EXPECT_EQ(1, PullAll(*produce, &context));
  dba.AdvanceCommand();
  EXPECT_EQ(2, PullAll(*produce, &context));
}

TEST(QueryPlan, NodeFilterMultipleLabels) {
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

  // add a few nodes to the database
  memgraph::storage::LabelId label1 = dba.NameToLabel("label1");
  memgraph::storage::LabelId label2 = dba.NameToLabel("label2");
  memgraph::storage::LabelId label3 = dba.NameToLabel("label3");
  // the test will look for nodes that have label1 and label2
  dba.InsertVertex();                                           // NOT accepted
  ASSERT_TRUE(dba.InsertVertex().AddLabel(label1).HasValue());  // NOT accepted
  ASSERT_TRUE(dba.InsertVertex().AddLabel(label2).HasValue());  // NOT accepted
  ASSERT_TRUE(dba.InsertVertex().AddLabel(label3).HasValue());  // NOT accepted
  auto v1 = dba.InsertVertex();                                 // YES accepted
  ASSERT_TRUE(v1.AddLabel(label1).HasValue());
  ASSERT_TRUE(v1.AddLabel(label2).HasValue());
  auto v2 = dba.InsertVertex();  // NOT accepted
  ASSERT_TRUE(v2.AddLabel(label1).HasValue());
  ASSERT_TRUE(v2.AddLabel(label3).HasValue());
  auto v3 = dba.InsertVertex();  // YES accepted
  ASSERT_TRUE(v3.AddLabel(label1).HasValue());
  ASSERT_TRUE(v3.AddLabel(label2).HasValue());
  ASSERT_TRUE(v3.AddLabel(label3).HasValue());
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  // make a scan all
  auto n = MakeScanAll(storage, symbol_table, "n");
  n.node_->labels_.emplace_back(storage.GetLabelIx(dba.LabelToName(label1)));
  n.node_->labels_.emplace_back(storage.GetLabelIx(dba.LabelToName(label2)));

  // node filtering
  auto *filter_expr = storage.Create<LabelsTest>(n.node_->identifier_, n.node_->labels_);
  auto node_filter = std::make_shared<Filter>(n.op_, std::vector<std::shared_ptr<LogicalOperator>>{}, filter_expr);

  // make a named expression and a produce
  auto output = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(node_filter, output);

  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 2);
}

TEST(QueryPlan, Cartesian) {
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

  auto add_vertex = [&dba](std::string label) {
    auto vertex = dba.InsertVertex();
    MG_ASSERT(vertex.AddLabel(dba.NameToLabel(label)).HasValue());
    return vertex;
  };

  std::vector<memgraph::query::VertexAccessor> vertices{add_vertex("v1"), add_vertex("v2"), add_vertex("v3")};
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto m = MakeScanAll(storage, symbol_table, "m");
  auto return_n = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto return_m = NEXPR("m", IDENT("m")->MapTo(m.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_2", true));

  std::vector<Symbol> left_symbols{n.sym_};
  std::vector<Symbol> right_symbols{m.sym_};
  auto cartesian_op = std::make_shared<Cartesian>(n.op_, left_symbols, m.op_, right_symbols);

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
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto m = MakeScanAll(storage, symbol_table, "m");
  auto return_n = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto return_m = NEXPR("m", IDENT("m")->MapTo(m.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_2", true));

  std::vector<Symbol> left_symbols{n.sym_};
  std::vector<Symbol> right_symbols{m.sym_};
  auto cartesian_op = std::make_shared<Cartesian>(n.op_, left_symbols, m.op_, right_symbols);

  auto produce = MakeProduce(cartesian_op, return_n, return_m);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 0);
}

TEST(QueryPlan, CartesianThreeWay) {
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);
  auto add_vertex = [&dba](std::string label) {
    auto vertex = dba.InsertVertex();
    MG_ASSERT(vertex.AddLabel(dba.NameToLabel(label)).HasValue());
    return vertex;
  };

  std::vector<memgraph::query::VertexAccessor> vertices{add_vertex("v1"), add_vertex("v2"), add_vertex("v3")};
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto m = MakeScanAll(storage, symbol_table, "m");
  auto l = MakeScanAll(storage, symbol_table, "l");
  auto return_n = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto return_m = NEXPR("m", IDENT("m")->MapTo(m.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_2", true));
  auto return_l = NEXPR("l", IDENT("l")->MapTo(l.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_3", true));

  std::vector<Symbol> n_symbols{n.sym_};
  std::vector<Symbol> m_symbols{m.sym_};
  std::vector<Symbol> n_m_symbols{n.sym_, m.sym_};
  std::vector<Symbol> l_symbols{l.sym_};
  auto cartesian_op_1 = std::make_shared<Cartesian>(n.op_, n_symbols, m.op_, m_symbols);

  auto cartesian_op_2 = std::make_shared<Cartesian>(cartesian_op_1, n_m_symbols, l.op_, l_symbols);

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
  memgraph::storage::Storage db;
  memgraph::storage::Storage::Accessor storage_dba{db.Access()};
  memgraph::query::DbAccessor dba{&storage_dba};
  AstStorage storage;
  SymbolTable symbol_table;

  // make a V-graph (v3)<-[r2]-(v1)-[r1]->(v2)
  memgraph::query::VertexAccessor v1{dba.InsertVertex()};
  memgraph::query::VertexAccessor v2{dba.InsertVertex()};
  memgraph::query::VertexAccessor v3{dba.InsertVertex()};
  memgraph::storage::EdgeTypeId edge_type{db.NameToEdgeType("Edge")};
  memgraph::query::EdgeAccessor r1{*dba.InsertEdge(&v1, &v2, edge_type)};
  memgraph::query::EdgeAccessor r2{*dba.InsertEdge(&v1, &v3, edge_type)};

  void SetUp() override {
    ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("l1")).HasValue());
    ASSERT_TRUE(v2.AddLabel(dba.NameToLabel("l2")).HasValue());
    ASSERT_TRUE(v3.AddLabel(dba.NameToLabel("l3")).HasValue());
    memgraph::license::global_license_checker.EnableTesting();

    dba.AdvanceCommand();
  }
};

TEST_F(ExpandFixture, Expand) {
  auto test_expand = [&](EdgeAtom::Direction direction, memgraph::storage::View view) {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", direction, {}, "m", false, view);

    // make a named expression and a produce
    auto output =
        NEXPR("m", IDENT("m")->MapTo(r_m.node_sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(r_m.op_, output);
    auto context = MakeContext(storage, symbol_table, &dba);
    return PullAll(*produce, &context);
  };

  // test that expand works well for both old and new graph state
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, edge_type).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v1, &v3, edge_type).HasValue());
  EXPECT_EQ(2, test_expand(EdgeAtom::Direction::OUT, memgraph::storage::View::OLD));
  EXPECT_EQ(2, test_expand(EdgeAtom::Direction::IN, memgraph::storage::View::OLD));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::BOTH, memgraph::storage::View::OLD));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::OUT, memgraph::storage::View::NEW));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::IN, memgraph::storage::View::NEW));
  EXPECT_EQ(8, test_expand(EdgeAtom::Direction::BOTH, memgraph::storage::View::NEW));
  dba.AdvanceCommand();
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::OUT, memgraph::storage::View::OLD));
  EXPECT_EQ(4, test_expand(EdgeAtom::Direction::IN, memgraph::storage::View::OLD));
  EXPECT_EQ(8, test_expand(EdgeAtom::Direction::BOTH, memgraph::storage::View::OLD));
}

#ifdef MG_ENTERPRISE
TEST_F(ExpandFixture, ExpandWithEdgeFiltering) {
  auto test_expand = [&](memgraph::auth::User user, EdgeAtom::Direction direction, memgraph::storage::View view) {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", direction, {}, "m", false, view);

    // make a named expression and a produce
    auto output =
        NEXPR("m", IDENT("m")->MapTo(r_m.node_sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(r_m.op_, output);
    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(storage, symbol_table, &dba, &auth_checker);
    return PullAll(*produce, &context);
  };

  auto user = memgraph::auth::User("test");

  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "Edge", memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_test",
                                                                   memgraph::auth::FineGrainedPermission::NOTHING);
  user.fine_grained_access_handler().label_permissions().Grant("*",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  memgraph::storage::EdgeTypeId edge_type_test{db.NameToEdgeType("edge_type_test")};

  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, edge_type_test).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v1, &v3, edge_type_test).HasValue());
  // test that expand works well for both old and new graph state
  EXPECT_EQ(2, test_expand(user, EdgeAtom::Direction::OUT, memgraph::storage::View::OLD));
  EXPECT_EQ(2, test_expand(user, EdgeAtom::Direction::IN, memgraph::storage::View::OLD));
  EXPECT_EQ(4, test_expand(user, EdgeAtom::Direction::BOTH, memgraph::storage::View::OLD));
  EXPECT_EQ(2, test_expand(user, EdgeAtom::Direction::OUT, memgraph::storage::View::NEW));
  EXPECT_EQ(2, test_expand(user, EdgeAtom::Direction::IN, memgraph::storage::View::NEW));
  EXPECT_EQ(4, test_expand(user, EdgeAtom::Direction::BOTH, memgraph::storage::View::NEW));

  dba.AdvanceCommand();

  EXPECT_EQ(2, test_expand(user, EdgeAtom::Direction::OUT, memgraph::storage::View::OLD));
  EXPECT_EQ(2, test_expand(user, EdgeAtom::Direction::IN, memgraph::storage::View::OLD));
  EXPECT_EQ(4, test_expand(user, EdgeAtom::Direction::BOTH, memgraph::storage::View::OLD));

  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "edge_type_test", memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  EXPECT_EQ(4, test_expand(user, EdgeAtom::Direction::OUT, memgraph::storage::View::OLD));
  EXPECT_EQ(4, test_expand(user, EdgeAtom::Direction::IN, memgraph::storage::View::OLD));
  EXPECT_EQ(8, test_expand(user, EdgeAtom::Direction::BOTH, memgraph::storage::View::OLD));
  EXPECT_EQ(4, test_expand(user, EdgeAtom::Direction::OUT, memgraph::storage::View::NEW));
  EXPECT_EQ(4, test_expand(user, EdgeAtom::Direction::IN, memgraph::storage::View::NEW));
  EXPECT_EQ(8, test_expand(user, EdgeAtom::Direction::BOTH, memgraph::storage::View::NEW));

  EXPECT_EQ(4, test_expand(user, EdgeAtom::Direction::OUT, memgraph::storage::View::OLD));
  EXPECT_EQ(4, test_expand(user, EdgeAtom::Direction::IN, memgraph::storage::View::OLD));
  EXPECT_EQ(8, test_expand(user, EdgeAtom::Direction::BOTH, memgraph::storage::View::OLD));
}
#endif

TEST_F(ExpandFixture, ExpandPath) {
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::OUT, {}, "m", false,
                        memgraph::storage::View::OLD);
  Symbol path_sym = symbol_table.CreateSymbol("path", true);
  auto path = std::make_shared<ConstructNamedPath>(r_m.op_, path_sym,
                                                   std::vector<Symbol>{n.sym_, r_m.edge_sym_, r_m.node_sym_});
  auto output =
      NEXPR("path", IDENT("path")->MapTo(path_sym))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(path, output);

  std::vector<memgraph::query::Path> expected_paths{memgraph::query::Path(v1, r2, v3),
                                                    memgraph::query::Path(v1, r1, v2)};
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), 2);
  std::vector<memgraph::query::Path> results_paths;
  for (const auto &result : results) results_paths.emplace_back(result[0].ValuePath());
  EXPECT_TRUE(std::is_permutation(expected_paths.begin(), expected_paths.end(), results_paths.begin()));
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

  memgraph::storage::Storage db;
  memgraph::storage::Storage::Accessor storage_dba{db.Access()};
  memgraph::query::DbAccessor dba{&storage_dba};
  // labels for layers in the double chain
  std::vector<memgraph::storage::LabelId> labels;

  AstStorage storage;
  SymbolTable symbol_table;

  // using std::nullopt
  std::nullopt_t nullopt = std::nullopt;

  void SetUp() {
    memgraph::license::global_license_checker.EnableTesting();

    // create the graph
    int chain_length = 3;
    std::vector<memgraph::query::VertexAccessor> layer;
    for (int from_layer_ind = -1; from_layer_ind < chain_length - 1; from_layer_ind++) {
      std::vector<memgraph::query::VertexAccessor> new_layer{dba.InsertVertex(), dba.InsertVertex()};
      auto label = dba.NameToLabel(std::to_string(from_layer_ind + 1));
      labels.push_back(label);
      for (size_t v_to_ind = 0; v_to_ind < new_layer.size(); v_to_ind++) {
        auto &v_to = new_layer[v_to_ind];
        ASSERT_TRUE(v_to.AddLabel(label).HasValue());
        for (size_t v_from_ind = 0; v_from_ind < layer.size(); v_from_ind++) {
          auto &v_from = layer[v_from_ind];
          auto edge_type = "edge_type_" + std::to_string(from_layer_ind + 1);
          memgraph::storage::EdgeTypeId edge_type_id = dba.NameToEdgeType(edge_type);

          auto edge = dba.InsertEdge(&v_from, &v_to, edge_type_id);
          ASSERT_TRUE(edge->SetProperty(dba.NameToProperty("p"),
                                        memgraph::storage::PropertyValue(fmt::format(
                                            "V{}{}->V{}{}", from_layer_ind, v_from_ind, from_layer_ind + 1, v_to_ind)))
                          .HasValue());
        }
      }
      layer = new_layer;
    }
    dba.AdvanceCommand();
    ASSERT_EQ(CountIterable(dba.Vertices(memgraph::storage::View::OLD)), 2 * chain_length);
    ASSERT_EQ(CountEdges(&dba, memgraph::storage::View::OLD), 4 * (chain_length - 1));
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
  std::shared_ptr<LogicalOperator> AddMatch(std::shared_ptr<LogicalOperator> input_op, const std::string &node_from,
                                            int layer, EdgeAtom::Direction direction,
                                            const std::vector<memgraph::storage::EdgeTypeId> &edge_types,
                                            std::optional<size_t> lower, std::optional<size_t> upper, Symbol edge_sym,
                                            const std::string &node_to, memgraph::storage::View view,
                                            bool is_reverse = false) {
    auto n_from = MakeScanAll(storage, symbol_table, node_from, input_op);
    auto filter_op = std::make_shared<Filter>(
        n_from.op_, std::vector<std::shared_ptr<LogicalOperator>>{},
        storage.Create<memgraph::query::LabelsTest>(
            n_from.node_->identifier_, std::vector<LabelIx>{storage.GetLabelIx(dba.LabelToName(labels[layer]))}));

    auto n_to = NODE(node_to);
    auto n_to_sym = symbol_table.CreateSymbol(node_to, true);
    n_to->identifier_->MapTo(n_to_sym);

    if (std::is_same<TExpansionOperator, ExpandVariable>::value) {
      // convert optional ints to optional expressions
      auto convert = [this](std::optional<size_t> bound) {
        return bound ? LITERAL(static_cast<int64_t>(bound.value())) : nullptr;
      };
      MG_ASSERT(view == memgraph::storage::View::OLD,
                "ExpandVariable should only be planned with memgraph::storage::View::OLD");

      return std::make_shared<ExpandVariable>(filter_op, n_from.sym_, n_to_sym, edge_sym, EdgeAtom::Type::DEPTH_FIRST,
                                              direction, edge_types, is_reverse, convert(lower), convert(upper), false,
                                              ExpansionLambda{symbol_table.CreateSymbol("inner_edge", false),
                                                              symbol_table.CreateSymbol("inner_node", false), nullptr},
                                              std::nullopt, std::nullopt);
    } else
      return std::make_shared<Expand>(filter_op, n_from.sym_, n_to_sym, edge_sym, direction, edge_types, false, view);
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
  auto GetListResults(std::shared_ptr<LogicalOperator> input_op, Symbol symbol, memgraph::auth::User *user = nullptr) {
    Frame frame(symbol_table.max_position());
    auto cursor = input_op->MakeCursor(memgraph::utils::NewDeleteResource());
    ExecutionContext context;
    if (user) {
#ifdef MG_ENTERPRISE
      memgraph::glue::FineGrainedAuthChecker auth_checker{*user, &dba};
      context = MakeContextWithFineGrainedChecker(storage, symbol_table, &dba, &auth_checker);
#endif
    } else {
      context = MakeContext(storage, symbol_table, &dba);
    }
    std::vector<memgraph::utils::pmr::vector<TypedValue>> results;
    while (cursor->Pull(frame, context)) results.emplace_back(frame[symbol].ValueList());
    return results;
  }

  /**
   * Pulls from the given input and returns the results under the given symbol.
   */
  auto GetPathResults(std::shared_ptr<LogicalOperator> input_op, Symbol symbol, memgraph::auth::User *user = nullptr) {
    Frame frame(symbol_table.max_position());
    auto cursor = input_op->MakeCursor(memgraph::utils::NewDeleteResource());
    ExecutionContext context;
    if (user) {
#ifdef MG_ENTERPRISE
      memgraph::glue::FineGrainedAuthChecker auth_checker{*user, &dba};
      context = MakeContextWithFineGrainedChecker(storage, symbol_table, &dba, &auth_checker);
#endif
    } else {
      context = MakeContext(storage, symbol_table, &dba);
    }
    std::vector<Path> results;
    while (cursor->Pull(frame, context)) results.emplace_back(frame[symbol].ValuePath());
    return results;
  }

  /**
   * Pulls from the given input and analyses the edge-list (result of variable
   * length expansion) found in the results under the given symbol.
   *
   * @return a map {edge_list_length -> number_of_results}
   */
  auto GetEdgeListSizes(std::shared_ptr<LogicalOperator> input_op, Symbol symbol,
                        memgraph::auth::User *user = nullptr) {
    map_int count_per_length;
    for (const auto &edge_list : GetListResults(input_op, symbol, user)) {
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
  auto test_expand = [&](int layer, EdgeAtom::Direction direction, std::optional<size_t> lower,
                         std::optional<size_t> upper, bool reverse) {
    auto e = Edge("r", direction);
    return GetEdgeListSizes(AddMatch<ExpandVariable>(nullptr, "n", layer, direction, {}, lower, upper, e, "m",
                                                     memgraph::storage::View::OLD, reverse),
                            e);
  };

  for (int reverse = 0; reverse < 2; ++reverse) {
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 0, 0, reverse), (map_int{{0, 2}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, 0, reverse), (map_int{{0, 2}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 0, 0, reverse), (map_int{{0, 2}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 1, 1, reverse), (map_int{}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, 1, reverse), (map_int{{1, 4}}));
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 1, 1, reverse), (map_int{{1, 4}}));
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 1, 1, reverse), (map_int{{1, 4}}));
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 1, 1, reverse), (map_int{{1, 8}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2, reverse), (map_int{{2, 8}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 3, reverse), (map_int{{2, 8}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, 2, reverse), (map_int{{1, 4}, {2, 8}}));

    // the following tests also check edge-uniqueness (cyphermorphism)
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 1, 2, reverse), (map_int{{1, 4}, {2, 12}}));
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, 4, reverse), (map_int{{4, 24}}));

    // default bound values (lower default is 1, upper default is inf)
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, nullopt, 0, reverse), (map_int{}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, nullopt, 1, reverse), (map_int{{1, 4}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, nullopt, 2, reverse), (map_int{{1, 4}, {2, 8}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 7, nullopt, reverse), (map_int{{7, 24}, {8, 24}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 8, nullopt, reverse), (map_int{{8, 24}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 9, nullopt, reverse), (map_int{}));
  }
}

#ifdef MG_ENTERPRISE
TEST_F(QueryPlanExpandVariable, FineGrainedOneVariableExpansion) {
  auto test_expand = [&](int layer, EdgeAtom::Direction direction, std::optional<size_t> lower,
                         std::optional<size_t> upper, bool reverse, memgraph::auth::User &user) {
    auto e = Edge("r", direction);
    return GetEdgeListSizes(AddMatch<ExpandVariable>(nullptr, "n", layer, direction, {}, lower, upper, e, "m",
                                                     memgraph::storage::View::OLD, reverse),
                            e, &user);
  };

  // All labels, All edge types granted
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

    for (auto reverse : {false, true}) {
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 0, 0, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, 0, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 0, 0, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 1, 1, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, 1, reverse, user), (map_int{{1, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 1, 1, reverse, user), (map_int{{1, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 1, 1, reverse, user), (map_int{{1, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 1, 1, reverse, user), (map_int{{1, 8}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2, reverse, user), (map_int{{2, 8}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 3, reverse, user), (map_int{{2, 8}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, 2, reverse, user), (map_int{{1, 4}, {2, 8}}));

      // the following tests also check edge-uniqueness (cyphermorphism)
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 1, 2, reverse, user), (map_int{{1, 4}, {2, 12}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, 4, reverse, user), (map_int{{4, 24}}));

      // default bound values (lower default is 1, upper default is inf)
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, nullopt, 0, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, nullopt, 1, reverse, user), (map_int{{1, 4}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, nullopt, 2, reverse, user), (map_int{{1, 4}, {2, 8}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 7, nullopt, reverse, user), (map_int{{7, 24}, {8, 24}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 8, nullopt, reverse, user), (map_int{{8, 24}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 9, nullopt, reverse, user), (map_int{}));
    }
  }

  // All labels, All edge types denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::NOTHING);
    for (auto reverse : {false, true}) {
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 1, nullopt, reverse, user), (map_int{}));
    }
  }

  // All labels granted, All edge types denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    for (auto reverse : {false, true}) {
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 1, nullopt, reverse, user), (map_int{}));
    }
  }

  // All labels denied, All edge types granted
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::NOTHING);
    for (auto reverse : {false, true}) {
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 1, nullopt, reverse, user), (map_int{}));
    }
  }

  // Layer 1 labels denied, All edge types granted
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("0", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("2", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("1", memgraph::auth::FineGrainedPermission::NOTHING);

    for (auto reverse : {false, true}) {
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 0, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 0, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 0, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 2, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 2, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 2, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 1, nullopt, reverse, user), (map_int{}));
    }
  }

  // All labels granted, Edge types from layer 0 to layer 1 denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_1",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_2",
                                                                     memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

    for (auto reverse : {false, true}) {
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 0, nullopt, reverse, user), (map_int{{1, 4}, {0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 0, nullopt, reverse, user),
                (map_int{{4, 4}, {3, 4}, {2, 4}, {1, 4}, {0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 1, nullopt, reverse, user), (map_int{{1, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 1, nullopt, reverse, user),
                (map_int{{1, 4}, {2, 4}, {3, 4}, {4, 4}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 1, nullopt, reverse, user), (map_int{}));
    }
  }

  // Layer 2 labels denied, All edge types granted
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("0", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("1", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("2", memgraph::auth::FineGrainedPermission::NOTHING);

    for (auto reverse : {false, true}) {
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, nullopt, reverse, user), (map_int{{0, 2}, {1, 4}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 0, nullopt, reverse, user),
                (map_int{{4, 4}, {3, 4}, {2, 4}, {1, 4}, {0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 0, nullopt, reverse, user), (map_int{{1, 4}, {0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 0, nullopt, reverse, user),
                (map_int{{4, 4}, {3, 4}, {2, 4}, {1, 4}, {0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 1, nullopt, reverse, user), (map_int{{1, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 1, nullopt, reverse, user),
                (map_int{{1, 4}, {2, 4}, {3, 4}, {4, 4}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, nullopt, reverse, user), (map_int{{1, 4}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 1, nullopt, reverse, user),
                (map_int{{4, 4}, {3, 4}, {2, 4}, {1, 4}}));
    }
  }

  // All labels granted, Edge types from layer 1 to layer 2 denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_1",
                                                                     memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_2",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);

    for (auto reverse : {false, true}) {
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, nullopt, reverse, user), (map_int{{1, 4}, {0, 2}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 0, nullopt, reverse, user),
                (map_int{{4, 4}, {3, 4}, {2, 4}, {1, 4}, {0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 0, nullopt, reverse, user), (map_int{{1, 4}, {0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 0, nullopt, reverse, user), (map_int{{0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 0, nullopt, reverse, user),
                (map_int{{4, 4}, {3, 4}, {2, 4}, {1, 4}, {0, 2}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 1, nullopt, reverse, user), (map_int{{1, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 1, nullopt, reverse, user),
                (map_int{{1, 4}, {2, 4}, {3, 4}, {4, 4}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, 1, nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 1, nullopt, reverse, user), (map_int{{1, 4}}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, 1, nullopt, reverse, user),
                (map_int{{4, 4}, {3, 4}, {2, 4}, {1, 4}}));
    }
  }
}
#endif

TEST_F(QueryPlanExpandVariable, EdgeUniquenessSingleAndVariableExpansion) {
  auto test_expand = [&](int layer, EdgeAtom::Direction direction, std::optional<size_t> lower,
                         std::optional<size_t> upper, bool single_expansion_before, bool add_uniqueness_check) {
    std::shared_ptr<LogicalOperator> last_op{nullptr};
    std::vector<Symbol> symbols;

    if (single_expansion_before) {
      symbols.push_back(Edge("r0", direction));
      last_op = AddMatch<Expand>(last_op, "n0", layer, direction, {}, lower, upper, symbols.back(), "m0",
                                 memgraph::storage::View::OLD);
    }

    auto var_length_sym = Edge("r1", direction);
    symbols.push_back(var_length_sym);
    last_op = AddMatch<ExpandVariable>(last_op, "n1", layer, direction, {}, lower, upper, var_length_sym, "m1",
                                       memgraph::storage::View::OLD);

    if (!single_expansion_before) {
      symbols.push_back(Edge("r2", direction));
      last_op = AddMatch<Expand>(last_op, "n2", layer, direction, {}, lower, upper, symbols.back(), "m2",
                                 memgraph::storage::View::OLD);
    }

    if (add_uniqueness_check) {
      auto last_symbol = symbols.back();
      symbols.pop_back();
      last_op = std::make_shared<EdgeUniquenessFilter>(last_op, last_symbol, symbols);
    }

    return GetEdgeListSizes(last_op, var_length_sym);
  };

  // no uniqueness between variable and single expansion
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 3, true, false), (map_int{{2, 4 * 8}}));
  // with uniqueness test, different ordering of (variable, single) expansion
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 3, true, true), (map_int{{2, 3 * 8}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 3, false, true), (map_int{{2, 3 * 8}}));
}

TEST_F(QueryPlanExpandVariable, EdgeUniquenessTwoVariableExpansions) {
  auto test_expand = [&](int layer, EdgeAtom::Direction direction, std::optional<size_t> lower,
                         std::optional<size_t> upper, bool add_uniqueness_check) {
    auto e1 = Edge("r1", direction);
    auto first = AddMatch<ExpandVariable>(nullptr, "n1", layer, direction, {}, lower, upper, e1, "m1",
                                          memgraph::storage::View::OLD);
    auto e2 = Edge("r2", direction);
    auto last_op = AddMatch<ExpandVariable>(first, "n2", layer, direction, {}, lower, upper, e2, "m2",
                                            memgraph::storage::View::OLD);
    if (add_uniqueness_check) {
      last_op = std::make_shared<EdgeUniquenessFilter>(last_op, e2, std::vector<Symbol>{e1});
    }

    return GetEdgeListSizes(last_op, e2);
  };

  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2, false), (map_int{{2, 8 * 8}}));
  EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2, true), (map_int{{2, 5 * 8}}));
}

#ifdef MG_ENTERPRISE
TEST_F(QueryPlanExpandVariable, FineGrainedEdgeUniquenessTwoVariableExpansions) {
  auto test_expand = [&](int layer, EdgeAtom::Direction direction, std::optional<size_t> lower,
                         std::optional<size_t> upper, bool add_uniqueness_check, memgraph::auth::User &user) {
    auto e1 = Edge("r1", direction);
    auto first = AddMatch<ExpandVariable>(nullptr, "n1", layer, direction, {}, lower, upper, e1, "m1",
                                          memgraph::storage::View::OLD);
    auto e2 = Edge("r2", direction);
    auto last_op = AddMatch<ExpandVariable>(first, "n2", layer, direction, {}, lower, upper, e2, "m2",
                                            memgraph::storage::View::OLD);
    if (add_uniqueness_check) {
      last_op = std::make_shared<EdgeUniquenessFilter>(last_op, e2, std::vector<Symbol>{e1});
    }

    return GetEdgeListSizes(last_op, e2, &user);
  };

  // All labels granted, All edge types granted
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2, false, user), (map_int{{2, 8 * 8}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2, true, user), (map_int{{2, 5 * 8}}));
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 0, 2, true, user), (map_int{{1, 20}, {0, 12}}));
  }

  // All labels denied, All edge types denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::NOTHING);

    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2, false, user), (map_int{}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 2, 2, true, user), (map_int{}));
  }
  // Layer 1 label denied, All edge types denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("0", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("2", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("1", memgraph::auth::FineGrainedPermission::NOTHING);

    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, 2, false, user), (map_int{{0, 4}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, 2, true, user), (map_int{{0, 4}}));
  }

  // Layer 2 label denied, All edge types denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("0", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("1", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("2", memgraph::auth::FineGrainedPermission::NOTHING);

    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, 2, false, user), (map_int{{1, 4}, {0, 2}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, 2, true, user), (map_int{{1, 4}, {0, 2}}));
  }

  // All labels granted, Edge type between layer 0 and layer 1 denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_1",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_2",
                                                                     memgraph::auth::FineGrainedPermission::READ);

    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, 2, false, user), (map_int{{0, 4}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, 2, true, user), (map_int{{0, 4}}));
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 0, 2, false, user), (map_int{{1, 24}, {0, 12}}));
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 0, 2, true, user), (map_int{{1, 20}, {0, 12}}));
  }

  // All labels granted, Edge type between layer 1 and layer 2 denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_1",
                                                                     memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_2",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);

    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, 2, false, user), (map_int{{1, 24}, {0, 12}}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, 0, 2, true, user), (map_int{{1, 20}, {0, 12}}));
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 0, 2, false, user), (map_int{{0, 4}}));
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 0, 2, true, user), (map_int{{0, 4}}));
  }
}
#endif

TEST_F(QueryPlanExpandVariable, NamedPath) {
  auto e = Edge("r", EdgeAtom::Direction::OUT);
  auto expand = AddMatch<ExpandVariable>(nullptr, "n", 0, EdgeAtom::Direction::OUT, {}, 2, 2, e, "m",
                                         memgraph::storage::View::OLD);
  auto find_symbol = [this](const std::string &name) {
    for (const auto &sym : symbol_table.table())
      if (sym.second.name() == name) return sym.second;
    throw std::runtime_error("Symbol not found");
  };

  auto path_symbol = symbol_table.CreateSymbol("path", true, Symbol::Type::PATH);
  auto create_path = std::make_shared<ConstructNamedPath>(expand, path_symbol,
                                                          std::vector<Symbol>{find_symbol("n"), e, find_symbol("m")});

  std::vector<memgraph::query::Path> expected_paths;
  for (const auto &v : dba.Vertices(memgraph::storage::View::OLD)) {
    if (!*v.HasLabel(memgraph::storage::View::OLD, labels[0])) continue;
    auto maybe_edges1 = v.OutEdges(memgraph::storage::View::OLD);
    for (const auto &e1 : *maybe_edges1) {
      auto maybe_edges2 = e1.To().OutEdges(memgraph::storage::View::OLD);
      for (const auto &e2 : *maybe_edges2) {
        expected_paths.emplace_back(v, e1, e1.To(), e2, e2.To());
      }
    }
  }
  ASSERT_EQ(expected_paths.size(), 8);

  auto results = GetPathResults(create_path, path_symbol);
  ASSERT_EQ(results.size(), 8);
  EXPECT_TRUE(std::is_permutation(results.begin(), results.end(), expected_paths.begin()));
}

#ifdef MG_ENTERPRISE
TEST_F(QueryPlanExpandVariable, FineGrainedFilterNamedPath) {
  auto e = Edge("r", EdgeAtom::Direction::OUT);
  auto expand = AddMatch<ExpandVariable>(nullptr, "n", 0, EdgeAtom::Direction::OUT, {}, 0, 2, e, "m",
                                         memgraph::storage::View::OLD);
  auto find_symbol = [this](const std::string &name) {
    for (const auto &sym : symbol_table.table())
      if (sym.second.name() == name) return sym.second;
    throw std::runtime_error("Symbol not found");
  };

  auto path_symbol = symbol_table.CreateSymbol("path", true, Symbol::Type::PATH);
  auto create_path = std::make_shared<ConstructNamedPath>(expand, path_symbol,
                                                          std::vector<Symbol>{find_symbol("n"), e, find_symbol("m")});

  // All labels and edge types granted
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    auto results = GetPathResults(create_path, path_symbol, &user);
    ASSERT_EQ(results.size(), 14);
  }

  // All labels and edge types denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::NOTHING);

    auto results = GetPathResults(create_path, path_symbol, &user);
    ASSERT_EQ(results.size(), 0);
  }

  // All labels denied, All edge types granted
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::NOTHING);

    auto results = GetPathResults(create_path, path_symbol, &user);
    ASSERT_EQ(results.size(), 0);
  }

  // All labels granted, All edge types denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

    auto results = GetPathResults(create_path, path_symbol, &user);
    ASSERT_EQ(results.size(), 2);
  }

  // 0 layer label denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("0", memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().label_permissions().Grant("1", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("2", memgraph::auth::FineGrainedPermission::READ);

    auto results = GetPathResults(create_path, path_symbol, &user);
    ASSERT_EQ(results.size(), 0);
  }

  // First layer label denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("0", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("1", memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().label_permissions().Grant("2", memgraph::auth::FineGrainedPermission::READ);

    auto results = GetPathResults(create_path, path_symbol, &user);
    ASSERT_EQ(results.size(), 2);
  }

  // Second layer label denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("0", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("1", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("2", memgraph::auth::FineGrainedPermission::NOTHING);

    auto results = GetPathResults(create_path, path_symbol, &user);
    ASSERT_EQ(results.size(), 6);

    std::vector<memgraph::query::Path> expected_paths;
    for (const auto &v : dba.Vertices(memgraph::storage::View::OLD)) {
      if (!*v.HasLabel(memgraph::storage::View::OLD, labels[0])) continue;
      expected_paths.emplace_back(v);
      auto maybe_edges1 = v.OutEdges(memgraph::storage::View::OLD);
      for (const auto &e1 : *maybe_edges1) {
        expected_paths.emplace_back(v, e1, e1.To());
      }
    }
    EXPECT_TRUE(std::is_permutation(results.begin(), results.end(), expected_paths.begin()));
  }

  // First layer edge type denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_1",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_2",
                                                                     memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

    auto results = GetPathResults(create_path, path_symbol, &user);
    ASSERT_EQ(results.size(), 2);
  }

  // Second layer edge type denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_1",
                                                                     memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_2",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

    auto results = GetPathResults(create_path, path_symbol, &user);
    ASSERT_EQ(results.size(), 6);

    std::vector<memgraph::query::Path> expected_paths;
    for (const auto &v : dba.Vertices(memgraph::storage::View::OLD)) {
      if (!*v.HasLabel(memgraph::storage::View::OLD, labels[0])) continue;
      expected_paths.emplace_back(v);
      auto maybe_edges1 = v.OutEdges(memgraph::storage::View::OLD);
      for (const auto &e1 : *maybe_edges1) {
        expected_paths.emplace_back(v, e1, e1.To());
      }
    }

    EXPECT_TRUE(std::is_permutation(results.begin(), results.end(), expected_paths.begin()));
  }
}
#endif

TEST_F(QueryPlanExpandVariable, ExpandToSameSymbol) {
  auto test_expand = [&](int layer, EdgeAtom::Direction direction, std::optional<size_t> lower,
                         std::optional<size_t> upper, bool reverse) {
    auto e = Edge("r", direction);

    auto node = NODE("n");
    auto symbol = symbol_table.CreateSymbol("n", true);
    node->identifier_->MapTo(symbol);
    auto logical_op = std::make_shared<ScanAll>(nullptr, symbol, memgraph::storage::View::OLD);
    auto n_from = ScanAllTuple{node, logical_op, symbol};

    auto filter_op = std::make_shared<Filter>(
        n_from.op_, std::vector<std::shared_ptr<LogicalOperator>>{},
        storage.Create<memgraph::query::LabelsTest>(
            n_from.node_->identifier_, std::vector<LabelIx>{storage.GetLabelIx(dba.LabelToName(labels[layer]))}));

    // convert optional ints to optional expressions
    auto convert = [this](std::optional<size_t> bound) {
      return bound ? LITERAL(static_cast<int64_t>(bound.value())) : nullptr;
    };

    return GetEdgeListSizes(std::make_shared<ExpandVariable>(
                                filter_op, symbol, symbol, e, EdgeAtom::Type::DEPTH_FIRST, direction,
                                std::vector<memgraph::storage::EdgeTypeId>{}, reverse, convert(lower), convert(upper),
                                /* existing = */ true,
                                ExpansionLambda{symbol_table.CreateSymbol("inner_edge", false),
                                                symbol_table.CreateSymbol("inner_node", false), nullptr},
                                std::nullopt, std::nullopt),
                            e);
  };

  // The graph is a double chain:
  // chain 0:   (v:0)-(v:1)-(v:2)
  //                 X     X
  // chain 1:   (v:0)-(v:1)-(v:2)

  // Expand from chain 0 v:0 to itself.
  //
  // It has a total of 3 cycles:
  // 1. C0 v:0 -> C0 v:1 -> C1 v:2 -> C1 v:1 -> C0 v:0
  // 2. C0 v:0 -> C0 v:1 -> C0 v:2 -> C1 v:1 -> C0 v:0
  // 3. C0 v:0 -> C0 v:1 -> C1 v:0 -> C1 v:1 -> C0 v:0
  //
  // Each cycle can be in two directions, also, we have two starting nodes: one
  // in chain 0 and the other in chain 1.
  for (auto reverse : {false, true}) {
    // Tests with both bounds set.
    for (int lower_bound = 0; lower_bound < 10; ++lower_bound) {
      for (int upper_bound = lower_bound; upper_bound < 10; ++upper_bound) {
        map_int expected_directed;
        map_int expected_undirected;
        if (lower_bound == 0) {
          expected_directed.emplace(0, 2);
          expected_undirected.emplace(0, 2);
        }
        if (lower_bound <= 4 && upper_bound >= 4) {
          expected_undirected.emplace(4, 12);
        }
        if (lower_bound <= 8 && upper_bound >= 8) {
          expected_undirected.emplace(8, 24);
        }

        EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, lower_bound, upper_bound, reverse), expected_directed);
        EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, lower_bound, upper_bound, reverse), expected_directed);
        EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, lower_bound, upper_bound, reverse), expected_undirected);
      }
    }

    // Test only upper bound.
    for (int upper_bound = 0; upper_bound < 10; ++upper_bound) {
      map_int expected_directed;
      map_int expected_undirected;
      if (upper_bound >= 4) {
        expected_undirected.emplace(4, 12);
      }
      if (upper_bound >= 8) {
        expected_undirected.emplace(8, 24);
      }

      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, std::nullopt, upper_bound, reverse), expected_directed);
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, std::nullopt, upper_bound, reverse), expected_directed);
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, std::nullopt, upper_bound, reverse), expected_undirected);
    }

    // Test only lower bound.
    for (int lower_bound = 0; lower_bound < 10; ++lower_bound) {
      map_int expected_directed;
      map_int expected_undirected;
      if (lower_bound == 0) {
        expected_directed.emplace(0, 2);
        expected_undirected.emplace(0, 2);
      }
      if (lower_bound <= 4) {
        expected_undirected.emplace(4, 12);
      }
      if (lower_bound <= 8) {
        expected_undirected.emplace(8, 24);
      }

      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, lower_bound, std::nullopt, reverse), expected_directed);
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, lower_bound, std::nullopt, reverse), expected_directed);
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, lower_bound, std::nullopt, reverse), expected_undirected);
    }

    // Test no bounds.
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, std::nullopt, std::nullopt, reverse), (map_int{}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, std::nullopt, std::nullopt, reverse), (map_int{}));
    EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, std::nullopt, std::nullopt, reverse),
              (map_int{{4, 12}, {8, 24}}));
  }

  // Expand from chain 0 v:1 to itself.
  //
  // It has a total of 6 cycles:
  // 1. C0 v:1 -> C1 v:0 -> C1 v:1 -> C1 v:2 -> C0 v:1
  // 2. C0 v:1 -> C1 v:0 -> C1 v:1 -> C0 v:2 -> C0 v:1
  // 3. C0 v:1 -> C0 v:0 -> C1 v:1 -> C1 v:2 -> C0 v:1
  // 4. C0 v:1 -> C0 v:0 -> C1 v:1 -> C0 v:2 -> C0 v:1
  // 5. C0 v:1 -> C1 v:0 -> C1 v:1 -> C0 v:0 -> C0 v:1
  // 6. C0 v:1 -> C1 v:2 -> C1 v:1 -> C0 v:2 -> C0 v:1
  //
  // Each cycle can be in two directions, also, we have two starting nodes: one
  // in chain 0 and the other in chain 1.
  for (auto reverse : {false, true}) {
    // Tests with both bounds set.
    for (int lower_bound = 0; lower_bound < 10; ++lower_bound) {
      for (int upper_bound = lower_bound; upper_bound < 10; ++upper_bound) {
        map_int expected_directed;
        map_int expected_undirected;
        if (lower_bound == 0) {
          expected_directed.emplace(0, 2);
          expected_undirected.emplace(0, 2);
        }
        if (lower_bound <= 4 && upper_bound >= 4) {
          expected_undirected.emplace(4, 24);
        }
        if (lower_bound <= 8 && upper_bound >= 8) {
          expected_undirected.emplace(8, 48);
        }

        EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, lower_bound, upper_bound, reverse), expected_directed);
        EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, lower_bound, upper_bound, reverse), expected_directed);
        EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, lower_bound, upper_bound, reverse), expected_undirected);
      }
    }

    // Test only upper bound.
    for (int upper_bound = 0; upper_bound < 10; ++upper_bound) {
      map_int expected_directed;
      map_int expected_undirected;
      if (upper_bound >= 4) {
        expected_undirected.emplace(4, 24);
      }
      if (upper_bound >= 8) {
        expected_undirected.emplace(8, 48);
      }

      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, std::nullopt, upper_bound, reverse), expected_directed);
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, std::nullopt, upper_bound, reverse), expected_directed);
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, std::nullopt, upper_bound, reverse), expected_undirected);
    }

    // Test only lower bound.
    for (int lower_bound = 0; lower_bound < 10; ++lower_bound) {
      map_int expected_directed;
      map_int expected_undirected;
      if (lower_bound == 0) {
        expected_directed.emplace(0, 2);
        expected_undirected.emplace(0, 2);
      }
      if (lower_bound <= 4) {
        expected_undirected.emplace(4, 24);
      }
      if (lower_bound <= 8) {
        expected_undirected.emplace(8, 48);
      }

      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, lower_bound, std::nullopt, reverse), expected_directed);
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, lower_bound, std::nullopt, reverse), expected_directed);
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, lower_bound, std::nullopt, reverse), expected_undirected);
    }

    // Test no bounds.
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, std::nullopt, std::nullopt, reverse), (map_int{}));
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, std::nullopt, std::nullopt, reverse), (map_int{}));
    EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, std::nullopt, std::nullopt, reverse),
              (map_int{{4, 24}, {8, 48}}));
  }
}

#ifdef MG_ENTERPRISE
TEST_F(QueryPlanExpandVariable, FineGrainedExpandToSameSymbol) {
  auto test_expand = [&](int layer, EdgeAtom::Direction direction, std::optional<size_t> lower,
                         std::optional<size_t> upper, bool reverse, memgraph::auth::User &user) {
    auto e = Edge("r", direction);

    auto node = NODE("n");
    auto symbol = symbol_table.CreateSymbol("n", true);
    node->identifier_->MapTo(symbol);
    auto logical_op = std::make_shared<ScanAll>(nullptr, symbol, memgraph::storage::View::OLD);
    auto n_from = ScanAllTuple{node, logical_op, symbol};

    auto filter_op = std::make_shared<Filter>(
        n_from.op_, std::vector<std::shared_ptr<LogicalOperator>>{},
        storage.Create<memgraph::query::LabelsTest>(
            n_from.node_->identifier_, std::vector<LabelIx>{storage.GetLabelIx(dba.LabelToName(labels[layer]))}));

    // convert optional ints to optional expressions
    auto convert = [this](std::optional<size_t> bound) {
      return bound ? LITERAL(static_cast<int64_t>(bound.value())) : nullptr;
    };

    return GetEdgeListSizes(std::make_shared<ExpandVariable>(
                                filter_op, symbol, symbol, e, EdgeAtom::Type::DEPTH_FIRST, direction,
                                std::vector<memgraph::storage::EdgeTypeId>{}, reverse, convert(lower), convert(upper),
                                /* existing = */ true,
                                ExpansionLambda{symbol_table.CreateSymbol("inner_edge", false),
                                                symbol_table.CreateSymbol("inner_node", false), nullptr},
                                std::nullopt, std::nullopt),
                            e, &user);
  };

  // All labels granted, All edge types granted
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

    for (auto reverse : {false, true}) {
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, std::nullopt, std::nullopt, reverse, user),
                (map_int{{4, 12}, {8, 24}}));

      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, std::nullopt, std::nullopt, reverse, user),
                (map_int{{4, 24}, {8, 48}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 4, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 4, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, std::nullopt, reverse, user), (map_int{{4, 24}, {8, 48}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, std::nullopt, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, std::nullopt, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, std::nullopt, 8, reverse, user), (map_int{{4, 24}, {8, 48}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 4, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 4, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, 8, reverse, user), (map_int{{4, 24}, {8, 48}}));
    }
  }

  // All labels denied, All edge types denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::NOTHING);

    for (auto reverse : {false, true}) {
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 4, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 4, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, std::nullopt, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, std::nullopt, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, std::nullopt, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 4, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 4, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, 8, reverse, user), (map_int{}));
    }
  }

  // First layer label denied, all edge types granted
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("0", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("1", memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().label_permissions().Grant("2", memgraph::auth::FineGrainedPermission::READ);

    for (auto reverse : {false, true}) {
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 4, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 4, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, std::nullopt, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, std::nullopt, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, std::nullopt, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 4, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 4, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, 8, reverse, user), (map_int{}));
    }
  }

  // Second layer label denied, all edge types granted
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("0", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("1", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("2", memgraph::auth::FineGrainedPermission::NOTHING);

    for (auto reverse : {false, true}) {
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, std::nullopt, std::nullopt, reverse, user),
                (map_int{{4, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, std::nullopt, std::nullopt, reverse, user),
                (map_int{{4, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 4, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 4, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, std::nullopt, reverse, user), (map_int{{4, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, std::nullopt, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, std::nullopt, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, std::nullopt, 8, reverse, user), (map_int{{4, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 4, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 4, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, 8, reverse, user), (map_int{{4, 4}}));
    }
  }

  // All labels granted, Edge type from layer 0 to layer 1 denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_1",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_2",
                                                                     memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

    for (auto reverse : {false, true}) {
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, std::nullopt, std::nullopt, reverse, user),
                (map_int{{4, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 4, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 4, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, std::nullopt, reverse, user), (map_int{{4, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, std::nullopt, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, std::nullopt, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, std::nullopt, 8, reverse, user), (map_int{{4, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 4, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 4, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, 8, reverse, user), (map_int{{4, 4}}));
    }
  }
  // All labels granted, Edge type from layer 1 to layer 2 denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_1",
                                                                     memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_2",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

    for (auto reverse : {false, true}) {
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::IN, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::OUT, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(0, EdgeAtom::Direction::BOTH, std::nullopt, std::nullopt, reverse, user),
                (map_int{{4, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, std::nullopt, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, std::nullopt, std::nullopt, reverse, user),
                (map_int{{4, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 4, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 4, std::nullopt, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, std::nullopt, reverse, user), (map_int{{4, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, std::nullopt, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, std::nullopt, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, std::nullopt, 8, reverse, user), (map_int{{4, 4}}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::IN, 4, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::OUT, 4, 8, reverse, user), (map_int{}));
      EXPECT_EQ(test_expand(1, EdgeAtom::Direction::BOTH, 4, 8, reverse, user), (map_int{{4, 4}}));
    }
  }
}
#endif

namespace std {
template <>
struct hash<std::pair<int, int>> {
  size_t operator()(const std::pair<int, int> &p) const { return p.first + 31 * p.second; }
};
}  // namespace std

/** A test fixture for weighted shortest path expansion */
class QueryPlanExpandWeightedShortestPath : public testing::Test {
 public:
  struct ResultType {
    std::vector<memgraph::query::EdgeAccessor> path;
    memgraph::query::VertexAccessor vertex;
    double total_weight;
  };

 protected:
  memgraph::storage::Storage db;
  memgraph::storage::Storage::Accessor storage_dba{db.Access()};
  memgraph::query::DbAccessor dba{&storage_dba};
  std::pair<std::string, memgraph::storage::PropertyId> prop = PROPERTY_PAIR("property");
  memgraph::storage::EdgeTypeId edge_type = dba.NameToEdgeType("edge_type");

  // make 5 vertices because we'll need to compare against them exactly
  // v[0] has `prop` with the value 0
  std::vector<memgraph::query::VertexAccessor> v;

  // make some edges too, in a map (from, to) vertex indices
  std::unordered_map<std::pair<int, int>, memgraph::query::EdgeAccessor> e;

  AstStorage storage;
  SymbolTable symbol_table;

  // inner edge and vertex symbols
  Symbol filter_edge = symbol_table.CreateSymbol("f_edge", true);
  Symbol filter_node = symbol_table.CreateSymbol("f_node", true);

  Symbol weight_edge = symbol_table.CreateSymbol("w_edge", true);
  Symbol weight_node = symbol_table.CreateSymbol("w_node", true);

  Symbol total_weight = symbol_table.CreateSymbol("total_weight", true);

  void SetUp() {
    memgraph::license::global_license_checker.EnableTesting();

    for (int i = 0; i < 5; i++) {
      v.push_back(dba.InsertVertex());
      ASSERT_TRUE(v.back().SetProperty(prop.second, memgraph::storage::PropertyValue(i)).HasValue());
      auto label = fmt::format("l{}", i);
      ASSERT_TRUE(v.back().AddLabel(db.NameToLabel(label)).HasValue());
    }

    auto add_edge = [&](int from, int to, double weight) {
      auto edge = dba.InsertEdge(&v[from], &v[to], edge_type);
      ASSERT_TRUE(edge->SetProperty(prop.second, memgraph::storage::PropertyValue(weight)).HasValue());
      e.emplace(std::make_pair(from, to), *edge);
    };

    add_edge(0, 1, 5);
    add_edge(1, 4, 5);
    add_edge(0, 2, 3);
    add_edge(2, 3, 3);
    add_edge(3, 4, 3);
    add_edge(4, 0, 12);

    dba.AdvanceCommand();
  }

  // defines and performs a weighted shortest expansion with the given
  // params returns a vector of pairs. each pair is (vector-of-edges,
  // vertex)
  auto ExpandWShortest(EdgeAtom::Direction direction, std::optional<int> max_depth, Expression *where,
                       std::optional<int> node_id = 0, ScanAllTuple *existing_node_input = nullptr,
                       memgraph::auth::User *user = nullptr) {
    // scan the nodes optionally filtering on property value
    auto n = MakeScanAll(storage, symbol_table, "n", existing_node_input ? existing_node_input->op_ : nullptr);
    auto last_op = n.op_;
    if (node_id) {
      last_op = std::make_shared<Filter>(last_op, std::vector<std::shared_ptr<LogicalOperator>>{},
                                         EQ(PROPERTY_LOOKUP(n.node_->identifier_, prop), LITERAL(*node_id)));
    }

    auto ident_e = IDENT("e");
    ident_e->MapTo(weight_edge);

    // expand wshortest
    auto node_sym = existing_node_input ? existing_node_input->sym_ : symbol_table.CreateSymbol("node", true);
    auto edge_list_sym = symbol_table.CreateSymbol("edgelist_", true);
    auto filter_lambda = last_op = std::make_shared<ExpandVariable>(
        last_op, n.sym_, node_sym, edge_list_sym, EdgeAtom::Type::WEIGHTED_SHORTEST_PATH, direction,
        std::vector<memgraph::storage::EdgeTypeId>{}, false, nullptr, max_depth ? LITERAL(max_depth.value()) : nullptr,
        existing_node_input != nullptr, ExpansionLambda{filter_edge, filter_node, where},
        ExpansionLambda{weight_edge, weight_node, PROPERTY_LOOKUP(ident_e, prop)}, total_weight);

    Frame frame(symbol_table.max_position());
    auto cursor = last_op->MakeCursor(memgraph::utils::NewDeleteResource());
    std::vector<ResultType> results;
    memgraph::query::ExecutionContext context;
    if (user) {
#ifdef MG_ENTERPRISE
      memgraph::glue::FineGrainedAuthChecker auth_checker{*user, &dba};
      context = MakeContextWithFineGrainedChecker(storage, symbol_table, &dba, &auth_checker);
#endif
    } else {
      context = MakeContext(storage, symbol_table, &dba);
    }

    while (cursor->Pull(frame, context)) {
      results.push_back(ResultType{std::vector<memgraph::query::EdgeAccessor>(), frame[node_sym].ValueVertex(),
                                   frame[total_weight].ValueDouble()});
      for (const TypedValue &edge : frame[edge_list_sym].ValueList())
        results.back().path.emplace_back(edge.ValueEdge());
    }

    return results;
  }

  template <typename TAccessor>
  auto GetProp(const TAccessor &accessor) {
    return accessor.GetProperty(memgraph::storage::View::OLD, prop.second)->ValueInt();
  }

  template <typename TAccessor>
  auto GetDoubleProp(const TAccessor &accessor) {
    return accessor.GetProperty(memgraph::storage::View::OLD, prop.second)->ValueDouble();
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
  auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true));

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
    auto results = ExpandWShortest(EdgeAtom::Direction::OUT, 1000, LITERAL(true));
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
    auto results = ExpandWShortest(EdgeAtom::Direction::IN, 1000, LITERAL(true));
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
    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, PropNe(filter_node, 2));
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(GetProp(results[0].vertex), 1);
    EXPECT_EQ(results[0].total_weight, 5);
    EXPECT_EQ(GetProp(results[1].vertex), 4);
    EXPECT_EQ(results[1].total_weight, 10);
    EXPECT_EQ(GetProp(results[2].vertex), 3);
    EXPECT_EQ(results[2].total_weight, 13);
  }
  {
    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, PropNe(filter_node, 1));
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
      auto filter =
          std::make_shared<Filter>(n0.op_, std::vector<std::shared_ptr<LogicalOperator>>{},
                                   EQ(PROPERTY_LOOKUP(n0.node_->identifier_, prop), LITERAL(*preceeding_node_id)));
      // inject the filter op into the ScanAllTuple. that way the filter
      // op can be passed into the ExpandWShortest function without too
      // much refactor
      n0.op_ = filter;
    }

    return ExpandWShortest(EdgeAtom::Direction::OUT, 1000, LITERAL(true), std::nullopt, &n0);
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
    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, std::nullopt, LITERAL(true));
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
    ASSERT_TRUE(new_vertex.SetProperty(prop.second, memgraph::storage::PropertyValue(5)).HasValue());
    auto edge = dba.InsertEdge(&v[4], &new_vertex, edge_type);
    ASSERT_TRUE(edge.HasValue());
    ASSERT_TRUE(edge->SetProperty(prop.second, memgraph::storage::PropertyValue(2)).HasValue());
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
  ASSERT_TRUE(new_vertex.SetProperty(prop.second, memgraph::storage::PropertyValue(5)).HasValue());
  auto edge = dba.InsertEdge(&v[4], &new_vertex, edge_type);
  ASSERT_TRUE(edge.HasValue());
  ASSERT_TRUE(edge->SetProperty(prop.second, memgraph::storage::PropertyValue("not a number")).HasValue());
  dba.AdvanceCommand();
  EXPECT_THROW(ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true)), QueryRuntimeException);
}

TEST_F(QueryPlanExpandWeightedShortestPath, NegativeWeight) {
  auto new_vertex = dba.InsertVertex();
  ASSERT_TRUE(new_vertex.SetProperty(prop.second, memgraph::storage::PropertyValue(5)).HasValue());
  auto edge = dba.InsertEdge(&v[4], &new_vertex, edge_type);
  ASSERT_TRUE(edge.HasValue());
  ASSERT_TRUE(edge->SetProperty(prop.second, memgraph::storage::PropertyValue(-10)).HasValue());  // negative weight
  dba.AdvanceCommand();
  EXPECT_THROW(ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true)), QueryRuntimeException);
}

TEST_F(QueryPlanExpandWeightedShortestPath, NegativeUpperBound) {
  EXPECT_THROW(ExpandWShortest(EdgeAtom::Direction::BOTH, -1, LITERAL(true)), QueryRuntimeException);
}

#if MG_ENTERPRISE
TEST_F(QueryPlanExpandWeightedShortestPath, FineGrainedFiltering) {
  // All edge_types and labels allowed
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true), 0, nullptr, &user);
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

  // Denied all labels
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true), 0, nullptr, &user);
    ASSERT_EQ(results.size(), 0);
  }

  // Denied all edge types
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true), 0, nullptr, &user);
    ASSERT_EQ(results.size(), 0);
  }

  // Denied first vertex label
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("l0", memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true), 0, nullptr, &user);
    ASSERT_EQ(results.size(), 0);
  }

  // Denied vertex label 2
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("l0", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("l1", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("l2", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("l3", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("l4", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true), 0, nullptr, &user);
    ASSERT_EQ(results.size(), 4);

    user.fine_grained_access_handler().label_permissions().Grant("l2", memgraph::auth::FineGrainedPermission::NOTHING);
    auto filtered_results = ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true), 0, nullptr, &user);
    ASSERT_EQ(filtered_results.size(), 3);
  }

  // Deny edge type (created vertex 5 and edge vertex 4 to vertex 5)
  {
    v.push_back(dba.InsertVertex());
    ASSERT_TRUE(v.back().SetProperty(prop.second, memgraph::storage::PropertyValue(5)).HasValue());
    ASSERT_TRUE(v.back().AddLabel(db.NameToLabel("l5")).HasValue());
    dba.AdvanceCommand();
    memgraph::storage::EdgeTypeId edge_type_filter = dba.NameToEdgeType("edge_type_filter");
    auto edge = dba.InsertEdge(&v[4], &v[5], edge_type_filter);
    ASSERT_TRUE(edge->SetProperty(prop.second, memgraph::storage::PropertyValue(1)).HasValue());
    e.emplace(std::make_pair(4, 5), *edge);
    dba.AdvanceCommand();

    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    auto results = ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true), 0, nullptr, &user);
    ASSERT_EQ(results.size(), 5);

    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type",
                                                                     memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_filter",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    auto filtered_results = ExpandWShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true), 0, nullptr, &user);
    ASSERT_EQ(filtered_results.size(), 4);
  }
}
#endif

/** A test fixture for all shortest paths expansion */
class QueryPlanExpandAllShortestPaths : public testing::Test {
 public:
  struct ResultType {
    std::vector<memgraph::query::EdgeAccessor> path;
    memgraph::query::VertexAccessor vertex;
    double total_weight;
  };

 protected:
  memgraph::storage::Storage db;
  memgraph::storage::Storage::Accessor storage_dba{db.Access()};
  memgraph::query::DbAccessor dba{&storage_dba};
  std::pair<std::string, memgraph::storage::PropertyId> prop = PROPERTY_PAIR("property");
  memgraph::storage::EdgeTypeId edge_type = dba.NameToEdgeType("edge_type");

  // make 5 vertices because we'll need to compare against them exactly
  // v[0] has `prop` with the value 0
  std::vector<memgraph::query::VertexAccessor> v;

  // make some edges too, in a map (from, to) vertex indices
  std::unordered_map<std::pair<int, int>, memgraph::query::EdgeAccessor> e;

  AstStorage storage;
  SymbolTable symbol_table;

  // inner edge and vertex symbols
  Symbol filter_edge = symbol_table.CreateSymbol("f_edge", true);
  Symbol filter_node = symbol_table.CreateSymbol("f_node", true);

  Symbol weight_edge = symbol_table.CreateSymbol("w_edge", true);
  Symbol weight_node = symbol_table.CreateSymbol("w_node", true);

  Symbol total_weight = symbol_table.CreateSymbol("total_weight", true);

  void SetUp() {
    memgraph::license::global_license_checker.EnableTesting();

    for (int i = 0; i < 5; i++) {
      v.push_back(dba.InsertVertex());
      ASSERT_TRUE(v.back().SetProperty(prop.second, memgraph::storage::PropertyValue(i)).HasValue());
      auto label = fmt::format("l{}", i);
      ASSERT_TRUE(v.back().AddLabel(db.NameToLabel(label)).HasValue());
    }

    auto add_edge = [&](int from, int to, double weight) {
      auto edge = dba.InsertEdge(&v[from], &v[to], edge_type);
      ASSERT_TRUE(edge->SetProperty(prop.second, memgraph::storage::PropertyValue(weight)).HasValue());
      e.emplace(std::make_pair(from, to), *edge);
    };

    add_edge(0, 1, 5);
    add_edge(1, 4, 5);
    add_edge(0, 2, 3);
    add_edge(2, 3, 3);
    add_edge(3, 4, 3);
    add_edge(4, 0, 12);

    dba.AdvanceCommand();
  }

  // defines and performs an all shortest paths expansion with the given
  // params returns a vector of pairs. each pair is (vector-of-edges,
  // vertex)
  auto ExpandAllShortest(EdgeAtom::Direction direction, std::optional<int> max_depth, Expression *where,
                         std::optional<int> node_id = 0, ScanAllTuple *existing_node_input = nullptr,
                         const memgraph::auth::User *user = nullptr) {
    // scan the nodes optionally filtering on property value
    auto n = MakeScanAll(storage, symbol_table, "n", existing_node_input ? existing_node_input->op_ : nullptr);
    auto last_op = n.op_;
    if (node_id) {
      last_op = std::make_shared<Filter>(last_op, std::vector<std::shared_ptr<LogicalOperator>>{},
                                         EQ(PROPERTY_LOOKUP(n.node_->identifier_, prop), LITERAL(*node_id)));
    }

    auto ident_e = IDENT("e");
    ident_e->MapTo(weight_edge);

    // expand allshortest
    auto node_sym = existing_node_input ? existing_node_input->sym_ : symbol_table.CreateSymbol("node", true);
    auto edge_list_sym = symbol_table.CreateSymbol("edgelist_", true);
    auto filter_lambda = last_op = std::make_shared<ExpandVariable>(
        last_op, n.sym_, node_sym, edge_list_sym, EdgeAtom::Type::ALL_SHORTEST_PATHS, direction,
        std::vector<memgraph::storage::EdgeTypeId>{}, false, nullptr, max_depth ? LITERAL(max_depth.value()) : nullptr,
        existing_node_input != nullptr, ExpansionLambda{filter_edge, filter_node, where},
        ExpansionLambda{weight_edge, weight_node, PROPERTY_LOOKUP(ident_e, prop)}, total_weight);

    Frame frame(symbol_table.max_position());
    auto cursor = last_op->MakeCursor(memgraph::utils::NewDeleteResource());
    std::vector<ResultType> results;
    ExecutionContext context;
    if (user) {
#ifdef MG_ENTERPRISE
      memgraph::glue::FineGrainedAuthChecker auth_checker{*user, &dba};
      context = MakeContextWithFineGrainedChecker(storage, symbol_table, &dba, &auth_checker);
#endif
    } else {
      context = MakeContext(storage, symbol_table, &dba);
    }
    while (cursor->Pull(frame, context)) {
      results.push_back(ResultType{std::vector<memgraph::query::EdgeAccessor>(), frame[node_sym].ValueVertex(),
                                   frame[total_weight].ValueDouble()});
      for (const TypedValue &edge : frame[edge_list_sym].ValueList())
        results.back().path.emplace_back(edge.ValueEdge());
    }

    return results;
  }

  template <typename TAccessor>
  auto GetProp(const TAccessor &accessor) {
    return accessor.GetProperty(memgraph::storage::View::OLD, prop.second)->ValueInt();
  }

  template <typename TAccessor>
  auto GetDoubleProp(const TAccessor &accessor) {
    return accessor.GetProperty(memgraph::storage::View::OLD, prop.second)->ValueDouble();
  }

  Expression *PropNe(Symbol symbol, int value) {
    auto ident = IDENT("inner_element");
    ident->MapTo(symbol);
    return NEQ(PROPERTY_LOOKUP(ident, prop), LITERAL(value));
  }
};

bool compareResultType(const QueryPlanExpandAllShortestPaths::ResultType &a,
                       const QueryPlanExpandAllShortestPaths::ResultType &b) {
  return a.total_weight < b.total_weight;
}

// Testing all shortest paths on this graph:
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

TEST_F(QueryPlanExpandAllShortestPaths, Basic) {
  auto results = ExpandAllShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true));
  sort(results.begin(), results.end(), compareResultType);

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

TEST_F(QueryPlanExpandAllShortestPaths, EdgeDirection) {
  {
    auto results = ExpandAllShortest(EdgeAtom::Direction::OUT, 1000, LITERAL(true));
    sort(results.begin(), results.end(), compareResultType);
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
    auto results = ExpandAllShortest(EdgeAtom::Direction::IN, 1000, LITERAL(true));
    sort(results.begin(), results.end(), compareResultType);
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

TEST_F(QueryPlanExpandAllShortestPaths, Where) {
  {
    auto results = ExpandAllShortest(EdgeAtom::Direction::BOTH, 1000, PropNe(filter_node, 2));
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(GetProp(results[0].vertex), 1);
    EXPECT_EQ(results[0].total_weight, 5);
    EXPECT_EQ(GetProp(results[1].vertex), 4);
    EXPECT_EQ(results[1].total_weight, 10);
    EXPECT_EQ(GetProp(results[2].vertex), 3);
    EXPECT_EQ(results[2].total_weight, 13);
  }
  {
    auto results = ExpandAllShortest(EdgeAtom::Direction::BOTH, 1000, PropNe(filter_node, 1));
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(GetProp(results[0].vertex), 2);
    EXPECT_EQ(results[0].total_weight, 3);
    EXPECT_EQ(GetProp(results[1].vertex), 3);
    EXPECT_EQ(results[1].total_weight, 6);
    EXPECT_EQ(GetProp(results[2].vertex), 4);
    EXPECT_EQ(results[2].total_weight, 9);
  }
}

TEST_F(QueryPlanExpandAllShortestPaths, UpperBound) {
  {
    auto results = ExpandAllShortest(EdgeAtom::Direction::BOTH, std::nullopt, LITERAL(true));
    std::sort(results.begin(), results.end(), compareResultType);
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
    auto results = ExpandAllShortest(EdgeAtom::Direction::BOTH, 2, LITERAL(true));
    std::sort(results.begin(), results.end(), compareResultType);
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
    auto results = ExpandAllShortest(EdgeAtom::Direction::BOTH, 1, LITERAL(true));
    std::sort(results.begin(), results.end(), compareResultType);
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
    ASSERT_TRUE(new_vertex.SetProperty(prop.second, memgraph::storage::PropertyValue(5)).HasValue());
    auto edge = dba.InsertEdge(&v[4], &new_vertex, edge_type);
    ASSERT_TRUE(edge.HasValue());
    ASSERT_TRUE(edge->SetProperty(prop.second, memgraph::storage::PropertyValue(2)).HasValue());
    dba.AdvanceCommand();

    auto results = ExpandAllShortest(EdgeAtom::Direction::BOTH, 3, LITERAL(true));
    std::sort(results.begin(), results.end(), compareResultType);
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

TEST_F(QueryPlanExpandAllShortestPaths, NonNumericWeight) {
  auto new_vertex = dba.InsertVertex();
  ASSERT_TRUE(new_vertex.SetProperty(prop.second, memgraph::storage::PropertyValue(5)).HasValue());
  auto edge = dba.InsertEdge(&v[4], &new_vertex, edge_type);
  ASSERT_TRUE(edge.HasValue());
  ASSERT_TRUE(edge->SetProperty(prop.second, memgraph::storage::PropertyValue("not a number")).HasValue());
  dba.AdvanceCommand();
  EXPECT_THROW(ExpandAllShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true)), QueryRuntimeException);
}

TEST_F(QueryPlanExpandAllShortestPaths, NegativeWeight) {
  auto new_vertex = dba.InsertVertex();
  ASSERT_TRUE(new_vertex.SetProperty(prop.second, memgraph::storage::PropertyValue(5)).HasValue());
  auto edge = dba.InsertEdge(&v[4], &new_vertex, edge_type);
  ASSERT_TRUE(edge.HasValue());
  ASSERT_TRUE(edge->SetProperty(prop.second, memgraph::storage::PropertyValue(-10)).HasValue());  // negative weight
  dba.AdvanceCommand();
  EXPECT_THROW(ExpandAllShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true)), QueryRuntimeException);
}

TEST_F(QueryPlanExpandAllShortestPaths, NegativeUpperBound) {
  EXPECT_THROW(ExpandAllShortest(EdgeAtom::Direction::BOTH, -1, LITERAL(true)), QueryRuntimeException);
}

// MultiplePaths testing on this graph:
//       5        5
//  [0]-->--[1]--->---[6]
//   |        \       /
//  \/ 3     5 >-[4]->
//   |           /   1
//  [2]-->--[3]->
//       3       3

TEST_F(QueryPlanExpandAllShortestPaths, MultiplePaths) {
  auto new_vertex = dba.InsertVertex();
  ASSERT_TRUE(new_vertex.SetProperty(prop.second, memgraph::storage::PropertyValue(6)).HasValue());

  auto edge = dba.InsertEdge(&v[4], &new_vertex, edge_type);
  ASSERT_TRUE(edge.HasValue());
  ASSERT_TRUE(edge->SetProperty(prop.second, memgraph::storage::PropertyValue(1)).HasValue());
  dba.AdvanceCommand();

  auto edge2 = dba.InsertEdge(&v[1], &new_vertex, edge_type);
  ASSERT_TRUE(edge2.HasValue());
  ASSERT_TRUE(edge2->SetProperty(prop.second, memgraph::storage::PropertyValue(5)).HasValue());
  dba.AdvanceCommand();

  auto results = ExpandAllShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true));
  std::sort(results.begin(), results.end(), compareResultType);
  ASSERT_EQ(results.size(), 6);
  EXPECT_EQ(GetProp(results[4].vertex), 6);
  EXPECT_EQ(results[4].total_weight, 10);
  EXPECT_EQ(GetProp(results[5].vertex), 6);
  EXPECT_EQ(results[5].total_weight, 10);
}

// Uses graph from Basic test, with double edge 2->-3 and 3->-4
TEST_F(QueryPlanExpandAllShortestPaths, MultiEdge) {
  auto edge = dba.InsertEdge(&v[2], &v[3], edge_type);
  ASSERT_TRUE(edge.HasValue());
  ASSERT_TRUE(edge->SetProperty(prop.second, memgraph::storage::PropertyValue(3)).HasValue());
  dba.AdvanceCommand();

  auto edge2 = dba.InsertEdge(&v[3], &v[4], edge_type);
  ASSERT_TRUE(edge2.HasValue());
  ASSERT_TRUE(edge2->SetProperty(prop.second, memgraph::storage::PropertyValue(3)).HasValue());
  dba.AdvanceCommand();

  auto results = ExpandAllShortest(EdgeAtom::Direction::OUT, 1000, LITERAL(true));
  std::sort(results.begin(), results.end(), compareResultType);
  ASSERT_EQ(results.size(), 8);
  EXPECT_EQ(GetProp(results[6].vertex), 4);
  EXPECT_EQ(results[4].total_weight, 9);
  EXPECT_EQ(GetProp(results[7].vertex), 4);
  EXPECT_EQ(results[5].total_weight, 9);
}

#ifdef MG_ENTERPRISE
TEST_F(QueryPlanExpandAllShortestPaths, BasicWithFineGrainedFiltering) {
  // All edge_types and labels allowed
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    auto results = ExpandAllShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true));
    sort(results.begin(), results.end(), compareResultType);

    EXPECT_EQ(results[0].path.size(), 1);
    EXPECT_EQ(results[1].path.size(), 1);
    EXPECT_EQ(results[2].path.size(), 2);

    EXPECT_EQ(GetDoubleProp(results[3].path[2]), 3);
  }
  // Denied all labels
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    auto results = ExpandAllShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true), 0, nullptr, &user);
    ASSERT_EQ(results.size(), 0);
  }

  // Denied first vertex label
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("l0", memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    auto results = ExpandAllShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true), 0, nullptr, &user);

    ASSERT_EQ(results.size(), 0);
  }

  // Denied vertex label 2
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("l0", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("l1", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("l2", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("l3", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().label_permissions().Grant("l4", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

    auto results = ExpandAllShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true), 0, nullptr, &user);
    ASSERT_EQ(results.size(), 4);
    user.fine_grained_access_handler().label_permissions().Grant("l2", memgraph::auth::FineGrainedPermission::NOTHING);
    auto filtered_results = ExpandAllShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true), 0, nullptr, &user);

    ASSERT_EQ(filtered_results.size(), 3);
  }

  // Deny edge type (created vertex 5 and edge vertex 4 to vertex 5)
  {
    v.push_back(dba.InsertVertex());
    ASSERT_TRUE(v.back().SetProperty(prop.second, memgraph::storage::PropertyValue(5)).HasValue());
    ASSERT_TRUE(v.back().AddLabel(db.NameToLabel("l5")).HasValue());
    dba.AdvanceCommand();
    memgraph::storage::EdgeTypeId edge_type_filter = dba.NameToEdgeType("edge_type_filter");
    auto edge = dba.InsertEdge(&v[4], &v[5], edge_type_filter);
    ASSERT_TRUE(edge->SetProperty(prop.second, memgraph::storage::PropertyValue(1)).HasValue());
    e.emplace(std::make_pair(4, 5), *edge);
    dba.AdvanceCommand();

    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    auto results = ExpandAllShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true), 0, nullptr, &user);
    ASSERT_EQ(results.size(), 5);

    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type",
                                                                     memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("edge_type_filter",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    auto filtered_results = ExpandAllShortest(EdgeAtom::Direction::BOTH, 1000, LITERAL(true), 0, nullptr, &user);

    ASSERT_EQ(filtered_results.size(), 4);
  }
}
#endif

TEST(QueryPlan, ExpandOptional) {
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

  AstStorage storage;
  SymbolTable symbol_table;

  // graph (v2 {p: 2})<-[:T]-(v1 {p: 1})-[:T]->(v3 {p: 2})
  auto prop = dba.NameToProperty("p");
  auto edge_type = dba.NameToEdgeType("T");
  auto v1 = dba.InsertVertex();
  ASSERT_TRUE(v1.SetProperty(prop, memgraph::storage::PropertyValue(1)).HasValue());
  auto v2 = dba.InsertVertex();
  ASSERT_TRUE(v2.SetProperty(prop, memgraph::storage::PropertyValue(2)).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, edge_type).HasValue());
  auto v3 = dba.InsertVertex();
  ASSERT_TRUE(v3.SetProperty(prop, memgraph::storage::PropertyValue(2)).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v1, &v3, edge_type).HasValue());
  dba.AdvanceCommand();

  // MATCH (n) OPTIONAL MATCH (n)-[r]->(m)
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m = MakeExpand(storage, symbol_table, nullptr, n.sym_, "r", EdgeAtom::Direction::OUT, {}, "m", false,
                        memgraph::storage::View::OLD);
  auto optional = std::make_shared<plan::Optional>(n.op_, r_m.op_, std::vector<Symbol>{r_m.edge_sym_, r_m.node_sym_});

  // RETURN n, r, m
  auto n_ne = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("n", true));
  auto r_ne = NEXPR("r", IDENT("r")->MapTo(r_m.edge_sym_))->MapTo(symbol_table.CreateSymbol("r", true));
  auto m_ne = NEXPR("m", IDENT("m")->MapTo(r_m.node_sym_))->MapTo(symbol_table.CreateSymbol("m", true));
  auto produce = MakeProduce(optional, n_ne, r_ne, m_ne);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(4, results.size());
  int v1_is_n_count = 0;
  for (auto &row : results) {
    ASSERT_EQ(row[0].type(), TypedValue::Type::Vertex);
    auto &va = row[0].ValueVertex();
    auto va_p = *va.GetProperty(memgraph::storage::View::OLD, prop);
    ASSERT_EQ(va_p.type(), memgraph::storage::PropertyValue::Type::Int);
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
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

  AstStorage storage;
  SymbolTable symbol_table;

  // OPTIONAL MATCH (n)
  auto n = MakeScanAll(storage, symbol_table, "n");
  // RETURN n
  auto n_ne = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("n", true));
  auto optional = std::make_shared<plan::Optional>(nullptr, n.op_, std::vector<Symbol>{n.sym_});
  auto produce = MakeProduce(optional, n_ne);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(1, results.size());
  EXPECT_EQ(results[0][0].type(), TypedValue::Type::Null);
}

TEST(QueryPlan, OptionalMatchEmptyDBExpandFromNode) {
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;
  // OPTIONAL MATCH (n)
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto optional = std::make_shared<plan::Optional>(nullptr, n.op_, std::vector<Symbol>{n.sym_});
  // WITH n
  auto n_ne = NEXPR("n", IDENT("n")->MapTo(n.sym_));
  auto with_n_sym = symbol_table.CreateSymbol("n", true);
  n_ne->MapTo(with_n_sym);
  auto with = MakeProduce(optional, n_ne);
  // MATCH (n) -[r]-> (m)
  auto r_m = MakeExpand(storage, symbol_table, with, with_n_sym, "r", EdgeAtom::Direction::OUT, {}, "m", false,
                        memgraph::storage::View::OLD);
  // RETURN m
  auto m_ne = NEXPR("m", IDENT("m")->MapTo(r_m.node_sym_))->MapTo(symbol_table.CreateSymbol("m", true));
  auto produce = MakeProduce(r_m.op_, m_ne);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(0, results.size());
}

TEST(QueryPlan, OptionalMatchThenExpandToMissingNode) {
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);
  // Make a graph with 2 connected, unlabeled nodes.
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto edge_type = dba.NameToEdgeType("edge_type");
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, edge_type).HasValue());
  dba.AdvanceCommand();
  EXPECT_EQ(2, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  EXPECT_EQ(1, CountEdges(&dba, memgraph::storage::View::OLD));
  AstStorage storage;
  SymbolTable symbol_table;
  // OPTIONAL MATCH (n :missing)
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto label_missing = "missing";
  n.node_->labels_.emplace_back(storage.GetLabelIx(label_missing));

  auto *filter_expr = storage.Create<LabelsTest>(n.node_->identifier_, n.node_->labels_);
  auto node_filter = std::make_shared<Filter>(n.op_, std::vector<std::shared_ptr<LogicalOperator>>{}, filter_expr);
  auto optional = std::make_shared<plan::Optional>(nullptr, node_filter, std::vector<Symbol>{n.sym_});
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
  auto expand =
      std::make_shared<plan::Expand>(m.op_, m.sym_, with_n_sym, edge_sym, edge_direction,
                                     std::vector<memgraph::storage::EdgeTypeId>{}, true, memgraph::storage::View::OLD);
  // RETURN m
  auto m_ne = NEXPR("m", IDENT("m")->MapTo(m.sym_))->MapTo(symbol_table.CreateSymbol("m", true));
  auto produce = MakeProduce(expand, m_ne);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(0, results.size());
}

TEST(QueryPlan, ExpandExistingNode) {
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

  // make a graph (v1)->(v2) that
  // has a recursive edge (v1)->(v1)
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto edge_type = dba.NameToEdgeType("Edge");
  ASSERT_TRUE(dba.InsertEdge(&v1, &v1, edge_type).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, edge_type).HasValue());
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto test_existing = [&](bool with_existing, int expected_result_count) {
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_n = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::OUT, {}, "n", with_existing,
                          memgraph::storage::View::OLD);
    if (with_existing)
      r_n.op_ = std::make_shared<Expand>(n.op_, n.sym_, n.sym_, r_n.edge_sym_, r_n.edge_->direction_,
                                         std::vector<memgraph::storage::EdgeTypeId>{}, with_existing,
                                         memgraph::storage::View::OLD);

    // make a named expression and a produce
    auto output = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
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
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

  auto v = dba.InsertVertex();
  ASSERT_TRUE(dba.InsertEdge(&v, &v, dba.NameToEdgeType("et")).HasValue());
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_ = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::BOTH, {}, "_", false,
                       memgraph::storage::View::OLD);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*r_.op_, &context));
}

TEST(QueryPlan, EdgeFilter) {
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

  // make an N-star expanding from (v1)
  // where only one edge will qualify
  // and there are all combinations of
  // (edge_type yes|no) * (property yes|absent|no)
  std::vector<memgraph::storage::EdgeTypeId> edge_types;
  for (int j = 0; j < 2; ++j) edge_types.push_back(dba.NameToEdgeType("et" + std::to_string(j)));
  std::vector<memgraph::query::VertexAccessor> vertices;
  for (int i = 0; i < 7; ++i) vertices.push_back(dba.InsertVertex());
  auto prop = PROPERTY_PAIR("property");
  std::vector<memgraph::query::EdgeAccessor> edges;
  for (int i = 0; i < 6; ++i) {
    edges.push_back(*dba.InsertEdge(&vertices[0], &vertices[i + 1], edge_types[i % 2]));
    switch (i % 3) {
      case 0:
        ASSERT_TRUE(edges.back().SetProperty(prop.second, memgraph::storage::PropertyValue(42)).HasValue());
        break;
      case 1:
        ASSERT_TRUE(edges.back().SetProperty(prop.second, memgraph::storage::PropertyValue(100)).HasValue());
        break;
      default:
        break;
    }
  }
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto test_filter = [&]() {
    // define an operator tree for query
    // MATCH (n)-[r :et0 {property: 42}]->(m) RETURN m

    auto n = MakeScanAll(storage, symbol_table, "n");
    const auto &edge_type = edge_types[0];
    auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::OUT, {edge_type}, "m", false,
                          memgraph::storage::View::OLD);
    r_m.edge_->edge_types_.push_back(storage.GetEdgeTypeIx(dba.EdgeTypeToName(edge_type)));
    std::get<0>(r_m.edge_->properties_)[storage.GetPropertyIx(prop.first)] = LITERAL(42);
    auto *filter_expr = EQ(PROPERTY_LOOKUP(r_m.edge_->identifier_, prop), LITERAL(42));
    auto edge_filter = std::make_shared<Filter>(r_m.op_, std::vector<std::shared_ptr<LogicalOperator>>{}, filter_expr);

    // make a named expression and a produce
    auto output =
        NEXPR("m", IDENT("m")->MapTo(r_m.node_sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(edge_filter, output);
    auto context = MakeContext(storage, symbol_table, &dba);
    return PullAll(*produce, &context);
  };

  EXPECT_EQ(1, test_filter());
  // test that edge filtering always filters on old state
  for (auto &edge : edges) ASSERT_TRUE(edge.SetProperty(prop.second, memgraph::storage::PropertyValue(42)).HasValue());
  EXPECT_EQ(1, test_filter());
  dba.AdvanceCommand();
  EXPECT_EQ(3, test_filter());
}

TEST(QueryPlan, EdgeFilterMultipleTypes) {
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto type_1 = dba.NameToEdgeType("type_1");
  auto type_2 = dba.NameToEdgeType("type_2");
  auto type_3 = dba.NameToEdgeType("type_3");
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, type_1).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, type_2).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, type_3).HasValue());
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  // make a scan all
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::OUT, {type_1, type_2}, "m",
                        false, memgraph::storage::View::OLD);

  // make a named expression and a produce
  auto output =
      NEXPR("m", IDENT("m")->MapTo(r_m.node_sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(r_m.op_, output);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 2);
}

TEST(QueryPlan, Filter) {
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

  // add a 6 nodes with property 'prop', 2 have true as value
  auto property = PROPERTY_PAIR("property");
  for (int i = 0; i < 6; ++i)
    ASSERT_TRUE(
        dba.InsertVertex().SetProperty(property.second, memgraph::storage::PropertyValue(i % 3 == 0)).HasValue());
  dba.InsertVertex();  // prop not set, gives NULL
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto e = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), property);
  auto f = std::make_shared<Filter>(n.op_, std::vector<std::shared_ptr<LogicalOperator>>{}, e);

  auto output = NEXPR("x", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
  auto produce = MakeProduce(f, output);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(CollectProduce(*produce, &context).size(), 2);
}

TEST(QueryPlan, EdgeUniquenessFilter) {
  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);

  // make a graph that has (v1)->(v2) and a recursive edge (v1)->(v1)
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto edge_type = dba.NameToEdgeType("edge_type");
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, edge_type).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v1, &v1, edge_type).HasValue());
  dba.AdvanceCommand();

  auto check_expand_results = [&](bool edge_uniqueness) {
    AstStorage storage;
    SymbolTable symbol_table;

    auto n1 = MakeScanAll(storage, symbol_table, "n1");
    auto r1_n2 = MakeExpand(storage, symbol_table, n1.op_, n1.sym_, "r1", EdgeAtom::Direction::OUT, {}, "n2", false,
                            memgraph::storage::View::OLD);
    std::shared_ptr<LogicalOperator> last_op = r1_n2.op_;
    auto r2_n3 = MakeExpand(storage, symbol_table, last_op, r1_n2.node_sym_, "r2", EdgeAtom::Direction::OUT, {}, "n3",
                            false, memgraph::storage::View::OLD);
    last_op = r2_n3.op_;
    if (edge_uniqueness)
      last_op = std::make_shared<EdgeUniquenessFilter>(last_op, r2_n3.edge_sym_, std::vector<Symbol>{r1_n2.edge_sym_});
    auto context = MakeContext(storage, symbol_table, &dba);
    return PullAll(*last_op, &context);
  };

  EXPECT_EQ(2, check_expand_results(false));
  EXPECT_EQ(1, check_expand_results(true));
}

TEST(QueryPlan, Distinct) {
  // test queries like
  // UNWIND [1, 2, 3, 3] AS x RETURN DISTINCT x

  memgraph::storage::Storage db;
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  auto check_distinct = [&](const std::vector<TypedValue> input, const std::vector<TypedValue> output,
                            bool assume_int_value) {
    auto input_expr = LITERAL(TypedValue(input));

    auto x = symbol_table.CreateSymbol("x", true);
    auto unwind = std::make_shared<plan::Unwind>(nullptr, input_expr, x);
    auto x_expr = IDENT("x");
    x_expr->MapTo(x);

    auto distinct = std::make_shared<plan::Distinct>(unwind, std::vector<Symbol>{x});

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

  check_distinct({TypedValue(1), TypedValue(1), TypedValue(2), TypedValue(3), TypedValue(3), TypedValue(3)},
                 {TypedValue(1), TypedValue(2), TypedValue(3)}, true);
  check_distinct({TypedValue(3), TypedValue(2), TypedValue(3), TypedValue(5), TypedValue(3), TypedValue(5),
                  TypedValue(2), TypedValue(1), TypedValue(2)},
                 {TypedValue(3), TypedValue(2), TypedValue(5), TypedValue(1)}, true);
  check_distinct(
      {TypedValue(3), TypedValue("two"), TypedValue(), TypedValue(3), TypedValue(true), TypedValue(false),
       TypedValue("TWO"), TypedValue()},
      {TypedValue(3), TypedValue("two"), TypedValue(), TypedValue(true), TypedValue(false), TypedValue("TWO")}, false);
}

TEST(QueryPlan, ScanAllByLabel) {
  memgraph::storage::Storage db;
  auto label = db.NameToLabel("label");
  [[maybe_unused]] auto _ = db.CreateIndex(label);
  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);
  // Add a vertex with a label and one without.
  auto labeled_vertex = dba.InsertVertex();
  ASSERT_TRUE(labeled_vertex.AddLabel(label).HasValue());
  dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(2, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  // MATCH (n :label)
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all_by_label = MakeScanAllByLabel(storage, symbol_table, "n", label);
  // RETURN n
  auto output = NEXPR("n", IDENT("n")->MapTo(scan_all_by_label.sym_))->MapTo(symbol_table.CreateSymbol("n", true));
  auto produce = MakeProduce(scan_all_by_label.op_, output);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), 1);
  auto result_row = results[0];
  ASSERT_EQ(result_row.size(), 1);
  EXPECT_EQ(result_row[0].ValueVertex(), labeled_vertex);
}

TEST(QueryPlan, ScanAllByLabelProperty) {
  memgraph::storage::Storage db;
  // Add 5 vertices with same label, but with different property values.
  auto label = db.NameToLabel("label");
  auto prop = db.NameToProperty("prop");
  // vertex property values that will be stored into the DB
  std::vector<memgraph::storage::PropertyValue> values{
      memgraph::storage::PropertyValue(true),
      memgraph::storage::PropertyValue(false),
      memgraph::storage::PropertyValue("a"),
      memgraph::storage::PropertyValue("b"),
      memgraph::storage::PropertyValue("c"),
      memgraph::storage::PropertyValue(0),
      memgraph::storage::PropertyValue(1),
      memgraph::storage::PropertyValue(2),
      memgraph::storage::PropertyValue(0.5),
      memgraph::storage::PropertyValue(1.5),
      memgraph::storage::PropertyValue(2.5),
      memgraph::storage::PropertyValue(
          std::vector<memgraph::storage::PropertyValue>{memgraph::storage::PropertyValue(0)}),
      memgraph::storage::PropertyValue(
          std::vector<memgraph::storage::PropertyValue>{memgraph::storage::PropertyValue(1)}),
      memgraph::storage::PropertyValue(
          std::vector<memgraph::storage::PropertyValue>{memgraph::storage::PropertyValue(2)})};
  {
    auto storage_dba = db.Access();
    memgraph::query::DbAccessor dba(&storage_dba);
    for (const auto &value : values) {
      auto vertex = dba.InsertVertex();
      ASSERT_TRUE(vertex.AddLabel(label).HasValue());
      ASSERT_TRUE(vertex.SetProperty(prop, value).HasValue());
    }
    ASSERT_FALSE(dba.Commit().HasError());
  }
  [[maybe_unused]] auto _ = db.CreateIndex(label, prop);

  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);
  ASSERT_EQ(14, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));

  auto run_scan_all = [&](const TypedValue &lower, Bound::Type lower_type, const TypedValue &upper,
                          Bound::Type upper_type) {
    AstStorage storage;
    SymbolTable symbol_table;
    auto scan_all =
        MakeScanAllByLabelPropertyRange(storage, symbol_table, "n", label, prop, "prop",
                                        Bound{LITERAL(lower), lower_type}, Bound{LITERAL(upper), upper_type});
    // RETURN n
    auto output = NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))->MapTo(symbol_table.CreateSymbol("n", true));
    auto produce = MakeProduce(scan_all.op_, output);
    auto context = MakeContext(storage, symbol_table, &dba);
    return CollectProduce(*produce, &context);
  };

  auto check = [&](TypedValue lower, Bound::Type lower_type, TypedValue upper, Bound::Type upper_type,
                   const std::vector<TypedValue> &expected) {
    auto results = run_scan_all(lower, lower_type, upper, upper_type);
    ASSERT_EQ(results.size(), expected.size());
    for (size_t i = 0; i < expected.size(); i++) {
      TypedValue equal =
          TypedValue(*results[i][0].ValueVertex().GetProperty(memgraph::storage::View::OLD, prop)) == expected[i];
      ASSERT_EQ(equal.type(), TypedValue::Type::Bool);
      EXPECT_TRUE(equal.ValueBool());
    }
  };

  // normal ranges that return something
  check(TypedValue("a"), Bound::Type::EXCLUSIVE, TypedValue("c"), Bound::Type::EXCLUSIVE, {TypedValue("b")});
  check(TypedValue(0), Bound::Type::EXCLUSIVE, TypedValue(2), Bound::Type::INCLUSIVE,
        {TypedValue(0.5), TypedValue(1), TypedValue(1.5), TypedValue(2)});
  check(TypedValue(1.5), Bound::Type::EXCLUSIVE, TypedValue(2.5), Bound::Type::INCLUSIVE,
        {TypedValue(2), TypedValue(2.5)});

  auto are_comparable = [](memgraph::storage::PropertyValue::Type a, memgraph::storage::PropertyValue::Type b) {
    auto is_numeric = [](const memgraph::storage::PropertyValue::Type t) {
      return t == memgraph::storage::PropertyValue::Type::Int || t == memgraph::storage::PropertyValue::Type::Double;
    };

    return a == b || (is_numeric(a) && is_numeric(b));
  };

  auto is_orderable = [](const memgraph::storage::PropertyValue &t) {
    return t.IsNull() || t.IsInt() || t.IsDouble() || t.IsString();
  };

  // when a range contains different types, nothing should get returned
  for (const auto &value_a : values) {
    for (const auto &value_b : values) {
      if (are_comparable(static_cast<memgraph::storage::PropertyValue>(value_a).type(),
                         static_cast<memgraph::storage::PropertyValue>(value_b).type()))
        continue;
      if (is_orderable(value_a) && is_orderable(value_b)) {
        check(TypedValue(value_a), Bound::Type::INCLUSIVE, TypedValue(value_b), Bound::Type::INCLUSIVE, {});
      } else {
        EXPECT_THROW(
            run_scan_all(TypedValue(value_a), Bound::Type::INCLUSIVE, TypedValue(value_b), Bound::Type::INCLUSIVE),
            QueryRuntimeException);
      }
    }
  }
  // These should all raise an exception due to type mismatch when using
  // `operator<`.
  EXPECT_THROW(run_scan_all(TypedValue(false), Bound::Type::INCLUSIVE, TypedValue(true), Bound::Type::EXCLUSIVE),
               QueryRuntimeException);
  EXPECT_THROW(run_scan_all(TypedValue(false), Bound::Type::EXCLUSIVE, TypedValue(true), Bound::Type::INCLUSIVE),
               QueryRuntimeException);
  EXPECT_THROW(run_scan_all(TypedValue(std::vector<TypedValue>{TypedValue(0.5)}), Bound::Type::EXCLUSIVE,
                            TypedValue(std::vector<TypedValue>{TypedValue(1.5)}), Bound::Type::INCLUSIVE),
               QueryRuntimeException);
}

TEST(QueryPlan, ScanAllByLabelPropertyEqualityNoError) {
  memgraph::storage::Storage db;
  // Add 2 vertices with same label, but with property values that cannot be
  // compared. On the other hand, equality works fine.
  auto label = db.NameToLabel("label");
  auto prop = db.NameToProperty("prop");
  {
    auto storage_dba = db.Access();
    memgraph::query::DbAccessor dba(&storage_dba);
    auto number_vertex = dba.InsertVertex();
    ASSERT_TRUE(number_vertex.AddLabel(label).HasValue());
    ASSERT_TRUE(number_vertex.SetProperty(prop, memgraph::storage::PropertyValue(42)).HasValue());
    auto string_vertex = dba.InsertVertex();
    ASSERT_TRUE(string_vertex.AddLabel(label).HasValue());
    ASSERT_TRUE(string_vertex.SetProperty(prop, memgraph::storage::PropertyValue("string")).HasValue());
    ASSERT_FALSE(dba.Commit().HasError());
  }
  [[maybe_unused]] auto _ = db.CreateIndex(label, prop);

  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);
  EXPECT_EQ(2, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  // MATCH (n :label {prop: 42})
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all = MakeScanAllByLabelPropertyValue(storage, symbol_table, "n", label, prop, "prop", LITERAL(42));
  // RETURN n
  auto output = NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))->MapTo(symbol_table.CreateSymbol("n", true));
  auto produce = MakeProduce(scan_all.op_, output);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), 1);
  const auto &row = results[0];
  ASSERT_EQ(row.size(), 1);
  auto vertex = row[0].ValueVertex();
  TypedValue value(*vertex.GetProperty(memgraph::storage::View::OLD, prop));
  TypedValue::BoolEqual eq;
  EXPECT_TRUE(eq(value, TypedValue(42)));
}

TEST(QueryPlan, ScanAllByLabelPropertyValueError) {
  memgraph::storage::Storage db;
  auto label = db.NameToLabel("label");
  auto prop = db.NameToProperty("prop");
  {
    auto storage_dba = db.Access();
    memgraph::query::DbAccessor dba(&storage_dba);
    for (int i = 0; i < 2; ++i) {
      auto vertex = dba.InsertVertex();
      ASSERT_TRUE(vertex.AddLabel(label).HasValue());
      ASSERT_TRUE(vertex.SetProperty(prop, memgraph::storage::PropertyValue(i)).HasValue());
    }
    ASSERT_FALSE(dba.Commit().HasError());
  }
  [[maybe_unused]] auto _ = db.CreateIndex(label, prop);

  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);
  EXPECT_EQ(2, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  // MATCH (m), (n :label {prop: m})
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all = MakeScanAll(storage, symbol_table, "m");
  auto *ident_m = IDENT("m");
  ident_m->MapTo(scan_all.sym_);
  auto scan_index =
      MakeScanAllByLabelPropertyValue(storage, symbol_table, "n", label, prop, "prop", ident_m, scan_all.op_);
  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_THROW(PullAll(*scan_index.op_, &context), QueryRuntimeException);
}

TEST(QueryPlan, ScanAllByLabelPropertyRangeError) {
  memgraph::storage::Storage db;
  auto label = db.NameToLabel("label");
  auto prop = db.NameToProperty("prop");
  {
    auto storage_dba = db.Access();
    memgraph::query::DbAccessor dba(&storage_dba);
    for (int i = 0; i < 2; ++i) {
      auto vertex = dba.InsertVertex();
      ASSERT_TRUE(vertex.AddLabel(label).HasValue());
      ASSERT_TRUE(vertex.SetProperty(prop, memgraph::storage::PropertyValue(i)).HasValue());
    }
    ASSERT_FALSE(dba.Commit().HasError());
  }
  [[maybe_unused]] auto _ = db.CreateIndex(label, prop);

  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);
  EXPECT_EQ(2, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  // MATCH (m), (n :label {prop: m})
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all = MakeScanAll(storage, symbol_table, "m");
  auto *ident_m = IDENT("m");
  ident_m->MapTo(scan_all.sym_);
  {
    // Lower bound isn't property value
    auto scan_index =
        MakeScanAllByLabelPropertyRange(storage, symbol_table, "n", label, prop, "prop",
                                        Bound{ident_m, Bound::Type::INCLUSIVE}, std::nullopt, scan_all.op_);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_THROW(PullAll(*scan_index.op_, &context), QueryRuntimeException);
  }
  {
    // Upper bound isn't property value
    auto scan_index = MakeScanAllByLabelPropertyRange(storage, symbol_table, "n", label, prop, "prop", std::nullopt,
                                                      Bound{ident_m, Bound::Type::INCLUSIVE}, scan_all.op_);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_THROW(PullAll(*scan_index.op_, &context), QueryRuntimeException);
  }
  {
    // Both bounds aren't property value
    auto scan_index = MakeScanAllByLabelPropertyRange(storage, symbol_table, "n", label, prop, "prop",
                                                      Bound{ident_m, Bound::Type::INCLUSIVE},
                                                      Bound{ident_m, Bound::Type::INCLUSIVE}, scan_all.op_);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_THROW(PullAll(*scan_index.op_, &context), QueryRuntimeException);
  }
}

TEST(QueryPlan, ScanAllByLabelPropertyEqualNull) {
  memgraph::storage::Storage db;
  // Add 2 vertices with the same label, but one has a property value while
  // the other does not. Checking if the value is equal to null, should
  // yield no results.
  auto label = db.NameToLabel("label");
  auto prop = db.NameToProperty("prop");
  {
    auto storage_dba = db.Access();
    memgraph::query::DbAccessor dba(&storage_dba);
    auto vertex = dba.InsertVertex();
    ASSERT_TRUE(vertex.AddLabel(label).HasValue());
    auto vertex_with_prop = dba.InsertVertex();
    ASSERT_TRUE(vertex_with_prop.AddLabel(label).HasValue());
    ASSERT_TRUE(vertex_with_prop.SetProperty(prop, memgraph::storage::PropertyValue(42)).HasValue());
    ASSERT_FALSE(dba.Commit().HasError());
  }
  [[maybe_unused]] auto _ = db.CreateIndex(label, prop);

  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);
  EXPECT_EQ(2, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  // MATCH (n :label {prop: 42})
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all =
      MakeScanAllByLabelPropertyValue(storage, symbol_table, "n", label, prop, "prop", LITERAL(TypedValue()));
  // RETURN n
  auto output = NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))->MapTo(symbol_table.CreateSymbol("n", true));
  auto produce = MakeProduce(scan_all.op_, output);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 0);
}

TEST(QueryPlan, ScanAllByLabelPropertyRangeNull) {
  memgraph::storage::Storage db;
  // Add 2 vertices with the same label, but one has a property value while
  // the other does not. Checking if the value is between nulls, should
  // yield no results.
  auto label = db.NameToLabel("label");
  auto prop = db.NameToProperty("prop");
  {
    auto storage_dba = db.Access();
    memgraph::query::DbAccessor dba(&storage_dba);
    auto vertex = dba.InsertVertex();
    ASSERT_TRUE(vertex.AddLabel(label).HasValue());
    auto vertex_with_prop = dba.InsertVertex();
    ASSERT_TRUE(vertex_with_prop.AddLabel(label).HasValue());
    ASSERT_TRUE(vertex_with_prop.SetProperty(prop, memgraph::storage::PropertyValue(42)).HasValue());
    ASSERT_FALSE(dba.Commit().HasError());
  }
  [[maybe_unused]] auto _ = db.CreateIndex(label, prop);

  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);
  EXPECT_EQ(2, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  // MATCH (n :label) WHERE null <= n.prop < null
  AstStorage storage;
  SymbolTable symbol_table;
  auto scan_all = MakeScanAllByLabelPropertyRange(storage, symbol_table, "n", label, prop, "prop",
                                                  Bound{LITERAL(TypedValue()), Bound::Type::INCLUSIVE},
                                                  Bound{LITERAL(TypedValue()), Bound::Type::EXCLUSIVE});
  // RETURN n
  auto output = NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))->MapTo(symbol_table.CreateSymbol("n", true));
  auto produce = MakeProduce(scan_all.op_, output);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 0);
}

TEST(QueryPlan, ScanAllByLabelPropertyNoValueInIndexContinuation) {
  memgraph::storage::Storage db;
  auto label = db.NameToLabel("label");
  auto prop = db.NameToProperty("prop");
  {
    auto storage_dba = db.Access();
    memgraph::query::DbAccessor dba(&storage_dba);
    auto v = dba.InsertVertex();
    ASSERT_TRUE(v.AddLabel(label).HasValue());
    ASSERT_TRUE(v.SetProperty(prop, memgraph::storage::PropertyValue(2)).HasValue());
    ASSERT_FALSE(dba.Commit().HasError());
  }
  [[maybe_unused]] auto _ = db.CreateIndex(label, prop);

  auto storage_dba = db.Access();
  memgraph::query::DbAccessor dba(&storage_dba);
  EXPECT_EQ(1, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));

  AstStorage storage;
  SymbolTable symbol_table;

  // UNWIND [1, 2, 3] as x
  auto input_expr = LIST(LITERAL(1), LITERAL(2), LITERAL(3));
  auto x = symbol_table.CreateSymbol("x", true);
  auto unwind = std::make_shared<plan::Unwind>(nullptr, input_expr, x);
  auto x_expr = IDENT("x");
  x_expr->MapTo(x);

  // MATCH (n :label {prop: x})
  auto scan_all = MakeScanAllByLabelPropertyValue(storage, symbol_table, "n", label, prop, "prop", x_expr, unwind);

  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(PullAll(*scan_all.op_, &context), 1);
}

TEST(QueryPlan, ScanAllEqualsScanAllByLabelProperty) {
  memgraph::storage::Storage db;
  auto label = db.NameToLabel("label");
  auto prop = db.NameToProperty("prop");

  // Insert vertices
  const int vertex_count = 300, vertex_prop_count = 50;
  const int prop_value1 = 42, prop_value2 = 69;

  for (int i = 0; i < vertex_count; ++i) {
    auto storage_dba = db.Access();
    memgraph::query::DbAccessor dba(&storage_dba);
    auto v = dba.InsertVertex();
    ASSERT_TRUE(v.AddLabel(label).HasValue());
    ASSERT_TRUE(v.SetProperty(prop, memgraph::storage::PropertyValue(i < vertex_prop_count ? prop_value1 : prop_value2))
                    .HasValue());
    ASSERT_FALSE(dba.Commit().HasError());
  }

  [[maybe_unused]] auto _ = db.CreateIndex(label, prop);

  // Make sure there are `vertex_count` vertices
  {
    auto storage_dba = db.Access();
    memgraph::query::DbAccessor dba(&storage_dba);
    EXPECT_EQ(vertex_count, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  }

  // Make sure there are `vertex_prop_count` results when using index
  auto count_with_index = [&db, &label, &prop](int prop_value, int prop_count) {
    AstStorage storage;
    SymbolTable symbol_table;
    auto storage_dba = db.Access();
    memgraph::query::DbAccessor dba(&storage_dba);
    auto scan_all_by_label_property_value =
        MakeScanAllByLabelPropertyValue(storage, symbol_table, "n", label, prop, "prop", LITERAL(prop_value));
    auto output = NEXPR("n", IDENT("n")->MapTo(scan_all_by_label_property_value.sym_))
                      ->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(scan_all_by_label_property_value.op_, output);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_EQ(PullAll(*produce, &context), prop_count);
  };

  // Make sure there are `vertex_count` results when using scan all
  auto count_with_scan_all = [&db, &prop](int prop_value, int prop_count) {
    AstStorage storage;
    SymbolTable symbol_table;
    auto storage_dba = db.Access();
    memgraph::query::DbAccessor dba(&storage_dba);
    auto scan_all = MakeScanAll(storage, symbol_table, "n");
    auto e = PROPERTY_LOOKUP(IDENT("n")->MapTo(scan_all.sym_), std::make_pair("prop", prop));
    auto filter = std::make_shared<Filter>(scan_all.op_, std::vector<std::shared_ptr<LogicalOperator>>{},
                                           EQ(e, LITERAL(prop_value)));
    auto output =
        NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(filter, output);
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_EQ(PullAll(*produce, &context), prop_count);
  };

  count_with_index(prop_value1, vertex_prop_count);
  count_with_scan_all(prop_value1, vertex_prop_count);

  count_with_index(prop_value2, vertex_count - vertex_prop_count);
  count_with_scan_all(prop_value2, vertex_count - vertex_prop_count);
}

class ExistsFixture : public testing::Test {
 protected:
  memgraph::storage::Storage db;
  memgraph::storage::Storage::Accessor storage_dba{db.Access()};
  memgraph::query::DbAccessor dba{&storage_dba};
  AstStorage storage;
  SymbolTable symbol_table;

  std::pair<std::string, memgraph::storage::PropertyId> prop = PROPERTY_PAIR("property");

  memgraph::query::VertexAccessor v1{dba.InsertVertex()};
  memgraph::query::VertexAccessor v2{dba.InsertVertex()};
  memgraph::storage::EdgeTypeId edge_type{db.NameToEdgeType("Edge")};
  memgraph::query::EdgeAccessor r1{*dba.InsertEdge(&v1, &v2, edge_type)};

  memgraph::query::VertexAccessor v3{dba.InsertVertex()};
  memgraph::query::VertexAccessor v4{dba.InsertVertex()};
  memgraph::storage::EdgeTypeId edge_type_unknown{db.NameToEdgeType("Other")};
  memgraph::query::EdgeAccessor r2{*dba.InsertEdge(&v3, &v4, edge_type_unknown)};

  void SetUp() override {
    // (:l1)-[:Edge]->(:l2), (:l3)-[:Other]->(:l4)
    ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("l1")).HasValue());
    ASSERT_TRUE(v2.AddLabel(dba.NameToLabel("l2")).HasValue());
    ASSERT_TRUE(v3.AddLabel(dba.NameToLabel("l3")).HasValue());
    ASSERT_TRUE(v4.AddLabel(dba.NameToLabel("l4")).HasValue());

    ASSERT_TRUE(v1.SetProperty(prop.second, memgraph::storage::PropertyValue(1)).HasValue());
    ASSERT_TRUE(v2.SetProperty(prop.second, memgraph::storage::PropertyValue(2)).HasValue());
    ASSERT_TRUE(v3.SetProperty(prop.second, memgraph::storage::PropertyValue(3)).HasValue());
    ASSERT_TRUE(v4.SetProperty(prop.second, memgraph::storage::PropertyValue(4)).HasValue());

    ASSERT_TRUE(r1.SetProperty(prop.second, memgraph::storage::PropertyValue(1)).HasValue());
    memgraph::license::global_license_checker.EnableTesting();

    dba.AdvanceCommand();
  }

  int TestExists(std::string match_label, EdgeAtom::Direction direction,
                 std::vector<memgraph::storage::EdgeTypeId> edge_types,
                 std::optional<std::string> destination_label = std::nullopt,
                 std::optional<int64_t> destination_prop = std::nullopt,
                 std::optional<int64_t> edge_prop = std::nullopt) {
    std::vector<std::string> edge_type_names;
    for (const auto &type : edge_types) {
      edge_type_names.emplace_back(db.EdgeTypeToName(type));
    }

    auto *source_node = NODE("n");
    auto source_sym = symbol_table.CreateSymbol("n", true);
    source_node->identifier_->MapTo(source_sym);

    auto *expansion_edge = EDGE("edge", direction, edge_type_names, false);
    auto edge_sym = symbol_table.CreateSymbol("edge", false);
    expansion_edge->identifier_->MapTo(edge_sym);

    auto *destination_node = NODE("n2", destination_label, false);
    auto dest_sym = symbol_table.CreateSymbol("n2", false);
    destination_node->identifier_->MapTo(dest_sym);

    auto *exists_expression = EXISTS(PATTERN(source_node, expansion_edge, destination_node));
    exists_expression->MapTo(symbol_table.CreateAnonymousSymbol());

    auto scan_all = MakeScanAll(storage, symbol_table, "n");
    scan_all.node_->labels_.emplace_back(storage.GetLabelIx(match_label));

    std::shared_ptr<LogicalOperator> last_op = std::make_shared<Expand>(
        nullptr, scan_all.sym_, dest_sym, edge_sym, direction, edge_types, false, memgraph::storage::View::OLD);

    if (destination_label.has_value() || destination_prop.has_value() || edge_prop.has_value()) {
      Expression *filter_expr = nullptr;

      if (destination_label.has_value()) {
        auto labelIx = storage.GetLabelIx(destination_label.value());
        destination_node->labels_.emplace_back(labelIx);

        auto label_expr = static_cast<Expression *>(
            storage.Create<LabelsTest>(destination_node->identifier_, std::vector<memgraph::query::LabelIx>{labelIx}));

        filter_expr = filter_expr ? AND(filter_expr, label_expr) : label_expr;
      }

      if (destination_prop.has_value()) {
        auto prop_expr = static_cast<Expression *>(
            EQ(PROPERTY_LOOKUP(destination_node->identifier_, prop), LITERAL(destination_prop.value())));
        filter_expr = filter_expr ? AND(filter_expr, prop_expr) : prop_expr;
      }

      if (edge_prop.has_value()) {
        auto prop_expr = static_cast<Expression *>(
            EQ(PROPERTY_LOOKUP(expansion_edge->identifier_, prop), LITERAL(edge_prop.value())));
        filter_expr = filter_expr ? AND(filter_expr, prop_expr) : prop_expr;
      }

      last_op =
          std::make_shared<Filter>(std::move(last_op), std::vector<std::shared_ptr<LogicalOperator>>{}, filter_expr);
    }

    last_op = std::make_shared<Limit>(std::move(last_op), storage.Create<PrimitiveLiteral>(1));
    last_op = std::make_shared<EvaluatePatternFilter>(std::move(last_op), symbol_table.at(*exists_expression));

    auto *total_expression =
        AND(storage.Create<LabelsTest>(scan_all.node_->identifier_, scan_all.node_->labels_), exists_expression);

    auto filter = std::make_shared<Filter>(scan_all.op_, std::vector<std::shared_ptr<LogicalOperator>>{last_op},
                                           total_expression);
    auto output =
        NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));

    auto produce = MakeProduce(filter, output);
    auto context = MakeContext(storage, symbol_table, &dba);
    return PullAll(*produce, &context);
  }

  int TestDoubleExists(std::string match_label, EdgeAtom::Direction direction,
                       std::vector<memgraph::storage::EdgeTypeId> first_edge_type,
                       std::vector<memgraph::storage::EdgeTypeId> second_edge_type, bool or_flag = false) {
    std::vector<std::string> first_edge_type_names;
    for (const auto &type : first_edge_type) {
      first_edge_type_names.emplace_back(db.EdgeTypeToName(type));
    }

    std::vector<std::string> second_edge_type_names;
    for (const auto &type : second_edge_type) {
      second_edge_type_names.emplace_back(db.EdgeTypeToName(type));
    }

    auto *source_node = NODE("n");
    auto source_sym = symbol_table.CreateSymbol("n", true);
    source_node->identifier_->MapTo(source_sym);

    auto *expansion_edge = EDGE("edge", direction, first_edge_type_names, false);
    auto edge_sym = symbol_table.CreateSymbol("edge", false);
    expansion_edge->identifier_->MapTo(edge_sym);

    auto *destination_node = NODE("n2", std::nullopt, false);
    auto dest_sym = symbol_table.CreateSymbol("n2", false);
    destination_node->identifier_->MapTo(dest_sym);

    auto *exists_expression = EXISTS(PATTERN(source_node, expansion_edge, destination_node));
    exists_expression->MapTo(symbol_table.CreateAnonymousSymbol());

    auto *expansion_edge2 = EDGE("edge2", direction, second_edge_type_names, false);
    auto edge_sym2 = symbol_table.CreateSymbol("edge2", false);
    expansion_edge2->identifier_->MapTo(edge_sym2);

    auto *destination_node2 = NODE("n22", std::nullopt, false);
    auto dest_sym2 = symbol_table.CreateSymbol("n22", false);
    destination_node2->identifier_->MapTo(dest_sym2);

    auto *exists_expression2 = EXISTS(PATTERN(source_node, expansion_edge2, destination_node2));
    exists_expression2->MapTo(symbol_table.CreateAnonymousSymbol());

    auto scan_all = MakeScanAll(storage, symbol_table, "n");
    scan_all.node_->labels_.emplace_back(storage.GetLabelIx(match_label));

    std::shared_ptr<LogicalOperator> last_op = std::make_shared<Expand>(
        nullptr, scan_all.sym_, dest_sym, edge_sym, direction, first_edge_type, false, memgraph::storage::View::OLD);
    last_op = std::make_shared<Limit>(std::move(last_op), storage.Create<PrimitiveLiteral>(1));
    last_op = std::make_shared<EvaluatePatternFilter>(std::move(last_op), symbol_table.at(*exists_expression));

    std::shared_ptr<LogicalOperator> last_op2 = std::make_shared<Expand>(
        nullptr, scan_all.sym_, dest_sym2, edge_sym2, direction, second_edge_type, false, memgraph::storage::View::OLD);
    last_op2 = std::make_shared<Limit>(std::move(last_op2), storage.Create<PrimitiveLiteral>(1));
    last_op2 = std::make_shared<EvaluatePatternFilter>(std::move(last_op2), symbol_table.at(*exists_expression2));

    Expression *total_expression = storage.Create<LabelsTest>(scan_all.node_->identifier_, scan_all.node_->labels_);

    if (or_flag) {
      total_expression = AND(total_expression, OR(exists_expression, exists_expression2));
    } else {
      total_expression = AND(total_expression, AND(exists_expression, exists_expression2));
    }

    auto filter = std::make_shared<Filter>(
        scan_all.op_, std::vector<std::shared_ptr<LogicalOperator>>{last_op, last_op2}, total_expression);
    auto output =
        NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));

    auto produce = MakeProduce(filter, output);
    auto context = MakeContext(storage, symbol_table, &dba);
    return PullAll(*produce, &context);
  }
};

TEST_F(ExistsFixture, BasicExists) {
  std::vector<memgraph::storage::EdgeTypeId> known_edge_types;
  known_edge_types.push_back(edge_type);
  std::vector<memgraph::storage::EdgeTypeId> unknown_edge_types;
  unknown_edge_types.push_back(edge_type_unknown);

  EXPECT_EQ(1, TestExists("l1", EdgeAtom::Direction::OUT, {}));
  EXPECT_EQ(1, TestExists("l1", EdgeAtom::Direction::BOTH, {}));
  EXPECT_EQ(0, TestExists("l1", EdgeAtom::Direction::IN, {}));
  EXPECT_EQ(1, TestExists("l1", EdgeAtom::Direction::OUT, known_edge_types));
  EXPECT_EQ(0, TestExists("l1", EdgeAtom::Direction::OUT, unknown_edge_types));
}

TEST_F(ExistsFixture, ExistsWithFiltering) {
  EXPECT_EQ(1, TestExists("l1", EdgeAtom::Direction::BOTH, {}, "l2"));
  EXPECT_EQ(0, TestExists("l1", EdgeAtom::Direction::BOTH, {}, "l3"));

  EXPECT_EQ(1, TestExists("l1", EdgeAtom::Direction::BOTH, {}, "l2", 2));
  EXPECT_EQ(0, TestExists("l1", EdgeAtom::Direction::BOTH, {}, "l2", 1));

  EXPECT_EQ(1, TestExists("l1", EdgeAtom::Direction::BOTH, {}, "l2", std::nullopt, 1));
  EXPECT_EQ(0, TestExists("l1", EdgeAtom::Direction::BOTH, {}, "l2", std::nullopt, 2));

  EXPECT_EQ(1, TestExists("l1", EdgeAtom::Direction::BOTH, {}, "l2", 2, 1));
  EXPECT_EQ(0, TestExists("l1", EdgeAtom::Direction::BOTH, {}, "l2", 1, 1));
}

TEST_F(ExistsFixture, DoubleFilters) {
  EXPECT_EQ(1, TestDoubleExists("l1", EdgeAtom::Direction::BOTH, {}, {}, true));
  EXPECT_EQ(1, TestDoubleExists("l1", EdgeAtom::Direction::BOTH, {}, {}, false));
}

class SubqueriesFeature : public testing::Test {
 protected:
  memgraph::storage::Storage db;
  memgraph::storage::Storage::Accessor storage_dba{db.Access()};
  memgraph::query::DbAccessor dba{&storage_dba};
  AstStorage storage;
  SymbolTable symbol_table;

  std::pair<std::string, memgraph::storage::PropertyId> prop = PROPERTY_PAIR("property");

  memgraph::query::VertexAccessor v1{dba.InsertVertex()};
  memgraph::query::VertexAccessor v2{dba.InsertVertex()};
  memgraph::storage::EdgeTypeId edge_type{db.NameToEdgeType("Edge")};
  memgraph::query::EdgeAccessor r1{*dba.InsertEdge(&v1, &v2, edge_type)};

  void SetUp() override {
    // (:l1)-[:Edge]->(:l2)
    ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("l1")).HasValue());
    ASSERT_TRUE(v2.AddLabel(dba.NameToLabel("l2")).HasValue());

    ASSERT_TRUE(v1.SetProperty(prop.second, memgraph::storage::PropertyValue(1)).HasValue());
    ASSERT_TRUE(v2.SetProperty(prop.second, memgraph::storage::PropertyValue(2)).HasValue());

    ASSERT_TRUE(r1.SetProperty(prop.second, memgraph::storage::PropertyValue(1)).HasValue());
    memgraph::license::global_license_checker.EnableTesting();

    dba.AdvanceCommand();
  }
};

TEST_F(SubqueriesFeature, BasicCartesian) {
  // MATCH (n) CALL { MATCH (m) RETURN m } RETURN n, m

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto return_n = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));

  auto m = MakeScanAll(storage, symbol_table, "m");
  auto return_m = NEXPR("m", IDENT("m")->MapTo(m.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_2", true));
  auto produce_subquery = MakeProduce(m.op_, return_m);

  auto apply = std::make_shared<Apply>(n.op_, produce_subquery, true);

  auto produce = MakeProduce(apply, return_n, return_m);

  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 4);
}

TEST_F(SubqueriesFeature, BasicCartesianWithFilter) {
  // MATCH (n) WHERE n.prop = 2 CALL { MATCH (m) RETURN m } RETURN n, m

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto *filter_expr = AND(storage.Create<LabelsTest>(n.node_->identifier_, n.node_->labels_),
                          EQ(PROPERTY_LOOKUP(n.node_->identifier_, prop), LITERAL(2)));
  auto filter = std::make_shared<Filter>(n.op_, std::vector<std::shared_ptr<LogicalOperator>>{}, filter_expr);

  auto return_n = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));

  auto m = MakeScanAll(storage, symbol_table, "m");
  auto return_m = NEXPR("m", IDENT("m")->MapTo(m.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_2", true));
  auto produce_subquery = MakeProduce(m.op_, return_m);

  auto apply = std::make_shared<Apply>(filter, produce_subquery, true);

  auto produce = MakeProduce(apply, return_n, return_m);

  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 2);
}

TEST_F(SubqueriesFeature, BasicCartesianWithFilterInsideSubquery) {
  // MATCH (n) CALL { MATCH (m) WHERE m.prop = 2 RETURN m } RETURN n, m

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto return_n = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));

  auto m = MakeScanAll(storage, symbol_table, "m");
  auto *filter_expr = AND(storage.Create<LabelsTest>(n.node_->identifier_, n.node_->labels_),
                          EQ(PROPERTY_LOOKUP(n.node_->identifier_, prop), LITERAL(2)));
  auto filter = std::make_shared<Filter>(m.op_, std::vector<std::shared_ptr<LogicalOperator>>{}, filter_expr);

  auto return_m = NEXPR("m", IDENT("m")->MapTo(m.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_2", true));
  auto produce_subquery = MakeProduce(filter, return_m);

  auto apply = std::make_shared<Apply>(n.op_, produce_subquery, true);

  auto produce = MakeProduce(apply, return_n, return_m);

  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 2);
}

TEST_F(SubqueriesFeature, BasicCartesianWithFilterNoResults) {
  // MATCH (n) WHERE n.prop = 3 CALL { MATCH (m) RETURN m } RETURN n, m

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto *filter_expr = AND(storage.Create<LabelsTest>(n.node_->identifier_, n.node_->labels_),
                          EQ(PROPERTY_LOOKUP(n.node_->identifier_, prop), LITERAL(3)));
  auto filter = std::make_shared<Filter>(n.op_, std::vector<std::shared_ptr<LogicalOperator>>{}, filter_expr);

  auto return_n = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));

  auto m = MakeScanAll(storage, symbol_table, "m");
  auto return_m = NEXPR("m", IDENT("m")->MapTo(m.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_2", true));
  auto produce_subquery = MakeProduce(m.op_, return_m);

  auto apply = std::make_shared<Apply>(filter, produce_subquery, true);

  auto produce = MakeProduce(apply, return_n, return_m);

  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 0);
}

TEST_F(SubqueriesFeature, SubqueryInsideSubqueryCartesian) {
  // MATCH (n) CALL { MATCH (m) CALL { MATCH (o) RETURN o} RETURN m, o } RETURN n, m, o

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto return_n = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));

  auto m = MakeScanAll(storage, symbol_table, "m");
  auto return_m = NEXPR("m", IDENT("m")->MapTo(m.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_2", true));

  auto o = MakeScanAll(storage, symbol_table, "o");
  auto return_o = NEXPR("o", IDENT("o")->MapTo(o.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_3", true));
  auto produce_nested_subquery = MakeProduce(o.op_, return_o);

  auto inner_apply = std::make_shared<Apply>(m.op_, produce_nested_subquery, true);
  auto produce_subquery = MakeProduce(inner_apply, return_o, return_m);

  auto outer_apply = std::make_shared<Apply>(n.op_, produce_subquery, true);
  auto produce = MakeProduce(outer_apply, return_n, return_m, return_o);

  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);

  EXPECT_EQ(results.size(), 8);
}

TEST_F(SubqueriesFeature, UnitSubquery) {
  // CALL { MATCH (m) RETURN m } RETURN m

  auto once = std::make_shared<Once>();

  auto o = MakeScanAll(storage, symbol_table, "o");
  auto return_o = NEXPR("o", IDENT("o")->MapTo(o.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_3", true));
  auto produce_subquery = MakeProduce(o.op_, return_o);

  auto apply = std::make_shared<Apply>(once, produce_subquery, true);
  auto produce = MakeProduce(apply, return_o);

  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);

  EXPECT_EQ(results.size(), 2);
}

TEST_F(SubqueriesFeature, SubqueryWithBoundedSymbol) {
  // MATCH (n) CALL { WITH n MATCH (n)-[]->(m) RETURN m } RETURN n, m

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto return_n = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));

  auto once = std::make_shared<Once>();
  auto produce_with = MakeProduce(once, return_n);
  auto expand = MakeExpand(storage, symbol_table, produce_with, n.sym_, "r", EdgeAtom::Direction::OUT, {}, "m", false,
                           memgraph::storage::View::OLD);
  auto return_m =
      NEXPR("m", IDENT("m")->MapTo(expand.node_sym_))->MapTo(symbol_table.CreateSymbol("named_expression_3", true));
  auto produce_subquery = MakeProduce(expand.op_, return_m);

  auto apply = std::make_shared<Apply>(n.op_, produce_subquery, true);
  auto produce = MakeProduce(apply, return_n, return_m);

  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);

  EXPECT_EQ(results.size(), 1);
}

TEST_F(SubqueriesFeature, SubqueryWithUnionAll) {
  // MATCH (n) CALL { MATCH (m) RETURN m UNION ALL MATCH (m) RETURN m } RETURN n, m

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto return_n = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));

  auto m1 = MakeScanAll(storage, symbol_table, "m");
  auto return_m = NEXPR("m", IDENT("m")->MapTo(m1.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_2", true));
  auto produce_left_union_subquery = MakeProduce(m1.op_, return_m);

  auto m2 = MakeScanAll(storage, symbol_table, "m");
  auto produce_right_union_subquery = MakeProduce(m2.op_, return_m);

  auto union_operator =
      std::make_shared<Union>(produce_left_union_subquery, produce_right_union_subquery, std::vector<Symbol>{m1.sym_},
                              produce_left_union_subquery->OutputSymbols(symbol_table),
                              produce_right_union_subquery->OutputSymbols(symbol_table));

  auto apply = std::make_shared<Apply>(n.op_, union_operator, true);

  auto produce = MakeProduce(apply, return_n, return_m);

  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 8);
}

TEST_F(SubqueriesFeature, SubqueryWithUnion) {
  // MATCH (n) CALL { MATCH (m) RETURN m UNION MATCH (m) RETURN m } RETURN n, m

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto return_n = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));

  auto m1 = MakeScanAll(storage, symbol_table, "m");

  auto subquery_return_symbol = symbol_table.CreateSymbol("named_expression_2", true);
  auto return_m = NEXPR("m", IDENT("m")->MapTo(m1.sym_))->MapTo(subquery_return_symbol);

  auto produce_left_union_subquery = MakeProduce(m1.op_, return_m);

  auto m2 = MakeScanAll(storage, symbol_table, "m");
  auto produce_right_union_subquery = MakeProduce(m2.op_, return_m);

  auto union_operator = std::make_shared<Union>(produce_left_union_subquery, produce_right_union_subquery,
                                                std::vector<Symbol>{subquery_return_symbol},
                                                produce_left_union_subquery->OutputSymbols(symbol_table),
                                                produce_right_union_subquery->OutputSymbols(symbol_table));

  auto union_output_symbols = union_operator->OutputSymbols(symbol_table);
  auto distinct = std::make_shared<Distinct>(union_operator, std::vector<Symbol>{union_output_symbols});

  auto apply = std::make_shared<Apply>(n.op_, distinct, true);

  auto produce = MakeProduce(apply, return_n);

  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 4);
}

TEST_F(SubqueriesFeature, SubqueriesWithForeach) {
  // MATCH (n) CALL { FOREACH (i in range(1, 5) | CREATE (n)) } RETURN n

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto return_n = NEXPR("n", IDENT("n")->MapTo(n.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));

  auto once_create = std::make_shared<Once>();
  NodeCreationInfo node_creation_info;
  node_creation_info.symbol = symbol_table.CreateSymbol("n", true);
  auto create = std::make_shared<plan::CreateNode>(once_create, node_creation_info);

  auto once_foreach = std::make_shared<Once>();
  auto iteration_symbol = symbol_table.CreateSymbol("i", true);
  auto iterating_list = LIST(LITERAL(1), LITERAL(2), LITERAL(3), LITERAL(4), LITERAL(5));
  auto foreach = std::make_shared<plan::Foreach>(once_foreach, create, iterating_list, iteration_symbol);
  auto empty_result = std::make_shared<EmptyResult>(foreach);

  auto apply = std::make_shared<Apply>(n.op_, empty_result, false);

  auto produce = MakeProduce(apply, return_n);

  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 2);
}

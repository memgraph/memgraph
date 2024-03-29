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

#include <iterator>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "auth/models.hpp"
#include "disk_test_utils.hpp"
#include "glue/auth_checker.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "license/license.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/frame.hpp"
#include "query/plan/operator.hpp"

#include "query_plan_common.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;
using memgraph::replication_coordination_glue::ReplicationRole;

template <typename StorageType>
class QueryPlanTest : public testing::Test {
 public:
  const std::string testSuite = "query_plan_create_set_remove_delete";
  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db = std::make_unique<StorageType>(config);
  AstStorage storage;

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(QueryPlanTest, StorageTypes);

TYPED_TEST(QueryPlanTest, CreateNodeWithAttributes) {
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());

  memgraph::storage::LabelId label = dba.NameToLabel("Person");
  auto property = PROPERTY_PAIR(dba, "prop");

  SymbolTable symbol_table;
  NodeCreationInfo node;
  node.symbol = symbol_table.CreateSymbol("n", true);
  node.labels.emplace_back(label);
  std::get<std::vector<std::pair<memgraph::storage::PropertyId, Expression *>>>(node.properties)
      .emplace_back(property.second, LITERAL(42));

  auto create = std::make_shared<CreateNode>(nullptr, node);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  PullAll(*create, &context);
  dba.AdvanceCommand();

  // count the number of vertices
  int vertex_count = 0;
  for (auto vertex : dba.Vertices(memgraph::storage::View::OLD)) {
    vertex_count++;
    auto maybe_labels = vertex.Labels(memgraph::storage::View::OLD);
    ASSERT_TRUE(maybe_labels.HasValue());
    const auto &labels = *maybe_labels;
    EXPECT_EQ(labels.size(), 1);
    EXPECT_EQ(*labels.begin(), label);
    auto maybe_properties = vertex.Properties(memgraph::storage::View::OLD);
    ASSERT_TRUE(maybe_properties.HasValue());
    const auto &properties = *maybe_properties;
    EXPECT_EQ(properties.size(), 1);
    auto maybe_prop = vertex.GetProperty(memgraph::storage::View::OLD, property.second);
    ASSERT_TRUE(maybe_prop.HasValue());
    auto prop_eq = TypedValue(*maybe_prop) == TypedValue(42);
    ASSERT_EQ(prop_eq.type(), TypedValue::Type::Bool);
    EXPECT_TRUE(prop_eq.ValueBool());
  }
  EXPECT_EQ(vertex_count, 1);
}

#ifdef MG_ENTERPRISE
TYPED_TEST(QueryPlanTest, FineGrainedCreateNodeWithAttributes) {
  memgraph::license::global_license_checker.EnableTesting();
  memgraph::query::SymbolTable symbol_table;
  auto dba = this->db->Access(ReplicationRole::MAIN);
  DbAccessor execution_dba(dba.get());
  const auto label = dba->NameToLabel("label1");
  const auto property = memgraph::storage::PropertyId::FromInt(1);

  memgraph::query::plan::NodeCreationInfo node;
  std::get<std::vector<std::pair<memgraph::storage::PropertyId, Expression *>>>(node.properties)
      .emplace_back(property, this->storage.template Create<PrimitiveLiteral>(42));

  node.symbol = symbol_table.CreateSymbol("n", true);
  node.labels.emplace_back(label);

  const auto test_create = [&](memgraph::auth::User &user) {
    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &execution_dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &execution_dba, &auth_checker);
    auto create = std::make_shared<CreateNode>(nullptr, node);

    return PullAll(*create, &context);
  };

  // Granted label
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("label1",
                                                                 memgraph::auth::FineGrainedPermission::CREATE_DELETE);
    EXPECT_EQ(test_create(user), 1);
  }

  // Denied label
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("label1",
                                                                 memgraph::auth::FineGrainedPermission::UPDATE);
    ASSERT_THROW(test_create(user), QueryRuntimeException);
  }
}
#endif

TYPED_TEST(QueryPlanTest, CreateReturn) {
  // test CREATE (n:Person {age: 42}) RETURN n, n.age
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());

  memgraph::storage::LabelId label = dba.NameToLabel("Person");
  auto property = PROPERTY_PAIR(dba, "property");

  SymbolTable symbol_table;
  NodeCreationInfo node;
  node.symbol = symbol_table.CreateSymbol("n", true);
  node.labels.emplace_back(label);
  std::get<std::vector<std::pair<memgraph::storage::PropertyId, Expression *>>>(node.properties)
      .emplace_back(property.second, LITERAL(42));

  auto create = std::make_shared<CreateNode>(nullptr, node);
  auto named_expr_n =
      NEXPR("n", IDENT("n")->MapTo(node.symbol))->MapTo(symbol_table.CreateSymbol("named_expr_n", true));
  auto prop_lookup = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(node.symbol), property);
  auto named_expr_n_p = NEXPR("n", prop_lookup)->MapTo(symbol_table.CreateSymbol("named_expr_n_p", true));

  auto produce = MakeProduce(create, named_expr_n, named_expr_n_p);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(2, results[0].size());
  EXPECT_EQ(TypedValue::Type::Vertex, results[0][0].type());
  auto maybe_labels = results[0][0].ValueVertex().Labels(memgraph::storage::View::NEW);
  EXPECT_EQ(1, maybe_labels->size());
  EXPECT_EQ(label, (*maybe_labels)[0]);
  EXPECT_EQ(TypedValue::Type::Int, results[0][1].type());
  EXPECT_EQ(42, results[0][1].ValueInt());

  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
}

#ifdef MG_ENTERPRISE
TYPED_TEST(QueryPlanTest, FineGrainedCreateReturn) {
  memgraph::license::global_license_checker.EnableTesting();

  // test CREATE (n:Person {age: 42}) RETURN n, n.age
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());

  const auto label = dba.NameToLabel("label");
  const auto property = PROPERTY_PAIR(dba, "property");

  SymbolTable symbol_table;
  NodeCreationInfo node;
  node.symbol = symbol_table.CreateSymbol("n", true);
  node.labels.emplace_back(label);
  std::get<std::vector<std::pair<memgraph::storage::PropertyId, Expression *>>>(node.properties)
      .emplace_back(property.second, LITERAL(42));

  auto create = std::make_shared<CreateNode>(nullptr, node);
  auto named_expr_n =
      NEXPR("n", IDENT("n")->MapTo(node.symbol))->MapTo(symbol_table.CreateSymbol("named_expr_n", true));
  auto prop_lookup = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(node.symbol), property);
  auto named_expr_n_p = NEXPR("n", prop_lookup)->MapTo(symbol_table.CreateSymbol("named_expr_n_p", true));

  auto produce = MakeProduce(create, named_expr_n, named_expr_n_p);

  // Granted label
  {
    memgraph::auth::User user{"Test"};
    user.fine_grained_access_handler().label_permissions().Grant("label",
                                                                 memgraph::auth::FineGrainedPermission::CREATE_DELETE);
    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);
    auto results = CollectProduce(*produce, &context);
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(2, results[0].size());
    EXPECT_EQ(TypedValue::Type::Vertex, results[0][0].type());
    auto maybe_labels = results[0][0].ValueVertex().Labels(memgraph::storage::View::NEW);
    EXPECT_EQ(1, maybe_labels->size());
    EXPECT_EQ(label, (*maybe_labels)[0]);
    EXPECT_EQ(TypedValue::Type::Int, results[0][1].type());
    EXPECT_EQ(42, results[0][1].ValueInt());

    dba.AdvanceCommand();
    EXPECT_EQ(1, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  }

  // Denied label
  {
    memgraph::auth::User user{"Test"};
    user.fine_grained_access_handler().label_permissions().Grant("label",
                                                                 memgraph::auth::FineGrainedPermission::UPDATE);
    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);
    ASSERT_THROW(CollectProduce(*produce, &context), QueryRuntimeException);
  }
}
#endif

TYPED_TEST(QueryPlanTest, CreateExpand) {
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());

  memgraph::storage::LabelId label_node_1 = dba.NameToLabel("Node1");
  memgraph::storage::LabelId label_node_2 = dba.NameToLabel("Node2");
  auto property = PROPERTY_PAIR(dba, "property");
  memgraph::storage::EdgeTypeId edge_type = dba.NameToEdgeType("edge_type");

  SymbolTable symbol_table;

  auto test_create_path = [&](bool cycle, int expected_nodes_created, int expected_edges_created) {
    int before_v = CountIterable(dba.Vertices(memgraph::storage::View::OLD));
    int before_e = CountEdges(&dba, memgraph::storage::View::OLD);

    // data for the first node
    NodeCreationInfo n;
    n.symbol = symbol_table.CreateSymbol("n", true);
    n.labels.emplace_back(label_node_1);
    std::get<std::vector<std::pair<memgraph::storage::PropertyId, Expression *>>>(n.properties)
        .emplace_back(property.second, LITERAL(1));

    // data for the second node
    NodeCreationInfo m;
    m.symbol = cycle ? n.symbol : symbol_table.CreateSymbol("m", true);
    m.labels.emplace_back(label_node_2);
    std::get<std::vector<std::pair<memgraph::storage::PropertyId, Expression *>>>(m.properties)
        .emplace_back(property.second, LITERAL(2));

    EdgeCreationInfo r;
    r.symbol = symbol_table.CreateSymbol("r", true);
    r.edge_type = edge_type;
    std::get<0>(r.properties).emplace_back(property.second, LITERAL(3));

    auto create_op = std::make_shared<CreateNode>(nullptr, n);
    auto create_expand = std::make_shared<CreateExpand>(m, r, create_op, n.symbol, cycle);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    PullAll(*create_expand, &context);
    dba.AdvanceCommand();

    EXPECT_EQ(CountIterable(dba.Vertices(memgraph::storage::View::OLD)) - before_v, expected_nodes_created);
    EXPECT_EQ(CountEdges(&dba, memgraph::storage::View::OLD) - before_e, expected_edges_created);
  };

  test_create_path(false, 2, 1);
  test_create_path(true, 1, 1);

  for (auto vertex : dba.Vertices(memgraph::storage::View::OLD)) {
    auto maybe_labels = vertex.Labels(memgraph::storage::View::OLD);
    MG_ASSERT(maybe_labels.HasValue());
    const auto &labels = *maybe_labels;
    EXPECT_EQ(labels.size(), 1);
    memgraph::storage::LabelId label = labels[0];
    if (label == label_node_1) {
      // node created by first op
      EXPECT_EQ(vertex.GetProperty(memgraph::storage::View::OLD, property.second)->ValueInt(), 1);
    } else if (label == label_node_2) {
      // node create by expansion
      EXPECT_EQ(vertex.GetProperty(memgraph::storage::View::OLD, property.second)->ValueInt(), 2);
    } else {
      // should not happen
      FAIL();
    }

    for (auto vertex : dba.Vertices(memgraph::storage::View::OLD)) {
      auto maybe_edges = vertex.OutEdges(memgraph::storage::View::OLD);
      MG_ASSERT(maybe_edges.HasValue());
      for (auto edge : maybe_edges->edges) {
        EXPECT_EQ(edge.EdgeType(), edge_type);
        EXPECT_EQ(edge.GetProperty(memgraph::storage::View::OLD, property.second)->ValueInt(), 3);
      }
    }
  }
}

#ifdef MG_ENTERPRISE
template <typename StorageType>
class CreateExpandWithAuthFixture : public QueryPlanTest<StorageType> {
 protected:
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba{this->db->Access(ReplicationRole::MAIN)};
  memgraph::query::DbAccessor dba{storage_dba.get()};
  SymbolTable symbol_table;

  void SetUp() override { memgraph::license::global_license_checker.EnableTesting(); }

  void ExecuteCreateExpand(bool cycle, memgraph::auth::User &user) {
    const auto label_node_1 = dba.NameToLabel("Node1");
    const auto label_node_2 = dba.NameToLabel("Node2");
    const auto property = PROPERTY_PAIR(dba, "property");
    const auto edge_type = dba.NameToEdgeType("edge_type");

    // data for the first node
    NodeCreationInfo n;
    n.symbol = symbol_table.CreateSymbol("n", true);
    n.labels.emplace_back(label_node_1);
    std::get<std::vector<std::pair<memgraph::storage::PropertyId, Expression *>>>(n.properties)
        .emplace_back(property.second, LITERAL(1));

    // data for the second node
    NodeCreationInfo m;
    m.symbol = cycle ? n.symbol : symbol_table.CreateSymbol("m", true);
    m.labels.emplace_back(label_node_2);
    std::get<std::vector<std::pair<memgraph::storage::PropertyId, Expression *>>>(m.properties)
        .emplace_back(property.second, LITERAL(2));

    EdgeCreationInfo r;
    r.symbol = symbol_table.CreateSymbol("r", true);
    r.edge_type = edge_type;
    std::get<0>(r.properties).emplace_back(property.second, LITERAL(3));

    auto create_op = std::make_shared<CreateNode>(nullptr, n);
    auto create_expand = std::make_shared<CreateExpand>(m, r, create_op, n.symbol, cycle);
    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);
    PullAll(*create_expand, &context);
    dba.AdvanceCommand();
  }

  void TestCreateExpandHypothesis(int expected_nodes_created, int expected_edges_created) {
    EXPECT_EQ(CountIterable(dba.Vertices(memgraph::storage::View::NEW)), expected_nodes_created);
    EXPECT_EQ(CountEdges(&dba, memgraph::storage::View::NEW), expected_edges_created);
  }
};

TYPED_TEST_CASE(CreateExpandWithAuthFixture, StorageTypes);

TYPED_TEST(CreateExpandWithAuthFixture, CreateExpandWithNoGrantsOnCreateDelete) {
  // All labels denied, All edge types denied
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);
  user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);
  ASSERT_THROW(this->ExecuteCreateExpand(false, user), QueryRuntimeException);
  ASSERT_THROW(this->ExecuteCreateExpand(true, user), QueryRuntimeException);
}

TYPED_TEST(CreateExpandWithAuthFixture, CreateExpandWithLabelsGrantedOnly) {
  // All labels granted, All edge types denied
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);

  ASSERT_THROW(this->ExecuteCreateExpand(false, user), QueryRuntimeException);
  ASSERT_THROW(this->ExecuteCreateExpand(true, user), QueryRuntimeException);
}

TYPED_TEST(CreateExpandWithAuthFixture, CreateExpandWithEdgeTypesGrantedOnly) {
  // All labels denied, All edge types granted
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "*", memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  ASSERT_THROW(this->ExecuteCreateExpand(false, user), QueryRuntimeException);
  ASSERT_THROW(this->ExecuteCreateExpand(true, user), QueryRuntimeException);
}

TYPED_TEST(CreateExpandWithAuthFixture, CreateExpandWithFirstLabelGranted) {
  // First label granted, All edge types granted
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("Node1",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().label_permissions().Grant("Node2", memgraph::auth::FineGrainedPermission::UPDATE);
  user.fine_grained_access_handler().label_permissions().Grant("Node2", memgraph::auth::FineGrainedPermission::READ);
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "*", memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  ASSERT_THROW(this->ExecuteCreateExpand(false, user), QueryRuntimeException);
  ASSERT_THROW(this->ExecuteCreateExpand(true, user), QueryRuntimeException);
}

TYPED_TEST(CreateExpandWithAuthFixture, CreateExpandWithSecondLabelGranted) {
  // Second label granted, All edge types granted
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("Node2",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().label_permissions().Grant("Node1", memgraph::auth::FineGrainedPermission::UPDATE);
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "*", memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  ASSERT_THROW(this->ExecuteCreateExpand(false, user), QueryRuntimeException);
  ASSERT_THROW(this->ExecuteCreateExpand(true, user), QueryRuntimeException);
}

TYPED_TEST(CreateExpandWithAuthFixture, CreateExpandWithoutCycleWithEverythingGranted) {
  // All labels granted, All edge types granted
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "*", memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  this->ExecuteCreateExpand(false, user);
  this->TestCreateExpandHypothesis(2, 1);
}

TYPED_TEST(CreateExpandWithAuthFixture, CreateExpandWithCycleWithEverythingGranted) {
  // All labels granted, All edge types granted
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "*", memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  this->ExecuteCreateExpand(true, user);
  this->TestCreateExpandHypothesis(1, 1);
}

TYPED_TEST(QueryPlanTest, MatchCreateNode) {
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());

  // add three nodes we'll match and expand-create from
  dba.InsertVertex();
  dba.InsertVertex();
  dba.InsertVertex();
  dba.AdvanceCommand();

  SymbolTable symbol_table;
  // first node
  auto n_scan_all = MakeScanAll(this->storage, symbol_table, "n");
  // second node
  NodeCreationInfo m;
  m.symbol = symbol_table.CreateSymbol("m", true);
  // creation op
  auto create_node = std::make_shared<CreateNode>(n_scan_all.op_, m);

  EXPECT_EQ(CountIterable(dba.Vertices(memgraph::storage::View::OLD)), 3);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  PullAll(*create_node, &context);
  dba.AdvanceCommand();
  EXPECT_EQ(CountIterable(dba.Vertices(memgraph::storage::View::OLD)), 6);
}

template <typename StorageType>
class MatchCreateNodeWithAuthFixture : public QueryPlanTest<StorageType> {
 protected:
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba{this->db->Access(ReplicationRole::MAIN)};
  memgraph::query::DbAccessor dba{storage_dba.get()};
  SymbolTable symbol_table;

  void SetUp() override { memgraph::license::global_license_checker.EnableTesting(); }

  void InitGraph() {
    // add three nodes we'll match and expand-create from
    memgraph::query::VertexAccessor v1{dba.InsertVertex()};
    memgraph::query::VertexAccessor v2{dba.InsertVertex()};
    memgraph::query::VertexAccessor v3{dba.InsertVertex()};
    ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("l1")).HasValue());
    ASSERT_TRUE(v2.AddLabel(dba.NameToLabel("l2")).HasValue());
    ASSERT_TRUE(v3.AddLabel(dba.NameToLabel("l3")).HasValue());
    dba.AdvanceCommand();
  }

  void ExecuteMatchCreate(memgraph::auth::User &user) {
    auto n_scan_all = MakeScanAll(this->storage, symbol_table, "n");
    // second node
    NodeCreationInfo m{};

    m.symbol = symbol_table.CreateSymbol("m", true);
    std::vector<StorageLabelType> labels{dba.NameToLabel("l2")};
    m.labels = labels;
    // creation op
    auto create_node = std::make_shared<CreateNode>(n_scan_all.op_, m);
    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);

    PullAll(*create_node, &context);
    dba.AdvanceCommand();
  }

  void MatchCreateAssertion(int expected_result_size) {
    EXPECT_EQ(CountIterable(dba.Vertices(memgraph::storage::View::OLD)), expected_result_size);
  }

  void ExecuteMatchCreateTestSuite(memgraph::auth::User &user, int expected_result_size) {
    InitGraph();
    ExecuteMatchCreate(user);
    MatchCreateAssertion(expected_result_size);
  }
};

TYPED_TEST_CASE(MatchCreateNodeWithAuthFixture, StorageTypes);

TYPED_TEST(MatchCreateNodeWithAuthFixture, MatchCreateWithAllLabelsDeniedThrows) {
  // All labels denied
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);

  ASSERT_THROW(this->ExecuteMatchCreateTestSuite(user, 3), QueryRuntimeException);
}

TYPED_TEST(MatchCreateNodeWithAuthFixture, MatchCreateWithAllLabelsGrantedExecutes) {
  // All labels granteddenieddenieddenied

  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  this->ExecuteMatchCreateTestSuite(user, 6);
}

TYPED_TEST(MatchCreateNodeWithAuthFixture, MatchCreateWithOneLabelDeniedThrows) {
  // Label2 denied
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("l1",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().label_permissions().Grant("l3",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  user.fine_grained_access_handler().label_permissions().Grant("l2", memgraph::auth::FineGrainedPermission::UPDATE);

  ASSERT_THROW(this->ExecuteMatchCreateTestSuite(user, 3), QueryRuntimeException);
}
#endif

TYPED_TEST(QueryPlanTest, MatchCreateExpand) {
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());

  // add three nodes we'll match and expand-create from
  dba.InsertVertex();
  dba.InsertVertex();
  dba.InsertVertex();
  dba.AdvanceCommand();

  //  memgraph::storage::LabelId label_node_1 = dba.NameToLabel("Node1");
  //  memgraph::storage::LabelId label_node_2 = dba.NameToLabel("Node2");
  //  memgraph::storage::PropertyId property = dba.NameToLabel("prop");
  memgraph::storage::EdgeTypeId edge_type = dba.NameToEdgeType("edge_type");
  SymbolTable symbol_table;

  auto test_create_path = [&](bool cycle, int expected_nodes_created, int expected_edges_created) {
    int before_v = CountIterable(dba.Vertices(memgraph::storage::View::OLD));
    int before_e = CountEdges(&dba, memgraph::storage::View::OLD);

    // data for the first node
    auto n_scan_all = MakeScanAll(this->storage, symbol_table, "n");

    // data for the second node
    NodeCreationInfo m;
    m.symbol = cycle ? n_scan_all.sym_ : symbol_table.CreateSymbol("m", true);

    EdgeCreationInfo r;
    r.symbol = symbol_table.CreateSymbol("r", true);
    r.direction = EdgeAtom::Direction::OUT;
    r.edge_type = edge_type;

    auto create_expand = std::make_shared<CreateExpand>(m, r, n_scan_all.op_, n_scan_all.sym_, cycle);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    PullAll(*create_expand, &context);
    dba.AdvanceCommand();

    EXPECT_EQ(CountIterable(dba.Vertices(memgraph::storage::View::OLD)) - before_v, expected_nodes_created);
    EXPECT_EQ(CountEdges(&dba, memgraph::storage::View::OLD) - before_e, expected_edges_created);
  };

  test_create_path(false, 3, 3);
  test_create_path(true, 0, 6);
}

#ifdef MG_ENTERPRISE
template <typename StorageType>
class MatchCreateExpandWithAuthFixture : public QueryPlanTest<StorageType> {
 protected:
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba{this->db->Access(ReplicationRole::MAIN)};
  memgraph::query::DbAccessor dba{storage_dba.get()};
  SymbolTable symbol_table;

  void SetUp() override { memgraph::license::global_license_checker.EnableTesting(); }

  void InitGraph() {
    // add three nodes we'll match and expand-create from
    memgraph::query::VertexAccessor v1{dba.InsertVertex()};
    memgraph::query::VertexAccessor v2{dba.InsertVertex()};
    memgraph::query::VertexAccessor v3{dba.InsertVertex()};
    ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("l1")).HasValue());
    ASSERT_TRUE(v2.AddLabel(dba.NameToLabel("l2")).HasValue());
    ASSERT_TRUE(v3.AddLabel(dba.NameToLabel("l3")).HasValue());

    dba.AdvanceCommand();
  }

  void ExecuteMatchCreateExpand(memgraph::auth::User &user, bool cycle) {
    // data for the first node
    auto n_scan_all = MakeScanAll(this->storage, symbol_table, "n");

    // data for the second node
    NodeCreationInfo m;
    m.symbol = cycle ? n_scan_all.sym_ : symbol_table.CreateSymbol("m", true);
    std::vector<StorageLabelType> labels{dba.NameToLabel("l2")};
    m.labels = labels;

    EdgeCreationInfo r;
    r.symbol = symbol_table.CreateSymbol("r", true);
    r.direction = EdgeAtom::Direction::OUT;
    r.edge_type = dba.NameToEdgeType("edge_type");
    ;

    auto create_expand = std::make_shared<CreateExpand>(m, r, n_scan_all.op_, n_scan_all.sym_, cycle);
    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);
    PullAll(*create_expand, &context);
    dba.AdvanceCommand();
  }

  void MatchCreateExpandAssertion(int expected_nodes_size, int expected_edges_size) {
    EXPECT_EQ(CountIterable(dba.Vertices(memgraph::storage::View::NEW)), expected_nodes_size);
    EXPECT_EQ(CountEdges(&dba, memgraph::storage::View::NEW), expected_edges_size);
  }

  void ExecuteMatchCreateExpandTestSuite(bool cycle, int expected_nodes_size, int expected_edges_size,
                                         memgraph::auth::User &user) {
    InitGraph();
    ExecuteMatchCreateExpand(user, cycle);
    MatchCreateExpandAssertion(expected_nodes_size, expected_edges_size);
  }
};

TYPED_TEST_CASE(MatchCreateExpandWithAuthFixture, StorageTypes);

TYPED_TEST(MatchCreateExpandWithAuthFixture, MatchCreateExpandThrowsWhenDeniedEverything) {
  // All labels denied, All edge types denied
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);
  user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);
  ASSERT_THROW(this->ExecuteMatchCreateExpandTestSuite(false, 0, 0, user), QueryRuntimeException);
  ASSERT_THROW(this->ExecuteMatchCreateExpandTestSuite(true, 0, 0, user), QueryRuntimeException);
}

TYPED_TEST(MatchCreateExpandWithAuthFixture, MatchCreateExpandThrowsWhenDeniedEdgeTypes) {
  // All labels granted, All edge types denied
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);
  ASSERT_THROW(this->ExecuteMatchCreateExpandTestSuite(false, 0, 0, user), QueryRuntimeException);
  ASSERT_THROW(this->ExecuteMatchCreateExpandTestSuite(true, 0, 0, user), QueryRuntimeException);
}

TYPED_TEST(MatchCreateExpandWithAuthFixture, MatchCreateExpandThrowsWhenDeniedLabels) {
  // All labels denied, All edge types granted
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "*", memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  ASSERT_THROW(this->ExecuteMatchCreateExpandTestSuite(false, 0, 0, user), QueryRuntimeException);
  ASSERT_THROW(this->ExecuteMatchCreateExpandTestSuite(true, 0, 0, user), QueryRuntimeException);
}

TYPED_TEST(MatchCreateExpandWithAuthFixture, MatchCreateExpandThrowsWhenDeniedOneLabel) {
  // First two label granted, All edge types granted
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("l1", memgraph::auth::FineGrainedPermission::UPDATE);
  user.fine_grained_access_handler().label_permissions().Grant("l3", memgraph::auth::FineGrainedPermission::UPDATE);
  user.fine_grained_access_handler().label_permissions().Grant("l2", memgraph::auth::FineGrainedPermission::READ);

  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "*", memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  ASSERT_THROW(this->ExecuteMatchCreateExpandTestSuite(false, 0, 0, user), QueryRuntimeException);
  ASSERT_THROW(this->ExecuteMatchCreateExpandTestSuite(true, 0, 0, user), QueryRuntimeException);
}

TYPED_TEST(MatchCreateExpandWithAuthFixture, MatchCreateExpandWithoutCycleExecutesWhenGrantedSpecificallyEverything) {
  // All label granted, Specific edge type granted
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "edge_type", memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  this->ExecuteMatchCreateExpandTestSuite(false, 6, 3, user);
}

TYPED_TEST(MatchCreateExpandWithAuthFixture, MatchCreateExpandWithCycleExecutesWhenGrantedSpecificallyEverything) {
  // All label granted, Specific edge type granted
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "edge_type", memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  this->ExecuteMatchCreateExpandTestSuite(true, 3, 3, user);
}

TYPED_TEST(MatchCreateExpandWithAuthFixture, MatchCreateExpandWithoutCycleExecutesWhenGrantedEverything) {
  // All labels granted, All edge types granted
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "*", memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  this->ExecuteMatchCreateExpandTestSuite(false, 6, 3, user);
}

TYPED_TEST(MatchCreateExpandWithAuthFixture, MatchCreateExpandWithCycleExecutesWhenGrantedEverything) {
  // All labels granted, All edge types granted
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "*", memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  this->ExecuteMatchCreateExpandTestSuite(true, 3, 3, user);
}
#endif

TYPED_TEST(QueryPlanTest, Delete) {
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());

  // make a fully-connected (one-direction, no cycles) with 4 nodes
  std::vector<memgraph::query::VertexAccessor> vertices;
  for (int i = 0; i < 4; ++i) vertices.push_back(dba.InsertVertex());
  auto type = dba.NameToEdgeType("type");
  for (int j = 0; j < 4; ++j)
    for (int k = j + 1; k < 4; ++k) ASSERT_TRUE(dba.InsertEdge(&vertices[j], &vertices[k], type).HasValue());

  dba.AdvanceCommand();
  EXPECT_EQ(4, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  EXPECT_EQ(6, CountEdges(&dba, memgraph::storage::View::OLD));

  SymbolTable symbol_table;
  // attempt to delete a vertex, and fail
  {
    auto n = MakeScanAll(this->storage, symbol_table, "n");
    auto n_get = this->storage.template Create<Identifier>("n")->MapTo(n.sym_);
    auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    EXPECT_THROW(PullAll(*delete_op, &context), QueryRuntimeException);
    dba.AdvanceCommand();
    EXPECT_EQ(4, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
    EXPECT_EQ(6, CountEdges(&dba, memgraph::storage::View::OLD));
  }

  // detach delete a single vertex
  // delete will not happen as we are deleting in bulk
  {
    auto n = MakeScanAll(this->storage, symbol_table, "n");
    auto n_get = this->storage.template Create<Identifier>("n")->MapTo(n.sym_);
    auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, true);
    Frame frame(symbol_table.max_position());
    auto context = MakeContext(this->storage, symbol_table, &dba);
    delete_op->MakeCursor(memgraph::utils::NewDeleteResource())->Pull(frame, context);
    dba.AdvanceCommand();
    EXPECT_EQ(4, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
    EXPECT_EQ(6, CountEdges(&dba, memgraph::storage::View::OLD));
  }

  // delete all remaining edges
  {
    auto n = MakeScanAll(this->storage, symbol_table, "n");
    auto r_m = MakeExpand(this->storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::OUT, {}, "m", false,
                          memgraph::storage::View::NEW);
    auto r_get = this->storage.template Create<Identifier>("r")->MapTo(r_m.edge_sym_);
    auto delete_op = std::make_shared<plan::Delete>(r_m.op_, std::vector<Expression *>{r_get}, false);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    PullAll(*delete_op, &context);
    dba.AdvanceCommand();
    EXPECT_EQ(4, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
    EXPECT_EQ(0, CountEdges(&dba, memgraph::storage::View::OLD));
  }

  // delete all remaining vertices
  {
    auto n = MakeScanAll(this->storage, symbol_table, "n");
    auto n_get = this->storage.template Create<Identifier>("n")->MapTo(n.sym_);
    auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    PullAll(*delete_op, &context);
    dba.AdvanceCommand();
    EXPECT_EQ(0, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
    EXPECT_EQ(0, CountEdges(&dba, memgraph::storage::View::OLD));
  }
}

#ifdef MG_ENTERPRISE
template <typename StorageType>
class DeleteOperatorWithAuthFixture : public QueryPlanTest<StorageType> {
 protected:
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba{this->db->Access(ReplicationRole::MAIN)};
  memgraph::query::DbAccessor dba{storage_dba.get()};
  SymbolTable symbol_table;

  void SetUp() override { memgraph::license::global_license_checker.EnableTesting(); }

  void InitGraph() {
    std::vector<memgraph::query::VertexAccessor> vertices;
    for (int i = 0; i < 4; ++i) {
      memgraph::query::VertexAccessor v{dba.InsertVertex()};
      auto label = "l" + std::to_string(i);
      ASSERT_TRUE(v.AddLabel(dba.NameToLabel(label)).HasValue());
      vertices.push_back(v);
    }
    for (int j = 0; j < 4; ++j) {
      auto edge_type = "type" + std::to_string(j);
      auto type = dba.NameToEdgeType(edge_type);
      for (int k = j + 1; k < 4; ++k) ASSERT_TRUE(dba.InsertEdge(&vertices[j], &vertices[k], type).HasValue());
    }

    dba.AdvanceCommand();

    assertInitGraphValid();
  }

  void TestDeleteNodesHypothesis(int expected_result_size) {
    EXPECT_EQ(expected_result_size, CountIterable(dba.Vertices(memgraph::storage::View::NEW)));
  };

  void DeleteAllNodes(memgraph::auth::User &user) {
    auto n = MakeScanAll(this->storage, symbol_table, "n");
    auto n_get = this->storage.template Create<Identifier>("n")->MapTo(n.sym_);
    Frame frame(symbol_table.max_position());
    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);
    auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, true);
    PullAll(*delete_op, &context);
    dba.AdvanceCommand();
  };

  void ExecuteDeleteNodesTestSuite(memgraph::auth::User &user, int expected_nodes) {
    // make a fully-connected (one-direction, no cycles) with 4 nodes
    InitGraph();
    DeleteAllNodes(user);
    TestDeleteNodesHypothesis(expected_nodes);
  };

  void TestDeleteEdgesHypothesis(int expected_result_size) {
    EXPECT_EQ(expected_result_size, CountEdges(&dba, memgraph::storage::View::NEW));
  };

  void DeleteAllEdges(memgraph::auth::User &user) {
    auto n = MakeScanAll(this->storage, symbol_table, "n");
    auto r_m = MakeExpand(this->storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::OUT, {}, "m", false,
                          memgraph::storage::View::NEW);
    auto r_get = this->storage.template Create<Identifier>("r")->MapTo(r_m.edge_sym_);
    auto delete_op = std::make_shared<plan::Delete>(r_m.op_, std::vector<Expression *>{r_get}, false);
    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);
    PullAll(*delete_op, &context);
    dba.AdvanceCommand();
  };

  void ExecuteDeleteEdgesTestSuite(memgraph::auth::User &user, int expected_edges) {
    // make a fully-connected (one-direction, no cycles) with 4 nodes
    InitGraph();
    DeleteAllEdges(user);
    TestDeleteEdgesHypothesis(expected_edges);
  };

 private:
  void assertInitGraphValid() {
    EXPECT_EQ(4, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
    EXPECT_EQ(6, CountEdges(&dba, memgraph::storage::View::OLD));
  }
};

TYPED_TEST_CASE(DeleteOperatorWithAuthFixture, StorageTypes);

TYPED_TEST(DeleteOperatorWithAuthFixture, DeleteNodeThrowsExceptionWhenAllLabelsDenied) {
  // All labels denied
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);

  ASSERT_THROW(this->ExecuteDeleteNodesTestSuite(user, 0), QueryRuntimeException);
}

TYPED_TEST(DeleteOperatorWithAuthFixture, DeleteNodeThrowsExceptionWhenPartialLabelsGranted) {
  // One Label granted
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("l1",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
  ASSERT_THROW(this->ExecuteDeleteNodesTestSuite(user, 0), QueryRuntimeException);
}

TYPED_TEST(DeleteOperatorWithAuthFixture, DeleteNodeExecutesWhenGrantedAllLabels) {
  // All labels granted
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  this->ExecuteDeleteNodesTestSuite(user, 0);
}
TYPED_TEST(DeleteOperatorWithAuthFixture, DeleteNodeThrowsExceptionWhenEdgeTypesNotGranted) {
  // All labels granted,All edge types denied
  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);
  user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);

  ASSERT_THROW(this->ExecuteDeleteNodesTestSuite(user, 0), QueryRuntimeException);
}
TYPED_TEST(DeleteOperatorWithAuthFixture, DeleteEdgesThrowsErrorWhenPartialGrant) {
  // Specific label granted, Specific edge types granted

  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("l1", memgraph::auth::FineGrainedPermission::UPDATE);
  user.fine_grained_access_handler().label_permissions().Grant("l2", memgraph::auth::FineGrainedPermission::UPDATE);
  user.fine_grained_access_handler().label_permissions().Grant("l3", memgraph::auth::FineGrainedPermission::READ);
  user.fine_grained_access_handler().label_permissions().Grant("l4", memgraph::auth::FineGrainedPermission::READ);
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "type0", memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "type1", memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().edge_type_permissions().Grant("type2",
                                                                   memgraph::auth::FineGrainedPermission::UPDATE);
  user.fine_grained_access_handler().edge_type_permissions().Grant("type3",
                                                                   memgraph::auth::FineGrainedPermission::UPDATE);

  ASSERT_THROW(this->ExecuteDeleteEdgesTestSuite(user, 0), QueryRuntimeException);
}

TYPED_TEST(DeleteOperatorWithAuthFixture, DeleteNodeAndDeleteEdgePerformWhenGranted) {
  // All labels granted, All edge_types granted

  memgraph::auth::User user{"test"};
  user.fine_grained_access_handler().label_permissions().Grant("*",
                                                               memgraph::auth::FineGrainedPermission::CREATE_DELETE);
  user.fine_grained_access_handler().edge_type_permissions().Grant(
      "*", memgraph::auth::FineGrainedPermission::CREATE_DELETE);

  this->InitGraph();
  this->DeleteAllNodes(user);
  this->TestDeleteNodesHypothesis(0);
  this->TestDeleteEdgesHypothesis(0);
}
#endif

TYPED_TEST(QueryPlanTest, DeleteTwiceDeleteBlockingEdge) {
  // test deleting the same vertex and edge multiple times
  //
  // also test vertex deletion succeeds if the prohibiting
  // edge is deleted in the same logical op
  //
  // we test both with the following queries (note the
  // undirected edge in MATCH):
  //
  // CREATE ()-[:T]->()
  // MATCH (n)-[r]-(m) [DETACH] DELETE n, r, m

  auto test_delete = [this](bool detach) {
    auto storage_dba = this->db->Access(ReplicationRole::MAIN);
    memgraph::query::DbAccessor dba(storage_dba.get());

    auto v1 = dba.InsertVertex();
    auto v2 = dba.InsertVertex();
    ASSERT_TRUE(dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("T")).HasValue());
    dba.AdvanceCommand();
    EXPECT_EQ(2, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
    EXPECT_EQ(1, CountEdges(&dba, memgraph::storage::View::OLD));

    SymbolTable symbol_table;

    auto n = MakeScanAll(this->storage, symbol_table, "n");
    auto r_m = MakeExpand(this->storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::BOTH, {}, "m", false,
                          memgraph::storage::View::OLD);

    // getter expressions for deletion
    auto n_get = this->storage.template Create<Identifier>("n")->MapTo(n.sym_);
    auto r_get = this->storage.template Create<Identifier>("r")->MapTo(r_m.edge_sym_);
    auto m_get = this->storage.template Create<Identifier>("m")->MapTo(r_m.node_sym_);

    auto delete_op = std::make_shared<plan::Delete>(r_m.op_, std::vector<Expression *>{n_get, r_get, m_get}, detach);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    EXPECT_EQ(2, PullAll(*delete_op, &context));
    dba.AdvanceCommand();
    EXPECT_EQ(0, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
    EXPECT_EQ(0, CountEdges(&dba, memgraph::storage::View::OLD));
  };

  test_delete(true);
  test_delete(false);
}

TYPED_TEST(QueryPlanTest, DeleteReturn) {
  // MATCH (n) DETACH DELETE n RETURN n
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());

  // graph with 4 vertices
  auto prop = PROPERTY_PAIR(dba, "property");
  for (int i = 0; i < 4; ++i) {
    auto va = dba.InsertVertex();
    ASSERT_TRUE(va.SetProperty(prop.second, memgraph::storage::PropertyValue(42)).HasValue());
  }

  dba.AdvanceCommand();
  EXPECT_EQ(4, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  EXPECT_EQ(0, CountEdges(&dba, memgraph::storage::View::OLD));

  SymbolTable symbol_table;

  auto n = MakeScanAll(this->storage, symbol_table, "n");

  auto n_get = this->storage.template Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, true);

  auto accumulate_op = std::make_shared<plan::Accumulate>(delete_op, delete_op->ModifiedSymbols(symbol_table), true);

  auto prop_lookup = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);
  auto n_p =
      this->storage.template Create<NamedExpression>("n", prop_lookup)->MapTo(symbol_table.CreateSymbol("bla", true));
  auto produce = MakeProduce(accumulate_op, n_p);

  auto context = MakeContext(this->storage, symbol_table, &dba);
  ASSERT_THROW(CollectProduce(*produce, &context), QueryRuntimeException);
}

TYPED_TEST(QueryPlanTest, DeleteNull) {
  // test (simplified) WITH Null as x delete x
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  SymbolTable symbol_table;

  auto once = std::make_shared<Once>();
  auto delete_op = std::make_shared<plan::Delete>(once, std::vector<Expression *>{LITERAL(TypedValue())}, false);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*delete_op, &context));
}

TYPED_TEST(QueryPlanTest, DeleteAdvance) {
  // test queries on empty DB:
  // CREATE (n)
  // MATCH (n) DELETE n WITH n ...
  // this fails only if the deleted record `n` is actually used in subsequent
  // clauses, which is compatible with Neo's behavior.
  SymbolTable symbol_table;

  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_get = this->storage.template Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
  auto advance = std::make_shared<Accumulate>(delete_op, std::vector<Symbol>{n.sym_}, true);
  auto res_sym = symbol_table.CreateSymbol("res", true);
  {
    auto storage_dba = this->db->Access(ReplicationRole::MAIN);
    memgraph::query::DbAccessor dba(storage_dba.get());
    dba.InsertVertex();
    dba.AdvanceCommand();
    auto produce = MakeProduce(advance, NEXPR("res", LITERAL(42))->MapTo(res_sym));
    auto context = MakeContext(this->storage, symbol_table, &dba);
    EXPECT_EQ(1, PullAll(*produce, &context));
  }
  {
    auto storage_dba = this->db->Access(ReplicationRole::MAIN);
    memgraph::query::DbAccessor dba(storage_dba.get());
    dba.InsertVertex();
    dba.AdvanceCommand();
    auto n_prop = PROPERTY_LOOKUP(dba, n_get, dba.NameToProperty("prop"));
    auto produce = MakeProduce(advance, NEXPR("res", n_prop)->MapTo(res_sym));
    auto context = MakeContext(this->storage, symbol_table, &dba);
    EXPECT_THROW(PullAll(*produce, &context), QueryRuntimeException);
  }
}

TYPED_TEST(QueryPlanTest, SetProperty) {
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());

  // graph with 4 vertices in connected pairs
  // the origin vertex in each par and both edges
  // have a property set
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto v3 = dba.InsertVertex();
  auto v4 = dba.InsertVertex();
  auto edge_type = dba.NameToEdgeType("edge_type");
  ASSERT_TRUE(dba.InsertEdge(&v1, &v3, edge_type).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v2, &v4, edge_type).HasValue());
  dba.AdvanceCommand();

  SymbolTable symbol_table;

  // scan (n)-[r]->(m)
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto r_m = MakeExpand(this->storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::OUT, {}, "m", false,
                        memgraph::storage::View::OLD);

  // set prop1 to 42 on n and r
  auto prop1 = dba.NameToProperty("prop1");
  auto literal = LITERAL(42);

  auto n_p = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop1);
  auto set_n_p = std::make_shared<plan::SetProperty>(r_m.op_, prop1, n_p, literal);

  auto r_p = PROPERTY_LOOKUP(dba, IDENT("r")->MapTo(r_m.edge_sym_), prop1);
  auto set_r_p = std::make_shared<plan::SetProperty>(set_n_p, prop1, r_p, literal);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*set_r_p, &context));
  dba.AdvanceCommand();

  EXPECT_EQ(CountEdges(&dba, memgraph::storage::View::OLD), 2);
  for (auto vertex : dba.Vertices(memgraph::storage::View::OLD)) {
    auto maybe_edges = vertex.OutEdges(memgraph::storage::View::OLD);
    ASSERT_TRUE(maybe_edges.HasValue());
    for (auto edge : maybe_edges->edges) {
      ASSERT_EQ(edge.GetProperty(memgraph::storage::View::OLD, prop1)->type(),
                memgraph::storage::PropertyValue::Type::Int);
      EXPECT_EQ(edge.GetProperty(memgraph::storage::View::OLD, prop1)->ValueInt(), 42);
      auto from = edge.From();
      auto to = edge.To();
      ASSERT_EQ(from.GetProperty(memgraph::storage::View::OLD, prop1)->type(),
                memgraph::storage::PropertyValue::Type::Int);
      EXPECT_EQ(from.GetProperty(memgraph::storage::View::OLD, prop1)->ValueInt(), 42);
      ASSERT_EQ(to.GetProperty(memgraph::storage::View::OLD, prop1)->type(),
                memgraph::storage::PropertyValue::Type::Null);
    }
  }
}

TYPED_TEST(QueryPlanTest, SetProperties) {
  auto test_set_properties = [this](bool update) {
    auto storage_dba = this->db->Access(ReplicationRole::MAIN);
    memgraph::query::DbAccessor dba(storage_dba.get());

    // graph: ({a: 0})-[:R {b:1}]->({c:2})
    auto prop_a = dba.NameToProperty("a");
    auto prop_b = dba.NameToProperty("b");
    auto prop_c = dba.NameToProperty("c");
    auto v1 = dba.InsertVertex();
    auto v2 = dba.InsertVertex();
    auto e = dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("R"));
    ASSERT_TRUE(v1.SetProperty(prop_a, memgraph::storage::PropertyValue(0)).HasValue());
    ASSERT_TRUE(e->SetProperty(prop_b, memgraph::storage::PropertyValue(1)).HasValue());
    ASSERT_TRUE(v2.SetProperty(prop_c, memgraph::storage::PropertyValue(2)).HasValue());
    dba.AdvanceCommand();

    SymbolTable symbol_table;

    // scan (n)-[r]->(m)
    auto n = MakeScanAll(this->storage, symbol_table, "n");
    auto r_m = MakeExpand(this->storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::OUT, {}, "m", false,
                          memgraph::storage::View::OLD);

    auto op = update ? plan::SetProperties::Op::UPDATE : plan::SetProperties::Op::REPLACE;

    // set properties on r to n, and on r to m
    auto r_ident = IDENT("r")->MapTo(r_m.edge_sym_);
    auto m_ident = IDENT("m")->MapTo(r_m.node_sym_);
    auto set_r_to_n = std::make_shared<plan::SetProperties>(r_m.op_, n.sym_, r_ident, op);
    auto set_m_to_r = std::make_shared<plan::SetProperties>(set_r_to_n, r_m.edge_sym_, m_ident, op);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    EXPECT_EQ(1, PullAll(*set_m_to_r, &context));
    dba.AdvanceCommand();

    EXPECT_EQ(CountEdges(&dba, memgraph::storage::View::OLD), 1);
    for (auto vertex : dba.Vertices(memgraph::storage::View::OLD)) {
      auto maybe_edges = vertex.OutEdges(memgraph::storage::View::OLD);
      ASSERT_TRUE(maybe_edges.HasValue());
      for (auto edge : maybe_edges->edges) {
        auto from = edge.From();
        EXPECT_EQ(from.Properties(memgraph::storage::View::OLD)->size(), update ? 2 : 1);
        if (update) {
          ASSERT_EQ(from.GetProperty(memgraph::storage::View::OLD, prop_a)->type(),
                    memgraph::storage::PropertyValue::Type::Int);
          EXPECT_EQ(from.GetProperty(memgraph::storage::View::OLD, prop_a)->ValueInt(), 0);
        }
        ASSERT_EQ(from.GetProperty(memgraph::storage::View::OLD, prop_b)->type(),
                  memgraph::storage::PropertyValue::Type::Int);
        EXPECT_EQ(from.GetProperty(memgraph::storage::View::OLD, prop_b)->ValueInt(), 1);

        EXPECT_EQ(edge.Properties(memgraph::storage::View::OLD)->size(), update ? 2 : 1);
        if (update) {
          ASSERT_EQ(edge.GetProperty(memgraph::storage::View::OLD, prop_b)->type(),
                    memgraph::storage::PropertyValue::Type::Int);
          EXPECT_EQ(edge.GetProperty(memgraph::storage::View::OLD, prop_b)->ValueInt(), 1);
        }
        ASSERT_EQ(edge.GetProperty(memgraph::storage::View::OLD, prop_c)->type(),
                  memgraph::storage::PropertyValue::Type::Int);
        EXPECT_EQ(edge.GetProperty(memgraph::storage::View::OLD, prop_c)->ValueInt(), 2);

        auto to = edge.To();
        EXPECT_EQ(to.Properties(memgraph::storage::View::OLD)->size(), 1);
        ASSERT_EQ(to.GetProperty(memgraph::storage::View::OLD, prop_c)->type(),
                  memgraph::storage::PropertyValue::Type::Int);
        EXPECT_EQ(to.GetProperty(memgraph::storage::View::OLD, prop_c)->ValueInt(), 2);
      }
    }
  };

  test_set_properties(true);
  test_set_properties(false);
}

TYPED_TEST(QueryPlanTest, SetLabels) {
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());

  auto label1 = dba.NameToLabel("label1");
  auto label2 = dba.NameToLabel("label2");
  auto label3 = dba.NameToLabel("label3");
  ASSERT_TRUE(dba.InsertVertex().AddLabel(label1).HasValue());
  ASSERT_TRUE(dba.InsertVertex().AddLabel(label1).HasValue());
  dba.AdvanceCommand();
  std::vector<StorageLabelType> labels;
  labels.emplace_back(label2);
  labels.emplace_back(label3);

  SymbolTable symbol_table;

  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto label_set = std::make_shared<plan::SetLabels>(n.op_, n.sym_, labels);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*label_set, &context));

  for (auto vertex : dba.Vertices(memgraph::storage::View::OLD)) {
    EXPECT_EQ(3, vertex.Labels(memgraph::storage::View::NEW)->size());
    EXPECT_TRUE(*vertex.HasLabel(memgraph::storage::View::NEW, label2));
    EXPECT_TRUE(*vertex.HasLabel(memgraph::storage::View::NEW, label3));
  }
}

#ifdef MG_ENTERPRISE
TYPED_TEST(QueryPlanTest, SetLabelsWithFineGrained) {
  memgraph::license::global_license_checker.EnableTesting();
  auto set_labels = [&](memgraph::auth::User user, memgraph::query::DbAccessor dba,
                        std::vector<memgraph::storage::LabelId> labels) {
    ASSERT_TRUE(dba.InsertVertex().AddLabel(labels[0]).HasValue());
    ASSERT_TRUE(dba.InsertVertex().AddLabel(labels[0]).HasValue());
    dba.AdvanceCommand();
    std::vector<StorageLabelType> labels_variant;
    labels_variant.emplace_back(labels[1]);
    labels_variant.emplace_back(labels[2]);

    SymbolTable symbol_table;

    auto n = MakeScanAll(this->storage, symbol_table, "n");
    auto label_set = std::make_shared<plan::SetLabels>(n.op_, n.sym_, labels_variant);
    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);

    PullAll(*label_set, &context);
  };

  // All labels granted
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("*",
                                                                 memgraph::auth::FineGrainedPermission::CREATE_DELETE);
    auto storage_dba = this->db->Access(ReplicationRole::MAIN);
    memgraph::query::DbAccessor dba(storage_dba.get());
    auto label1 = dba.NameToLabel("label1");
    auto label2 = dba.NameToLabel("label2");
    auto label3 = dba.NameToLabel("label3");
    set_labels(user, dba, std::vector<memgraph::storage::LabelId>{label1, label2, label3});
    for (auto vertex : dba.Vertices(memgraph::storage::View::OLD)) {
      EXPECT_EQ(3, vertex.Labels(memgraph::storage::View::NEW)->size());
      EXPECT_TRUE(*vertex.HasLabel(memgraph::storage::View::NEW, label2));
      EXPECT_TRUE(*vertex.HasLabel(memgraph::storage::View::NEW, label3));
    }
  }

  // All labels denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);
    auto storage_dba = this->db->Access(ReplicationRole::MAIN);
    memgraph::query::DbAccessor dba(storage_dba.get());
    auto label1 = dba.NameToLabel("label1");
    auto label2 = dba.NameToLabel("label2");
    auto label3 = dba.NameToLabel("label3");
    ASSERT_THROW(set_labels(user, dba, std::vector<memgraph::storage::LabelId>{label1, label2, label3}),
                 QueryRuntimeException);
  }

  // label2 denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("label1",
                                                                 memgraph::auth::FineGrainedPermission::CREATE_DELETE);
    user.fine_grained_access_handler().label_permissions().Grant("label2",
                                                                 memgraph::auth::FineGrainedPermission::UPDATE);
    user.fine_grained_access_handler().label_permissions().Grant("label3",
                                                                 memgraph::auth::FineGrainedPermission::CREATE_DELETE);

    auto storage_dba = this->db->Access(ReplicationRole::MAIN);
    memgraph::query::DbAccessor dba(storage_dba.get());
    auto label1 = dba.NameToLabel("label1");
    auto label2 = dba.NameToLabel("label2");
    auto label3 = dba.NameToLabel("label3");
    ASSERT_THROW(set_labels(user, dba, std::vector<memgraph::storage::LabelId>{label1, label2, label3}),
                 QueryRuntimeException);
  }
}
#endif

TYPED_TEST(QueryPlanTest, RemoveProperty) {
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());

  // graph with 4 vertices in connected pairs
  // the origin vertex in each par and both edges
  // have a property set
  auto prop1 = dba.NameToProperty("prop1");
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto v3 = dba.InsertVertex();
  auto v4 = dba.InsertVertex();
  auto edge_type = dba.NameToEdgeType("edge_type");
  {
    auto e = dba.InsertEdge(&v1, &v3, edge_type);
    ASSERT_TRUE(e.HasValue());
    ASSERT_TRUE(e->SetProperty(prop1, memgraph::storage::PropertyValue(42)).HasValue());
  }
  ASSERT_TRUE(dba.InsertEdge(&v2, &v4, edge_type).HasValue());
  ASSERT_TRUE(v2.SetProperty(prop1, memgraph::storage::PropertyValue(42)).HasValue());
  ASSERT_TRUE(v3.SetProperty(prop1, memgraph::storage::PropertyValue(42)).HasValue());
  ASSERT_TRUE(v4.SetProperty(prop1, memgraph::storage::PropertyValue(42)).HasValue());
  auto prop2 = dba.NameToProperty("prop2");
  ASSERT_TRUE(v1.SetProperty(prop2, memgraph::storage::PropertyValue(0)).HasValue());
  ASSERT_TRUE(v2.SetProperty(prop2, memgraph::storage::PropertyValue(0)).HasValue());
  dba.AdvanceCommand();

  SymbolTable symbol_table;

  // scan (n)-[r]->(m)
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto r_m = MakeExpand(this->storage, symbol_table, n.op_, n.sym_, "r", EdgeAtom::Direction::OUT, {}, "m", false,
                        memgraph::storage::View::OLD);

  auto n_p = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop1);
  auto set_n_p = std::make_shared<plan::RemoveProperty>(r_m.op_, prop1, n_p);

  auto r_p = PROPERTY_LOOKUP(dba, IDENT("r")->MapTo(r_m.edge_sym_), prop1);
  auto set_r_p = std::make_shared<plan::RemoveProperty>(set_n_p, prop1, r_p);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*set_r_p, &context));
  dba.AdvanceCommand();

  EXPECT_EQ(CountEdges(&dba, memgraph::storage::View::OLD), 2);
  for (auto vertex : dba.Vertices(memgraph::storage::View::OLD)) {
    auto maybe_edges = vertex.OutEdges(memgraph::storage::View::OLD);
    ASSERT_TRUE(maybe_edges.HasValue());
    for (auto edge : maybe_edges->edges) {
      EXPECT_EQ(edge.GetProperty(memgraph::storage::View::OLD, prop1)->type(),
                memgraph::storage::PropertyValue::Type::Null);
      auto from = edge.From();
      auto to = edge.To();
      EXPECT_EQ(from.GetProperty(memgraph::storage::View::OLD, prop1)->type(),
                memgraph::storage::PropertyValue::Type::Null);
      EXPECT_EQ(from.GetProperty(memgraph::storage::View::OLD, prop2)->type(),
                memgraph::storage::PropertyValue::Type::Int);
      EXPECT_EQ(to.GetProperty(memgraph::storage::View::OLD, prop1)->type(),
                memgraph::storage::PropertyValue::Type::Int);
    }
  }
}

TYPED_TEST(QueryPlanTest, RemoveLabels) {
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());

  auto label1 = dba.NameToLabel("label1");
  auto label2 = dba.NameToLabel("label2");
  auto label3 = dba.NameToLabel("label3");
  auto v1 = dba.InsertVertex();
  ASSERT_TRUE(v1.AddLabel(label1).HasValue());
  ASSERT_TRUE(v1.AddLabel(label2).HasValue());
  ASSERT_TRUE(v1.AddLabel(label3).HasValue());
  auto v2 = dba.InsertVertex();
  ASSERT_TRUE(v2.AddLabel(label1).HasValue());
  ASSERT_TRUE(v2.AddLabel(label3).HasValue());
  dba.AdvanceCommand();
  std::vector<StorageLabelType> labels;
  labels.emplace_back(label1);
  labels.emplace_back(label2);

  SymbolTable symbol_table;

  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto label_remove = std::make_shared<plan::RemoveLabels>(n.op_, n.sym_, labels);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*label_remove, &context));

  for (auto vertex : dba.Vertices(memgraph::storage::View::OLD)) {
    EXPECT_EQ(1, vertex.Labels(memgraph::storage::View::NEW)->size());
    EXPECT_FALSE(*vertex.HasLabel(memgraph::storage::View::NEW, label1));
    EXPECT_FALSE(*vertex.HasLabel(memgraph::storage::View::NEW, label2));
  }
}

#ifdef MG_ENTERPRISE
TYPED_TEST(QueryPlanTest, RemoveLabelsFineGrainedFiltering) {
  memgraph::license::global_license_checker.EnableTesting();
  auto remove_labels = [&](memgraph::auth::User user, memgraph::query::DbAccessor dba,
                           std::vector<memgraph::storage::LabelId> labels) {
    auto v1 = dba.InsertVertex();
    ASSERT_TRUE(v1.AddLabel(labels[0]).HasValue());
    ASSERT_TRUE(v1.AddLabel(labels[1]).HasValue());
    ASSERT_TRUE(v1.AddLabel(labels[2]).HasValue());
    auto v2 = dba.InsertVertex();
    ASSERT_TRUE(v2.AddLabel(labels[0]).HasValue());
    ASSERT_TRUE(v2.AddLabel(labels[2]).HasValue());
    dba.AdvanceCommand();
    std::vector<StorageLabelType> labels_variant;
    labels_variant.emplace_back(labels[0]);
    labels_variant.emplace_back(labels[1]);

    SymbolTable symbol_table;

    auto n = MakeScanAll(this->storage, symbol_table, "n");
    auto label_remove = std::make_shared<plan::RemoveLabels>(n.op_, n.sym_, labels_variant);
    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};

    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);

    PullAll(*label_remove, &context);
  };

  // All labels granted
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("*",
                                                                 memgraph::auth::FineGrainedPermission::CREATE_DELETE);
    auto storage_dba = this->db->Access(ReplicationRole::MAIN);
    memgraph::query::DbAccessor dba(storage_dba.get());
    auto label1 = dba.NameToLabel("label1");
    auto label2 = dba.NameToLabel("label2");
    auto label3 = dba.NameToLabel("label3");
    remove_labels(user, dba, std::vector<memgraph::storage::LabelId>{label1, label2, label3});
    for (auto vertex : dba.Vertices(memgraph::storage::View::OLD)) {
      EXPECT_EQ(1, vertex.Labels(memgraph::storage::View::NEW)->size());
      EXPECT_FALSE(*vertex.HasLabel(memgraph::storage::View::NEW, label2));
      EXPECT_TRUE(*vertex.HasLabel(memgraph::storage::View::NEW, label3));
    }
  }

  // All labels denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);
    auto storage_dba = this->db->Access(ReplicationRole::MAIN);
    memgraph::query::DbAccessor dba(storage_dba.get());
    auto label1 = dba.NameToLabel("label1");
    auto label2 = dba.NameToLabel("label2");
    auto label3 = dba.NameToLabel("label3");
    ASSERT_THROW(remove_labels(user, dba, std::vector<memgraph::storage::LabelId>{label1, label2, label3}),
                 QueryRuntimeException);
  }

  // label2 denied
  {
    memgraph::auth::User user{"test"};
    user.fine_grained_access_handler().label_permissions().Grant("label1",
                                                                 memgraph::auth::FineGrainedPermission::CREATE_DELETE);
    user.fine_grained_access_handler().label_permissions().Grant("label2",
                                                                 memgraph::auth::FineGrainedPermission::UPDATE);
    user.fine_grained_access_handler().label_permissions().Grant("label3",
                                                                 memgraph::auth::FineGrainedPermission::CREATE_DELETE);

    auto storage_dba = this->db->Access(ReplicationRole::MAIN);
    memgraph::query::DbAccessor dba(storage_dba.get());
    auto label1 = dba.NameToLabel("label1");
    auto label2 = dba.NameToLabel("label2");
    auto label3 = dba.NameToLabel("label3");
    ASSERT_THROW(remove_labels(user, dba, std::vector<memgraph::storage::LabelId>{label1, label2, label3}),
                 QueryRuntimeException);
  }
}
#endif

TYPED_TEST(QueryPlanTest, NodeFilterSet) {
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  // Create a graph such that (v1 {prop: 42}) is connected to v2 and v3.
  auto v1 = dba.InsertVertex();
  auto prop = PROPERTY_PAIR(dba, "property");
  ASSERT_TRUE(v1.SetProperty(prop.second, memgraph::storage::PropertyValue(42)).HasValue());
  auto v2 = dba.InsertVertex();
  auto v3 = dba.InsertVertex();
  auto edge_type = dba.NameToEdgeType("Edge");
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, edge_type).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v1, &v3, edge_type).HasValue());
  dba.AdvanceCommand();
  // Create operations which match (v1 {prop: 42}) -- (v) and increment the
  // v1.prop. The expected result is two incremenentations, since v1 is matched
  // twice for 2 edges it has.
  SymbolTable symbol_table;
  // MATCH (n {prop: 42}) -[r]- (m)
  auto scan_all = MakeScanAll(this->storage, symbol_table, "n");
  std::get<0>(scan_all.node_->properties_)[this->storage.GetPropertyIx(prop.first)] = LITERAL(42);
  auto expand = MakeExpand(this->storage, symbol_table, scan_all.op_, scan_all.sym_, "r", EdgeAtom::Direction::BOTH, {},
                           "m", false, memgraph::storage::View::OLD);
  auto *filter_expr = EQ(this->storage.template Create<PropertyLookup>(scan_all.node_->identifier_,
                                                                       this->storage.GetPropertyIx(prop.first)),
                         LITERAL(42));
  auto node_filter = std::make_shared<Filter>(expand.op_, std::vector<std::shared_ptr<LogicalOperator>>{}, filter_expr);
  // SET n.prop = n.prop + 1
  auto set_prop = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(scan_all.sym_), prop);
  auto add = ADD(set_prop, LITERAL(1));
  auto set = std::make_shared<plan::SetProperty>(node_filter, prop.second, set_prop, add);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*set, &context));
  dba.AdvanceCommand();
  auto prop_eq = TypedValue(*v1.GetProperty(memgraph::storage::View::OLD, prop.second)) == TypedValue(42 + 2);
  ASSERT_EQ(prop_eq.type(), TypedValue::Type::Bool);
  EXPECT_TRUE(prop_eq.ValueBool());
}

TYPED_TEST(QueryPlanTest, FilterRemove) {
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  // Create a graph such that (v1 {prop: 42}) is connected to v2 and v3.
  auto v1 = dba.InsertVertex();
  auto prop = PROPERTY_PAIR(dba, "property");
  ASSERT_TRUE(v1.SetProperty(prop.second, memgraph::storage::PropertyValue(42)).HasValue());
  auto v2 = dba.InsertVertex();
  auto v3 = dba.InsertVertex();
  auto edge_type = dba.NameToEdgeType("Edge");
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, edge_type).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v1, &v3, edge_type).HasValue());
  dba.AdvanceCommand();
  // Create operations which match (v1 {prop: 42}) -- (v) and remove v1.prop.
  // The expected result is two matches, for each edge of v1.
  SymbolTable symbol_table;
  // MATCH (n) -[r]- (m) WHERE n.prop < 43
  auto scan_all = MakeScanAll(this->storage, symbol_table, "n");
  std::get<0>(scan_all.node_->properties_)[this->storage.GetPropertyIx(prop.first)] = LITERAL(42);
  auto expand = MakeExpand(this->storage, symbol_table, scan_all.op_, scan_all.sym_, "r", EdgeAtom::Direction::BOTH, {},
                           "m", false, memgraph::storage::View::OLD);
  auto filter_prop = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(scan_all.sym_), prop);
  auto filter = std::make_shared<Filter>(expand.op_, std::vector<std::shared_ptr<LogicalOperator>>{},
                                         LESS(filter_prop, LITERAL(43)));
  // REMOVE n.prop
  auto rem_prop = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(scan_all.sym_), prop);
  auto rem = std::make_shared<plan::RemoveProperty>(filter, prop.second, rem_prop);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_EQ(2, PullAll(*rem, &context));
  dba.AdvanceCommand();
  EXPECT_EQ(v1.GetProperty(memgraph::storage::View::OLD, prop.second)->type(),
            memgraph::storage::PropertyValue::Type::Null);
}

TYPED_TEST(QueryPlanTest, SetRemove) {
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto v = dba.InsertVertex();
  auto label1 = dba.NameToLabel("label1");
  auto label2 = dba.NameToLabel("label2");
  dba.AdvanceCommand();
  std::vector<StorageLabelType> labels;
  labels.emplace_back(label1);
  labels.emplace_back(label2);
  // Create operations which match (v) and set and remove v :label.
  // The expected result is single (v) as it was at the start.
  SymbolTable symbol_table;
  // MATCH (n) SET n :label1 :label2 REMOVE n :label1 :label2
  auto scan_all = MakeScanAll(this->storage, symbol_table, "n");
  auto set = std::make_shared<plan::SetLabels>(scan_all.op_, scan_all.sym_, labels);
  auto rem = std::make_shared<plan::RemoveLabels>(set, scan_all.sym_, labels);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*rem, &context));
  dba.AdvanceCommand();
  EXPECT_FALSE(*v.HasLabel(memgraph::storage::View::OLD, label1));
  EXPECT_FALSE(*v.HasLabel(memgraph::storage::View::OLD, label2));
}

TYPED_TEST(QueryPlanTest, Merge) {
  // test setup:
  //  - three nodes, two of them connected with T
  //  - merge input branch matches all nodes
  //  - merge_match branch looks for an expansion (any direction)
  //    and sets some property (for result validation)
  //  - merge_create branch just sets some other property
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("Type")).HasValue());
  auto v3 = dba.InsertVertex();
  dba.AdvanceCommand();

  SymbolTable symbol_table;

  auto prop = PROPERTY_PAIR(dba, "property");
  auto n = MakeScanAll(this->storage, symbol_table, "n");

  // merge_match branch
  auto r_m = MakeExpand(this->storage, symbol_table, std::make_shared<Once>(), n.sym_, "r", EdgeAtom::Direction::BOTH,
                        {}, "m", false, memgraph::storage::View::OLD);
  auto m_p = PROPERTY_LOOKUP(dba, IDENT("m")->MapTo(r_m.node_sym_), prop);
  auto m_set = std::make_shared<plan::SetProperty>(r_m.op_, prop.second, m_p, LITERAL(1));

  // merge_create branch
  auto n_p = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);
  auto n_set = std::make_shared<plan::SetProperty>(std::make_shared<Once>(), prop.second, n_p, LITERAL(2));

  auto merge = std::make_shared<plan::Merge>(n.op_, m_set, n_set);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  ASSERT_EQ(3, PullAll(*merge, &context));
  dba.AdvanceCommand();

  ASSERT_EQ(v1.GetProperty(memgraph::storage::View::OLD, prop.second)->type(),
            memgraph::storage::PropertyValue::Type::Int);
  ASSERT_EQ(v1.GetProperty(memgraph::storage::View::OLD, prop.second)->ValueInt(), 1);
  ASSERT_EQ(v2.GetProperty(memgraph::storage::View::OLD, prop.second)->type(),
            memgraph::storage::PropertyValue::Type::Int);
  ASSERT_EQ(v2.GetProperty(memgraph::storage::View::OLD, prop.second)->ValueInt(), 1);
  ASSERT_EQ(v3.GetProperty(memgraph::storage::View::OLD, prop.second)->type(),
            memgraph::storage::PropertyValue::Type::Int);
  ASSERT_EQ(v3.GetProperty(memgraph::storage::View::OLD, prop.second)->ValueInt(), 2);
}

TYPED_TEST(QueryPlanTest, MergeNoInput) {
  // merge with no input, creates a single node

  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  SymbolTable symbol_table;

  NodeCreationInfo node;
  node.symbol = symbol_table.CreateSymbol("n", true);
  auto create = std::make_shared<CreateNode>(nullptr, node);
  auto merge = std::make_shared<plan::Merge>(nullptr, create, create);

  EXPECT_EQ(0, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*merge, &context));
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
}

TYPED_TEST(QueryPlanTest, SetPropertyWithCaching) {
  // SET (Null).prop = 42
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  SymbolTable symbol_table;
  auto prop = PROPERTY_PAIR(dba, "property");
  auto null = LITERAL(TypedValue());
  auto literal = LITERAL(42);
  auto n_prop = PROPERTY_LOOKUP(dba, null, prop);
  auto once = std::make_shared<Once>();
  auto set_op = std::make_shared<plan::SetProperty>(once, prop.second, n_prop, literal);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*set_op, &context));
}

TYPED_TEST(QueryPlanTest, SetPropertyOnNull) {
  // SET (Null).prop = 42
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  SymbolTable symbol_table;
  auto prop = PROPERTY_PAIR(dba, "property");
  auto null = LITERAL(TypedValue());
  auto literal = LITERAL(42);
  auto n_prop = PROPERTY_LOOKUP(dba, null, prop);
  auto once = std::make_shared<Once>();
  auto set_op = std::make_shared<plan::SetProperty>(once, prop.second, n_prop, literal);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*set_op, &context));
}

TYPED_TEST(QueryPlanTest, SetPropertiesOnNull) {
  // OPTIONAL MATCH (n) SET n = n
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_ident = IDENT("n")->MapTo(n.sym_);
  auto optional = std::make_shared<plan::Optional>(nullptr, n.op_, std::vector<Symbol>{n.sym_});
  auto set_op = std::make_shared<plan::SetProperties>(optional, n.sym_, n_ident, plan::SetProperties::Op::REPLACE);
  EXPECT_EQ(0, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*set_op, &context));
}

TYPED_TEST(QueryPlanTest, UpdateSetPropertiesFromMap) {
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  // Add a single vertex. ( {property: 43})
  auto vertex_accessor = dba.InsertVertex();
  auto old_value = vertex_accessor.SetProperty(dba.NameToProperty("property"), memgraph::storage::PropertyValue{43});
  EXPECT_EQ(old_value.HasError(), false);
  EXPECT_EQ(*old_value, memgraph::storage::PropertyValue());
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  SymbolTable symbol_table;
  // MATCH (n) SET n += {property: "updated", new_property:"a"}
  auto n = MakeScanAll(this->storage, symbol_table, "n");

  auto prop_property = PROPERTY_PAIR(dba, "property");
  auto prop_new_property = PROPERTY_PAIR(dba, "new_property");

  std::unordered_map<PropertyIx, Expression *> prop_map;
  prop_map.emplace(this->storage.GetPropertyIx(prop_property.first), LITERAL("updated"));
  prop_map.emplace(this->storage.GetPropertyIx(prop_new_property.first), LITERAL("a"));
  auto *rhs = this->storage.template Create<MapLiteral>(prop_map);

  auto op_type{plan::SetProperties::Op::UPDATE};
  auto set_op = std::make_shared<plan::SetProperties>(n.op_, n.sym_, rhs, op_type);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  PullAll(*set_op, &context);
  dba.AdvanceCommand();
  auto new_properties = vertex_accessor.Properties(memgraph::storage::View::OLD);
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> expected_properties;
  expected_properties.emplace(dba.NameToProperty("property"), memgraph::storage::PropertyValue("updated"));
  expected_properties.emplace(dba.NameToProperty("new_property"), memgraph::storage::PropertyValue("a"));
  EXPECT_EQ(new_properties.HasError(), false);
  EXPECT_EQ(*new_properties, expected_properties);
}

TYPED_TEST(QueryPlanTest, SetPropertiesFromMapWithCaching) {
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());

  // Add a single vertex. ({prop1: 43, prop2: 44})
  auto vertex_accessor = dba.InsertVertex();
  auto old_value = vertex_accessor.SetProperty(dba.NameToProperty("prop1"), memgraph::storage::PropertyValue{43});
  old_value = vertex_accessor.SetProperty(dba.NameToProperty("prop2"), memgraph::storage::PropertyValue{44});
  EXPECT_EQ(old_value.HasError(), false);
  EXPECT_EQ(*old_value, memgraph::storage::PropertyValue());
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));

  SymbolTable symbol_table;
  // MATCH (n) SET n += {new_prop1: n.prop1, new_prop2: n.prop2};
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto prop_new_prop1 = PROPERTY_PAIR(dba, "new_prop1");
  auto prop_new_prop2 = PROPERTY_PAIR(dba, "new_prop2");
  std::unordered_map<PropertyIx, Expression *> prop_map;
  prop_map.emplace(this->storage.GetPropertyIx(prop_new_prop1.first), LITERAL(43));
  prop_map.emplace(this->storage.GetPropertyIx(prop_new_prop2.first), LITERAL(44));
  auto *rhs = this->storage.template Create<MapLiteral>(prop_map);

  auto op_type{plan::SetProperties::Op::UPDATE};
  auto set_op = std::make_shared<plan::SetProperties>(n.op_, n.sym_, rhs, op_type);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  PullAll(*set_op, &context);
  dba.AdvanceCommand();

  auto new_properties = vertex_accessor.Properties(memgraph::storage::View::OLD);
  std::map<memgraph::storage::PropertyId, memgraph::storage::PropertyValue> expected_properties;
  expected_properties.emplace(dba.NameToProperty("prop1"), memgraph::storage::PropertyValue(43));
  expected_properties.emplace(dba.NameToProperty("prop2"), memgraph::storage::PropertyValue(44));
  expected_properties.emplace(dba.NameToProperty("new_prop1"), memgraph::storage::PropertyValue(43));
  expected_properties.emplace(dba.NameToProperty("new_prop2"), memgraph::storage::PropertyValue(44));
  EXPECT_EQ(new_properties.HasError(), false);
  EXPECT_EQ(*new_properties, expected_properties);
}

TYPED_TEST(QueryPlanTest, SetLabelsOnNull) {
  // OPTIONAL MATCH (n) SET n :label
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto label = dba.NameToLabel("label");
  std::vector<StorageLabelType> labels;
  labels.emplace_back(label);
  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto optional = std::make_shared<plan::Optional>(nullptr, n.op_, std::vector<Symbol>{n.sym_});
  auto set_op = std::make_shared<plan::SetLabels>(optional, n.sym_, labels);
  EXPECT_EQ(0, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*set_op, &context));
}

TYPED_TEST(QueryPlanTest, RemovePropertyOnNull) {
  // REMOVE (Null).prop
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  SymbolTable symbol_table;
  auto prop = PROPERTY_PAIR(dba, "property");
  auto null = LITERAL(TypedValue());
  auto n_prop = PROPERTY_LOOKUP(dba, null, prop);
  auto once = std::make_shared<Once>();
  auto remove_op = std::make_shared<plan::RemoveProperty>(once, prop.second, n_prop);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*remove_op, &context));
}

TYPED_TEST(QueryPlanTest, RemoveLabelsOnNull) {
  // OPTIONAL MATCH (n) REMOVE n :label
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto label = dba.NameToLabel("label");
  std::vector<StorageLabelType> labels;
  labels.emplace_back(label);
  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto optional = std::make_shared<plan::Optional>(nullptr, n.op_, std::vector<Symbol>{n.sym_});
  auto remove_op = std::make_shared<plan::RemoveLabels>(optional, n.sym_, labels);
  EXPECT_EQ(0, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*remove_op, &context));
}

TYPED_TEST(QueryPlanTest, DeleteSetProperty) {
  // MATCH (n) DELETE n SET n.property = 42 RETURN n
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  // Add a single vertex.
  dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_get = this->storage.template Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
  auto prop = PROPERTY_PAIR(dba, "property");
  auto n_prop = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);
  auto set_op = std::make_shared<plan::SetProperty>(delete_op, prop.second, n_prop, LITERAL(42));
  auto accumulate_op = std::make_shared<plan::Accumulate>(set_op, set_op->ModifiedSymbols(symbol_table), true);

  auto prop_lookup = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);
  auto n_p =
      this->storage.template Create<NamedExpression>("n", prop_lookup)->MapTo(symbol_table.CreateSymbol("bla", true));
  auto produce = MakeProduce(accumulate_op, n_p);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  ASSERT_THROW(CollectProduce(*produce, &context), QueryRuntimeException);
}

TYPED_TEST(QueryPlanTest, DeleteSetPropertiesFromMap) {
  // MATCH (n) DELETE n SET n = {property: 42} return n
  // MATCH (n) DELETE n SET n += {property: 42} return n
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  // Add a single vertex.
  dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_get = this->storage.template Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
  auto prop = PROPERTY_PAIR(dba, "property");
  std::unordered_map<PropertyIx, Expression *> prop_map;
  prop_map.emplace(this->storage.GetPropertyIx(prop.first), LITERAL(42));
  auto *rhs = this->storage.template Create<MapLiteral>(prop_map);
  for (auto op_type : {plan::SetProperties::Op::REPLACE, plan::SetProperties::Op::UPDATE}) {
    auto set_op = std::make_shared<plan::SetProperties>(delete_op, n.sym_, rhs, op_type);
    auto accumulate_op = std::make_shared<plan::Accumulate>(set_op, set_op->ModifiedSymbols(symbol_table), false);
    auto prop_lookup = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);
    auto n_p =
        this->storage.template Create<NamedExpression>("n", prop_lookup)->MapTo(symbol_table.CreateSymbol("bla", true));
    auto produce = MakeProduce(accumulate_op, n_p);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    ASSERT_THROW(CollectProduce(*produce, &context), QueryRuntimeException);
  }
}

TYPED_TEST(QueryPlanTest, DeleteSetPropertiesFrom) {
  // MATCH (n) DELETE n SET n = n RETURN n
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  // Add a single vertex.
  {
    auto v = dba.InsertVertex();
    ASSERT_TRUE(v.SetProperty(dba.NameToProperty("property"), memgraph::storage::PropertyValue(1)).HasValue());
  }
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_get = this->storage.template Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
  auto *rhs = IDENT("n")->MapTo(n.sym_);
  auto prop = PROPERTY_PAIR(dba, "property");
  for (auto op_type : {plan::SetProperties::Op::REPLACE, plan::SetProperties::Op::UPDATE}) {
    auto set_op = std::make_shared<plan::SetProperties>(delete_op, n.sym_, rhs, op_type);
    auto accumulate_op = std::make_shared<plan::Accumulate>(set_op, set_op->ModifiedSymbols(symbol_table), false);
    auto prop_lookup = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);
    auto n_p =
        this->storage.template Create<NamedExpression>("n", prop_lookup)->MapTo(symbol_table.CreateSymbol("bla", true));
    auto produce = MakeProduce(accumulate_op, n_p);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    ASSERT_THROW(CollectProduce(*produce, &context), QueryRuntimeException);
  }
}

TYPED_TEST(QueryPlanTest, DeleteRemoveLabels) {
  // MATCH (n) DELETE n REMOVE n :label return n
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  // Add a single vertex.
  dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_get = this->storage.template Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
  std::vector<StorageLabelType> labels{dba.NameToLabel("label")};
  auto rem_op = std::make_shared<plan::RemoveLabels>(delete_op, n.sym_, labels);
  auto accumulate_op = std::make_shared<plan::Accumulate>(rem_op, rem_op->ModifiedSymbols(symbol_table), true);

  auto prop = PROPERTY_PAIR(dba, "property");
  auto prop_lookup = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);
  auto n_p =
      this->storage.template Create<NamedExpression>("n", prop_lookup)->MapTo(symbol_table.CreateSymbol("bla", true));
  auto produce = MakeProduce(accumulate_op, n_p);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  ASSERT_THROW(CollectProduce(*produce, &context), QueryRuntimeException);
}

TYPED_TEST(QueryPlanTest, DeleteRemoveProperty) {
  // MATCH (n) DELETE n REMOVE n.property RETURN n
  auto storage_dba = this->db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  // Add a single vertex.
  dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(1, CountIterable(dba.Vertices(memgraph::storage::View::OLD)));
  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_get = this->storage.template Create<Identifier>("n")->MapTo(n.sym_);
  auto delete_op = std::make_shared<plan::Delete>(n.op_, std::vector<Expression *>{n_get}, false);
  auto prop = PROPERTY_PAIR(dba, "property");
  auto n_prop = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);
  auto rem_op = std::make_shared<plan::RemoveProperty>(delete_op, prop.second, n_prop);
  auto accumulate_op = std::make_shared<plan::Accumulate>(rem_op, rem_op->ModifiedSymbols(symbol_table), true);

  auto prop_lookup = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);
  auto n_p =
      this->storage.template Create<NamedExpression>("n", prop_lookup)->MapTo(symbol_table.CreateSymbol("bla", true));
  auto produce = MakeProduce(accumulate_op, n_p);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  ASSERT_THROW(CollectProduce(*produce, &context), QueryRuntimeException);
}

//////////////////////////////////////////////
////     FINE GRAINED AUTHORIZATION      /////
//////////////////////////////////////////////
#ifdef MG_ENTERPRISE
template <typename StorageType>
class UpdatePropertiesWithAuthFixture : public QueryPlanTest<StorageType> {
 protected:
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba{this->db->Access(ReplicationRole::MAIN)};
  memgraph::query::DbAccessor dba{storage_dba.get()};
  SymbolTable symbol_table;

  const std::string vertex_label_name = "l1";
  const memgraph::storage::LabelId vertex_label = dba.NameToLabel(vertex_label_name);

  const std::string entity_prop_name = "prop";
  const memgraph::storage::PropertyId entity_prop{dba.NameToProperty(entity_prop_name)};
  const memgraph::storage::PropertyValue entity_prop_value{1};

  const std::string label_name_1 = "l1";
  const memgraph::storage::LabelId label_1{dba.NameToLabel(label_name_1)};
  const std::string label_name_2 = "l1";
  const memgraph::storage::LabelId label_2{dba.NameToLabel(label_name_2)};

  const std::string edge_prop_name = "prop";
  const memgraph::storage::PropertyId edge_prop{dba.NameToProperty(edge_prop_name)};
  const memgraph::storage::PropertyValue edge_prop_value{1};

  void SetUp() override { memgraph::license::global_license_checker.EnableTesting(); }

  void SetVertexProperty(memgraph::query::VertexAccessor vertex) {
    static_cast<void>(vertex.SetProperty(entity_prop, entity_prop_value));
  }

  void SetEdgeProperty(memgraph::query::EdgeAccessor edge) {
    static_cast<void>(edge.SetProperty(entity_prop, entity_prop_value));
  }

  void ExecuteSetPropertyOnVertex(memgraph::auth::User user, int new_property_value) {
    // MATCH (n) SET n.prop = 2
    auto scan_all = MakeScanAll(this->storage, symbol_table, "n");

    auto literal = LITERAL(new_property_value);
    auto n_p = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(scan_all.sym_), entity_prop);
    auto set_property = std::make_shared<plan::SetProperty>(scan_all.op_, entity_prop, n_p, literal);

    // produce the node
    auto output =
        NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(set_property, output);

    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);

    PullAll(*produce, &context);
    dba.AdvanceCommand();
  };

  void ExecuteSetPropertyOnEdge(memgraph::auth::User user, int new_property_value) {
    // MATCH (n)-[r]->(m) SET r.prop = 2
    auto scan_all = MakeScanAll(this->storage, symbol_table, "n");
    auto expand = MakeExpand(this->storage, symbol_table, scan_all.op_, scan_all.sym_, "r", EdgeAtom::Direction::OUT,
                             {}, "m", false, memgraph::storage::View::OLD);
    // set property to 2 on n
    auto literal = LITERAL(new_property_value);
    auto n_p = PROPERTY_LOOKUP(dba, IDENT("r")->MapTo(expand.edge_sym_), entity_prop);
    auto set_property = std::make_shared<plan::SetProperty>(expand.op_, entity_prop, n_p, literal);

    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);

    PullAll(*set_property, &context);
    dba.AdvanceCommand();
  };

  void ExecuteSetPropertiesOnVertex(memgraph::auth::User user, int new_property_value) {
    // MATCH (n) SET n = {prop: 2};
    auto scan_all = MakeScanAll(this->storage, symbol_table, "n");

    auto prop = PROPERTY_PAIR(dba, entity_prop_name);
    std::unordered_map<PropertyIx, Expression *> prop_map;
    prop_map.emplace(this->storage.GetPropertyIx(prop.first), LITERAL(new_property_value));
    auto *rhs = this->storage.template Create<MapLiteral>(prop_map);
    auto set_properties =
        std::make_shared<plan::SetProperties>(scan_all.op_, scan_all.sym_, rhs, plan::SetProperties::Op::UPDATE);

    auto output =
        NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(set_properties, output);

    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);

    PullAll(*produce, &context);
    dba.AdvanceCommand();
  };

  void ExecuteSetPropertiesOnEdge(memgraph::auth::User user, int new_property_value) {
    // MATCH (n)-[r]->(m) SET r = {prop: 2};
    auto scan_all = MakeScanAll(this->storage, symbol_table, "n");
    auto expand = MakeExpand(this->storage, symbol_table, scan_all.op_, scan_all.sym_, "r", EdgeAtom::Direction::OUT,
                             {}, "m", false, memgraph::storage::View::OLD);

    auto prop = PROPERTY_PAIR(dba, entity_prop_name);
    std::unordered_map<PropertyIx, Expression *> prop_map;
    prop_map.emplace(this->storage.GetPropertyIx(prop.first), LITERAL(new_property_value));
    auto *rhs = this->storage.template Create<MapLiteral>(prop_map);
    auto set_properties =
        std::make_shared<plan::SetProperties>(expand.op_, expand.edge_sym_, rhs, plan::SetProperties::Op::UPDATE);

    auto output =
        NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(set_properties, output);

    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);

    PullAll(*produce, &context);
    dba.AdvanceCommand();
  };

  void ExecuteRemovePropertyOnVertex(memgraph::auth::User user) {
    // MATCH (n) REMOVE n.prop
    auto scan_all = MakeScanAll(this->storage, symbol_table, "n");

    auto n_p = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(scan_all.sym_), entity_prop);
    auto remove_property = std::make_shared<plan::RemoveProperty>(scan_all.op_, entity_prop, n_p);

    // produce the node
    auto output =
        NEXPR("n", IDENT("n")->MapTo(scan_all.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_1", true));
    auto produce = MakeProduce(remove_property, output);

    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);

    PullAll(*produce, &context);
    dba.AdvanceCommand();
  };

  void ExecuteRemovePropertyOnEdge(memgraph::auth::User user) {
    // MATCH (n)-[r]->(m) REMOVE r.prop
    auto scan_all = MakeScanAll(this->storage, symbol_table, "n");
    auto expand = MakeExpand(this->storage, symbol_table, scan_all.op_, scan_all.sym_, "r", EdgeAtom::Direction::OUT,
                             {}, "m", false, memgraph::storage::View::OLD);
    // set property to 2 on n
    auto n_p = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(expand.edge_sym_), entity_prop);
    auto remove_property = std::make_shared<plan::RemoveProperty>(expand.op_, entity_prop, n_p);

    memgraph::glue::FineGrainedAuthChecker auth_checker{user, &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);

    PullAll(*remove_property, &context);
    dba.AdvanceCommand();
  };
};

TYPED_TEST_CASE(UpdatePropertiesWithAuthFixture, StorageTypes);

TYPED_TEST(UpdatePropertiesWithAuthFixture, SetPropertyWithAuthChecker) {
  // Add a single vertex
  auto v = this->dba.InsertVertex();
  ASSERT_TRUE(v.AddLabel(this->vertex_label).HasValue());
  ASSERT_TRUE(v.SetProperty(this->entity_prop, this->entity_prop_value).HasValue());
  this->dba.AdvanceCommand();

  EXPECT_EQ(1, CountIterable(this->dba.Vertices(memgraph::storage::View::OLD)));

  auto test_hypothesis = [&](int expected_property_value) {
    auto vertex = *this->dba.Vertices(memgraph::storage::View::NEW).begin();
    auto maybe_properties = vertex.Properties(memgraph::storage::View::NEW);
    ASSERT_TRUE(maybe_properties.HasValue());
    const auto &properties = *maybe_properties;
    EXPECT_EQ(properties.size(), 1);
    auto maybe_prop = vertex.GetProperty(memgraph::storage::View::NEW, this->entity_prop);
    ASSERT_TRUE(maybe_prop.HasValue());
    ASSERT_EQ(maybe_prop->ValueInt(), expected_property_value);
  };

  auto test_remove_hypothesis = [&](int properties_size) {
    auto vertex = *this->dba.Vertices(memgraph::storage::View::NEW).begin();
    auto maybe_properties = vertex.Properties(memgraph::storage::View::NEW);
    ASSERT_TRUE(maybe_properties.HasValue());
    const auto &properties = *maybe_properties;
    EXPECT_EQ(properties.size(), properties_size);
  };

  {
    auto user = memgraph::auth::User{"denied_global"};

    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::NOTHING);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertyOnVertex(user, 2);
    test_hypothesis(1);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertiesOnVertex(user, 2);
    test_hypothesis(1);

    this->SetVertexProperty(v);
    this->ExecuteRemovePropertyOnVertex(user);
    test_remove_hypothesis(1);
  }

  {
    auto user = memgraph::auth::User{"denied_label"};

    user.fine_grained_access_handler().label_permissions().Grant(this->vertex_label_name,
                                                                 memgraph::auth::FineGrainedPermission::NOTHING);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertyOnVertex(user, 2);
    test_hypothesis(1);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertiesOnVertex(user, 2);
    test_hypothesis(1);

    this->SetVertexProperty(v);
    this->ExecuteRemovePropertyOnVertex(user);
    test_remove_hypothesis(1);
  }

  {
    auto user = memgraph::auth::User{"granted_global"};

    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertyOnVertex(user, 2);
    test_hypothesis(2);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertiesOnVertex(user, 2);
    test_hypothesis(2);

    this->SetVertexProperty(v);
    this->ExecuteRemovePropertyOnVertex(user);
    test_remove_hypothesis(0);
  }

  {
    auto user = memgraph::auth::User{"granted_label"};

    user.fine_grained_access_handler().label_permissions().Grant(this->vertex_label_name,
                                                                 memgraph::auth::FineGrainedPermission::UPDATE);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertyOnVertex(user, 2);
    test_hypothesis(2);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertiesOnVertex(user, 2);
    test_hypothesis(2);

    this->SetVertexProperty(v);
    this->ExecuteRemovePropertyOnVertex(user);
    test_remove_hypothesis(0);
  }

  {
    auto user = memgraph::auth::User{"granted_read_label"};

    user.fine_grained_access_handler().label_permissions().Grant(this->vertex_label_name,
                                                                 memgraph::auth::FineGrainedPermission::READ);

    this->SetVertexProperty(v);
    ASSERT_THROW(this->ExecuteSetPropertyOnVertex(user, 2), QueryRuntimeException);
    test_hypothesis(1);

    this->SetVertexProperty(v);
    ASSERT_THROW(this->ExecuteSetPropertiesOnVertex(user, 2), QueryRuntimeException);
    test_hypothesis(1);

    this->SetVertexProperty(v);
    ASSERT_THROW(this->ExecuteRemovePropertyOnVertex(user), QueryRuntimeException);
    test_remove_hypothesis(1);
  }

  {
    auto user = memgraph::auth::User{"granted_read_global"};

    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

    this->SetVertexProperty(v);
    ASSERT_THROW(this->ExecuteSetPropertyOnVertex(user, 2), QueryRuntimeException);
    test_hypothesis(1);

    this->SetVertexProperty(v);
    ASSERT_THROW(this->ExecuteSetPropertiesOnVertex(user, 2), QueryRuntimeException);
    test_hypothesis(1);

    this->SetVertexProperty(v);
    ASSERT_THROW(this->ExecuteRemovePropertyOnVertex(user), QueryRuntimeException);
    test_remove_hypothesis(1);
  }

  {
    auto user = memgraph::auth::User{"granted_update_label_denied_read_global"};

    user.fine_grained_access_handler().label_permissions().Grant(this->vertex_label_name,
                                                                 memgraph::auth::FineGrainedPermission::UPDATE);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::NOTHING);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertyOnVertex(user, 2);
    test_hypothesis(2);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertiesOnVertex(user, 2);
    test_hypothesis(2);

    this->SetVertexProperty(v);
    this->ExecuteRemovePropertyOnVertex(user);
    test_remove_hypothesis(0);
  }

  {
    auto user = memgraph::auth::User{"granted_update_global_denied_read_label"};

    user.fine_grained_access_handler().label_permissions().Grant(this->vertex_label_name,
                                                                 memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::UPDATE);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertyOnVertex(user, 2);
    test_hypothesis(1);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertiesOnVertex(user, 2);
    test_hypothesis(1);

    this->SetVertexProperty(v);
    this->ExecuteRemovePropertyOnVertex(user);
    test_remove_hypothesis(1);
  }

  {
    auto user = memgraph::auth::User{"granted_create_delete_label_denied_read_global"};

    user.fine_grained_access_handler().label_permissions().Grant(this->vertex_label_name,
                                                                 memgraph::auth::FineGrainedPermission::CREATE_DELETE);
    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::NOTHING);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertyOnVertex(user, 2);
    test_hypothesis(2);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertiesOnVertex(user, 2);
    test_hypothesis(2);

    this->SetVertexProperty(v);
    this->ExecuteRemovePropertyOnVertex(user);
    test_remove_hypothesis(0);
  }

  {
    auto user = memgraph::auth::User{"granted_create_delete_global_denied_read_label"};

    user.fine_grained_access_handler().label_permissions().Grant(this->vertex_label_name,
                                                                 memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().label_permissions().Grant("*",
                                                                 memgraph::auth::FineGrainedPermission::CREATE_DELETE);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertyOnVertex(user, 2);
    test_hypothesis(1);

    this->SetVertexProperty(v);
    this->ExecuteSetPropertiesOnVertex(user, 2);
    test_hypothesis(1);

    this->SetVertexProperty(v);
    this->ExecuteRemovePropertyOnVertex(user);
    test_remove_hypothesis(1);
  }
}

TYPED_TEST(UpdatePropertiesWithAuthFixture, SetPropertyExpandWithAuthChecker) {
  // Add a single vertex
  auto v1 = this->dba.InsertVertex();
  ASSERT_TRUE(v1.AddLabel(this->label_1).HasValue());

  auto v2 = this->dba.InsertVertex();
  ASSERT_TRUE(v2.AddLabel(this->label_2).HasValue());

  auto edge_type_name = "edge_type";
  auto edge_type_id = this->dba.NameToEdgeType(edge_type_name);
  auto edge = this->dba.InsertEdge(&v1, &v2, edge_type_id);
  ASSERT_TRUE(edge.HasValue());
  ASSERT_TRUE(edge->SetProperty(this->entity_prop, this->entity_prop_value).HasValue());
  this->dba.AdvanceCommand();

  EXPECT_EQ(2, CountIterable(this->dba.Vertices(memgraph::storage::View::OLD)));

  auto test_hypothesis = [&](int expected_property_value) {
    for (auto vertex : this->dba.Vertices(memgraph::storage::View::NEW)) {
      if (vertex.OutEdges(memgraph::storage::View::NEW).HasValue()) {
        auto maybe_edges = vertex.OutEdges(memgraph::storage::View::NEW);
        for (auto edge : maybe_edges->edges) {
          EXPECT_EQ(edge.EdgeType(), edge_type_id);
          auto maybe_properties = edge.Properties(memgraph::storage::View::NEW);
          ASSERT_TRUE(maybe_properties.HasValue());
          const auto &properties = *maybe_properties;
          EXPECT_EQ(properties.size(), 1);
          auto maybe_prop = edge.GetProperty(memgraph::storage::View::NEW, this->entity_prop);
          ASSERT_TRUE(maybe_prop.HasValue());
          ASSERT_EQ(maybe_prop->ValueInt(), expected_property_value);
        }
      }
    }
  };

  auto test_remove_hypothesis = [&](int properties_size) {
    for (auto vertex : this->dba.Vertices(memgraph::storage::View::NEW)) {
      if (vertex.OutEdges(memgraph::storage::View::NEW).HasValue()) {
        auto maybe_edges = vertex.OutEdges(memgraph::storage::View::NEW);
        for (auto edge : maybe_edges->edges) {
          EXPECT_EQ(edge.EdgeType(), edge_type_id);
          auto maybe_properties = edge.Properties(memgraph::storage::View::NEW);
          ASSERT_TRUE(maybe_properties.HasValue());
          const auto &properties = *maybe_properties;
          EXPECT_EQ(properties.size(), properties_size);
        }
      }
    }
  };

  {
    auto user = memgraph::auth::User{"denied_global"};

    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertyOnEdge(user, 2);
    test_hypothesis(1);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertiesOnEdge(user, 2);
    test_hypothesis(1);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteRemovePropertyOnEdge(user);
    test_remove_hypothesis(1);
  }

  {
    auto user = memgraph::auth::User{"denied_edge_type"};

    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant(edge_type_name,
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertyOnEdge(user, 2);
    test_hypothesis(1);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertiesOnEdge(user, 2);
    test_hypothesis(1);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteRemovePropertyOnEdge(user);
    test_remove_hypothesis(1);
  }

  {
    auto user = memgraph::auth::User{"granted_global"};

    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                     memgraph::auth::FineGrainedPermission::UPDATE);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertyOnEdge(user, 2);
    test_hypothesis(2);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertiesOnEdge(user, 2);
    test_hypothesis(2);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteRemovePropertyOnEdge(user);
    test_remove_hypothesis(0);
  }

  {
    auto user = memgraph::auth::User{"granted_edge_type"};

    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant(edge_type_name,
                                                                     memgraph::auth::FineGrainedPermission::UPDATE);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertyOnEdge(user, 2);
    test_hypothesis(2);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertiesOnEdge(user, 2);
    test_hypothesis(2);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteRemovePropertyOnEdge(user);
    test_remove_hypothesis(0);
  }

  {
    auto user = memgraph::auth::User{"granted_update_edge_type_denied_read_global"};

    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant(edge_type_name,
                                                                     memgraph::auth::FineGrainedPermission::UPDATE);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertyOnEdge(user, 2);
    test_hypothesis(2);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertiesOnEdge(user, 2);
    test_hypothesis(2);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteRemovePropertyOnEdge(user);
    test_remove_hypothesis(0);
  }

  {
    auto user = memgraph::auth::User{"granted_read_edge_type"};

    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant(edge_type_name,
                                                                     memgraph::auth::FineGrainedPermission::READ);

    this->SetEdgeProperty(edge.GetValue());
    ASSERT_THROW(this->ExecuteSetPropertyOnEdge(user, 2), QueryRuntimeException);
    test_hypothesis(1);

    this->SetEdgeProperty(edge.GetValue());
    ASSERT_THROW(this->ExecuteSetPropertiesOnEdge(user, 2), QueryRuntimeException);
    test_hypothesis(1);

    this->SetEdgeProperty(edge.GetValue());
    ASSERT_THROW(this->ExecuteRemovePropertyOnEdge(user), QueryRuntimeException);
    test_remove_hypothesis(1);
  }

  {
    auto user = memgraph::auth::User{"granted_read_global"};

    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);

    this->SetEdgeProperty(edge.GetValue());
    ASSERT_THROW(this->ExecuteSetPropertyOnEdge(user, 2), QueryRuntimeException);
    test_hypothesis(1);

    this->SetEdgeProperty(edge.GetValue());
    ASSERT_THROW(this->ExecuteSetPropertiesOnEdge(user, 2), QueryRuntimeException);
    test_hypothesis(1);

    this->SetEdgeProperty(edge.GetValue());
    ASSERT_THROW(this->ExecuteRemovePropertyOnEdge(user), QueryRuntimeException);
    test_remove_hypothesis(1);
  }

  {
    auto user = memgraph::auth::User{"granted_update_global_denied_read_edge_type"};

    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant(edge_type_name,
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                     memgraph::auth::FineGrainedPermission::UPDATE);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertyOnEdge(user, 2);
    test_hypothesis(1);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertiesOnEdge(user, 2);
    test_hypothesis(1);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteRemovePropertyOnEdge(user);
    test_remove_hypothesis(1);
  }

  {
    auto user = memgraph::auth::User{"granted_create_delete_edge_type_denied_read_global"};

    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant(
        edge_type_name, memgraph::auth::FineGrainedPermission::CREATE_DELETE);
    user.fine_grained_access_handler().edge_type_permissions().Grant("*",
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertyOnEdge(user, 2);
    test_hypothesis(2);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertiesOnEdge(user, 2);
    test_hypothesis(2);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteRemovePropertyOnEdge(user);
    test_remove_hypothesis(0);
  }

  {
    auto user = memgraph::auth::User{"granted_create_delete_global_denied_read_edge_type"};

    user.fine_grained_access_handler().label_permissions().Grant("*", memgraph::auth::FineGrainedPermission::READ);
    user.fine_grained_access_handler().edge_type_permissions().Grant(edge_type_name,
                                                                     memgraph::auth::FineGrainedPermission::NOTHING);
    user.fine_grained_access_handler().edge_type_permissions().Grant(
        "*", memgraph::auth::FineGrainedPermission::CREATE_DELETE);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertyOnEdge(user, 2);
    test_hypothesis(1);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteSetPropertiesOnEdge(user, 2);
    test_hypothesis(1);

    this->SetEdgeProperty(edge.GetValue());
    this->ExecuteRemovePropertyOnEdge(user);
    test_remove_hypothesis(1);
  }
}
#endif

template <typename StorageType>
class DynamicExpandFixture : public testing::Test {
 protected:
  const std::string testSuite = "query_plan_create_set_remove_delete_dynamic_expand";
  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db{new StorageType(config)};
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba{db->Access(ReplicationRole::MAIN)};
  memgraph::query::DbAccessor dba{storage_dba.get()};
  SymbolTable symbol_table;
  AstStorage storage;

  // make 2 nodes connected to the third node
  memgraph::query::VertexAccessor v1{dba.InsertVertex()};
  memgraph::query::VertexAccessor v2{dba.InsertVertex()};
  memgraph::query::VertexAccessor v3{dba.InsertVertex()};
  memgraph::query::VertexAccessor v4{dba.InsertVertex()};
  memgraph::query::VertexAccessor v5{dba.InsertVertex()};
  memgraph::storage::EdgeTypeId edge_type{db->NameToEdgeType("Edge")};
  memgraph::query::EdgeAccessor r1{*dba.InsertEdge(&v1, &v5, edge_type)};
  memgraph::query::EdgeAccessor r2{*dba.InsertEdge(&v2, &v5, edge_type)};
  memgraph::query::EdgeAccessor r3{*dba.InsertEdge(&v3, &v5, edge_type)};
  memgraph::query::EdgeAccessor r4{*dba.InsertEdge(&v4, &v5, edge_type)};

  memgraph::storage::LabelId node_label{dba.NameToLabel("Node")};
  memgraph::storage::LabelId supernode_label{dba.NameToLabel("Supernode")};

  void SetUp() override {
    ASSERT_TRUE(v1.AddLabel(node_label).HasValue());
    ASSERT_TRUE(v2.AddLabel(node_label).HasValue());
    ASSERT_TRUE(v3.AddLabel(node_label).HasValue());
    ASSERT_TRUE(v4.AddLabel(node_label).HasValue());
    ASSERT_TRUE(v5.AddLabel(supernode_label).HasValue());
    memgraph::license::global_license_checker.EnableTesting();

    dba.AdvanceCommand();
  }

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(DynamicExpandFixture, StorageTypes);

TYPED_TEST(DynamicExpandFixture, Expand) {
  using ExpandCursor = memgraph::query::plan::Expand::ExpandCursor;

  auto scan_node_by_label = MakeScanAllByLabel(this->storage, this->symbol_table, "n", this->node_label);
  auto scan_supernode_by_label =
      MakeScanAllByLabel(this->storage, this->symbol_table, "s", this->supernode_label, scan_node_by_label.op_);

  auto once = std::make_shared<Once>();

  auto edge_sym = this->symbol_table.CreateSymbol("r", true);
  auto my_expand = std::make_shared<Expand>(
      scan_supernode_by_label.op_, scan_supernode_by_label.sym_, scan_node_by_label.sym_, edge_sym,
      EdgeAtom::Direction::OUT, std::vector<memgraph::storage::EdgeTypeId>{}, true, memgraph::storage::View::OLD);

  auto context = MakeContext(this->storage, this->symbol_table, &this->dba);

  Frame frame{context.symbol_table.max_position()};
  frame[scan_supernode_by_label.sym_] = this->v4;
  frame[scan_node_by_label.sym_] = this->v1;

  auto *mem = memgraph::utils::NewDeleteResource();
  auto initial_cursor_ptr = MakeUniqueCursorPtr<ExpandCursor>(mem, *my_expand, -1, -1, mem);
  auto *initial_cursor = dynamic_cast<ExpandCursor *>(initial_cursor_ptr.get());
  auto expansion_info = initial_cursor->GetExpansionInfo(frame);

  ASSERT_EQ(expansion_info.input_node.value(), this->v4);
  ASSERT_EQ(expansion_info.direction, EdgeAtom::Direction::OUT);
  ASSERT_EQ(expansion_info.existing_node.value(), this->v1);

  auto expanded_first_cursor_ptr = MakeUniqueCursorPtr<ExpandCursor>(mem, *my_expand, 1, -1, mem);
  auto *expanded_first_cursor = dynamic_cast<ExpandCursor *>(expanded_first_cursor_ptr.get());
  expansion_info = expanded_first_cursor->GetExpansionInfo(frame);

  ASSERT_EQ(expansion_info.input_node.value(), this->v1);
  ASSERT_EQ(expansion_info.direction, EdgeAtom::Direction::IN);
  ASSERT_EQ(expansion_info.existing_node.value(), this->v4);

  auto expanded_both_take_first_cursor_ptr = MakeUniqueCursorPtr<ExpandCursor>(mem, *my_expand, 1, 100, mem);
  auto *expanded_both_take_first = dynamic_cast<ExpandCursor *>(expanded_both_take_first_cursor_ptr.get());
  expansion_info = expanded_both_take_first->GetExpansionInfo(frame);

  ASSERT_EQ(expansion_info.input_node.value(), this->v4);
  ASSERT_EQ(expansion_info.direction, EdgeAtom::Direction::OUT);
  ASSERT_EQ(expansion_info.existing_node.value(), this->v1);

  auto expanded_both_take_second_cursor_ptr = MakeUniqueCursorPtr<ExpandCursor>(mem, *my_expand, 100, 1, mem);
  auto *expanded_both_take_second = dynamic_cast<ExpandCursor *>(expanded_both_take_second_cursor_ptr.get());
  expansion_info = expanded_both_take_second->GetExpansionInfo(frame);

  ASSERT_EQ(expansion_info.input_node.value(), this->v1);
  ASSERT_EQ(expansion_info.direction, EdgeAtom::Direction::IN);
  ASSERT_EQ(expansion_info.existing_node.value(), this->v4);

  auto expanded_equal_take_second_cursror_ptr = MakeUniqueCursorPtr<ExpandCursor>(mem, *my_expand, 5, 5, mem);
  auto *expanded_equal_take_second = dynamic_cast<ExpandCursor *>(expanded_equal_take_second_cursror_ptr.get());
  expansion_info = expanded_equal_take_second->GetExpansionInfo(frame);

  ASSERT_EQ(expansion_info.input_node.value(), this->v1);
  ASSERT_EQ(expansion_info.direction, EdgeAtom::Direction::IN);
  ASSERT_EQ(expansion_info.existing_node.value(), this->v4);
}

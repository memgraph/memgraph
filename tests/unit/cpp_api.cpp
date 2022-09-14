// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <queue>
#include <unordered_map>
#include <unordered_set>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "mg_exceptions.hpp"
#include "mgp.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "storage/v2/view.hpp"

struct CppApiTestFixture : public ::testing::Test {
 protected:
  virtual void SetUp() { mgp::memory = &memory; }

  mgp_graph CreateGraph(const memgraph::storage::View view = memgraph::storage::View::NEW) {
    // the execution context can be null as it shouldn't be used in these tests
    return mgp_graph{&CreateDbAccessor(memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION), view, ctx_.get()};
  }

  memgraph::query::DbAccessor &CreateDbAccessor(const memgraph::storage::IsolationLevel isolationLevel) {
    accessors_.push_back(storage.Access(isolationLevel));
    db_accessors_.emplace_back(&accessors_.back());
    return db_accessors_.back();
  }

  memgraph::storage::Storage storage;
  mgp_memory memory{memgraph::utils::NewDeleteResource()};

 private:
  std::list<memgraph::storage::Storage::Accessor> accessors_;
  std::list<memgraph::query::DbAccessor> db_accessors_;
  std::unique_ptr<memgraph::query::ExecutionContext> ctx_ = std::make_unique<memgraph::query::ExecutionContext>();
};

TEST_F(CppApiTestFixture, TestGraph) {
  mgp_graph raw_graph = CreateGraph();
  auto graph = mgp::Graph(&raw_graph);

  auto node_1 = graph.CreateNode();

  ASSERT_EQ(graph.Order(), 1);
  ASSERT_EQ(graph.Size(), 0);

  auto node_2 = graph.CreateNode();

  ASSERT_EQ(graph.Order(), 2);
  ASSERT_EQ(graph.Size(), 0);

  auto relationship = graph.CreateRelationship(node_1, node_2, "edge_type");

  ASSERT_EQ(graph.Order(), 2);
  ASSERT_EQ(graph.Size(), 1);

  ASSERT_EQ(graph.ContainsNode(node_1), true);
  ASSERT_EQ(graph.ContainsNode(node_2), true);

  ASSERT_EQ(graph.ContainsRelationship(relationship), true);
}

TEST_F(CppApiTestFixture, TestId) {
  int64_t int_1 = 8;
  uint64_t int_2 = 8;
  int64_t int_3 = 7;
  uint64_t int_4 = 7;

  auto id_1 = mgp::Id::FromInt(int_1);
  auto id_2 = mgp::Id::FromUint(int_2);
  auto id_3 = mgp::Id::FromInt(int_3);
  auto id_4 = mgp::Id::FromUint(int_4);

  ASSERT_EQ(id_1.AsInt(), 8);
  ASSERT_EQ(id_1.AsUint(), 8);
  ASSERT_EQ(id_2.AsInt(), 8);
  ASSERT_EQ(id_2.AsUint(), 8);

  ASSERT_EQ(id_1, id_2);

  ASSERT_EQ(id_1 != id_2, false);

  ASSERT_EQ(id_1 == id_3, false);
  ASSERT_EQ(id_1 == id_4, false);
  ASSERT_EQ(id_2 == id_3, false);
  ASSERT_EQ(id_2 == id_4, false);

  ASSERT_NE(id_1, id_3);
  ASSERT_NE(id_1, id_4);
  ASSERT_NE(id_2, id_3);
  ASSERT_NE(id_2, id_4);
}

TEST_F(CppApiTestFixture, TestList) {
  auto list_1 = mgp::List();

  ASSERT_EQ(list_1.Size(), 0);

  auto list_2 = mgp::List(10);

  ASSERT_EQ(list_2.Size(), 0);
  ASSERT_EQ(list_1, list_2);

  auto a = mgp::Value("a");
  list_2.Append(a);
  list_2.AppendExtend(a);

  ASSERT_EQ(list_2.Size(), 2);

  std::vector<mgp::Value> values{mgp::Value("a"), mgp::Value("b"), mgp::Value("c")};
  auto list_3 = mgp::List(values);

  ASSERT_EQ(list_3.Size(), 3);

  auto list_4 = mgp::List({mgp::Value("d"), mgp::Value("e"), mgp::Value("f")});
  ASSERT_EQ(list_4.Size(), 3);
}

TEST_F(CppApiTestFixture, TestMap) {
  auto map_1 = mgp::Map();

  std::map<std::string_view, mgp::Value> map_1a;
  for (const auto &e : map_1) {
    map_1a.insert(std::pair<std::string_view, mgp::Value>(e.key, e.value));
  }

  ASSERT_EQ(map_1.Size(), 0);
  ASSERT_EQ(map_1a.size(), 0);

  auto map_2 = mgp::Map();

  auto y = mgp::Value("y");
  map_2.Insert("x", y);

  ASSERT_EQ(map_2.Size(), 1);
  ASSERT_NE(map_1, map_2);

  auto v_1 = mgp::Value("1");
  auto v_2 = mgp::Value("2");
  auto p_1 = std::pair{"a", v_1};
  auto p_2 = std::pair{"b", v_2};
  auto map_3 = mgp::Map({p_1, p_2});

  ASSERT_EQ(map_3.Size(), 2);
}

TEST_F(CppApiTestFixture, TestNode) {
  mgp_graph raw_graph = CreateGraph();
  auto graph = mgp::Graph(&raw_graph);

  auto node_1 = graph.CreateNode();

  ASSERT_EQ(node_1.HasLabel("L1"), false);

  node_1.AddLabel("L1");
  ASSERT_EQ(node_1.HasLabel("L1"), true);

  node_1.AddLabel("L2");
  ASSERT_EQ(node_1.HasLabel("L1"), true);
  ASSERT_EQ(node_1.HasLabel("L2"), true);

  ASSERT_EQ(node_1.Properties().Size(), 0);

  auto node_2 = graph.GetNodeById(node_1.Id());

  ASSERT_EQ(node_1, node_2);

  int count_out_relationships = 0;
  for (const auto _ : node_1.OutRelationships()) {
    count_out_relationships++;
  }
  ASSERT_EQ(count_out_relationships, 0);

  int count_in_relationships = 0;
  for (const auto _ : node_1.InRelationships()) {
    count_in_relationships++;
  }

  ASSERT_EQ(count_in_relationships, 0);
}

TEST_F(CppApiTestFixture, TestNodeWithNeighbors) {
  mgp_graph raw_graph = CreateGraph();
  auto graph = mgp::Graph(&raw_graph);

  auto node_1 = graph.CreateNode();
  auto node_2 = graph.CreateNode();

  auto relationship = graph.CreateRelationship(node_1, node_2, "edge_type");

  int count_out_relationships = 0;
  int count_in_relationships = 0;
  for (const auto &node : graph.Nodes()) {
    for (const auto _ : node.OutRelationships()) {
      count_out_relationships++;
    }

    for (const auto _ : node.OutRelationships()) {
      count_in_relationships++;
    }
  }

  ASSERT_EQ(count_out_relationships, 1);
  ASSERT_EQ(count_in_relationships, 1);
}

TEST_F(CppApiTestFixture, TestRelationship) {
  mgp_graph raw_graph = CreateGraph();
  auto graph = mgp::Graph(&raw_graph);

  auto node_1 = graph.CreateNode();
  auto node_2 = graph.CreateNode();

  auto relationship = graph.CreateRelationship(node_1, node_2, "edge_type");

  ASSERT_EQ(relationship.Type(), "edge_type");
  ASSERT_EQ(relationship.Properties().Size(), 0);
  ASSERT_EQ(relationship.From().Id(), node_1.Id());
  ASSERT_EQ(relationship.To().Id(), node_2.Id());
}

TEST_F(CppApiTestFixture, TestPath) {
  mgp_graph raw_graph = CreateGraph();
  auto graph = mgp::Graph(&raw_graph);

  auto node_1 = graph.CreateNode();
  auto node_2 = graph.CreateNode();

  auto relationship = graph.CreateRelationship(node_1, node_2, "edge_type");

  auto node_0 = graph.GetNodeById(mgp::Id::FromInt(0));
  auto path = mgp::Path(node_0);

  ASSERT_EQ(path.Length(), 0);

  path.Expand(relationship);

  ASSERT_EQ(path.Length(), 1);
  ASSERT_EQ(path.GetNodeAt(0).Id(), node_0.Id());
  ASSERT_EQ(path.GetRelationshipAt(0).Id(), relationship.Id());
}

TEST_F(CppApiTestFixture, TestDate) {
  auto date_1 = mgp::Date("2022-04-09");
  auto date_2 = mgp::Date(2022, 4, 9);

  auto date_3 = mgp::Date::Now();

  ASSERT_EQ(date_1.Year(), 2022);
  ASSERT_EQ(date_1.Month(), 4);
  ASSERT_EQ(date_1.Day(), 9);
  ASSERT_EQ(date_1.Timestamp() >= 0, true);

  ASSERT_EQ(date_1, date_2);
  ASSERT_NE(date_2, date_3);
}

TEST_F(CppApiTestFixture, TestLocalTime) {
  auto lt_1 = mgp::LocalTime("09:15:00");
  auto lt_2 = mgp::LocalTime(9, 15, 0, 0, 0);
  auto lt_3 = mgp::LocalTime::Now();

  ASSERT_EQ(lt_1.Hour(), 9);
  ASSERT_EQ(lt_1.Minute(), 15);
  ASSERT_EQ(lt_1.Second(), 0);
  ASSERT_EQ(lt_1.Millisecond() >= 0, true);
  ASSERT_EQ(lt_1.Microsecond() >= 0, true);
  ASSERT_EQ(lt_1.Timestamp() >= 0, true);

  ASSERT_EQ(lt_1, lt_2);
  ASSERT_NE(lt_2, lt_3);
}

TEST_F(CppApiTestFixture, TestLocalDateTime) {
  auto ldt_1 = mgp::LocalDateTime("2021-10-05T14:15:00");
  auto ldt_2 = mgp::LocalDateTime(2021, 10, 5, 14, 15, 0, 0, 0);

  ASSERT_EQ(ldt_1.Year(), 2021);
  ASSERT_EQ(ldt_1.Month(), 10);
  ASSERT_EQ(ldt_1.Day(), 5);
  ASSERT_EQ(ldt_1.Hour(), 14);
  ASSERT_EQ(ldt_1.Minute(), 15);
  ASSERT_EQ(ldt_1.Second(), 0);
  ASSERT_EQ(ldt_1.Millisecond() >= 0, true);
  ASSERT_EQ(ldt_1.Microsecond() >= 0, true);
  ASSERT_EQ(ldt_1.Timestamp() >= 0, true);

  ASSERT_EQ(ldt_1, ldt_2);
}

TEST_F(CppApiTestFixture, TestDuration) {
  auto duration_2 = mgp::Duration("PT2M2.33S");
  auto duration_3 = mgp::Duration(1465355);
  auto duration_4 = mgp::Duration(5, 14, 15, 0, 0, 0);

  ASSERT_EQ(duration_3.Microseconds(), 1465355);
  ASSERT_NE(duration_2, duration_3);
  ASSERT_NE(duration_3, duration_4);
}

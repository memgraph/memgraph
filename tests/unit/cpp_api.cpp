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

#include <queue>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "disk_test_utils.hpp"
#include "mgp.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"

template <typename StorageType>
struct CppApiTestFixture : public ::testing::Test {
 protected:
  void SetUp() override { mgp::mrd.Register(&memory); }

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
    mgp::mrd.UnRegister();
  }

  mgp_graph CreateGraph(const memgraph::storage::View view = memgraph::storage::View::NEW) {
    // the execution context can be null as it shouldn't be used in these tests
    return mgp_graph{&CreateDbAccessor(memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION), view, ctx_.get()};
  }

  memgraph::query::DbAccessor &CreateDbAccessor(const memgraph::storage::IsolationLevel isolationLevel) {
    accessors_.push_back(storage->Access(isolationLevel));
    db_accessors_.emplace_back(accessors_.back().get());
    return db_accessors_.back();
  }

  const std::string testSuite = "cpp_api";

  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> storage{new StorageType(config)};
  mgp_memory memory{memgraph::utils::NewDeleteResource()};

 private:
  std::list<std::unique_ptr<memgraph::storage::Storage::Accessor>> accessors_;
  std::list<memgraph::query::DbAccessor> db_accessors_;
  std::unique_ptr<memgraph::query::ExecutionContext> ctx_ = std::make_unique<memgraph::query::ExecutionContext>();
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(CppApiTestFixture, StorageTypes);

TYPED_TEST(CppApiTestFixture, TestGraph) {
  mgp_graph raw_graph = this->CreateGraph();
  auto graph = mgp::Graph(&raw_graph);

  auto node_1 = graph.CreateNode();

  ASSERT_EQ(graph.Order(), 1);
  ASSERT_EQ(graph.Size(), 0);

  auto node_2 = graph.CreateNode();

  ASSERT_EQ(graph.Order(), 2);
  ASSERT_EQ(graph.Size(), 0);

  auto relationship_1 = graph.CreateRelationship(node_1, node_2, "edge_type");

  ASSERT_EQ(graph.Order(), 2);
  ASSERT_EQ(graph.Size(), 1);

  ASSERT_EQ(graph.ContainsNode(node_1), true);
  ASSERT_EQ(graph.ContainsNode(node_2), true);

  ASSERT_EQ(graph.ContainsRelationship(relationship_1), true);

  auto node_3 = graph.CreateNode();
  auto relationship_2 = graph.CreateRelationship(node_1, node_3, "edge_type");
  auto relationship_3 = graph.CreateRelationship(node_2, node_3, "edge_type");

  for (const auto &n : graph.Nodes()) {
    ASSERT_EQ(graph.ContainsNode(n), true);
  }

  std::uint64_t n_rels = 0;
  for (const auto &r : graph.Relationships()) {
    ASSERT_EQ(graph.ContainsRelationship(r), true);
    n_rels++;
  }

  ASSERT_EQ(n_rels, 3);
}

TYPED_TEST(CppApiTestFixture, TestId) {
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

TYPED_TEST(CppApiTestFixture, TestList) {
  auto list_1 = mgp::List();

  ASSERT_EQ(list_1.Size(), 0);

  auto list_2 = mgp::List(10);

  ASSERT_EQ(list_2.Size(), 0);
  ASSERT_EQ(list_1, list_2);

  auto a = mgp::Value("a");
  list_2.Append(a);
  list_2.AppendExtend(a);
  list_2.AppendExtend(mgp::Value("b"));

  ASSERT_EQ(list_2.Size(), 3);

  std::vector<mgp::Value> values{mgp::Value("a"), mgp::Value("b"), mgp::Value("c")};
  auto list_3 = mgp::List(values);

  ASSERT_EQ(list_3.Size(), 3);

  auto list_4 = mgp::List({mgp::Value("d"), mgp::Value("e"), mgp::Value("f")});
  ASSERT_EQ(list_4.Size(), 3);

  // Use copy assignment
  auto list_x = list_1;

  // Use move assignment
  std::vector<mgp::List> vector_x;
  vector_x.emplace_back();

  // Use Value copy constructor
  auto value_x = mgp::Value(list_1);
  // Use Value move constructor
  auto value_y = mgp::Value(mgp::List());
}

TYPED_TEST(CppApiTestFixture, TestMap) {
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

  // Use copy assignment
  auto map_x = map_1;

  // Use move assignment
  std::vector<mgp::Map> vector_x;
  vector_x.emplace_back();

  // Use Value copy constructor
  auto value_x = mgp::Value(map_1);
  // Use Value move constructor
  auto value_y = mgp::Value(mgp::Map());

  auto value_z = value_x;
}

TYPED_TEST(CppApiTestFixture, TestNode) {
  mgp_graph raw_graph = this->CreateGraph();
  auto graph = mgp::Graph(&raw_graph);

  auto node_1 = graph.CreateNode();

  ASSERT_EQ(node_1.HasLabel("L1"), false);

  node_1.AddLabel("L1");
  ASSERT_EQ(node_1.HasLabel("L1"), true);

  node_1.AddLabel("L2");
  ASSERT_EQ(node_1.HasLabel("L1"), true);
  ASSERT_EQ(node_1.HasLabel("L2"), true);

  ASSERT_EQ(node_1.Properties().size(), 0);

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

  // Use copy assignment
  auto node_x = node_1;

  // Use move assignment
  std::vector<mgp::Node> vector_x;
  vector_x.push_back(graph.CreateNode());

  // Use Value copy constructor
  auto value_x = mgp::Value(node_1);
  // Use Value move constructor
  auto value_y = mgp::Value(graph.CreateNode());
}

TYPED_TEST(CppApiTestFixture, TestNodeWithNeighbors) {
  mgp_graph raw_graph = this->CreateGraph();
  auto graph = mgp::Graph(&raw_graph);

  auto node_1 = graph.CreateNode();
  auto node_2 = graph.CreateNode();
  auto node_3 = graph.CreateNode();

  auto relationship_1 = graph.CreateRelationship(node_1, node_2, "edge_type");
  auto relationship_2 = graph.CreateRelationship(node_1, node_3, "edge_type");

  int count_out_relationships = 0;
  int count_in_relationships = 0;
  for (const auto &node : graph.Nodes()) {
    for (const auto _ : node.OutRelationships()) {
      count_out_relationships++;
    }

    for (const auto _ : node.InRelationships()) {
      count_in_relationships++;
    }
  }

  ASSERT_EQ(count_out_relationships, 2);
  ASSERT_EQ(count_in_relationships, 2);
}

TYPED_TEST(CppApiTestFixture, TestRelationship) {
  mgp_graph raw_graph = this->CreateGraph();
  auto graph = mgp::Graph(&raw_graph);

  auto node_1 = graph.CreateNode();
  auto node_2 = graph.CreateNode();

  auto relationship = graph.CreateRelationship(node_1, node_2, "edge_type");

  ASSERT_EQ(relationship.Type(), "edge_type");
  ASSERT_EQ(relationship.Properties().size(), 0);
  ASSERT_EQ(relationship.From().Id(), node_1.Id());
  ASSERT_EQ(relationship.To().Id(), node_2.Id());

  // Use copy assignment
  auto relationship_x = relationship;

  // Use move assignment
  std::vector<mgp::Relationship> vector_x;
  vector_x.push_back(graph.CreateRelationship(node_2, node_1, "relationship_x"));

  // Use Value copy constructor
  auto value_x = mgp::Value(relationship);
  // Use Value move constructor
  auto value_y = mgp::Value(graph.CreateRelationship(node_2, node_1, "edge_type"));
}

TYPED_TEST(CppApiTestFixture, TestPath) {
  mgp_graph raw_graph = this->CreateGraph();
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

  // Use copy assignment
  auto path_x = path;

  // Use move assignment
  std::vector<mgp::Path> vector_x;
  vector_x.emplace_back(node_0);

  // Use Value copy constructor
  auto value_x = mgp::Value(path);
  // Use Value move constructor
  auto value_y = mgp::Value(mgp::Path(node_0));

  path.Pop();
  ASSERT_EQ(path.Length(), 0);
}

TYPED_TEST(CppApiTestFixture, TestDate) {
  auto date_1 = mgp::Date("2022-04-09");
  auto date_2 = mgp::Date(2022, 4, 9);

  auto date_3 = mgp::Date::Now();

  ASSERT_EQ(date_1.Year(), 2022);
  ASSERT_EQ(date_1.Month(), 4);
  ASSERT_EQ(date_1.Day(), 9);
  ASSERT_EQ(date_1.Timestamp() >= 0, true);

  ASSERT_EQ(date_1, date_2);
  ASSERT_NE(date_2, date_3);

  // Use copy assignment
  auto date_x = date_1;

  // Use move assignment
  std::vector<mgp::Date> vector_x;
  vector_x.emplace_back("2022-04-09");

  // Use Value copy constructor
  auto value_x = mgp::Value(date_1);
  // Use Value move constructor
  auto value_y = mgp::Value(mgp::Date("2022-04-09"));
}

TYPED_TEST(CppApiTestFixture, TestLocalTime) {
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

  // Use copy assignment
  auto lt_x = lt_1;

  // Use move assignment
  std::vector<mgp::LocalTime> vector_x;
  vector_x.emplace_back("09:15:00");

  // Use Value copy constructor
  auto value_x = mgp::Value(lt_1);
  // Use Value move constructor
  auto value_y = mgp::Value(mgp::LocalTime("09:15:00"));
}

TYPED_TEST(CppApiTestFixture, TestLocalDateTime) {
  auto ldt_1 = mgp::LocalDateTime("2021-10-05T14:15:00");
  auto ldt_2 = mgp::LocalDateTime(2021, 10, 5, 14, 15, 0, 0, 0);

  ASSERT_ANY_THROW(mgp::LocalDateTime(
      2021, 10, 0, 14, 15, 0, 0,
      0));  // ...10, 0, 14... <- 0 is an illegal value for the `day` parameter; must throw an exception

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

  // Use copy assignment
  auto ldt_x = ldt_1;

  // Use move assignment
  std::vector<mgp::LocalDateTime> vector_x;
  vector_x.emplace_back("2021-10-05T14:15:00");

  // Use Value copy constructor
  auto value_x = mgp::Value(ldt_1);
  // Use Value move constructor
  auto value_y = mgp::Value(mgp::LocalDateTime("2021-10-05T14:15:00"));
}

TYPED_TEST(CppApiTestFixture, TestDuration) {
  auto duration_1 = mgp::Duration("PT2M2.33S");
  auto duration_2 = mgp::Duration(1465355);
  auto duration_3 = mgp::Duration(5, 14, 15, 0, 0, 0);

  ASSERT_EQ(duration_2.Microseconds(), 1465355);
  ASSERT_NE(duration_1, duration_2);
  ASSERT_NE(duration_2, duration_3);

  // Use copy assignment
  auto duration_x = duration_1;

  // Use move assignment
  std::vector<mgp::Duration> vector_x;
  vector_x.emplace_back("PT2M2.33S");

  // Use Value copy constructor
  auto value_x = mgp::Value(duration_1);
  // Use Value move constructor
  auto value_y = mgp::Value(mgp::Duration("PT2M2.33S"));
}

TYPED_TEST(CppApiTestFixture, TestNodeProperties) {
  mgp_graph raw_graph = this->CreateGraph(memgraph::storage::View::NEW);
  auto graph = mgp::Graph(&raw_graph);

  auto node_1 = graph.CreateNode();

  ASSERT_EQ(node_1.Properties().size(), 0);

  std::unordered_map<std::string, mgp::Value> node1_prop = node_1.Properties();
  node_1.SetProperty("b", mgp::Value("b"));

  ASSERT_EQ(node_1.Properties().size(), 1);
  ASSERT_EQ(node_1.Properties()["b"].ValueString(), "b");
  ASSERT_EQ(node_1.GetProperty("b").ValueString(), "b");
}

TYPED_TEST(CppApiTestFixture, TestValueOperatorLessThan) {
  const int64_t int1 = 3;
  const int64_t int2 = 4;
  const double double1 = 3.5;
  const mgp::List list1 = mgp::List();
  const mgp::Map map1 = mgp::Map();
  const mgp::Value int_test1 = mgp::Value(int1);
  const mgp::Value int_test2 = mgp::Value(int2);
  const mgp::Value double_test1 = mgp::Value(double1);
  const mgp::Value list_test = mgp::Value(list1);
  const mgp::Value map_test = mgp::Value(map1);

  ASSERT_TRUE(int_test1 < int_test2);
  ASSERT_TRUE(double_test1 < int_test2);

  const std::string string1 = "string";
  const mgp::Value string_test1 = mgp::Value(string1);

  ASSERT_THROW(int_test1 < string_test1, mgp::ValueException);
  ASSERT_THROW(list_test < map_test, mgp::ValueException);
  ASSERT_THROW(list_test < list_test, mgp::ValueException);
}
TYPED_TEST(CppApiTestFixture, TestNumberEquality) {
  mgp::Value double_1{1.0};
  mgp::Value int_1{static_cast<int64_t>(1)};
  ASSERT_TRUE(double_1 == int_1);
  mgp::Value double_2{2.01};
  mgp::Value int_2{static_cast<int64_t>(2)};
  ASSERT_FALSE(double_2 == int_2);
}

TYPED_TEST(CppApiTestFixture, TestTypeOperatorStream) {
  std::string string1 = "string";
  int64_t int1 = 4;
  mgp::List list = mgp::List();

  mgp::Value string_value = mgp::Value(string1);
  mgp::Value int_value = mgp::Value(int1);
  mgp::Value list_value = mgp::Value(list);

  std::ostringstream oss_str;
  oss_str << string_value.Type();
  std::string str_test = oss_str.str();

  std::ostringstream oss_int;
  oss_int << int_value.Type();
  std::string int_test = oss_int.str();

  std::ostringstream oss_list;
  oss_list << list_value.Type();
  std::string list_test = oss_list.str();

  ASSERT_EQ(str_test, "string");
  ASSERT_EQ(int_test, "int");
  ASSERT_EQ(list_test, "list");
}

TYPED_TEST(CppApiTestFixture, TestMapUpdate) {
  mgp::Map map{};
  mgp::Value double_1{1.0};
  mgp::Value double_2{2.0};

  map.Update("1", double_1);
  ASSERT_EQ(map.At("1"), double_1);

  map.Update("1", double_2);
  ASSERT_EQ(map.At("1"), double_2);
}

TYPED_TEST(CppApiTestFixture, TestMapErase) {
  mgp::Map map{};
  mgp::Value double_1{1.0};
  mgp::Value double_2{2.0};

  map.Insert("1", double_1);
  map.Insert("2", double_2);
  ASSERT_EQ(map.Size(), 2);

  map.Erase("1");
  ASSERT_EQ(map.Size(), 1);

  map.Erase("1");
  ASSERT_EQ(map.Size(), 1);

  map.Erase("2");
  ASSERT_EQ(map.Size(), 0);
}

TYPED_TEST(CppApiTestFixture, TestNodeRemoveProperty) {
  mgp_graph raw_graph = this->CreateGraph(memgraph::storage::View::NEW);
  auto graph = mgp::Graph(&raw_graph);
  auto node = graph.CreateNode();

  int64_t int1 = 100;
  mgp::Value value{int1};
  node.SetProperty("key", value);
  ASSERT_EQ(node.Properties().size(), 1);
  node.RemoveProperty("key");
  ASSERT_EQ(node.Properties().size(), 0);
}

TYPED_TEST(CppApiTestFixture, TestRelationshipRemoveProperty) {
  mgp_graph raw_graph = this->CreateGraph(memgraph::storage::View::NEW);
  auto graph = mgp::Graph(&raw_graph);
  auto node_1 = graph.CreateNode();
  auto node_2 = graph.CreateNode();
  auto relationship = graph.CreateRelationship(node_1, node_2, "Relationship");

  int64_t int_1{0};
  mgp::Value value{int_1};
  relationship.SetProperty("property", value);

  ASSERT_EQ(relationship.Properties().size(), 1);
  relationship.RemoveProperty("property");
  ASSERT_EQ(relationship.Properties().size(), 0);
}

TYPED_TEST(CppApiTestFixture, TestValuePrint) {
  std::string string_1{"abc"};
  int64_t int_1{4};
  mgp::Date date_1{"2020-12-12"};

  mgp::Value string_value{string_1};
  mgp::Value int_value{int_1};
  mgp::Value date_value{date_1};

  std::ostringstream oss_str;
  oss_str << string_value;
  std::string str_test = oss_str.str();
  ASSERT_EQ(string_1, str_test);

  std::ostringstream oss_int;
  oss_int << int_value;
  std::string int_test = oss_int.str();
  ASSERT_EQ("4", int_test);

  std::ostringstream oss_date;
  oss_date << date_value;
  std::string date_test = oss_date.str();
  ASSERT_EQ("2020-12-12", date_test);
}

TYPED_TEST(CppApiTestFixture, TestValueToString) {
  /*graph and node shared by multiple types*/
  mgp_graph raw_graph = this->CreateGraph(memgraph::storage::View::NEW);
  auto graph = mgp::Graph(&raw_graph);

  /*null*/
  ASSERT_EQ(mgp::Value().ToString(), "");
  /*bool*/
  ASSERT_EQ(mgp::Value(false).ToString(), "false");
  /*int*/
  const int64_t int1 = 60;
  ASSERT_EQ(mgp::Value(int1).ToString(), "60");
  /*double*/
  const double double1 = 2.567891;
  ASSERT_EQ(mgp::Value(double1).ToString(), "2.567891");
  /*string*/
  const std::string str = "string";
  ASSERT_EQ(mgp::Value(str).ToString(), "string");
  /*list*/
  mgp::List list;
  auto node_list = graph.CreateNode();
  node_list.AddLabel("Label_list");
  list.AppendExtend(mgp::Value("inside"));
  list.AppendExtend(mgp::Value("2"));
  list.AppendExtend(mgp::Value(node_list));
  ASSERT_EQ(mgp::Value(list).ToString(), "[inside, 2, (id: 0, :Label_list, properties: {})]");
  /*map*/
  mgp::Map map;
  auto node_map = graph.CreateNode();
  node_map.AddLabel("Label_map");
  map.Insert("key", mgp::Value(int1));
  map.Insert("node", mgp::Value(node_map));
  ASSERT_EQ(mgp::Value(map).ToString(), "{key: 60, node: (id: 1, :Label_map, properties: {})}");
  /*date*/
  mgp::Date date_1{"2020-12-12"};
  ASSERT_EQ(mgp::Value(date_1).ToString(), "2020-12-12");

  /*local time*/
  mgp::LocalTime local_time{"09:15:00.360"};
  ASSERT_EQ(mgp::Value(local_time).ToString(), "9:15:0,3600");

  /*local date time*/
  mgp::LocalDateTime local_date_time{"2021-10-05T14:15:00"};
  ASSERT_EQ(mgp::Value(local_date_time).ToString(), "2021-10-5T14:15:0,00");

  /*duration*/
  mgp::Duration duration{"P14DT17H2M45S"};
  ASSERT_EQ(mgp::Value(duration).ToString(), "1270965000000ms");

  /*node and relationship*/
  auto node1 = graph.CreateNode();
  node1.AddLabel("Label1");
  node1.AddLabel("Label2");
  auto node2 = graph.CreateNode();
  node2.SetProperty("key", mgp::Value("node_property"));
  node2.SetProperty("key2", mgp::Value("node_property2"));
  auto rel = graph.CreateRelationship(node1, node2, "Loves");

  rel.SetProperty("key", mgp::Value("property"));
  ASSERT_EQ(mgp::Value(rel).ToString(),
            "(id: 2, :Label1:Label2, properties: {})-[type: Loves, id: 0, properties: {key: property}]->(id: 3, "
            "properties: {key: node_property, key2: node_property2})");
  /*path*/
  mgp::Path path = mgp::Path(node1);
  path.Expand(rel);
  auto node3 = graph.CreateNode();
  auto rel2 = graph.CreateRelationship(node2, node3, "Loves2");
  path.Expand(rel2);
  ASSERT_EQ(
      mgp::Value(path).ToString(),
      "(id: 2, :Label1:Label2, properties: {})-[type: Loves, id: 0, properties: {key: property}]->(id: 3, properties: "
      "{key: node_property, key2: node_property2})-[type: Loves2, id: 1, properties: {}]->(id: 4, properties: {})");
}

TYPED_TEST(CppApiTestFixture, TestRelationshipChangeFrom) {
  // Changing Relationship end vertex is not implemented for ondisk storage yet.
  if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
    return;
  }

  mgp_graph raw_graph = this->CreateGraph();
  auto graph = mgp::Graph(&raw_graph);

  auto node_1 = graph.CreateNode();
  auto node_2 = graph.CreateNode();
  auto node_3 = graph.CreateNode();

  auto relationship = graph.CreateRelationship(node_1, node_2, "Edge");
  relationship.SetProperty("property", mgp::Value(true));
  ASSERT_EQ(relationship.GetProperty("property"), mgp::Value(true));

  ASSERT_EQ(relationship.From().Id(), node_1.Id());
  graph.SetFrom(relationship, node_3);

  ASSERT_EQ(std::string(relationship.Type()), "Edge");
  ASSERT_EQ(relationship.From().Id(), node_3.Id());
  ASSERT_EQ(relationship.To().Id(), node_2.Id());
  ASSERT_EQ(relationship.GetProperty("property"), mgp::Value(true));
}

TYPED_TEST(CppApiTestFixture, TestRelationshipChangeTo) {
  // Changing Relationship start vertex is not implemented for ondisk storage yet.
  if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
    return;
  }

  mgp_graph raw_graph = this->CreateGraph();
  auto graph = mgp::Graph(&raw_graph);

  auto node_1 = graph.CreateNode();
  auto node_2 = graph.CreateNode();
  auto node_3 = graph.CreateNode();

  auto relationship = graph.CreateRelationship(node_1, node_2, "Edge");
  relationship.SetProperty("property", mgp::Value(true));
  ASSERT_EQ(relationship.GetProperty("property"), mgp::Value(true));

  ASSERT_EQ(relationship.To().Id(), node_2.Id());
  graph.SetTo(relationship, node_3);

  ASSERT_EQ(std::string(relationship.Type()), "Edge");
  ASSERT_EQ(relationship.From().Id(), node_1.Id());
  ASSERT_EQ(relationship.To().Id(), node_3.Id());
  ASSERT_EQ(relationship.GetProperty("property"), mgp::Value(true));
}

TYPED_TEST(CppApiTestFixture, TestInAndOutDegrees) {
  mgp_graph raw_graph = this->CreateGraph(memgraph::storage::View::NEW);
  auto graph = mgp::Graph(&raw_graph);
  auto node_1 = graph.CreateNode();
  auto node_2 = graph.CreateNode();
  auto node_3 = graph.CreateNode();
  auto relationship = graph.CreateRelationship(node_1, node_2, "Relationship1");
  auto relationship2 = graph.CreateRelationship(node_1, node_2, "Relationship2");
  auto relationship3 = graph.CreateRelationship(node_1, node_2, "Relationship3");
  auto relationship4 = graph.CreateRelationship(node_1, node_2, "Relationship4");
  auto relationship5 = graph.CreateRelationship(node_1, node_3, "Relationship5");
  auto relationship6 = graph.CreateRelationship(node_1, node_3, "Relationship6");

  ASSERT_EQ(node_1.OutDegree(), 6);
  ASSERT_EQ(node_2.InDegree(), 4);
  ASSERT_EQ(node_3.InDegree(), 2);

  ASSERT_EQ(node_1.InDegree(), 0);
  ASSERT_EQ(node_2.OutDegree(), 0);
  ASSERT_EQ(node_3.OutDegree(), 0);
}

TYPED_TEST(CppApiTestFixture, TestChangeRelationshipType) {
  // Changing relationship types is not implemented for ondisk storage yet.
  if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
    return;
  }

  mgp_graph raw_graph = this->CreateGraph();
  auto graph = mgp::Graph(&raw_graph);

  auto node_1 = graph.CreateNode();
  auto node_2 = graph.CreateNode();

  auto relationship = graph.CreateRelationship(node_1, node_2, "Type");
  relationship.SetProperty("property", mgp::Value(true));
  ASSERT_EQ(relationship.Type(), "Type");
  ASSERT_EQ(relationship.GetProperty("property"), mgp::Value(true));

  graph.ChangeType(relationship, "NewType");
  ASSERT_EQ(relationship.Type(), "NewType");
  ASSERT_EQ(relationship.GetProperty("property"), mgp::Value(true));
}

TYPED_TEST(CppApiTestFixture, TestMapKeyExist) {
  mgp::Map map = mgp::Map();
  map.Insert("key", mgp::Value("string"));
  ASSERT_EQ(true, map.KeyExists("key"));
  ASSERT_EQ(false, map.KeyExists("no_existo"));
  map.Insert("null_key", mgp::Value());
  ASSERT_EQ(true, map.KeyExists("null_key"));
}

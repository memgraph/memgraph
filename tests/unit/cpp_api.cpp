// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "disk_test_utils.hpp"
#include "flags/experimental.hpp"
#include "mg_procedure.h"
#include "mgp.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "tests/unit/timezone_handler.hpp"

template <typename StorageType>
struct CppApiTestFixture : public ::testing::Test {
 protected:
  void SetUp() override { mgp::MemoryDispatcher::Register(&memory); }

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
    mgp::MemoryDispatcher::UnRegister(nullptr);
  }

  mgp_graph CreateGraph(memgraph::query::DbAccessor *db_acc) {
    // the execution context can be null as it shouldn't be used in these tests
    return mgp_graph{db_acc, memgraph::storage::View::NEW, ctx_.get(),
                     memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL};
  }

  auto CreateIndexAccessor() -> std::unique_ptr<memgraph::storage::Storage::Accessor> {
    if constexpr (std::is_same_v<StorageType, memgraph::storage::InMemoryStorage>) {
      return this->storage->ReadOnlyAccess();
    } else {
      return this->storage->UniqueAccess();
    }
  }

  auto DropIndexAccessor() -> std::unique_ptr<memgraph::storage::Storage::Accessor> {
    if constexpr (std::is_same_v<StorageType, memgraph::storage::InMemoryStorage>) {
      return this->storage->Access(memgraph::storage::StorageAccessType::READ);
    } else {
      return this->storage->UniqueAccess();
    }
  }

  const std::string testSuite = "cpp_api";

  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> storage{new StorageType(config)};
  mgp_memory memory{memgraph::utils::NewDeleteResource()};

 private:
  std::unique_ptr<memgraph::query::ExecutionContext> ctx_ = std::make_unique<memgraph::query::ExecutionContext>();
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
using AccessorType = memgraph::storage::StorageAccessType;
TYPED_TEST_SUITE(CppApiTestFixture, StorageTypes);

TYPED_TEST(CppApiTestFixture, TestGraph) {
  auto storage_acc = this->storage->Access(AccessorType::WRITE);
  auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
  mgp_graph raw_graph = this->CreateGraph(db_acc.get());
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
  vector_x.push_back(mgp::List());

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
  vector_x.push_back(mgp::Map());

  // Use Value copy constructor
  auto value_x = mgp::Value(map_1);
  // Use Value move constructor
  auto value_y = mgp::Value(mgp::Map());

  auto value_z = value_x;
}

TYPED_TEST(CppApiTestFixture, TestNode) {
  auto storage_acc = this->storage->Access(AccessorType::WRITE);
  auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
  mgp_graph raw_graph = this->CreateGraph(db_acc.get());
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
  auto storage_acc = this->storage->Access(AccessorType::WRITE);
  auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
  mgp_graph raw_graph = this->CreateGraph(db_acc.get());
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
  auto storage_acc = this->storage->Access(AccessorType::WRITE);
  auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
  mgp_graph raw_graph = this->CreateGraph(db_acc.get());
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
  auto storage_acc = this->storage->Access(AccessorType::WRITE);
  auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
  mgp_graph raw_graph = this->CreateGraph(db_acc.get());
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
  vector_x.push_back(mgp::Path(node_0));

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
  vector_x.push_back(mgp::Date("2022-04-09"));

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
  vector_x.push_back(mgp::LocalTime("09:15:00"));

  // Use Value copy constructor
  auto value_x = mgp::Value(lt_1);
  // Use Value move constructor
  auto value_y = mgp::Value(mgp::LocalTime("09:15:00"));
}

void test_TestLocalDateTime() {
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
  vector_x.push_back(mgp::LocalDateTime("2021-10-05T14:15:00"));

  // Use Value copy constructor
  auto value_x = mgp::Value(ldt_1);
  // Use Value move constructor
  auto value_y = mgp::Value(mgp::LocalDateTime("2021-10-05T14:15:00"));

  // Add and subtract durations, and get duration difference between
  // two LocalDateTimes.
  auto duration = mgp::Duration("PT1H30M");
  auto future = ldt_1 + duration;
  EXPECT_EQ(future.Hour(), 15);
  EXPECT_EQ(future.Minute(), 45);

  auto past = ldt_1 - duration;
  EXPECT_EQ(past.Hour(), 12);
  EXPECT_EQ(past.Minute(), 45);

  auto diff = future - past;
  static constexpr auto three_hours_in_microseconds = 10'800'000'000;
  EXPECT_EQ(diff.Microseconds(), three_hours_in_microseconds);

  // Check that two discrete calls to `Now()` return times very close to
  // one another. We'll just check they are within a second.
  auto now1 = mgp::LocalDateTime::Now();
  auto now2 = mgp::LocalDateTime::Now();
  EXPECT_LE((now2 - now1).Microseconds(), 1'000'000);
}

TYPED_TEST(CppApiTestFixture, TestLocalDateTime) { test_TestLocalDateTime(); }

TYPED_TEST(CppApiTestFixture, TestLocalDateTimeTZ) {
  HandleTimezone htz;
  htz.Set("Europe/Rome");
  test_TestLocalDateTime();
  htz.Set("America/Los_Angeles");
  test_TestLocalDateTime();
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
  vector_x.push_back(mgp::Duration("PT2M2.33S"));

  // Use Value copy constructor
  auto value_x = mgp::Value(duration_1);
  // Use Value move constructor
  auto value_y = mgp::Value(mgp::Duration("PT2M2.33S"));
}

void test_TestZonedDateTime() {
  auto ldt_1 = mgp::ZonedDateTime("2021-10-05T14:15:00");
  auto ldt_2 = mgp::ZonedDateTime(2021, 10, 5, 14, 15, 0, 0, 0, "Etc/UTC");

  ASSERT_ANY_THROW(mgp::ZonedDateTime(
      2021, 10, 0, 14, 15, 0, 0, 0,
      0));  // ...10, 0, 14... <- 0 is an illegal value for the `day` parameter; must throw an exception

  // Disallow any named timezone that is not in the tz database
  ASSERT_ANY_THROW(mgp::ZonedDateTime(2021, 10, 1, 14, 15, 0, 0, 0, "Mars/Cydonia"));

  // Disallow offset timezone that is out of range
  ASSERT_ANY_THROW(mgp::ZonedDateTime(2021, 10, 1, 14, 15, 0, 0, 0, 1200));

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
  std::vector<mgp::ZonedDateTime> vector_x;
  vector_x.push_back(mgp::ZonedDateTime("2021-10-05T14:15:00"));

  // Use Value copy constructor
  auto value_x = mgp::Value(ldt_1);
  // Use Value move constructor
  auto value_y = mgp::Value(mgp::ZonedDateTime("2021-10-05T14:15:00"));
}

TYPED_TEST(CppApiTestFixture, ZonedDateTime_CanBeCreated) {
  // Created from an ISO 8601 string without timezone
  auto zdt1 = mgp::ZonedDateTime("2021-10-05T14:15:00");
  EXPECT_EQ(zdt1.Year(), 2021);
  EXPECT_EQ(zdt1.Month(), 10);
  EXPECT_EQ(zdt1.Day(), 5);
  EXPECT_EQ(zdt1.Hour(), 14);
  EXPECT_EQ(zdt1.Minute(), 15);
  EXPECT_EQ(zdt1.Second(), 0);
  EXPECT_EQ(zdt1.Millisecond() >= 0, true);
  EXPECT_EQ(zdt1.Microsecond() >= 0, true);
  EXPECT_EQ(zdt1.Timestamp() >= 0, true);
  EXPECT_STREQ(zdt1.Timezone(), "Etc/UTC");
  EXPECT_EQ(zdt1.Offset(), 0);

  // Created from an ISO 8601 string with named timezone
  auto zdt2 = mgp::ZonedDateTime("2021-10-05T14:15:00[Europe/London]");
  EXPECT_EQ(zdt2.Year(), 2021);
  EXPECT_EQ(zdt2.Month(), 10);
  EXPECT_EQ(zdt2.Day(), 5);
  EXPECT_EQ(zdt2.Hour(), 14);
  EXPECT_EQ(zdt2.Minute(), 15);
  EXPECT_EQ(zdt2.Second(), 0);
  EXPECT_EQ(zdt2.Millisecond() >= 0, true);
  EXPECT_EQ(zdt2.Microsecond() >= 0, true);
  EXPECT_EQ(zdt2.Timestamp() >= 0, true);
  EXPECT_STREQ(zdt2.Timezone(), "Europe/London");
  EXPECT_EQ(zdt2.Offset(), 60);

  // Created from an ISO 8601 string with negative offset
  auto zdt3 = mgp::ZonedDateTime("2021-10-05T14:15:00-02:00");
  EXPECT_EQ(zdt3.Year(), 2021);
  EXPECT_EQ(zdt3.Month(), 10);
  EXPECT_EQ(zdt3.Day(), 5);
  EXPECT_EQ(zdt3.Hour(), 14);
  EXPECT_EQ(zdt3.Minute(), 15);
  EXPECT_EQ(zdt3.Second(), 0);
  EXPECT_EQ(zdt3.Millisecond() >= 0, true);
  EXPECT_EQ(zdt3.Microsecond() >= 0, true);
  EXPECT_EQ(zdt3.Timestamp() >= 0, true);
  EXPECT_EQ(zdt3.Offset(), -120);
  EXPECT_STREQ(zdt3.Timezone(), "");

  // Created from an ISO 8601 string with positive offset
  auto zdt4 = mgp::ZonedDateTime("2021-10-05T14:15:00+02:00");
  EXPECT_EQ(zdt4.Year(), 2021);
  EXPECT_EQ(zdt4.Month(), 10);
  EXPECT_EQ(zdt4.Day(), 5);
  EXPECT_EQ(zdt4.Hour(), 14);
  EXPECT_EQ(zdt4.Minute(), 15);
  EXPECT_EQ(zdt4.Second(), 0);
  EXPECT_EQ(zdt4.Millisecond() >= 0, true);
  EXPECT_EQ(zdt4.Microsecond() >= 0, true);
  EXPECT_EQ(zdt4.Timestamp() >= 0, true);
  EXPECT_EQ(zdt4.Offset(), 120);
  EXPECT_STREQ(zdt4.Timezone(), "");

  // Create from discrete parameters with named timezone
  auto zdt5 = mgp::ZonedDateTime(2021, 10, 5, 14, 15, 0, 0, 0, "Europe/Paris");
  EXPECT_EQ(zdt5.Year(), 2021);
  EXPECT_EQ(zdt5.Month(), 10);
  EXPECT_EQ(zdt5.Day(), 5);
  EXPECT_EQ(zdt5.Hour(), 14);
  EXPECT_EQ(zdt5.Minute(), 15);
  EXPECT_EQ(zdt5.Second(), 0);
  EXPECT_EQ(zdt5.Millisecond() >= 0, true);
  EXPECT_EQ(zdt5.Microsecond() >= 0, true);
  EXPECT_EQ(zdt5.Timestamp() >= 0, true);
  EXPECT_STREQ(zdt5.Timezone(), "Europe/Paris");
  EXPECT_EQ(zdt5.Offset(), 120);

  // Create from discrete parameters with offset (in minutes)
  auto zdt6 = mgp::ZonedDateTime(2021, 10, 5, 14, 15, 0, 0, 0, 120);
  EXPECT_EQ(zdt6.Year(), 2021);
  EXPECT_EQ(zdt6.Month(), 10);
  EXPECT_EQ(zdt6.Day(), 5);
  EXPECT_EQ(zdt6.Hour(), 14);
  EXPECT_EQ(zdt6.Minute(), 15);
  EXPECT_EQ(zdt6.Second(), 0);
  EXPECT_EQ(zdt6.Millisecond() >= 0, true);
  EXPECT_EQ(zdt6.Microsecond() >= 0, true);
  EXPECT_EQ(zdt6.Timestamp() >= 0, true);
  EXPECT_EQ(zdt6.Offset(), 120);
  EXPECT_STREQ(zdt6.Timezone(), "");
}

TYPED_TEST(CppApiTestFixture, ZonedDateTime_CannotBeCreatedWithInvalidParameters) {
  // There is no 0th of October
  EXPECT_ANY_THROW(mgp::ZonedDateTime(2021, 10, 0, 14, 15, 0, 0, 0, 0));

  // Tz region is not recognised
  EXPECT_ANY_THROW(mgp::ZonedDateTime(2021, 10, 0, 14, 15, 0, 0, 0, "Mars/Cydonia"));

  // Offset is beyond acceptable range
  EXPECT_ANY_THROW(mgp::ZonedDateTime(2021, 10, 0, 14, 15, 0, 0, 0, 2000));
}

TYPED_TEST(CppApiTestFixture, ZonedDateTime_CanBeTestedForEquality) {
  auto zdt1 = mgp::ZonedDateTime("2021-10-05T14:15:00");
  auto zdt2 = mgp::ZonedDateTime(2021, 10, 5, 14, 15, 0, 0, 0, "Etc/UTC");
  auto zdt3 = mgp::ZonedDateTime("2021-11-05T14:15:00");
  auto zdt4 = mgp::ZonedDateTime("2021-11-05T14:15:00[Europe/London]");

  EXPECT_EQ(zdt1, zdt2);
  EXPECT_NE(zdt1, zdt3);
  EXPECT_NE(zdt1, zdt4);
  EXPECT_NE(zdt2, zdt3);
  EXPECT_NE(zdt3, zdt4);
}

TYPED_TEST(CppApiTestFixture, ZonedDateTime_CanBeCopyMoveAndValueConstructed) {
  // Copy assignment
  {
    auto zdt = mgp::ZonedDateTime("2021-10-05T14:15:00[Europe/London]");
    auto copy = zdt;

    EXPECT_EQ(copy.Year(), 2021);
    EXPECT_EQ(copy.Month(), 10);
    EXPECT_EQ(copy.Day(), 5);
    EXPECT_EQ(copy.Hour(), 14);
    EXPECT_EQ(copy.Minute(), 15);
    EXPECT_EQ(copy.Second(), 0);
    EXPECT_EQ(copy.Millisecond() >= 0, true);
    EXPECT_EQ(copy.Microsecond() >= 0, true);
    EXPECT_EQ(copy.Timestamp() >= 0, true);
    EXPECT_STREQ(copy.Timezone(), "Europe/London");
    EXPECT_EQ(copy.Offset(), 60);
  }

  // Use move assignment
  {
    std::vector<mgp::ZonedDateTime> vec;
    vec.push_back(mgp::ZonedDateTime("2021-10-05T14:15:00"));
    EXPECT_EQ(vec[0].Year(), 2021);
    EXPECT_EQ(vec[0].Month(), 10);
    EXPECT_EQ(vec[0].Day(), 5);
    EXPECT_EQ(vec[0].Hour(), 14);
    EXPECT_EQ(vec[0].Minute(), 15);
    EXPECT_EQ(vec[0].Second(), 0);
    EXPECT_EQ(vec[0].Millisecond() >= 0, true);
    EXPECT_EQ(vec[0].Microsecond() >= 0, true);
    EXPECT_EQ(vec[0].Timestamp() >= 0, true);
    EXPECT_STREQ(vec[0].Timezone(), "Etc/UTC");
    EXPECT_EQ(vec[0].Offset(), 0);
  }

  {
    auto zdt = mgp::ZonedDateTime("2021-11-05T14:15:00[Europe/London]");
    auto copied_value = mgp::Value(zdt);
    ASSERT_TRUE(copied_value.IsZonedDateTime());
    ASSERT_EQ(copied_value.Type(), mgp::Type::ZonedDateTime);
    EXPECT_EQ(copied_value.ValueZonedDateTime(), zdt);
    auto moved_value = mgp::Value(mgp::ZonedDateTime("2021-11-05T14:15:00[Europe/London]"));
    ASSERT_TRUE(moved_value.IsZonedDateTime());
    ASSERT_EQ(moved_value.Type(), mgp::Type::ZonedDateTime);
    EXPECT_EQ(moved_value.ValueZonedDateTime(), zdt);
  }
}

TYPED_TEST(CppApiTestFixture, ZonedDateTime_CanPerformOperationsWithDurations) {
  auto zdt = mgp::ZonedDateTime("2021-10-05T14:15:00[Europe/London]");

  auto duration = mgp::Duration("PT1H30M");
  auto future = zdt + duration;
  EXPECT_EQ(future.Hour(), 15);
  EXPECT_EQ(future.Minute(), 45);

  auto past = zdt - duration;
  EXPECT_EQ(past.Hour(), 12);
  EXPECT_EQ(past.Minute(), 45);

  auto diff = future - past;
  static constexpr auto three_hours_in_microseconds = 10'800'000'000;
  EXPECT_EQ(diff.Microseconds(), three_hours_in_microseconds);
}

TYPED_TEST(CppApiTestFixture, ZonedDateTime_NowReturnsCurrentTime) {
  auto now1 = mgp::ZonedDateTime::Now();
  auto now2 = mgp::ZonedDateTime::Now();
  EXPECT_LE((now2 - now1).Microseconds(), 1'000'000);
}

TYPED_TEST(CppApiTestFixture, ZonedDateTime_CanBeStoredAsNodeProperty) {
  auto storage_acc = this->storage->Access(AccessorType::WRITE);
  auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
  mgp_graph raw_graph = this->CreateGraph(db_acc.get());
  auto graph = mgp::Graph(&raw_graph);

  auto node = graph.CreateNode();

  {
    auto zdt = mgp::ZonedDateTime("2024-01-01T13:02:40.100050+01:00[Europe/Zagreb]");
    node.SetProperty("created_at", mgp::Value(zdt));
    auto retrieved = node.GetProperty("created_at");
    ASSERT_TRUE(retrieved.IsZonedDateTime());
    EXPECT_EQ(retrieved.ValueZonedDateTime().Offset(), 60);
    EXPECT_EQ(retrieved.ValueZonedDateTime(), zdt);
  }

  {
    auto zdt_without_timezone = mgp::ZonedDateTime("2024-01-01T13:02:40.100050+01:00");
    node.SetProperty("modified_at", mgp::Value(zdt_without_timezone));
    auto retrieved = node.GetProperty("modified_at");
    ASSERT_TRUE(retrieved.IsZonedDateTime());
    EXPECT_EQ(retrieved.ValueZonedDateTime(), zdt_without_timezone);
    EXPECT_EQ(retrieved.ValueZonedDateTime().Offset(), 60);
    EXPECT_STREQ(retrieved.ValueZonedDateTime().Timezone(), "");
  }
}

TYPED_TEST(CppApiTestFixture, TestNodeProperties) {
  auto storage_acc = this->storage->Access(AccessorType::WRITE);
  auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
  mgp_graph raw_graph = this->CreateGraph(db_acc.get());
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
  auto storage_acc = this->storage->Access(AccessorType::WRITE);
  auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
  mgp_graph raw_graph = this->CreateGraph(db_acc.get());
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
  auto storage_acc = this->storage->Access(AccessorType::WRITE);
  auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
  mgp_graph raw_graph = this->CreateGraph(db_acc.get());
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
  {
    mgp::Value string_value{std::string{"abc"}};
    std::ostringstream oss_str;
    oss_str << string_value;
    ASSERT_EQ("abc", oss_str.str());
  }

  {
    mgp::Value int_value{int64_t(4)};
    std::ostringstream oss_int;
    oss_int << int_value;
    ASSERT_EQ("4", oss_int.str());
  }

  {
    mgp::Value date_value{mgp::Date{"2020-12-12"}};
    std::ostringstream oss_date;
    oss_date << date_value;
    ASSERT_EQ("2020-12-12", oss_date.str());
  }

  {
    mgp::Value local_time_value{mgp::LocalTime{"09:15:00.360"}};
    std::ostringstream oss_local_time;
    oss_local_time << local_time_value;
    ASSERT_EQ("09:15:00.360000", oss_local_time.str());
  }

  {
    mgp::Value local_date_time_value{mgp::LocalDateTime{"2021-02-05T14:15:00"}};
    std::ostringstream oss_local_date_time;
    oss_local_date_time << local_date_time_value;
    ASSERT_EQ("2021-02-05T14:15:00.000000", oss_local_date_time.str());
  }

  {
    mgp::Value duration_value{mgp::Duration{"P14DT17H2M45S"}};
    std::ostringstream oss_duration;
    oss_duration << duration_value;
    ASSERT_EQ("1270965000000ms", oss_duration.str());
  }

  {
    mgp::Value zoned_date_time_value{mgp::ZonedDateTime{"2024-01-01T13:02:40.100050+01:00[Europe/Zagreb]"}};
    std::ostringstream oss_zoned_date_time;
    oss_zoned_date_time << zoned_date_time_value;
    ASSERT_EQ("2024-01-01T13:02:40.100050+01:00[Europe/Zagreb]", oss_zoned_date_time.str());
  }

  {
    mgp::Value zoned_date_time_value{mgp::ZonedDateTime{"2024-01-01T13:02:40.100050+01:00"}};
    std::ostringstream oss_zoned_date_time;
    oss_zoned_date_time << zoned_date_time_value;
    ASSERT_EQ("2024-01-01T13:02:40.100050+01:00", oss_zoned_date_time.str());
  }
}

TYPED_TEST(CppApiTestFixture, TestValueToString) {
  /*graph and node shared by multiple types*/
  auto storage_acc = this->storage->Access(AccessorType::WRITE);
  auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
  mgp_graph raw_graph = this->CreateGraph(db_acc.get());
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
  ASSERT_EQ(mgp::Value(local_time).ToString(), "09:15:00.360000");

  /*local date time*/
  mgp::LocalDateTime local_date_time{"2021-02-05T14:15:00"};
  ASSERT_EQ(mgp::Value(local_date_time).ToString(), "2021-02-05T14:15:00.000000");

  /*duration*/
  mgp::Duration duration{"P14DT17H2M45S"};
  ASSERT_EQ(mgp::Value(duration).ToString(), "1270965000000ms");

  /*zoned date time*/
  mgp::ZonedDateTime zoned_date_time_with_tz{"2024-01-01T13:02:40.100050+01:00[Europe/Zagreb]"};
  ASSERT_EQ(mgp::Value(zoned_date_time_with_tz).ToString(), "2024-01-01T13:02:40.100050+01:00[Europe/Zagreb]");

  mgp::ZonedDateTime zoned_date_time_without_tz{"2024-01-01T13:02:40.100050+01:00"};
  ASSERT_EQ(mgp::Value(zoned_date_time_without_tz).ToString(), "2024-01-01T13:02:40.100050+01:00");

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

TYPED_TEST(CppApiTestFixture, TestInAndOutDegrees) {
  auto storage_acc = this->storage->Access(AccessorType::WRITE);
  auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
  mgp_graph raw_graph = this->CreateGraph(db_acc.get());
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

TYPED_TEST(CppApiTestFixture, TestMapKeyExist) {
  mgp::Map map = mgp::Map();
  map.Insert("key", mgp::Value("string"));
  ASSERT_EQ(true, map.KeyExists("key"));
  ASSERT_EQ(false, map.KeyExists("no_existo"));
  map.Insert("null_key", mgp::Value());
  ASSERT_EQ(true, map.KeyExists("null_key"));
}

TYPED_TEST(CppApiTestFixture, TestLabelIndex) {
  {
    auto storage_acc = this->CreateIndexAccessor();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());

    ASSERT_TRUE(mgp::CreateLabelIndex(&raw_graph, "Person"));
    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  {
    auto storage_acc = this->storage->UniqueAccess();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());
    auto indices = mgp::ListAllLabelIndices(&raw_graph);
    ASSERT_EQ(indices.Size(), 1);
    ASSERT_EQ(indices[0].ValueString(), "Person");
  }
  {
    auto storage_acc = this->DropIndexAccessor();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());
    ASSERT_TRUE(mgp::DropLabelIndex(&raw_graph, "Person"));
    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
  {
    auto storage_acc = this->storage->UniqueAccess();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());
    auto updated_indices = mgp::ListAllLabelIndices(&raw_graph);
    ASSERT_EQ(updated_indices.Size(), 0);
  }
  {
    auto storage_acc = this->DropIndexAccessor();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());
    ASSERT_FALSE(mgp::DropLabelIndex(&raw_graph, "NonExistentLabel"));
    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
}

TYPED_TEST(CppApiTestFixture, TestLabelPropertyIndex) {
  {
    auto storage_acc = this->CreateIndexAccessor();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());
    ASSERT_TRUE(mgp::CreateLabelPropertyIndex(&raw_graph, "User", "name"));
    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
  {
    auto storage_acc = this->storage->UniqueAccess();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());

    auto indices = mgp::ListAllLabelPropertyIndices(&raw_graph);
    ASSERT_EQ(indices.Size(), 1);
    auto index_info = indices[0].ValueString();
    ASSERT_EQ(index_info, "User:name");
  }
  {
    auto storage_acc = this->DropIndexAccessor();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());
    ASSERT_TRUE(mgp::DropLabelPropertyIndex(&raw_graph, "User", "name"));
    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
  {
    auto storage_acc = this->storage->UniqueAccess();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());
    auto updated_indices = mgp::ListAllLabelPropertyIndices(&raw_graph);
    ASSERT_EQ(updated_indices.Size(), 0);
  }
  {
    auto storage_acc = this->DropIndexAccessor();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());
    ASSERT_FALSE(mgp::DropLabelPropertyIndex(&raw_graph, "User", "nonexistent"));
    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
}

TYPED_TEST(CppApiTestFixture, TestNestedIndex) {
  if constexpr (!std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
    GTEST_SKIP() << "TestNestedIndex runs only on InMemoryStorage.";
  }
  {
    auto storage_acc = this->CreateIndexAccessor();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());

    ASSERT_TRUE(mgp::CreateLabelPropertyIndex(&raw_graph, "Label", "nested1.nested2.nested3"));
    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
  {
    auto storage_acc = this->storage->UniqueAccess();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());
    auto indices = mgp::ListAllLabelPropertyIndices(&raw_graph);
    ASSERT_EQ(indices.Size(), 1);
    auto index_info = indices[0].ValueString();
    ASSERT_EQ(index_info, "Label:nested1.nested2.nested3");
  }
  {
    auto storage_acc = this->DropIndexAccessor();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());
    ASSERT_TRUE(mgp::DropLabelPropertyIndex(&raw_graph, "Label", "nested1.nested2.nested3"));
    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
  {
    auto storage_acc = this->storage->UniqueAccess();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());

    auto updated_indices = mgp::ListAllLabelPropertyIndices(&raw_graph);
    ASSERT_EQ(updated_indices.Size(), 0);
  }
}

TYPED_TEST(CppApiTestFixture, TestExistenceConstraint) {
  auto storage_acc = this->storage->UniqueAccess();
  auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
  mgp_graph raw_graph = this->CreateGraph(db_acc.get());

  ASSERT_TRUE(mgp::CreateExistenceConstraint(&raw_graph, "User", "email"));

  auto constraints = mgp::ListAllExistenceConstraints(&raw_graph);
  ASSERT_EQ(constraints.Size(), 1);
  ASSERT_EQ(constraints[0].ValueString(), "User:email");

  ASSERT_TRUE(mgp::DropExistenceConstraint(&raw_graph, "User", "email"));

  auto updated_constraints = mgp::ListAllExistenceConstraints(&raw_graph);
  ASSERT_EQ(updated_constraints.Size(), 0);

  ASSERT_FALSE(mgp::DropExistenceConstraint(&raw_graph, "User", "nonexistent"));
}

TYPED_TEST(CppApiTestFixture, TestUniqueConstraint) {
  auto storage_acc = this->storage->UniqueAccess();
  auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
  mgp_graph raw_graph = this->CreateGraph(db_acc.get());

  // Prepare the properties list: ["username"]
  mgp_list list_props({mgp_value("username", this->memory.impl)}, this->memory.impl);
  ASSERT_TRUE(mgp::CreateUniqueConstraint(&raw_graph, "Account", &list_props));
  auto constraints = mgp::ListAllUniqueConstraints(&raw_graph);
  ASSERT_EQ(constraints.Size(), 1);
  ASSERT_EQ(constraints[0].ValueList().Size(), 2);
  ASSERT_EQ(constraints[0].ValueList()[0].ValueString(), "Account");
  ASSERT_EQ(constraints[0].ValueList()[1].ValueString(), "username");

  ASSERT_TRUE(mgp::DropUniqueConstraint(&raw_graph, "Account", &list_props));

  auto updated_constraints = mgp::ListAllUniqueConstraints(&raw_graph);
  ASSERT_EQ(updated_constraints.Size(), 0);

  mgp_list fake_props({mgp_value("nonexistent", this->memory.impl)}, this->memory.impl);
  ASSERT_FALSE(mgp::DropUniqueConstraint(&raw_graph, "Account", &fake_props));
}

TYPED_TEST(CppApiTestFixture, TestVectorSearch) {
  if constexpr (!std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
    GTEST_SKIP() << "TestVectorSearch runs only on InMemoryStorage.";
  }
  constexpr auto index_name = "index";
  constexpr auto label_name = "label";
  constexpr auto property_name = "property";
  constexpr auto metric_as_str = "l2sq";
  constexpr auto scalar_kind_as_str = "f32";
  constexpr auto metric = unum::usearch::metric_kind_t::l2sq_k;
  constexpr auto dimension = 2;
  constexpr auto resize_coefficient = 2;
  constexpr auto max_elements = 10;
  constexpr auto scalar_kind = unum::usearch::scalar_kind_t::f32_k;
  constexpr auto capacity_reserved = 64;
  constexpr auto size = 1;

  {
    auto storage_acc = this->storage->UniqueAccess();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    auto label = db_acc->NameToLabel(label_name);
    auto property = db_acc->NameToProperty(property_name);
    auto spec = memgraph::storage::VectorIndexSpec{index_name,         label,        property,   metric, dimension,
                                                   resize_coefficient, max_elements, scalar_kind};
    ASSERT_FALSE(db_acc->CreateVectorIndex(spec).HasError());
    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  auto storage_acc = this->storage->Access(AccessorType::WRITE);
  auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
  mgp_graph raw_graph = this->CreateGraph(db_acc.get());
  auto graph = mgp::Graph(&raw_graph);
  auto node1 = graph.CreateNode();
  node1.AddLabel("label");
  node1.SetProperty("property", mgp::Value(mgp::List({mgp::Value(1.0), mgp::Value(2.0)})));

  auto vector_index_info = mgp::GetVectorIndexInfo(&raw_graph);
  ASSERT_EQ(vector_index_info.Size(), 1);
  auto vector_index_info_list = vector_index_info[0].ValueList();
  ASSERT_EQ(vector_index_info_list.Size(), 9);
  ASSERT_EQ(vector_index_info_list[0].ValueString(), index_name);
  ASSERT_EQ(vector_index_info_list[1].ValueString(), label_name);
  ASSERT_EQ(vector_index_info_list[2].ValueString(), property_name);
  ASSERT_EQ(vector_index_info_list[3].ValueString(), metric_as_str);
  ASSERT_EQ(vector_index_info_list[4].ValueInt(), dimension);
  ASSERT_EQ(vector_index_info_list[5].ValueInt(), capacity_reserved);
  ASSERT_EQ(vector_index_info_list[6].ValueInt(), size);
  ASSERT_EQ(vector_index_info_list[7].ValueString(), scalar_kind_as_str);

  auto list_to_find = mgp::List({mgp::Value(1.0), mgp::Value(2.0)});
  auto found_nodes = mgp::SearchVectorIndex(&raw_graph, "index", list_to_find, 1);
  ASSERT_EQ(found_nodes.Size(), 1);
  ASSERT_EQ(found_nodes[0].ValueList()[0].ValueNode().Id(), node1.Id());
}

TYPED_TEST(CppApiTestFixture, TestVectorSearchOnEdges) {
  if constexpr (!std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
    GTEST_SKIP() << "TestVectorSearchOnEdges runs only on InMemoryStorage.";
  }
  constexpr auto index_name = "index";
  constexpr auto edge_type = "edge_type";
  constexpr auto property_name = "property";
  constexpr auto metric_as_str = "l2sq";
  constexpr auto scalar_kind_as_str = "f32";
  constexpr auto metric = unum::usearch::metric_kind_t::l2sq_k;
  constexpr auto dimension = 2;
  constexpr auto resize_coefficient = 2;
  constexpr auto max_elements = 10;
  constexpr auto scalar_kind = unum::usearch::scalar_kind_t::f32_k;
  constexpr auto capacity_reserved = 64;
  constexpr auto size = 1;

  {
    auto storage_acc = this->storage->UniqueAccess();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    auto edge = db_acc->NameToEdgeType(edge_type);
    auto property = db_acc->NameToProperty(property_name);
    auto spec = memgraph::storage::VectorEdgeIndexSpec{index_name,         edge,         property,   metric, dimension,
                                                       resize_coefficient, max_elements, scalar_kind};
    ASSERT_FALSE(db_acc->CreateVectorEdgeIndex(spec).HasError());
    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  auto storage_acc = this->storage->Access(AccessorType::WRITE);
  auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
  mgp_graph raw_graph = this->CreateGraph(db_acc.get());
  auto graph = mgp::Graph(&raw_graph);
  auto node1 = graph.CreateNode();
  auto node2 = graph.CreateNode();
  auto edge1 = graph.CreateRelationship(node1, node2, edge_type);
  edge1.SetProperty(property_name, mgp::Value(mgp::List({mgp::Value(1.0), mgp::Value(2.0)})));

  auto vector_index_info = mgp::GetVectorIndexInfo(&raw_graph);
  ASSERT_EQ(vector_index_info.Size(), 1);
  auto vector_index_info_list = vector_index_info[0].ValueList();
  ASSERT_EQ(vector_index_info_list.Size(), 9);
  ASSERT_EQ(vector_index_info_list[0].ValueString(), index_name);
  ASSERT_EQ(vector_index_info_list[1].ValueString(), edge_type);
  ASSERT_EQ(vector_index_info_list[2].ValueString(), property_name);
  ASSERT_EQ(vector_index_info_list[3].ValueString(), metric_as_str);
  ASSERT_EQ(vector_index_info_list[4].ValueInt(), dimension);
  ASSERT_EQ(vector_index_info_list[5].ValueInt(), capacity_reserved);
  ASSERT_EQ(vector_index_info_list[6].ValueInt(), size);
  ASSERT_EQ(vector_index_info_list[7].ValueString(), scalar_kind_as_str);

  auto list_to_find = mgp::List({mgp::Value(1.0), mgp::Value(2.0)});
  auto found_edges = mgp::SearchVectorIndexOnEdges(&raw_graph, "index", list_to_find, 1);
  ASSERT_EQ(found_edges.Size(), 1);
  ASSERT_EQ(found_edges[0].ValueList()[0].ValueRelationship().Id(), edge1.Id());
}

TYPED_TEST(CppApiTestFixture, TestTextIndexOnNodes) {
  constexpr auto index_name = "node_text_index";
  constexpr auto label_name = "Document";
  constexpr auto property_name = "content";
  constexpr auto text_search_limit = 10;
  // Create text index
  {
    auto storage_acc = this->storage->UniqueAccess();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    auto label = db_acc->NameToLabel(label_name);
    auto property = db_acc->NameToProperty(property_name);
    auto text_index_spec =
        memgraph::storage::TextIndexSpec{.index_name = index_name, .label = label, .properties = {property}};
    ASSERT_FALSE(db_acc->CreateTextIndex(text_index_spec).HasError());
    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // Create nodes with text content and test search
  {
    auto storage_acc = this->storage->UniqueAccess();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());
    auto graph = mgp::Graph(&raw_graph);

    // Create nodes with text content
    auto node1 = graph.CreateNode();
    node1.AddLabel(label_name);
    node1.SetProperty(property_name, mgp::Value("This is a test document"));

    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // Test text index search
  {
    auto storage_acc = this->storage->Access(memgraph::storage::StorageAccessType::READ);
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());

    // Test search functionality
    auto search_results = mgp::SearchTextIndex(&raw_graph, index_name, "data.content:document",
                                               text_search_mode::SPECIFIED_PROPERTIES, text_search_limit);
    ASSERT_GE(search_results.Size(), 1);
  }

  // Cleanup - drop text index
  {
    auto storage_acc = this->storage->UniqueAccess();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    ASSERT_FALSE(db_acc->DropTextIndex(index_name).HasError());
    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
}

TYPED_TEST(CppApiTestFixture, TestTextIndexOnEdges) {
  if constexpr (!std::is_same<TypeParam, memgraph::storage::InMemoryStorage>::value) {
    GTEST_SKIP() << "TestTextIndexOnEdges runs only on InMemoryStorage.";
  }
  constexpr auto index_name = "edge_text_index";
  constexpr auto edge_type = "CONTAINS";
  constexpr auto property_name = "description";
  constexpr auto text_search_limit = 10;

  // Create text edge index
  {
    auto storage_acc = this->storage->UniqueAccess();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    auto edge_type_id = db_acc->NameToEdgeType(edge_type);
    auto property = db_acc->NameToProperty(property_name);
    auto text_edge_index_spec = memgraph::storage::TextEdgeIndexSpec{
        .index_name = index_name, .edge_type = edge_type_id, .properties = {property}};
    ASSERT_FALSE(db_acc->CreateTextEdgeIndex(text_edge_index_spec).HasError());
    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // Create edges with text content and test search
  {
    auto storage_acc = this->storage->UniqueAccess();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());
    auto graph = mgp::Graph(&raw_graph);

    // Create nodes and edges with text content
    auto node1 = graph.CreateNode();
    auto node2 = graph.CreateNode();

    auto edge1 = graph.CreateRelationship(node1, node2, edge_type);
    edge1.SetProperty(property_name, mgp::Value("This edge contains important information about data structures"));
    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // Test text edge index search
  {
    auto storage_acc = this->storage->Access(memgraph::storage::StorageAccessType::READ);
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    mgp_graph raw_graph = this->CreateGraph(db_acc.get());

    // Test search functionality
    auto search_results = mgp::SearchTextEdgeIndex(&raw_graph, index_name, "data.description:information",
                                                   text_search_mode::SPECIFIED_PROPERTIES, text_search_limit);
    ASSERT_GE(search_results.Size(), 1);
  }

  // Cleanup - drop text index
  {
    auto storage_acc = this->storage->UniqueAccess();
    auto db_acc = std::make_unique<memgraph::query::DbAccessor>(storage_acc.get());
    ASSERT_FALSE(db_acc->DropTextIndex(index_name).HasError());
    ASSERT_FALSE(db_acc->Commit(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
}

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

#include <gtest/gtest-typed-test.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <optional>
#include <set>
#include <vector>

#include "communication/result_stream_faker.hpp"
#include "dbms/database.hpp"
#include "disk_test_utils.hpp"
#include "query/config.hpp"
#include "query/dump.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/stream/streams.hpp"
#include "query/trigger.hpp"
#include "query/trigger_context.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/temporal.hpp"

const char *kPropertyId = "property_id";

const char *kCreateInternalIndex = "CREATE INDEX ON :__mg_vertex__(__mg_id__);";
const char *kDropInternalIndex = "DROP INDEX ON :__mg_vertex__(__mg_id__);";
const char *kRemoveInternalLabelProperty = "MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;";

// A helper struct that contains info about database that is used to compare
// two databases (to check if their states are the same). It is assumed that
// each vertex and each edge have unique integer property under key
// `kPropertyId`.
struct DatabaseState {
  struct Vertex {
    int64_t id;
    std::set<std::string, std::less<>> labels;
    std::map<std::string, memgraph::storage::PropertyValue> props;
  };

  struct Edge {
    int64_t from, to;
    std::string edge_type;
    std::map<std::string, memgraph::storage::PropertyValue> props;
  };

  struct LabelItem {
    std::string label;
  };

  struct LabelPropertyItem {
    std::string label;
    std::string property;
  };

  struct TextItem {
    std::string index_name;
    std::string label;
  };

  struct LabelPropertiesItem {
    std::string label;
    std::set<std::string, std::less<>> properties;
  };

  std::set<Vertex> vertices;
  std::set<Edge> edges;
  std::set<LabelItem> label_indices;
  std::set<LabelPropertyItem> label_property_indices;
  std::set<TextItem> text_indices;
  std::set<LabelPropertyItem> existence_constraints;
  std::set<LabelPropertiesItem> unique_constraints;
};

bool operator<(const DatabaseState::Vertex &first, const DatabaseState::Vertex &second) {
  if (first.id != second.id) return first.id < second.id;
  if (first.labels != second.labels) return first.labels < second.labels;
  return first.props < second.props;
}

bool operator<(const DatabaseState::Edge &first, const DatabaseState::Edge &second) {
  if (first.from != second.from) return first.from < second.from;
  if (first.to != second.to) return first.to < second.to;
  if (first.edge_type != second.edge_type) return first.edge_type < second.edge_type;
  return first.props < second.props;
}

bool operator<(const DatabaseState::LabelItem &first, const DatabaseState::LabelItem &second) {
  return first.label < second.label;
}

bool operator<(const DatabaseState::LabelPropertyItem &first, const DatabaseState::LabelPropertyItem &second) {
  if (first.label != second.label) return first.label < second.label;
  return first.property < second.property;
}

bool operator<(const DatabaseState::LabelPropertiesItem &first, const DatabaseState::LabelPropertiesItem &second) {
  if (first.label != second.label) return first.label < second.label;
  return first.properties < second.properties;
}

bool operator==(const DatabaseState::Vertex &first, const DatabaseState::Vertex &second) {
  return first.id == second.id && first.labels == second.labels && first.props == second.props;
}

bool operator==(const DatabaseState::Edge &first, const DatabaseState::Edge &second) {
  return first.from == second.from && first.to == second.to && first.edge_type == second.edge_type &&
         first.props == second.props;
}

bool operator==(const DatabaseState::LabelItem &first, const DatabaseState::LabelItem &second) {
  return first.label == second.label;
}

bool operator==(const DatabaseState::LabelPropertyItem &first, const DatabaseState::LabelPropertyItem &second) {
  return first.label == second.label && first.property == second.property;
}

bool operator==(const DatabaseState::LabelPropertiesItem &first, const DatabaseState::LabelPropertiesItem &second) {
  return first.label == second.label && first.properties == second.properties;
}

bool operator==(const DatabaseState &first, const DatabaseState &second) {
  return first.vertices == second.vertices && first.edges == second.edges &&
         first.label_indices == second.label_indices && first.label_property_indices == second.label_property_indices &&
         first.existence_constraints == second.existence_constraints &&
         first.unique_constraints == second.unique_constraints;
}

DatabaseState GetState(memgraph::storage::Storage *db) {
  // Capture all vertices
  std::map<memgraph::storage::Gid, int64_t> gid_mapping;
  std::set<DatabaseState::Vertex> vertices;
  auto dba = db->Access(memgraph::replication_coordination_glue::ReplicationRole::MAIN);
  for (const auto &vertex : dba->Vertices(memgraph::storage::View::NEW)) {
    std::set<std::string, std::less<>> labels;
    auto maybe_labels = vertex.Labels(memgraph::storage::View::NEW);
    MG_ASSERT(maybe_labels.HasValue());
    for (const auto &label : *maybe_labels) {
      labels.insert(dba->LabelToName(label));
    }
    std::map<std::string, memgraph::storage::PropertyValue> props;
    auto maybe_properties = vertex.Properties(memgraph::storage::View::NEW);
    MG_ASSERT(maybe_properties.HasValue());
    for (const auto &kv : *maybe_properties) {
      props.emplace(dba->PropertyToName(kv.first), kv.second);
    }
    MG_ASSERT(props.count(kPropertyId) == 1);
    const auto id = props[kPropertyId].ValueInt();
    gid_mapping[vertex.Gid()] = id;
    vertices.insert({id, labels, props});
  }

  // Capture all edges
  std::set<DatabaseState::Edge> edges;
  for (const auto &vertex : dba->Vertices(memgraph::storage::View::NEW)) {
    auto maybe_edges = vertex.OutEdges(memgraph::storage::View::NEW);
    MG_ASSERT(maybe_edges.HasValue());
    for (const auto &edge : maybe_edges->edges) {
      const auto &edge_type_name = dba->EdgeTypeToName(edge.EdgeType());
      std::map<std::string, memgraph::storage::PropertyValue> props;
      auto maybe_properties = edge.Properties(memgraph::storage::View::NEW);
      MG_ASSERT(maybe_properties.HasValue());
      for (const auto &kv : *maybe_properties) {
        props.emplace(dba->PropertyToName(kv.first), kv.second);
      }
      const auto from = gid_mapping[edge.FromVertex().Gid()];
      const auto to = gid_mapping[edge.ToVertex().Gid()];
      edges.insert({from, to, edge_type_name, props});
    }
  }

  // Capture all indices
  std::set<DatabaseState::LabelItem> label_indices;
  std::set<DatabaseState::LabelPropertyItem> label_property_indices;
  std::set<DatabaseState::TextItem> text_indices;
  {
    auto info = dba->ListAllIndices();
    for (const auto &item : info.label) {
      label_indices.insert({dba->LabelToName(item)});
    }
    for (const auto &item : info.label_property) {
      label_property_indices.insert({dba->LabelToName(item.first), dba->PropertyToName(item.second)});
    }
    for (const auto &item : info.text) {
      text_indices.insert({item.first, dba->PropertyToName(item.second)});
    }
  }

  // Capture all constraints
  std::set<DatabaseState::LabelPropertyItem> existence_constraints;
  std::set<DatabaseState::LabelPropertiesItem> unique_constraints;
  {
    auto info = dba->ListAllConstraints();
    for (const auto &item : info.existence) {
      existence_constraints.insert({dba->LabelToName(item.first), dba->PropertyToName(item.second)});
    }
    for (const auto &item : info.unique) {
      std::set<std::string, std::less<>> properties;
      for (const auto &property : item.second) {
        properties.insert(dba->PropertyToName(property));
      }
      unique_constraints.insert({dba->LabelToName(item.first), std::move(properties)});
    }
  }

  return {vertices,          edges, label_indices, label_property_indices, text_indices, existence_constraints,
          unique_constraints};
}

auto Execute(memgraph::query::InterpreterContext *context, memgraph::dbms::DatabaseAccess db,
             const std::string &query) {
  memgraph::query::Interpreter interpreter(context, db);
  ResultStreamFaker stream(db->storage());

  auto [header, _1, qid, _2] = interpreter.Prepare(query, {}, {});
  stream.Header(header);
  auto summary = interpreter.PullAll(&stream);
  stream.Summary(summary);

  return stream;
}

memgraph::storage::VertexAccessor CreateVertex(memgraph::storage::Storage::Accessor *dba,
                                               const std::vector<std::string> &labels,
                                               const std::map<std::string, memgraph::storage::PropertyValue> &props,
                                               bool add_property_id = true) {
  MG_ASSERT(dba);
  auto vertex = dba->CreateVertex();
  for (const auto &label_name : labels) {
    MG_ASSERT(vertex.AddLabel(dba->NameToLabel(label_name)).HasValue());
  }
  for (const auto &kv : props) {
    MG_ASSERT(vertex.SetProperty(dba->NameToProperty(kv.first), kv.second).HasValue());
  }
  if (add_property_id) {
    MG_ASSERT(
        vertex.SetProperty(dba->NameToProperty(kPropertyId), memgraph::storage::PropertyValue(vertex.Gid().AsInt()))
            .HasValue());
  }
  return vertex;
}

memgraph::storage::EdgeAccessor CreateEdge(memgraph::storage::Storage::Accessor *dba,
                                           memgraph::storage::VertexAccessor *from,
                                           memgraph::storage::VertexAccessor *to, const std::string &edge_type_name,
                                           const std::map<std::string, memgraph::storage::PropertyValue> &props,
                                           bool add_property_id = true) {
  MG_ASSERT(dba);
  auto edge = dba->CreateEdge(from, to, dba->NameToEdgeType(edge_type_name));
  MG_ASSERT(edge.HasValue());
  auto edgeAcc = std::move(edge.GetValue());
  for (const auto &kv : props) {
    MG_ASSERT(edgeAcc.SetProperty(dba->NameToProperty(kv.first), kv.second).HasValue());
  }
  if (add_property_id) {
    MG_ASSERT(
        edgeAcc.SetProperty(dba->NameToProperty(kPropertyId), memgraph::storage::PropertyValue(edgeAcc.Gid().AsInt()))
            .HasValue());
  }
  return edgeAcc;
}

template <class... TArgs>
void VerifyQueries(const std::vector<std::vector<memgraph::communication::bolt::Value>> &results, TArgs &&...args) {
  std::vector<std::string> expected{std::forward<TArgs>(args)...};
  std::vector<std::string> got;
  got.reserve(results.size());
  for (const auto &result : results) {
    ASSERT_EQ(result.size(), 1);
    ASSERT_TRUE(result[0].IsString());
    got.push_back(result[0].ValueString());
  }
  std::sort(got.begin(), got.end());
  std::sort(expected.begin(), expected.end());
  ASSERT_EQ(got, expected);
}

template <typename StorageType>
class DumpTest : public ::testing::Test {
 public:
  const std::string testSuite = "query_dump";
  std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "MG_tests_unit_query_dump_class"};

  memgraph::storage::Config config{
      [&]() {
        memgraph::storage::Config config{};
        config.durability.storage_directory = data_directory;
        config.disk.main_storage_directory = config.durability.storage_directory / "disk";
        if constexpr (std::is_same_v<StorageType, memgraph::storage::DiskStorage>) {
          config.disk = disk_test_utils::GenerateOnDiskConfig(testSuite).disk;
          config.force_on_disk = true;
        }
        return config;
      }()  // iile
  };

  memgraph::replication::ReplicationState repl_state{memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config, repl_state};
  memgraph::dbms::DatabaseAccess db{
      [&]() {
        auto db_acc_opt = db_gk.access();
        MG_ASSERT(db_acc_opt, "Failed to access db");
        auto &db_acc = *db_acc_opt;
        MG_ASSERT(db_acc->GetStorageMode() == (std::is_same_v<StorageType, memgraph::storage::DiskStorage>
                                                   ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                                   : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL),
                  "Wrong storage mode!");
        return db_acc;
      }()  // iile
  };

  memgraph::query::InterpreterContext context{memgraph::query::InterpreterConfig{}, nullptr, &repl_state};

  void TearDown() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
    std::filesystem::remove_all(data_directory);
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
TYPED_TEST_CASE(DumpTest, StorageTypes);

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, EmptyGraph) {
  ResultStreamFaker stream(this->db->storage());
  memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
  {
    auto acc = this->db->Access();
    memgraph::query::DbAccessor dba(acc.get());
    memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
  }
  ASSERT_EQ(stream.GetResults().size(), 0);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, SingleVertex) {
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {}, {}, false);
    ASSERT_FALSE(dba->Commit().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, VertexWithSingleLabel) {
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {"Label1"}, {}, false);
    ASSERT_FALSE(dba->Commit().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__:`Label1` {__mg_id__: 0});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, VertexWithMultipleLabels) {
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {"Label1", "Label 2"}, {}, false);
    ASSERT_FALSE(dba->Commit().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex,
                  "CREATE (:__mg_vertex__:`Label1`:`Label 2` {__mg_id__: 0});", kDropInternalIndex,
                  kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, VertexWithSingleProperty) {
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {}, {{"prop", memgraph::storage::PropertyValue(42)}}, false);
    ASSERT_FALSE(dba->Commit().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0, `prop`: 42});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, MultipleVertices) {
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {}, {}, false);
    CreateVertex(dba.get(), {}, {}, false);
    CreateVertex(dba.get(), {}, {}, false);
    ASSERT_FALSE(dba->Commit().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0});",
                  "CREATE (:__mg_vertex__ {__mg_id__: 1});", "CREATE (:__mg_vertex__ {__mg_id__: 2});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

TYPED_TEST(DumpTest, PropertyValue) {
  {
    auto dba = this->db->Access();
    auto null_value = memgraph::storage::PropertyValue();
    auto int_value = memgraph::storage::PropertyValue(13);
    auto bool_value = memgraph::storage::PropertyValue(true);
    auto double_value = memgraph::storage::PropertyValue(-1.2);
    auto str_value = memgraph::storage::PropertyValue("hello 'world'");
    auto map_value = memgraph::storage::PropertyValue({{"prop 1", int_value}, {"prop`2`", bool_value}});
    auto dt = memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
        memgraph::storage::TemporalType::Date, memgraph::utils::Date({1994, 12, 7}).MicrosecondsSinceEpoch()));
    auto lt = memgraph::storage::PropertyValue(
        memgraph::storage::TemporalData(memgraph::storage::TemporalType::LocalTime,
                                        memgraph::utils::LocalTime({14, 10, 44, 99, 99}).MicrosecondsSinceEpoch()));
    auto ldt = memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
        memgraph::storage::TemporalType::LocalDateTime,
        memgraph::utils::LocalDateTime({1994, 12, 7}, {14, 10, 44, 99, 99}).MicrosecondsSinceEpoch()));
    auto dur = memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
        memgraph::storage::TemporalType::Duration, memgraph::utils::Duration({3, 4, 5, 6, 10, 11}).microseconds));
    auto list_value = memgraph::storage::PropertyValue({map_value, null_value, double_value, dt, lt, ldt, dur});
    CreateVertex(dba.get(), {}, {{"p1", list_value}, {"p2", str_value}}, false);
    ASSERT_FALSE(dba->Commit().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex,
                  "CREATE (:__mg_vertex__ {__mg_id__: 0, `p1`: [{`prop 1`: 13, "
                  "`prop``2```: true}, Null, -1.2, DATE(\"1994-12-07\"), "
                  "LOCALTIME(\"14:10:44.099099\"), LOCALDATETIME(\"1994-12-07T14:10:44.099099\"), "
                  "DURATION(\"P3DT4H5M6.010011S\")"
                  "], `p2`: \"hello \\'world\\'\"});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}
// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, SingleEdge) {
  {
    auto dba = this->db->Access();
    auto u = CreateVertex(dba.get(), {}, {}, false);
    auto v = CreateVertex(dba.get(), {}, {}, false);
    CreateEdge(dba.get(), &u, &v, "EdgeType", {}, false);
    ASSERT_FALSE(dba->Commit().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0});",
                  "CREATE (:__mg_vertex__ {__mg_id__: 1});",
                  "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND "
                  "v.__mg_id__ = 1 CREATE (u)-[:`EdgeType`]->(v);",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, MultipleEdges) {
  {
    auto dba = this->db->Access();
    auto u = CreateVertex(dba.get(), {}, {}, false);
    auto v = CreateVertex(dba.get(), {}, {}, false);
    auto w = CreateVertex(dba.get(), {}, {}, false);
    CreateEdge(dba.get(), &u, &v, "EdgeType", {}, false);
    CreateEdge(dba.get(), &v, &u, "EdgeType 2", {}, false);
    CreateEdge(dba.get(), &v, &w, "EdgeType `!\"", {}, false);
    ASSERT_FALSE(dba->Commit().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0});",
                  "CREATE (:__mg_vertex__ {__mg_id__: 1});", "CREATE (:__mg_vertex__ {__mg_id__: 2});",
                  "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND "
                  "v.__mg_id__ = 1 CREATE (u)-[:`EdgeType`]->(v);",
                  "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND "
                  "v.__mg_id__ = 0 CREATE (u)-[:`EdgeType 2`]->(v);",
                  "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND "
                  "v.__mg_id__ = 2 CREATE (u)-[:`EdgeType ``!\"`]->(v);",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, EdgeWithProperties) {
  {
    auto dba = this->db->Access();
    auto u = CreateVertex(dba.get(), {}, {}, false);
    auto v = CreateVertex(dba.get(), {}, {}, false);
    CreateEdge(dba.get(), &u, &v, "EdgeType", {{"prop", memgraph::storage::PropertyValue(13)}}, false);
    ASSERT_FALSE(dba->Commit().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0});",
                  "CREATE (:__mg_vertex__ {__mg_id__: 1});",
                  "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND "
                  "v.__mg_id__ = 1 CREATE (u)-[:`EdgeType` {`prop`: 13}]->(v);",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, IndicesKeys) {
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {"Label1", "Label 2"}, {{"p", memgraph::storage::PropertyValue(1)}}, false);
    ASSERT_FALSE(dba->Commit().HasError());
  }

  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(
        unique_acc->CreateIndex(this->db->storage()->NameToLabel("Label1"), this->db->storage()->NameToProperty("prop"))
            .HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(
        unique_acc
            ->CreateIndex(this->db->storage()->NameToLabel("Label 2"), this->db->storage()->NameToProperty("prop `"))
            .HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), "CREATE INDEX ON :`Label1`(`prop`);", "CREATE INDEX ON :`Label 2`(`prop ```);",
                  kCreateInternalIndex, "CREATE (:__mg_vertex__:`Label1`:`Label 2` {__mg_id__: 0, `p`: 1});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, ExistenceConstraints) {
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {"L`abel 1"}, {{"prop", memgraph::storage::PropertyValue(1)}}, false);
    ASSERT_FALSE(dba->Commit().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    auto res = unique_acc->CreateExistenceConstraint(this->db->storage()->NameToLabel("L`abel 1"),
                                                     this->db->storage()->NameToProperty("prop"));
    ASSERT_FALSE(res.HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(), "CREATE CONSTRAINT ON (u:`L``abel 1`) ASSERT EXISTS (u.`prop`);",
                  kCreateInternalIndex, "CREATE (:__mg_vertex__:`L``abel 1` {__mg_id__: 0, `prop`: 1});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

TYPED_TEST(DumpTest, UniqueConstraints) {
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {"Label"},
                 {{"prop", memgraph::storage::PropertyValue(1)}, {"prop2", memgraph::storage::PropertyValue(2)}},
                 false);
    CreateVertex(dba.get(), {"Label"},
                 {{"prop", memgraph::storage::PropertyValue(2)}, {"prop2", memgraph::storage::PropertyValue(2)}},
                 false);
    ASSERT_FALSE(dba->Commit().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    auto res = unique_acc->CreateUniqueConstraint(
        this->db->storage()->NameToLabel("Label"),
        {this->db->storage()->NameToProperty("prop"), this->db->storage()->NameToProperty("prop2")});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), memgraph::storage::UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    VerifyQueries(stream.GetResults(),
                  "CREATE CONSTRAINT ON (u:`Label`) ASSERT u.`prop`, u.`prop2` "
                  "IS UNIQUE;",
                  kCreateInternalIndex,
                  "CREATE (:__mg_vertex__:`Label` {__mg_id__: 0, `prop`: 1, "
                  "`prop2`: 2});",
                  "CREATE (:__mg_vertex__:`Label` {__mg_id__: 1, `prop`: 2, "
                  "`prop2`: 2});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, CheckStateVertexWithMultipleProperties) {
  {
    auto dba = this->db->Access();
    std::map<std::string, memgraph::storage::PropertyValue> prop1 = {
        {"nested1", memgraph::storage::PropertyValue(1337)}, {"nested2", memgraph::storage::PropertyValue(3.14)}};

    CreateVertex(
        dba.get(), {"Label1", "Label2"},
        {{"prop1", memgraph::storage::PropertyValue(prop1)}, {"prop2", memgraph::storage::PropertyValue("$'\t'")}});

    ASSERT_FALSE(dba->Commit().HasError());
  }

  memgraph::storage::Config config{};
  config.durability.storage_directory = this->data_directory / "s1";
  config.disk.main_storage_directory = config.durability.storage_directory / "disk";
  if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    config.disk = disk_test_utils::GenerateOnDiskConfig("query-dump-s1").disk;
    config.force_on_disk = true;
  }
  auto clean_up_s1 = memgraph::utils::OnScopeExit{[&] {
    if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs("query-dump-s1");
    }
    std::filesystem::remove_all(config.durability.storage_directory);
  }};

  memgraph::replication::ReplicationState repl_state(ReplicationStateRootPath(config));

  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk(config, repl_state);
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt) << "Failed to access db";
  auto &db_acc = *db_acc_opt;
  ASSERT_TRUE(db_acc->GetStorageMode() == (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>
                                               ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                               : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL))
      << "Wrong storage mode!";

  memgraph::query::InterpreterContext interpreter_context(memgraph::query::InterpreterConfig{}, nullptr, &repl_state);

  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    const auto &results = stream.GetResults();
    ASSERT_GE(results.size(), 1);
    for (const auto &item : results) {
      ASSERT_EQ(item.size(), 1);
      ASSERT_TRUE(item[0].IsString());
      Execute(&interpreter_context, db_acc, item[0].ValueString());
    }
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, CheckStateSimpleGraph) {
  {
    auto dba = this->db->Access();
    auto u = CreateVertex(dba.get(), {"Person"}, {{"name", memgraph::storage::PropertyValue("Ivan")}});
    auto v = CreateVertex(dba.get(), {"Person"}, {{"name", memgraph::storage::PropertyValue("Josko")}});
    auto w = CreateVertex(
        dba.get(), {"Person"},
        {{"name", memgraph::storage::PropertyValue("Bosko")}, {"id", memgraph::storage::PropertyValue(0)}});
    auto z =
        CreateVertex(dba.get(), {"Person"},
                     {{"name", memgraph::storage::PropertyValue("Buha")}, {"id", memgraph::storage::PropertyValue(1)}});
    CreateEdge(dba.get(), &u, &v, "Knows", {});
    CreateEdge(dba.get(), &v, &w, "Knows", {{"how_long", memgraph::storage::PropertyValue(5)}});
    CreateEdge(dba.get(), &w, &u, "Knows", {{"how", memgraph::storage::PropertyValue("distant past")}});
    CreateEdge(dba.get(), &v, &u, "Knows", {});
    CreateEdge(dba.get(), &v, &u, "Likes", {});
    CreateEdge(dba.get(), &z, &u, "Knows", {});
    CreateEdge(dba.get(), &w, &z, "Knows", {{"how", memgraph::storage::PropertyValue("school")}});
    CreateEdge(dba.get(), &w, &z, "Likes", {{"how", memgraph::storage::PropertyValue("1234567890")}});
    CreateEdge(dba.get(), &w, &z, "Date",
               {{"time", memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
                             memgraph::storage::TemporalType::Date,
                             memgraph::utils::Date({1994, 12, 7}).MicrosecondsSinceEpoch()))}});
    CreateEdge(dba.get(), &w, &z, "LocalTime",
               {{"time", memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
                             memgraph::storage::TemporalType::LocalTime,
                             memgraph::utils::LocalTime({14, 10, 44, 99, 99}).MicrosecondsSinceEpoch()))}});
    CreateEdge(
        dba.get(), &w, &z, "LocalDateTime",
        {{"time", memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
                      memgraph::storage::TemporalType::LocalDateTime,
                      memgraph::utils::LocalDateTime({1994, 12, 7}, {14, 10, 44, 99, 99}).MicrosecondsSinceEpoch()))}});
    CreateEdge(dba.get(), &w, &z, "Duration",
               {{"time", memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
                             memgraph::storage::TemporalType::Duration,
                             memgraph::utils::Duration({3, 4, 5, 6, 10, 11}).microseconds))}});
    CreateEdge(dba.get(), &w, &z, "NegativeDuration",
               {{"time", memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
                             memgraph::storage::TemporalType::Duration,
                             memgraph::utils::Duration({-3, -4, -5, -6, -10, -11}).microseconds))}});
    ASSERT_FALSE(dba->Commit().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    auto ret = unique_acc->CreateExistenceConstraint(this->db->storage()->NameToLabel("Person"),
                                                     this->db->storage()->NameToProperty("name"));
    ASSERT_FALSE(ret.HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    auto ret = unique_acc->CreateUniqueConstraint(this->db->storage()->NameToLabel("Person"),
                                                  {this->db->storage()->NameToProperty("name")});
    ASSERT_TRUE(ret.HasValue());
    ASSERT_EQ(ret.GetValue(), memgraph::storage::UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }

  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(
        unique_acc->CreateIndex(this->db->storage()->NameToLabel("Person"), this->db->storage()->NameToProperty("id"))
            .HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }
  {
    auto unique_acc = this->db->UniqueAccess();
    ASSERT_FALSE(unique_acc
                     ->CreateIndex(this->db->storage()->NameToLabel("Person"),
                                   this->db->storage()->NameToProperty("unexisting_property"))
                     .HasError());
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }

  const auto &db_initial_state = GetState(this->db->storage());
  memgraph::storage::Config config{};
  config.durability.storage_directory = this->data_directory / "s2";
  config.disk.main_storage_directory = config.durability.storage_directory / "disk";
  if constexpr (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>) {
    config.disk = disk_test_utils::GenerateOnDiskConfig("query-dump-s2").disk;
    config.force_on_disk = true;
  }
  auto clean_up_s2 = memgraph::utils::OnScopeExit{[&] {
    if (std::is_same<TypeParam, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs("query-dump-s2");
    }
    std::filesystem::remove_all(config.durability.storage_directory);
  }};

  memgraph::replication::ReplicationState repl_state{ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config, repl_state};
  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt) << "Failed to access db";
  auto &db_acc = *db_acc_opt;
  ASSERT_TRUE(db_acc->GetStorageMode() == (std::is_same_v<TypeParam, memgraph::storage::DiskStorage>
                                               ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                               : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL))
      << "Wrong storage mode!";

  memgraph::query::InterpreterContext interpreter_context(memgraph::query::InterpreterConfig{}, nullptr, &repl_state);
  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = this->db->Access();
      memgraph::query::DbAccessor dba(acc.get());
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db);
    }
    const auto &results = stream.GetResults();
    // Indices and constraints are 4 queries and there must be at least one more
    // query for the data.
    ASSERT_GE(results.size(), 5);
    int i = 0;
    for (const auto &item : results) {
      ASSERT_EQ(item.size(), 1);
      ASSERT_TRUE(item[0].IsString());
      spdlog::debug("Query: {}", item[0].ValueString());
      Execute(&interpreter_context, db_acc, item[0].ValueString());
      ++i;
    }
  }
  ASSERT_EQ(GetState(this->db->storage()), db_initial_state);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, ExecuteDumpDatabase) {
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {}, {}, false);
    ASSERT_FALSE(dba->Commit().HasError());
  }

  {
    auto stream = Execute(&this->context, this->db, "DUMP DATABASE");
    const auto &header = stream.GetHeader();
    const auto &results = stream.GetResults();
    ASSERT_EQ(header.size(), 1U);
    EXPECT_EQ(header[0], "QUERY");
    EXPECT_EQ(results.size(), 4U);
    for (const auto &item : results) {
      EXPECT_EQ(item.size(), 1);
      EXPECT_TRUE(item[0].IsString());
    }
    EXPECT_EQ(results[0][0].ValueString(), "CREATE INDEX ON :__mg_vertex__(__mg_id__);");
    EXPECT_EQ(results[1][0].ValueString(), "CREATE (:__mg_vertex__ {__mg_id__: 0});");
    EXPECT_EQ(results[2][0].ValueString(), "DROP INDEX ON :__mg_vertex__(__mg_id__);");
    EXPECT_EQ(results[3][0].ValueString(), "MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;");
  }
}

class StatefulInterpreter {
 public:
  explicit StatefulInterpreter(memgraph::query::InterpreterContext *context, memgraph::dbms::DatabaseAccess db)
      : context_(context), interpreter_(context_, db) {}

  auto Execute(const std::string &query) {
    ResultStreamFaker stream(interpreter_.current_db_.db_acc_->get()->storage());

    auto [header, _1, qid, _2] = interpreter_.Prepare(query, {}, {});
    stream.Header(header);
    auto summary = interpreter_.PullAll(&stream);
    stream.Summary(summary);

    return stream;
  }

 private:
  memgraph::query::InterpreterContext *context_;
  memgraph::query::Interpreter interpreter_;
};

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, ExecuteDumpDatabaseInMulticommandTransaction) {
  StatefulInterpreter interpreter(&this->context, this->db);

  // Begin the transaction before the vertex is created.
  interpreter.Execute("BEGIN");

  // Verify that nothing is dumped.
  {
    auto stream = interpreter.Execute("DUMP DATABASE");
    const auto &header = stream.GetHeader();
    const auto &results = stream.GetResults();
    ASSERT_EQ(header.size(), 1U);
    ASSERT_EQ(header[0], "QUERY");
    ASSERT_EQ(results.size(), 0U);
  }

  // Create the vertex.
  {
    auto dba = this->db->Access();
    CreateVertex(dba.get(), {}, {}, false);
    ASSERT_FALSE(dba->Commit().HasError());
  }

  // Verify that nothing is dumped.
  {
    auto stream = interpreter.Execute("DUMP DATABASE");
    const auto &header = stream.GetHeader();
    const auto &results = stream.GetResults();
    ASSERT_EQ(header.size(), 1U);
    ASSERT_EQ(header[0], "QUERY");
    ASSERT_EQ(results.size(), 0U);
  }

  // Rollback the transaction.
  interpreter.Execute("ROLLBACK");

  // Start a new transaction, this transaction should see the vertex.
  interpreter.Execute("BEGIN");

  // Verify that the vertex is dumped.
  {
    auto stream = interpreter.Execute("DUMP DATABASE");
    const auto &header = stream.GetHeader();
    const auto &results = stream.GetResults();
    ASSERT_EQ(header.size(), 1U);
    EXPECT_EQ(header[0], "QUERY");
    EXPECT_EQ(results.size(), 4U);
    for (const auto &item : results) {
      EXPECT_EQ(item.size(), 1);
      EXPECT_TRUE(item[0].IsString());
    }
    EXPECT_EQ(results[0][0].ValueString(), "CREATE INDEX ON :__mg_vertex__(__mg_id__);");
    EXPECT_EQ(results[1][0].ValueString(), "CREATE (:__mg_vertex__ {__mg_id__: 0});");
    EXPECT_EQ(results[2][0].ValueString(), "DROP INDEX ON :__mg_vertex__(__mg_id__);");
    EXPECT_EQ(results[3][0].ValueString(), "MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;");
  }

  // Rollback the transaction.
  interpreter.Execute("ROLLBACK");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(DumpTest, MultiplePartialPulls) {
  {
    // Create indices
    {
      auto unique_acc = this->db->UniqueAccess();
      ASSERT_FALSE(
          unique_acc
              ->CreateIndex(this->db->storage()->NameToLabel("PERSON"), this->db->storage()->NameToProperty("name"))
              .HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }
    {
      auto unique_acc = this->db->UniqueAccess();
      ASSERT_FALSE(
          unique_acc
              ->CreateIndex(this->db->storage()->NameToLabel("PERSON"), this->db->storage()->NameToProperty("surname"))
              .HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }

    // Create existence constraints
    {
      auto unique_acc = this->db->UniqueAccess();
      auto res = unique_acc->CreateExistenceConstraint(this->db->storage()->NameToLabel("PERSON"),
                                                       this->db->storage()->NameToProperty("name"));
      ASSERT_FALSE(res.HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }
    {
      auto unique_acc = this->db->UniqueAccess();
      auto res = unique_acc->CreateExistenceConstraint(this->db->storage()->NameToLabel("PERSON"),
                                                       this->db->storage()->NameToProperty("surname"));
      ASSERT_FALSE(res.HasError());
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }

    // Create unique constraints
    {
      auto unique_acc = this->db->UniqueAccess();
      auto res = unique_acc->CreateUniqueConstraint(this->db->storage()->NameToLabel("PERSON"),
                                                    {this->db->storage()->NameToProperty("name")});
      ASSERT_TRUE(res.HasValue());
      ASSERT_EQ(res.GetValue(), memgraph::storage::UniqueConstraints::CreationStatus::SUCCESS);
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }
    {
      auto unique_acc = this->db->UniqueAccess();
      auto res = unique_acc->CreateUniqueConstraint(this->db->storage()->NameToLabel("PERSON"),
                                                    {this->db->storage()->NameToProperty("surname")});
      ASSERT_TRUE(res.HasValue());
      ASSERT_EQ(res.GetValue(), memgraph::storage::UniqueConstraints::CreationStatus::SUCCESS);
      ASSERT_FALSE(unique_acc->Commit().HasError());
    }

    auto dba = this->db->Access();
    auto p1 = CreateVertex(dba.get(), {"PERSON"},
                           {{"name", memgraph::storage::PropertyValue("Person1")},
                            {"surname", memgraph::storage::PropertyValue("Unique1")}},
                           false);
    auto p2 = CreateVertex(dba.get(), {"PERSON"},
                           {{"name", memgraph::storage::PropertyValue("Person2")},
                            {"surname", memgraph::storage::PropertyValue("Unique2")}},
                           false);
    auto p3 = CreateVertex(dba.get(), {"PERSON"},
                           {{"name", memgraph::storage::PropertyValue("Person3")},
                            {"surname", memgraph::storage::PropertyValue("Unique3")}},
                           false);
    auto p4 = CreateVertex(dba.get(), {"PERSON"},
                           {{"name", memgraph::storage::PropertyValue("Person4")},
                            {"surname", memgraph::storage::PropertyValue("Unique4")}},
                           false);
    auto p5 = CreateVertex(dba.get(), {"PERSON"},
                           {{"name", memgraph::storage::PropertyValue("Person5")},
                            {"surname", memgraph::storage::PropertyValue("Unique5")}},
                           false);
    CreateEdge(dba.get(), &p1, &p2, "REL", {}, false);
    CreateEdge(dba.get(), &p1, &p3, "REL", {}, false);
    CreateEdge(dba.get(), &p4, &p5, "REL", {}, false);
    CreateEdge(dba.get(), &p2, &p5, "REL", {}, false);
    ASSERT_FALSE(dba->Commit().HasError());
  }

  ResultStreamFaker stream(this->db->storage());
  memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
  auto acc = this->db->Access();
  memgraph::query::DbAccessor dba(acc.get());

  memgraph::query::PullPlanDump pullPlan{&dba, this->db};

  auto offset_index = 0U;
  auto check_next = [&](const std::string &expected_row) mutable {
    pullPlan.Pull(&query_stream, 1);
    const auto &results{stream.GetResults()};
    ASSERT_EQ(results.size(), offset_index + 1);
    VerifyQueries({results.begin() + offset_index, results.end()}, expected_row);
    ++offset_index;
  };

  check_next("CREATE INDEX ON :`PERSON`(`name`);");
  check_next("CREATE INDEX ON :`PERSON`(`surname`);");
  check_next("CREATE CONSTRAINT ON (u:`PERSON`) ASSERT EXISTS (u.`name`);");
  check_next("CREATE CONSTRAINT ON (u:`PERSON`) ASSERT EXISTS (u.`surname`);");
  check_next("CREATE CONSTRAINT ON (u:`PERSON`) ASSERT u.`name` IS UNIQUE;");
  check_next("CREATE CONSTRAINT ON (u:`PERSON`) ASSERT u.`surname` IS UNIQUE;");
  check_next(kCreateInternalIndex);
  check_next(R"r(CREATE (:__mg_vertex__:`PERSON` {__mg_id__: 0, `name`: "Person1", `surname`: "Unique1"});)r");
  check_next(R"r(CREATE (:__mg_vertex__:`PERSON` {__mg_id__: 1, `name`: "Person2", `surname`: "Unique2"});)r");
  check_next(R"r(CREATE (:__mg_vertex__:`PERSON` {__mg_id__: 2, `name`: "Person3", `surname`: "Unique3"});)r");
  check_next(R"r(CREATE (:__mg_vertex__:`PERSON` {__mg_id__: 3, `name`: "Person4", `surname`: "Unique4"});)r");
  check_next(R"r(CREATE (:__mg_vertex__:`PERSON` {__mg_id__: 4, `name`: "Person5", `surname`: "Unique5"});)r");

  pullPlan.Pull(&query_stream, 4);
  const auto edge_results = stream.GetResults();
  /// NOTE: For disk storage, the order of returned edges isn't guaranteed so we check them together and we guarantee
  /// the order by sorting.
  VerifyQueries(
      {edge_results.end() - 4, edge_results.end()},
      "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND v.__mg_id__ = 1 CREATE (u)-[:`REL`]->(v);",
      "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND v.__mg_id__ = 2 CREATE (u)-[:`REL`]->(v);",
      "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND v.__mg_id__ = 4 CREATE (u)-[:`REL`]->(v);",
      "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 3 AND v.__mg_id__ = 4 CREATE (u)-[:`REL`]->(v);");
  offset_index += 4;

  check_next(kDropInternalIndex);
  check_next(kRemoveInternalLabelProperty);
}

TYPED_TEST(DumpTest, DumpDatabaseWithTriggers) {
  auto acc = this->db->storage()->Access(memgraph::replication_coordination_glue::ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(acc.get());
  {
    auto trigger_store = this->db.get()->trigger_store();
    const std::string trigger_name = "test_trigger";
    const std::string trigger_statement = "UNWIND createdVertices AS newNodes SET newNodes.created = timestamp()";
    memgraph::query::TriggerEventType trigger_event_type = memgraph::query::TriggerEventType::VERTEX_CREATE;
    memgraph::query::TriggerPhase trigger_phase = memgraph::query::TriggerPhase::AFTER_COMMIT;
    memgraph::utils::SkipList<memgraph::query::QueryCacheEntry> ast_cache;
    memgraph::query::AllowEverythingAuthChecker auth_checker;
    memgraph::query::InterpreterConfig::Query query_config;
    memgraph::query::DbAccessor dba(acc.get());
    const std::map<std::string, memgraph::storage::PropertyValue> props;
    trigger_store->AddTrigger(trigger_name, trigger_statement, props, trigger_event_type, trigger_phase, &ast_cache,
                              &dba, query_config, std::nullopt, &auth_checker);
  }
  {
    ResultStreamFaker stream(this->db->storage());
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    { memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream, this->db); }
    VerifyQueries(stream.GetResults(),
                  "CREATE TRIGGER test_trigger ON () CREATE AFTER COMMIT EXECUTE UNWIND createdVertices AS newNodes "
                  "SET newNodes.created = timestamp();");
  }
}

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

#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <set>
#include <vector>

#include "communication/result_stream_faker.hpp"
#include "query/config.hpp"
#include "query/dump.hpp"
#include "query/interpreter.hpp"
#include "query/typed_value.hpp"
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
    std::set<std::string> labels;
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

  struct LabelPropertiesItem {
    std::string label;
    std::set<std::string> properties;
  };

  std::set<Vertex> vertices;
  std::set<Edge> edges;
  std::set<LabelItem> label_indices;
  std::set<LabelPropertyItem> label_property_indices;
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
  auto dba = db->Access();
  for (const auto &vertex : dba.Vertices(memgraph::storage::View::NEW)) {
    std::set<std::string> labels;
    auto maybe_labels = vertex.Labels(memgraph::storage::View::NEW);
    MG_ASSERT(maybe_labels.HasValue());
    for (const auto &label : *maybe_labels) {
      labels.insert(dba.LabelToName(label));
    }
    std::map<std::string, memgraph::storage::PropertyValue> props;
    auto maybe_properties = vertex.Properties(memgraph::storage::View::NEW);
    MG_ASSERT(maybe_properties.HasValue());
    for (const auto &kv : *maybe_properties) {
      props.emplace(dba.PropertyToName(kv.first), kv.second);
    }
    MG_ASSERT(props.count(kPropertyId) == 1);
    const auto id = props[kPropertyId].ValueInt();
    gid_mapping[vertex.Gid()] = id;
    vertices.insert({id, labels, props});
  }

  // Capture all edges
  std::set<DatabaseState::Edge> edges;
  for (const auto &vertex : dba.Vertices(memgraph::storage::View::NEW)) {
    auto maybe_edges = vertex.OutEdges(memgraph::storage::View::NEW);
    MG_ASSERT(maybe_edges.HasValue());
    for (const auto &edge : *maybe_edges) {
      const auto &edge_type_name = dba.EdgeTypeToName(edge.EdgeType());
      std::map<std::string, memgraph::storage::PropertyValue> props;
      auto maybe_properties = edge.Properties(memgraph::storage::View::NEW);
      MG_ASSERT(maybe_properties.HasValue());
      for (const auto &kv : *maybe_properties) {
        props.emplace(dba.PropertyToName(kv.first), kv.second);
      }
      const auto from = gid_mapping[edge.FromVertex().Gid()];
      const auto to = gid_mapping[edge.ToVertex().Gid()];
      edges.insert({from, to, edge_type_name, props});
    }
  }

  // Capture all indices
  std::set<DatabaseState::LabelItem> label_indices;
  std::set<DatabaseState::LabelPropertyItem> label_property_indices;
  {
    auto info = dba.ListAllIndices();
    for (const auto &item : info.label) {
      label_indices.insert({dba.LabelToName(item)});
    }
    for (const auto &item : info.label_property) {
      label_property_indices.insert({dba.LabelToName(item.first), dba.PropertyToName(item.second)});
    }
  }

  // Capture all constraints
  std::set<DatabaseState::LabelPropertyItem> existence_constraints;
  std::set<DatabaseState::LabelPropertiesItem> unique_constraints;
  {
    auto info = dba.ListAllConstraints();
    for (const auto &item : info.existence) {
      existence_constraints.insert({dba.LabelToName(item.first), dba.PropertyToName(item.second)});
    }
    for (const auto &item : info.unique) {
      std::set<std::string> properties;
      for (const auto &property : item.second) {
        properties.insert(dba.PropertyToName(property));
      }
      unique_constraints.insert({dba.LabelToName(item.first), std::move(properties)});
    }
  }

  return {vertices, edges, label_indices, label_property_indices, existence_constraints, unique_constraints};
}

auto Execute(memgraph::storage::Storage *db, const std::string &query) {
  auto data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_query_dump";
  memgraph::query::InterpreterContext context(db, memgraph::query::InterpreterConfig{}, data_directory);
  memgraph::query::Interpreter interpreter(&context);
  ResultStreamFaker stream(db);

  auto [header, _, qid] = interpreter.Prepare(query, {}, nullptr);
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
  for (const auto &kv : props) {
    MG_ASSERT(edge->SetProperty(dba->NameToProperty(kv.first), kv.second).HasValue());
  }
  if (add_property_id) {
    MG_ASSERT(edge->SetProperty(dba->NameToProperty(kPropertyId), memgraph::storage::PropertyValue(edge->Gid().AsInt()))
                  .HasValue());
  }
  return *edge;
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
  ASSERT_EQ(got, expected);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, EmptyGraph) {
  memgraph::storage::Storage db;
  ResultStreamFaker stream(&db);
  memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
  {
    auto acc = db.Access();
    memgraph::query::DbAccessor dba(&acc);
    memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream);
  }
  ASSERT_EQ(stream.GetResults().size(), 0);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, SingleVertex) {
  memgraph::storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {}, {}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = db.Access();
      memgraph::query::DbAccessor dba(&acc);
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, VertexWithSingleLabel) {
  memgraph::storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {"Label1"}, {}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = db.Access();
      memgraph::query::DbAccessor dba(&acc);
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__:`Label1` {__mg_id__: 0});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, VertexWithMultipleLabels) {
  memgraph::storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {"Label1", "Label 2"}, {}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = db.Access();
      memgraph::query::DbAccessor dba(&acc);
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex,
                  "CREATE (:__mg_vertex__:`Label1`:`Label 2` {__mg_id__: 0});", kDropInternalIndex,
                  kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, VertexWithSingleProperty) {
  memgraph::storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {}, {{"prop", memgraph::storage::PropertyValue(42)}}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = db.Access();
      memgraph::query::DbAccessor dba(&acc);
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0, `prop`: 42});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, MultipleVertices) {
  memgraph::storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {}, {}, false);
    CreateVertex(&dba, {}, {}, false);
    CreateVertex(&dba, {}, {}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = db.Access();
      memgraph::query::DbAccessor dba(&acc);
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0});",
                  "CREATE (:__mg_vertex__ {__mg_id__: 1});", "CREATE (:__mg_vertex__ {__mg_id__: 2});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

TEST(DumpTest, PropertyValue) {
  memgraph::storage::Storage db;
  {
    auto dba = db.Access();
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
    CreateVertex(&dba, {}, {{"p1", list_value}, {"p2", str_value}}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = db.Access();
      memgraph::query::DbAccessor dba(&acc);
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream);
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
TEST(DumpTest, SingleEdge) {
  memgraph::storage::Storage db;
  {
    auto dba = db.Access();
    auto u = CreateVertex(&dba, {}, {}, false);
    auto v = CreateVertex(&dba, {}, {}, false);
    CreateEdge(&dba, &u, &v, "EdgeType", {}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = db.Access();
      memgraph::query::DbAccessor dba(&acc);
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0});",
                  "CREATE (:__mg_vertex__ {__mg_id__: 1});",
                  "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND "
                  "v.__mg_id__ = 1 CREATE (u)-[:`EdgeType`]->(v);",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, MultipleEdges) {
  memgraph::storage::Storage db;
  {
    auto dba = db.Access();
    auto u = CreateVertex(&dba, {}, {}, false);
    auto v = CreateVertex(&dba, {}, {}, false);
    auto w = CreateVertex(&dba, {}, {}, false);
    CreateEdge(&dba, &u, &v, "EdgeType", {}, false);
    CreateEdge(&dba, &v, &u, "EdgeType 2", {}, false);
    CreateEdge(&dba, &v, &w, "EdgeType `!\"", {}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = db.Access();
      memgraph::query::DbAccessor dba(&acc);
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream);
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
TEST(DumpTest, EdgeWithProperties) {
  memgraph::storage::Storage db;
  {
    auto dba = db.Access();
    auto u = CreateVertex(&dba, {}, {}, false);
    auto v = CreateVertex(&dba, {}, {}, false);
    CreateEdge(&dba, &u, &v, "EdgeType", {{"prop", memgraph::storage::PropertyValue(13)}}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = db.Access();
      memgraph::query::DbAccessor dba(&acc);
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex, "CREATE (:__mg_vertex__ {__mg_id__: 0});",
                  "CREATE (:__mg_vertex__ {__mg_id__: 1});",
                  "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND "
                  "v.__mg_id__ = 1 CREATE (u)-[:`EdgeType` {`prop`: 13}]->(v);",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, IndicesKeys) {
  memgraph::storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {"Label1", "Label 2"}, {{"p", memgraph::storage::PropertyValue(1)}}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }
  ASSERT_TRUE(db.CreateIndex(db.NameToLabel("Label1"), db.NameToProperty("prop")));
  ASSERT_TRUE(db.CreateIndex(db.NameToLabel("Label 2"), db.NameToProperty("prop `")));

  {
    ResultStreamFaker stream(&db);
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = db.Access();
      memgraph::query::DbAccessor dba(&acc);
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(), "CREATE INDEX ON :`Label1`(`prop`);", "CREATE INDEX ON :`Label 2`(`prop ```);",
                  kCreateInternalIndex, "CREATE (:__mg_vertex__:`Label1`:`Label 2` {__mg_id__: 0, `p`: 1});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, ExistenceConstraints) {
  memgraph::storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {"L`abel 1"}, {{"prop", memgraph::storage::PropertyValue(1)}}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }
  {
    auto res = db.CreateExistenceConstraint(db.NameToLabel("L`abel 1"), db.NameToProperty("prop"));
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());
  }

  {
    ResultStreamFaker stream(&db);
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = db.Access();
      memgraph::query::DbAccessor dba(&acc);
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(), "CREATE CONSTRAINT ON (u:`L``abel 1`) ASSERT EXISTS (u.`prop`);",
                  kCreateInternalIndex, "CREATE (:__mg_vertex__:`L``abel 1` {__mg_id__: 0, `prop`: 1});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

TEST(DumpTest, UniqueConstraints) {
  memgraph::storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {"Label"},
                 {{"prop", memgraph::storage::PropertyValue(1)}, {"prop2", memgraph::storage::PropertyValue(2)}},
                 false);
    CreateVertex(&dba, {"Label"},
                 {{"prop", memgraph::storage::PropertyValue(2)}, {"prop2", memgraph::storage::PropertyValue(2)}},
                 false);
    ASSERT_FALSE(dba.Commit().HasError());
  }
  {
    auto res =
        db.CreateUniqueConstraint(db.NameToLabel("Label"), {db.NameToProperty("prop"), db.NameToProperty("prop2")});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), memgraph::storage::UniqueConstraints::CreationStatus::SUCCESS);
  }

  {
    ResultStreamFaker stream(&db);
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = db.Access();
      memgraph::query::DbAccessor dba(&acc);
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream);
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
TEST(DumpTest, CheckStateVertexWithMultipleProperties) {
  memgraph::storage::Storage db;
  {
    auto dba = db.Access();
    std::map<std::string, memgraph::storage::PropertyValue> prop1 = {
        {"nested1", memgraph::storage::PropertyValue(1337)}, {"nested2", memgraph::storage::PropertyValue(3.14)}};
    CreateVertex(
        &dba, {"Label1", "Label2"},
        {{"prop1", memgraph::storage::PropertyValue(prop1)}, {"prop2", memgraph::storage::PropertyValue("$'\t'")}});
    ASSERT_FALSE(dba.Commit().HasError());
  }

  memgraph::storage::Storage db_dump;
  {
    ResultStreamFaker stream(&db);
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = db.Access();
      memgraph::query::DbAccessor dba(&acc);
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    const auto &results = stream.GetResults();
    ASSERT_GE(results.size(), 1);
    for (const auto &item : results) {
      ASSERT_EQ(item.size(), 1);
      ASSERT_TRUE(item[0].IsString());
      Execute(&db_dump, item[0].ValueString());
    }
  }
  ASSERT_EQ(GetState(&db), GetState(&db_dump));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, CheckStateSimpleGraph) {
  memgraph::storage::Storage db;
  {
    auto dba = db.Access();
    auto u = CreateVertex(&dba, {"Person"}, {{"name", memgraph::storage::PropertyValue("Ivan")}});
    auto v = CreateVertex(&dba, {"Person"}, {{"name", memgraph::storage::PropertyValue("Josko")}});
    auto w = CreateVertex(
        &dba, {"Person"},
        {{"name", memgraph::storage::PropertyValue("Bosko")}, {"id", memgraph::storage::PropertyValue(0)}});
    auto z =
        CreateVertex(&dba, {"Person"},
                     {{"name", memgraph::storage::PropertyValue("Buha")}, {"id", memgraph::storage::PropertyValue(1)}});
    CreateEdge(&dba, &u, &v, "Knows", {});
    CreateEdge(&dba, &v, &w, "Knows", {{"how_long", memgraph::storage::PropertyValue(5)}});
    CreateEdge(&dba, &w, &u, "Knows", {{"how", memgraph::storage::PropertyValue("distant past")}});
    CreateEdge(&dba, &v, &u, "Knows", {});
    CreateEdge(&dba, &v, &u, "Likes", {});
    CreateEdge(&dba, &z, &u, "Knows", {});
    CreateEdge(&dba, &w, &z, "Knows", {{"how", memgraph::storage::PropertyValue("school")}});
    CreateEdge(&dba, &w, &z, "Likes", {{"how", memgraph::storage::PropertyValue("very much")}});
    CreateEdge(&dba, &w, &z, "Date",
               {{"time", memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
                             memgraph::storage::TemporalType::Date,
                             memgraph::utils::Date({1994, 12, 7}).MicrosecondsSinceEpoch()))}});
    CreateEdge(&dba, &w, &z, "LocalTime",
               {{"time", memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
                             memgraph::storage::TemporalType::LocalTime,
                             memgraph::utils::LocalTime({14, 10, 44, 99, 99}).MicrosecondsSinceEpoch()))}});
    CreateEdge(
        &dba, &w, &z, "LocalDateTime",
        {{"time", memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
                      memgraph::storage::TemporalType::LocalDateTime,
                      memgraph::utils::LocalDateTime({1994, 12, 7}, {14, 10, 44, 99, 99}).MicrosecondsSinceEpoch()))}});
    CreateEdge(&dba, &w, &z, "Duration",
               {{"time", memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
                             memgraph::storage::TemporalType::Duration,
                             memgraph::utils::Duration({3, 4, 5, 6, 10, 11}).microseconds))}});
    CreateEdge(&dba, &w, &z, "NegativeDuration",
               {{"time", memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
                             memgraph::storage::TemporalType::Duration,
                             memgraph::utils::Duration({-3, -4, -5, -6, -10, -11}).microseconds))}});
    ASSERT_FALSE(dba.Commit().HasError());
  }
  {
    auto ret = db.CreateExistenceConstraint(db.NameToLabel("Person"), db.NameToProperty("name"));
    ASSERT_TRUE(ret.HasValue());
    ASSERT_TRUE(ret.GetValue());
  }
  {
    auto ret = db.CreateUniqueConstraint(db.NameToLabel("Person"), {db.NameToProperty("name")});
    ASSERT_TRUE(ret.HasValue());
    ASSERT_EQ(ret.GetValue(), memgraph::storage::UniqueConstraints::CreationStatus::SUCCESS);
  }
  ASSERT_TRUE(db.CreateIndex(db.NameToLabel("Person"), db.NameToProperty("id")));
  ASSERT_TRUE(db.CreateIndex(db.NameToLabel("Person"), db.NameToProperty("unexisting_property")));

  const auto &db_initial_state = GetState(&db);
  memgraph::storage::Storage db_dump;
  {
    ResultStreamFaker stream(&db);
    memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
    {
      auto acc = db.Access();
      memgraph::query::DbAccessor dba(&acc);
      memgraph::query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    const auto &results = stream.GetResults();
    // Indices and constraints are 4 queries and there must be at least one more
    // query for the data.
    ASSERT_GE(results.size(), 5);
    for (const auto &item : results) {
      ASSERT_EQ(item.size(), 1);
      ASSERT_TRUE(item[0].IsString());
      Execute(&db_dump, item[0].ValueString());
    }
  }
  ASSERT_EQ(GetState(&db), GetState(&db_dump));
  // Make sure that dump function doesn't make changes on the database.
  ASSERT_EQ(GetState(&db), db_initial_state);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, ExecuteDumpDatabase) {
  memgraph::storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {}, {}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    auto stream = Execute(&db, "DUMP DATABASE");
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
  explicit StatefulInterpreter(memgraph::storage::Storage *db)
      : db_(db), context_(db_, memgraph::query::InterpreterConfig{}, data_directory_), interpreter_(&context_) {}

  auto Execute(const std::string &query) {
    ResultStreamFaker stream(db_);

    auto [header, _, qid] = interpreter_.Prepare(query, {}, nullptr);
    stream.Header(header);
    auto summary = interpreter_.PullAll(&stream);
    stream.Summary(summary);

    return stream;
  }

 private:
  static const std::filesystem::path data_directory_;

  memgraph::storage::Storage *db_;
  memgraph::query::InterpreterContext context_;
  memgraph::query::Interpreter interpreter_;
};

const std::filesystem::path StatefulInterpreter::data_directory_{std::filesystem::temp_directory_path() /
                                                                 "MG_tests_unit_query_dump_stateful"};

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, ExecuteDumpDatabaseInMulticommandTransaction) {
  memgraph::storage::Storage db;
  StatefulInterpreter interpreter(&db);

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
    auto dba = db.Access();
    CreateVertex(&dba, {}, {}, false);
    ASSERT_FALSE(dba.Commit().HasError());
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
TEST(DumpTest, MultiplePartialPulls) {
  memgraph::storage::Storage db;
  {
    // Create indices
    db.CreateIndex(db.NameToLabel("PERSON"), db.NameToProperty("name"));
    db.CreateIndex(db.NameToLabel("PERSON"), db.NameToProperty("surname"));

    // Create existence constraints
    {
      auto res = db.CreateExistenceConstraint(db.NameToLabel("PERSON"), db.NameToProperty("name"));
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }
    {
      auto res = db.CreateExistenceConstraint(db.NameToLabel("PERSON"), db.NameToProperty("surname"));
      ASSERT_TRUE(res.HasValue());
      ASSERT_TRUE(res.GetValue());
    }

    // Create unique constraints
    {
      auto res = db.CreateUniqueConstraint(db.NameToLabel("PERSON"), {db.NameToProperty("name")});
      ASSERT_TRUE(res.HasValue());
      ASSERT_EQ(res.GetValue(), memgraph::storage::UniqueConstraints::CreationStatus::SUCCESS);
    }
    {
      auto res = db.CreateUniqueConstraint(db.NameToLabel("PERSON"), {db.NameToProperty("surname")});
      ASSERT_TRUE(res.HasValue());
      ASSERT_EQ(res.GetValue(), memgraph::storage::UniqueConstraints::CreationStatus::SUCCESS);
    }

    auto dba = db.Access();
    auto p1 = CreateVertex(&dba, {"PERSON"},
                           {{"name", memgraph::storage::PropertyValue("Person1")},
                            {"surname", memgraph::storage::PropertyValue("Unique1")}},
                           false);
    auto p2 = CreateVertex(&dba, {"PERSON"},
                           {{"name", memgraph::storage::PropertyValue("Person2")},
                            {"surname", memgraph::storage::PropertyValue("Unique2")}},
                           false);
    auto p3 = CreateVertex(&dba, {"PERSON"},
                           {{"name", memgraph::storage::PropertyValue("Person3")},
                            {"surname", memgraph::storage::PropertyValue("Unique3")}},
                           false);
    auto p4 = CreateVertex(&dba, {"PERSON"},
                           {{"name", memgraph::storage::PropertyValue("Person4")},
                            {"surname", memgraph::storage::PropertyValue("Unique4")}},
                           false);
    auto p5 = CreateVertex(&dba, {"PERSON"},
                           {{"name", memgraph::storage::PropertyValue("Person5")},
                            {"surname", memgraph::storage::PropertyValue("Unique5")}},
                           false);
    CreateEdge(&dba, &p1, &p2, "REL", {}, false);
    CreateEdge(&dba, &p1, &p3, "REL", {}, false);
    CreateEdge(&dba, &p4, &p5, "REL", {}, false);
    CreateEdge(&dba, &p2, &p5, "REL", {}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  ResultStreamFaker stream(&db);
  memgraph::query::AnyStream query_stream(&stream, memgraph::utils::NewDeleteResource());
  auto acc = db.Access();
  memgraph::query::DbAccessor dba(&acc);

  memgraph::query::PullPlanDump pullPlan{&dba};

  auto check_next = [&, offset_index = 0U](const std::string &expected_row) mutable {
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
  check_next(
      "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND "
      "v.__mg_id__ = 1 CREATE (u)-[:`REL`]->(v);");
  check_next(
      "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND "
      "v.__mg_id__ = 2 CREATE (u)-[:`REL`]->(v);");
  check_next(
      "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND "
      "v.__mg_id__ = 4 CREATE (u)-[:`REL`]->(v);");
  check_next(
      "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 3 AND "
      "v.__mg_id__ = 4 CREATE (u)-[:`REL`]->(v);");
  check_next(kDropInternalIndex);
  check_next(kRemoveInternalLabelProperty);
}

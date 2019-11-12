#include <gtest/gtest.h>

#include <map>
#include <set>
#include <vector>

#include <glog/logging.h>

#include "communication/result_stream_faker.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "query/dump.hpp"
#include "query/interpreter.hpp"
#include "query/typed_value.hpp"
#include "storage/common/types/property_value.hpp"

using database::GraphDbAccessor;

const char *kPropertyId = "property_id";

const char *kCreateInternalIndex = "CREATE INDEX ON :__mg_vertex__(__mg_id__);";
const char *kDropInternalIndex = "DROP INDEX ON :__mg_vertex__(__mg_id__);";
const char *kRemoveInternalLabelProperty =
    "MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;";

// A helper struct that contains info about database that is used to compare
// two databases (to check if their states are the same). It is assumed that
// each vertex and each edge have unique integer property under key
// `kPropertyId`.
struct DatabaseState {
  struct Vertex {
    int64_t id;
    std::set<std::string> labels;
    std::map<std::string, PropertyValue> props;
  };

  struct Edge {
    int64_t from, to;
    std::string edge_type;
    std::map<std::string, PropertyValue> props;
  };

  struct IndexKey {
    std::string label;
    std::string property;
  };

  struct UniqueConstraint {
    std::string label;
    std::set<std::string> props;
  };

  std::set<Vertex> vertices;
  std::set<Edge> edges;
  std::set<IndexKey> indices;
  std::set<UniqueConstraint> constraints;
};

bool operator<(const DatabaseState::Vertex &first,
               const DatabaseState::Vertex &second) {
  if (first.id != second.id) return first.id < second.id;
  if (first.labels != second.labels) return first.labels < second.labels;
  return first.props < second.props;
}

bool operator<(const DatabaseState::Edge &first,
               const DatabaseState::Edge &second) {
  if (first.from != second.from) return first.from < second.from;
  if (first.to != second.to) return first.to < second.to;
  if (first.edge_type != second.edge_type)
    return first.edge_type < second.edge_type;
  return first.props < second.props;
}

bool operator<(const DatabaseState::IndexKey &first,
               const DatabaseState::IndexKey &second) {
  if (first.label != second.label) return first.label < second.label;
  return first.property < second.property;
}

bool operator<(const DatabaseState::UniqueConstraint &first,
               const DatabaseState::UniqueConstraint &second) {
  if (first.label != second.label) return first.label < second.label;
  return first.props < second.props;
}

bool operator==(const DatabaseState::Vertex &first,
                const DatabaseState::Vertex &second) {
  return first.id == second.id && first.labels == second.labels &&
         first.props == second.props;
}

bool operator==(const DatabaseState::Edge &first,
                const DatabaseState::Edge &second) {
  return first.from == second.from && first.to == second.to &&
         first.edge_type == second.edge_type && first.props == second.props;
}

bool operator==(const DatabaseState::IndexKey &first,
                const DatabaseState::IndexKey &second) {
  return first.label == second.label && first.property == second.property;
}

bool operator==(const DatabaseState::UniqueConstraint &first,
                const DatabaseState::UniqueConstraint &second) {
  return first.label == second.label && first.props == second.props;
}

bool operator==(const DatabaseState &first, const DatabaseState &second) {
  return first.vertices == second.vertices && first.edges == second.edges &&
         first.indices == second.indices;
}

DatabaseState GetState(database::GraphDb *db) {
  // Capture all vertices
  std::map<storage::Gid, int64_t> gid_mapping;
  std::set<DatabaseState::Vertex> vertices;
  auto dba = db->Access();
  for (const auto &vertex : dba.Vertices(false)) {
    std::set<std::string> labels;
    for (const auto &label : vertex.labels()) {
      labels.insert(dba.LabelName(label));
    }
    std::map<std::string, PropertyValue> props;
    for (const auto &kv : vertex.Properties()) {
      props.emplace(dba.PropertyName(kv.first), kv.second);
    }
    CHECK(props.count(kPropertyId) == 1);
    const auto id = props[kPropertyId].ValueInt();
    gid_mapping[vertex.gid()] = id;
    vertices.insert({id, labels, props});
  }

  // Capture all edges
  std::set<DatabaseState::Edge> edges;
  for (const auto &edge : dba.Edges(false)) {
    const auto &edge_type_name = dba.EdgeTypeName(edge.EdgeType());
    std::map<std::string, PropertyValue> props;
    for (const auto &kv : edge.Properties()) {
      props.emplace(dba.PropertyName(kv.first), kv.second);
    }
    const auto from = gid_mapping[edge.from().gid()];
    const auto to = gid_mapping[edge.to().gid()];
    edges.insert({from, to, edge_type_name, props});
  }

  // Capture all indices
  std::set<DatabaseState::IndexKey> indices;
  for (const auto &key : dba.GetIndicesKeys()) {
    indices.insert(
        {dba.LabelName(key.label_), dba.PropertyName(key.property_)});
  }

  // Capture all unique constraints
  std::set<DatabaseState::UniqueConstraint> constraints;
  for (const auto &constraint : dba.ListUniqueConstraints()) {
    std::set<std::string> props;
    for (const auto &prop : constraint.properties) {
      props.insert(dba.PropertyName(prop));
    }
    constraints.insert({dba.LabelName(constraint.label), props});
  }

  return {vertices, edges, indices, constraints};
}

auto Execute(database::GraphDb *db, const std::string &query) {
  query::InterpreterContext context(db);
  query::Interpreter interpreter(&context);
  ResultStreamFaker stream;

  auto [header, _] = interpreter.Prepare(query, {});
  stream.Header(header);
  auto summary = interpreter.PullAll(&stream);
  stream.Summary(summary);

  return stream;
}

VertexAccessor CreateVertex(GraphDbAccessor *dba,
                            const std::vector<std::string> &labels,
                            const std::map<std::string, PropertyValue> &props,
                            bool add_property_id = true) {
  CHECK(dba);
  auto vertex = dba->InsertVertex();
  for (const auto &label_name : labels) {
    vertex.add_label(dba->Label(label_name));
  }
  for (const auto &kv : props) {
    vertex.PropsSet(dba->Property(kv.first), kv.second);
  }
  if (add_property_id) {
    vertex.PropsSet(dba->Property(kPropertyId),
                    PropertyValue(vertex.gid().AsInt()));
  }
  return vertex;
}

EdgeAccessor CreateEdge(GraphDbAccessor *dba, VertexAccessor from,
                        VertexAccessor to, const std::string &edge_type_name,
                        const std::map<std::string, PropertyValue> &props,
                        bool add_property_id = true) {
  CHECK(dba);
  auto edge = dba->InsertEdge(from, to, dba->EdgeType(edge_type_name));
  for (const auto &kv : props) {
    edge.PropsSet(dba->Property(kv.first), kv.second);
  }
  if (add_property_id) {
    edge.PropsSet(dba->Property(kPropertyId),
                  PropertyValue(edge.gid().AsInt()));
  }
  return edge;
}

template <class... TArgs>
void VerifyQueries(
    const std::vector<std::vector<communication::bolt::Value>> &results,
    TArgs &&... args) {
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
  database::GraphDb db;
  ResultStreamFaker stream;
  query::AnyStream query_stream(&stream, utils::NewDeleteResource());
  {
    auto acc = db.Access();
    query::DbAccessor dba(&acc);
    query::DumpDatabaseToCypherQueries(&dba, &query_stream);
  }
  ASSERT_EQ(stream.GetResults().size(), 0);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, SingleVertex) {
  database::GraphDb db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {}, {}, false);
    dba.Commit();
  }

  {
    ResultStreamFaker stream;
    query::AnyStream query_stream(&stream, utils::NewDeleteResource());
    {
      auto acc = db.Access();
      query::DbAccessor dba(&acc);
      query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex,
                  "CREATE (:__mg_vertex__ {__mg_id__: 0});", kDropInternalIndex,
                  kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, VertexWithSingleLabel) {
  database::GraphDb db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {"Label1"}, {}, false);
    dba.Commit();
  }

  {
    ResultStreamFaker stream;
    query::AnyStream query_stream(&stream, utils::NewDeleteResource());
    {
      auto acc = db.Access();
      query::DbAccessor dba(&acc);
      query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex,
                  "CREATE (:__mg_vertex__:Label1 {__mg_id__: 0});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, VertexWithMultipleLabels) {
  database::GraphDb db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {"Label1", "Label2"}, {}, false);
    dba.Commit();
  }

  {
    ResultStreamFaker stream;
    query::AnyStream query_stream(&stream, utils::NewDeleteResource());
    {
      auto acc = db.Access();
      query::DbAccessor dba(&acc);
      query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex,
                  "CREATE (:__mg_vertex__:Label1:Label2 {__mg_id__: 0});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, VertexWithSingleProperty) {
  database::GraphDb db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {}, {{"prop", PropertyValue(42)}}, false);
    dba.Commit();
  }

  {
    ResultStreamFaker stream;
    query::AnyStream query_stream(&stream, utils::NewDeleteResource());
    {
      auto acc = db.Access();
      query::DbAccessor dba(&acc);
      query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex,
                  "CREATE (:__mg_vertex__ {__mg_id__: 0, prop: 42});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, MultipleVertices) {
  database::GraphDb db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {}, {}, false);
    CreateVertex(&dba, {}, {}, false);
    CreateVertex(&dba, {}, {}, false);
    dba.Commit();
  }

  {
    ResultStreamFaker stream;
    query::AnyStream query_stream(&stream, utils::NewDeleteResource());
    {
      auto acc = db.Access();
      query::DbAccessor dba(&acc);
      query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(), kCreateInternalIndex,
                  "CREATE (:__mg_vertex__ {__mg_id__: 0});",
                  "CREATE (:__mg_vertex__ {__mg_id__: 1});",
                  "CREATE (:__mg_vertex__ {__mg_id__: 2});", kDropInternalIndex,
                  kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, SingleEdge) {
  database::GraphDb db;
  {
    auto dba = db.Access();
    auto u = CreateVertex(&dba, {}, {}, false);
    auto v = CreateVertex(&dba, {}, {}, false);
    CreateEdge(&dba, u, v, "EdgeType", {}, false);
    dba.Commit();
  }

  {
    ResultStreamFaker stream;
    query::AnyStream query_stream(&stream, utils::NewDeleteResource());
    {
      auto acc = db.Access();
      query::DbAccessor dba(&acc);
      query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(
        stream.GetResults(), kCreateInternalIndex,
        "CREATE (:__mg_vertex__ {__mg_id__: 0});",
        "CREATE (:__mg_vertex__ {__mg_id__: 1});",
        "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND "
        "v.__mg_id__ = 1 CREATE (u)-[:EdgeType]->(v);",
        kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, MultipleEdges) {
  database::GraphDb db;
  {
    auto dba = db.Access();
    auto u = CreateVertex(&dba, {}, {}, false);
    auto v = CreateVertex(&dba, {}, {}, false);
    auto w = CreateVertex(&dba, {}, {}, false);
    CreateEdge(&dba, u, v, "EdgeType", {}, false);
    CreateEdge(&dba, v, u, "EdgeType", {}, false);
    CreateEdge(&dba, v, w, "EdgeType", {}, false);
    dba.Commit();
  }

  {
    ResultStreamFaker stream;
    query::AnyStream query_stream(&stream, utils::NewDeleteResource());
    {
      auto acc = db.Access();
      query::DbAccessor dba(&acc);
      query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(
        stream.GetResults(), kCreateInternalIndex,
        "CREATE (:__mg_vertex__ {__mg_id__: 0});",
        "CREATE (:__mg_vertex__ {__mg_id__: 1});",
        "CREATE (:__mg_vertex__ {__mg_id__: 2});",
        "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND "
        "v.__mg_id__ = 1 CREATE (u)-[:EdgeType]->(v);",
        "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND "
        "v.__mg_id__ = 0 CREATE (u)-[:EdgeType]->(v);",
        "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 1 AND "
        "v.__mg_id__ = 2 CREATE (u)-[:EdgeType]->(v);",
        kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, EdgeWithProperties) {
  database::GraphDb db;
  {
    auto dba = db.Access();
    auto u = CreateVertex(&dba, {}, {}, false);
    auto v = CreateVertex(&dba, {}, {}, false);
    CreateEdge(&dba, u, v, "EdgeType", {{"prop", PropertyValue(13)}}, false);
    dba.Commit();
  }

  {
    ResultStreamFaker stream;
    query::AnyStream query_stream(&stream, utils::NewDeleteResource());
    {
      auto acc = db.Access();
      query::DbAccessor dba(&acc);
      query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(
        stream.GetResults(), kCreateInternalIndex,
        "CREATE (:__mg_vertex__ {__mg_id__: 0});",
        "CREATE (:__mg_vertex__ {__mg_id__: 1});",
        "MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 0 AND "
        "v.__mg_id__ = 1 CREATE (u)-[:EdgeType {prop: 13}]->(v);",
        kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, IndicesKeys) {
  database::GraphDb db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {"Label1", "Label2"}, {{"p", PropertyValue(1)}}, false);
    dba.BuildIndex(dba.Label("Label1"), dba.Property("prop"));
    dba.BuildIndex(dba.Label("Label2"), dba.Property("prop"));
    dba.Commit();
  }

  {
    ResultStreamFaker stream;
    query::AnyStream query_stream(&stream, utils::NewDeleteResource());
    {
      auto acc = db.Access();
      query::DbAccessor dba(&acc);
      query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(), "CREATE INDEX ON :Label1(prop);",
                  "CREATE INDEX ON :Label2(prop);", kCreateInternalIndex,
                  "CREATE (:__mg_vertex__:Label1:Label2 {__mg_id__: 0, p: 1});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, UniqueConstraints) {
  database::GraphDb db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {"Label"}, {{"prop", PropertyValue(1)}}, false);
    dba.BuildUniqueConstraint(dba.Label("Label"), {dba.Property("prop")});
    // Create one with multiple properties.
    dba.BuildUniqueConstraint(dba.Label("Label"),
                              {dba.Property("prop1"), dba.Property("prop2")});
    dba.Commit();
  }

  {
    ResultStreamFaker stream;
    query::AnyStream query_stream(&stream, utils::NewDeleteResource());
    {
      auto acc = db.Access();
      query::DbAccessor dba(&acc);
      query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(
        stream.GetResults(),
        "CREATE CONSTRAINT ON (u:Label) ASSERT u.prop IS UNIQUE;",
        "CREATE CONSTRAINT ON (u:Label) ASSERT u.prop1, u.prop2 IS UNIQUE;",
        kCreateInternalIndex,
        "CREATE (:__mg_vertex__:Label {__mg_id__: 0, prop: 1});",
        kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, CheckStateVertexWithMultipleProperties) {
  database::GraphDb db;
  {
    auto dba = db.Access();
    std::map<std::string, PropertyValue> prop1 = {
        {"nested1", PropertyValue(1337)}, {"nested2", PropertyValue(3.14)}};
    CreateVertex(
        &dba, {"Label1", "Label2"},
        {{"prop1", PropertyValue(prop1)}, {"prop2", PropertyValue("$'\t'")}});
    dba.Commit();
  }

  database::GraphDb db_dump;
  {
    ResultStreamFaker stream;
    query::AnyStream query_stream(&stream, utils::NewDeleteResource());
    {
      auto acc = db.Access();
      query::DbAccessor dba(&acc);
      query::DumpDatabaseToCypherQueries(&dba, &query_stream);
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
  database::GraphDb db;
  {
    auto dba = db.Access();
    auto u = CreateVertex(&dba, {"Person"}, {{"name", PropertyValue("Ivan")}});
    auto v = CreateVertex(&dba, {"Person"}, {{"name", PropertyValue("Josko")}});
    auto w = CreateVertex(
        &dba, {"Person"},
        {{"name", PropertyValue("Bosko")}, {"id", PropertyValue(0)}});
    auto z = CreateVertex(
        &dba, {"Person"},
        {{"name", PropertyValue("Buha")}, {"id", PropertyValue(1)}});
    CreateEdge(&dba, u, v, "Knows", {});
    CreateEdge(&dba, v, w, "Knows", {{"how_long", PropertyValue(5)}});
    CreateEdge(&dba, w, u, "Knows", {{"how", PropertyValue("distant past")}});
    CreateEdge(&dba, v, u, "Knows", {});
    CreateEdge(&dba, v, u, "Likes", {});
    CreateEdge(&dba, z, u, "Knows", {});
    CreateEdge(&dba, w, z, "Knows", {{"how", PropertyValue("school")}});
    CreateEdge(&dba, w, z, "Likes", {{"how", PropertyValue("very much")}});
    dba.Commit();
  }
  {
    auto dba = db.Access();
    dba.BuildUniqueConstraint(dba.Label("Person"), {dba.Property("name")});
    dba.BuildIndex(dba.Label("Person"), dba.Property("id"));
    dba.BuildIndex(dba.Label("Person"), dba.Property("unexisting_property"));
    dba.Commit();
  }

  const auto &db_initial_state = GetState(&db);
  database::GraphDb db_dump;
  {
    ResultStreamFaker stream;
    query::AnyStream query_stream(&stream, utils::NewDeleteResource());
    {
      auto acc = db.Access();
      query::DbAccessor dba(&acc);
      query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    const auto &results = stream.GetResults();
    // Indices and constraints are 3 queries and there must be at least one more
    // query for the data.
    ASSERT_GE(results.size(), 4);
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
  database::GraphDb db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {}, {}, false);
    dba.Commit();
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
    EXPECT_EQ(results[0][0].ValueString(),
              "CREATE INDEX ON :__mg_vertex__(__mg_id__);");
    EXPECT_EQ(results[1][0].ValueString(),
              "CREATE (:__mg_vertex__ {__mg_id__: 0});");
    EXPECT_EQ(results[2][0].ValueString(),
              "DROP INDEX ON :__mg_vertex__(__mg_id__);");
    EXPECT_EQ(results[3][0].ValueString(),
              "MATCH (u) REMOVE u:__mg_vertex__, u.__mg_id__;");
  }
}

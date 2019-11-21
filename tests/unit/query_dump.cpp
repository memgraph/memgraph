#include <gtest/gtest.h>

#include <map>
#include <set>
#include <vector>

#include <glog/logging.h>

#include "communication/result_stream_faker.hpp"
#include "query/dump.hpp"
#include "query/interpreter.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/storage.hpp"

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

  struct LabelItem {
    std::string label;
  };

  struct LabelPropertyItem {
    std::string label;
    std::string property;
  };

  std::set<Vertex> vertices;
  std::set<Edge> edges;
  std::set<LabelItem> label_indices;
  std::set<LabelPropertyItem> label_property_indices;
  std::set<LabelPropertyItem> existence_constraints;
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

bool operator<(const DatabaseState::LabelItem &first,
               const DatabaseState::LabelItem &second) {
  return first.label < second.label;
}

bool operator<(const DatabaseState::LabelPropertyItem &first,
               const DatabaseState::LabelPropertyItem &second) {
  if (first.label != second.label) return first.label < second.label;
  return first.property < second.property;
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

bool operator==(const DatabaseState::LabelItem &first,
                const DatabaseState::LabelItem &second) {
  return first.label == second.label;
}

bool operator==(const DatabaseState::LabelPropertyItem &first,
                const DatabaseState::LabelPropertyItem &second) {
  return first.label == second.label && first.property == second.property;
}

bool operator==(const DatabaseState &first, const DatabaseState &second) {
  return first.vertices == second.vertices && first.edges == second.edges &&
         first.label_indices == second.label_indices &&
         first.label_property_indices == second.label_property_indices &&
         first.existence_constraints == second.existence_constraints;
}

DatabaseState GetState(storage::Storage *db) {
  // Capture all vertices
  std::map<storage::Gid, int64_t> gid_mapping;
  std::set<DatabaseState::Vertex> vertices;
  auto dba = db->Access();
  for (const auto &vertex : dba.Vertices(storage::View::NEW)) {
    std::set<std::string> labels;
    auto maybe_labels = vertex.Labels(storage::View::NEW);
    CHECK(maybe_labels.HasValue());
    for (const auto &label : *maybe_labels) {
      labels.insert(dba.LabelToName(label));
    }
    std::map<std::string, PropertyValue> props;
    auto maybe_properties = vertex.Properties(storage::View::NEW);
    CHECK(maybe_properties.HasValue());
    for (const auto &kv : *maybe_properties) {
      props.emplace(dba.PropertyToName(kv.first), kv.second);
    }
    CHECK(props.count(kPropertyId) == 1);
    const auto id = props[kPropertyId].ValueInt();
    gid_mapping[vertex.Gid()] = id;
    vertices.insert({id, labels, props});
  }

  // Capture all edges
  std::set<DatabaseState::Edge> edges;
  for (const auto &vertex : dba.Vertices(storage::View::NEW)) {
    auto maybe_edges = vertex.OutEdges({}, storage::View::NEW);
    CHECK(maybe_edges.HasValue());
    for (const auto &edge : *maybe_edges) {
      const auto &edge_type_name = dba.EdgeTypeToName(edge.EdgeType());
      std::map<std::string, PropertyValue> props;
      auto maybe_properties = edge.Properties(storage::View::NEW);
      CHECK(maybe_properties.HasValue());
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
      label_property_indices.insert(
          {dba.LabelToName(item.first), dba.PropertyToName(item.second)});
    }
  }

  // Capture all constraints
  std::set<DatabaseState::LabelPropertyItem> existence_constraints;
  {
    auto info = dba.ListAllConstraints();
    for (const auto &item : info.existence) {
      existence_constraints.insert(
          {dba.LabelToName(item.first), dba.PropertyToName(item.second)});
    }
  }

  return {vertices, edges, label_indices, label_property_indices,
          existence_constraints};
}

auto Execute(storage::Storage *db, const std::string &query) {
  query::InterpreterContext context(db);
  query::Interpreter interpreter(&context);
  ResultStreamFaker stream(db);

  auto [header, _] = interpreter.Prepare(query, {});
  stream.Header(header);
  auto summary = interpreter.PullAll(&stream);
  stream.Summary(summary);

  return stream;
}

VertexAccessor CreateVertex(storage::Storage::Accessor *dba,
                            const std::vector<std::string> &labels,
                            const std::map<std::string, PropertyValue> &props,
                            bool add_property_id = true) {
  CHECK(dba);
  auto vertex = dba->CreateVertex();
  for (const auto &label_name : labels) {
    CHECK(vertex.AddLabel(dba->NameToLabel(label_name)).HasValue());
  }
  for (const auto &kv : props) {
    CHECK(vertex.SetProperty(dba->NameToProperty(kv.first), kv.second)
              .HasValue());
  }
  if (add_property_id) {
    CHECK(vertex
              .SetProperty(dba->NameToProperty(kPropertyId),
                           PropertyValue(vertex.Gid().AsInt()))
              .HasValue());
  }
  return vertex;
}

EdgeAccessor CreateEdge(storage::Storage::Accessor *dba, VertexAccessor *from,
                        VertexAccessor *to, const std::string &edge_type_name,
                        const std::map<std::string, PropertyValue> &props,
                        bool add_property_id = true) {
  CHECK(dba);
  auto edge = dba->CreateEdge(from, to, dba->NameToEdgeType(edge_type_name));
  CHECK(edge.HasValue());
  for (const auto &kv : props) {
    CHECK(
        edge->SetProperty(dba->NameToProperty(kv.first), kv.second).HasValue());
  }
  if (add_property_id) {
    CHECK(edge->SetProperty(dba->NameToProperty(kPropertyId),
                            PropertyValue(edge->Gid().AsInt()))
              .HasValue());
  }
  return *edge;
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
  storage::Storage db;
  ResultStreamFaker stream(&db);
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
  storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {}, {}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
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
  storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {"Label1"}, {}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
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
  storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {"Label1", "Label2"}, {}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
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
  storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {}, {{"prop", PropertyValue(42)}}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
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
  storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {}, {}, false);
    CreateVertex(&dba, {}, {}, false);
    CreateVertex(&dba, {}, {}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
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
  storage::Storage db;
  {
    auto dba = db.Access();
    auto u = CreateVertex(&dba, {}, {}, false);
    auto v = CreateVertex(&dba, {}, {}, false);
    CreateEdge(&dba, &u, &v, "EdgeType", {}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
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
  storage::Storage db;
  {
    auto dba = db.Access();
    auto u = CreateVertex(&dba, {}, {}, false);
    auto v = CreateVertex(&dba, {}, {}, false);
    auto w = CreateVertex(&dba, {}, {}, false);
    CreateEdge(&dba, &u, &v, "EdgeType", {}, false);
    CreateEdge(&dba, &v, &u, "EdgeType", {}, false);
    CreateEdge(&dba, &v, &w, "EdgeType", {}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
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
  storage::Storage db;
  {
    auto dba = db.Access();
    auto u = CreateVertex(&dba, {}, {}, false);
    auto v = CreateVertex(&dba, {}, {}, false);
    CreateEdge(&dba, &u, &v, "EdgeType", {{"prop", PropertyValue(13)}}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    ResultStreamFaker stream(&db);
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
  storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {"Label1", "Label2"}, {{"p", PropertyValue(1)}}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }
  ASSERT_TRUE(
      db.CreateIndex(db.NameToLabel("Label1"), db.NameToProperty("prop")));
  ASSERT_TRUE(
      db.CreateIndex(db.NameToLabel("Label2"), db.NameToProperty("prop")));

  {
    ResultStreamFaker stream(&db);
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
TEST(DumpTest, ExistenceConstraints) {
  storage::Storage db;
  {
    auto dba = db.Access();
    CreateVertex(&dba, {"Label"}, {{"prop", PropertyValue(1)}}, false);
    ASSERT_FALSE(dba.Commit().HasError());
  }
  {
    auto res = db.CreateExistenceConstraint(db.NameToLabel("Label"),
                                            db.NameToProperty("prop"));
    ASSERT_TRUE(res.HasValue());
    ASSERT_TRUE(res.GetValue());
  }

  {
    ResultStreamFaker stream(&db);
    query::AnyStream query_stream(&stream, utils::NewDeleteResource());
    {
      auto acc = db.Access();
      query::DbAccessor dba(&acc);
      query::DumpDatabaseToCypherQueries(&dba, &query_stream);
    }
    VerifyQueries(stream.GetResults(),
                  "CREATE CONSTRAINT ON (u:Label) ASSERT EXISTS (u.prop);",
                  kCreateInternalIndex,
                  "CREATE (:__mg_vertex__:Label {__mg_id__: 0, prop: 1});",
                  kDropInternalIndex, kRemoveInternalLabelProperty);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(DumpTest, CheckStateVertexWithMultipleProperties) {
  storage::Storage db;
  {
    auto dba = db.Access();
    std::map<std::string, PropertyValue> prop1 = {
        {"nested1", PropertyValue(1337)}, {"nested2", PropertyValue(3.14)}};
    CreateVertex(
        &dba, {"Label1", "Label2"},
        {{"prop1", PropertyValue(prop1)}, {"prop2", PropertyValue("$'\t'")}});
    ASSERT_FALSE(dba.Commit().HasError());
  }

  storage::Storage db_dump;
  {
    ResultStreamFaker stream(&db);
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
  storage::Storage db;
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
    CreateEdge(&dba, &u, &v, "Knows", {});
    CreateEdge(&dba, &v, &w, "Knows", {{"how_long", PropertyValue(5)}});
    CreateEdge(&dba, &w, &u, "Knows", {{"how", PropertyValue("distant past")}});
    CreateEdge(&dba, &v, &u, "Knows", {});
    CreateEdge(&dba, &v, &u, "Likes", {});
    CreateEdge(&dba, &z, &u, "Knows", {});
    CreateEdge(&dba, &w, &z, "Knows", {{"how", PropertyValue("school")}});
    CreateEdge(&dba, &w, &z, "Likes", {{"how", PropertyValue("very much")}});
    ASSERT_FALSE(dba.Commit().HasError());
  }
  {
    auto ret = db.CreateExistenceConstraint(db.NameToLabel("Person"),
                                            db.NameToProperty("name"));
    ASSERT_TRUE(ret.HasValue());
    ASSERT_TRUE(ret.GetValue());
  }
  ASSERT_TRUE(
      db.CreateIndex(db.NameToLabel("Person"), db.NameToProperty("id")));
  ASSERT_TRUE(db.CreateIndex(db.NameToLabel("Person"),
                             db.NameToProperty("unexisting_property")));

  const auto &db_initial_state = GetState(&db);
  storage::Storage db_dump;
  {
    ResultStreamFaker stream(&db);
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
  storage::Storage db;
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

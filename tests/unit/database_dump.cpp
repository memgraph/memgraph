#include <gtest/gtest.h>

#include <map>
#include <set>
#include <vector>

#include <glog/logging.h>

#include "communication/result_stream_faker.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "database/single_node/dump.hpp"
#include "query/interpreter.hpp"
#include "query/typed_value.hpp"
#include "storage/common/types/property_value.hpp"

const char *kPropertyId = "property_id";

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

  std::set<Vertex> vertices;
  std::set<Edge> edges;
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

bool operator==(const DatabaseState &first, const DatabaseState &second) {
  return first.vertices == second.vertices && first.edges == second.edges;
}

class DatabaseEnvironment {
 public:
  std::string DumpStr() {
    auto dba = db_.Access();
    std::ostringstream oss;
    database::DumpToCypher(&oss, &dba);
    return oss.str();
  }

  void Execute(const std::string &query) {
    auto dba = db_.Access();
    ResultStreamFaker<query::TypedValue> results;
    query::Interpreter()(query, dba, {}, false).PullAll(results);
    dba.Commit();
  }

  VertexAccessor CreateVertex(const std::vector<std::string> &labels,
                              const std::map<std::string, PropertyValue> &props,
                              bool add_property_id = true) {
    auto dba = db_.Access();
    auto vertex = dba.InsertVertex();
    for (const auto &label_name : labels) {
      vertex.add_label(dba.Label(label_name));
    }
    for (const auto &kv : props) {
      vertex.PropsSet(dba.Property(kv.first), kv.second);
    }
    if (add_property_id) {
      vertex.PropsSet(dba.Property(kPropertyId),
                      PropertyValue(static_cast<int64_t>(vertex.gid())));
    }
    dba.Commit();
    return vertex;
  }

  EdgeAccessor CreateEdge(VertexAccessor from, VertexAccessor to,
                          const std::string &edge_type_name,
                          const std::map<std::string, PropertyValue> &props,
                          bool add_property_id = true) {
    auto dba = db_.Access();
    auto edge = dba.InsertEdge(from, to, dba.EdgeType(edge_type_name));
    for (const auto &kv : props) {
      edge.PropsSet(dba.Property(kv.first), kv.second);
    }
    if (add_property_id) {
      edge.PropsSet(dba.Property(kPropertyId),
                    PropertyValue(static_cast<int64_t>(edge.gid())));
    }
    dba.Commit();
    return edge;
  }

  DatabaseState GetState() {
    // Capture all vertices
    std::map<gid::Gid, int64_t> gid_mapping;
    std::set<DatabaseState::Vertex> vertices;
    auto dba = db_.Access();
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
      const auto id = props[kPropertyId].Value<int64_t>();
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

    return {vertices, edges};
  }

 private:
  database::GraphDb db_;
};

TEST(DumpTest, EmptyGraph) {
  DatabaseEnvironment db;
  EXPECT_EQ("", db.DumpStr());
}

TEST(DumpTest, SingleVertex) {
  DatabaseEnvironment db;
  db.CreateVertex({}, {}, false);
  EXPECT_EQ(db.DumpStr(), "CREATE (n0);");
}

TEST(DumpTest, VertexWithSingleLabel) {
  DatabaseEnvironment db;
  db.CreateVertex({"Label1"}, {}, false);
  EXPECT_EQ(db.DumpStr(), "CREATE (n0:Label1);");
}

TEST(DumpTest, VertexWithMultipleLabels) {
  DatabaseEnvironment db;
  db.CreateVertex({"Label1", "Label2"}, {}, false);
  EXPECT_EQ(db.DumpStr(), "CREATE (n0:Label1:Label2);");
}

TEST(DumpTest, VertexWithSingleProperty) {
  DatabaseEnvironment db;
  db.CreateVertex({}, {{"prop", PropertyValue(42)}}, false);
  EXPECT_EQ(db.DumpStr(), "CREATE (n0 {prop: 42});");
}

TEST(DumpTest, MultipleVertices) {
  DatabaseEnvironment db;
  db.CreateVertex({}, {}, false);
  db.CreateVertex({}, {}, false);
  db.CreateVertex({}, {}, false);
  EXPECT_EQ(db.DumpStr(), "CREATE (n0), (n1), (n2);");
}

TEST(DumpTest, SingleEdge) {
  DatabaseEnvironment db;
  auto u = db.CreateVertex({}, {}, false);
  auto v = db.CreateVertex({}, {}, false);
  db.CreateEdge(u, v, "EdgeType", {}, false);
  EXPECT_EQ(db.DumpStr(), "CREATE (n0), (n1), (n0)-[:EdgeType]->(n1);");
}

TEST(DumpTest, MultipleEdges) {
  DatabaseEnvironment db;
  auto u = db.CreateVertex({}, {}, false);
  auto v = db.CreateVertex({}, {}, false);
  auto w = db.CreateVertex({}, {}, false);
  db.CreateEdge(u, v, "EdgeType", {}, false);
  db.CreateEdge(v, u, "EdgeType", {}, false);
  db.CreateEdge(v, w, "EdgeType", {}, false);
  const char *expected =
      "CREATE (n0), (n1), (n2), (n0)-[:EdgeType]->(n1), "
      "(n1)-[:EdgeType]->(n0), (n1)-[:EdgeType]->(n2);";
  EXPECT_EQ(db.DumpStr(), expected);
}

TEST(DumpTest, EdgeWithProperties) {
  DatabaseEnvironment db;
  auto u = db.CreateVertex({}, {}, false);
  auto v = db.CreateVertex({}, {}, false);
  db.CreateEdge(u, v, "EdgeType", {{"prop", PropertyValue(13)}}, false);
  EXPECT_EQ(db.DumpStr(),
            "CREATE (n0), (n1), (n0)-[:EdgeType {prop: 13}]->(n1);");
}

TEST(DumpTest, CheckStateVertexWithMultipleProperties) {
  DatabaseEnvironment db;
  std::map<std::string, PropertyValue> prop1 = {
      {"nested1", PropertyValue(1337)}, {"nested2", PropertyValue(3.14)}};
  db.CreateVertex({"Label1", "Label2"},
                  {{"prop1", prop1}, {"prop2", PropertyValue("$'\t'")}});
  DatabaseEnvironment db_dump;
  db_dump.Execute(db.DumpStr());
  EXPECT_EQ(db.GetState(), db_dump.GetState());
}

TEST(DumpTest, CheckStateSimpleGraph) {
  DatabaseEnvironment db;
  auto u = db.CreateVertex({"Person"}, {{"name", "Ivan"}});
  auto v = db.CreateVertex({"Person"}, {{"name", "Josko"}});
  auto w = db.CreateVertex({"Person"}, {{"name", "Bosko"}});
  auto z = db.CreateVertex({"Person"}, {{"name", "Buha"}});
  db.CreateEdge(u, v, "Knows", {});
  db.CreateEdge(v, w, "Knows", {{"how_long", 5}});
  db.CreateEdge(w, u, "Knows", {{"how", "distant past"}});
  db.CreateEdge(v, u, "Knows", {});
  db.CreateEdge(v, u, "Likes", {});
  db.CreateEdge(z, u, "Knows", {});
  db.CreateEdge(w, z, "Knows", {{"how", "school"}});
  db.CreateEdge(w, z, "Likes", {{"how", "very much"}});
  DatabaseEnvironment db_dump;
  db_dump.Execute(db.DumpStr());
  EXPECT_EQ(db.GetState(), db_dump.GetState());
}

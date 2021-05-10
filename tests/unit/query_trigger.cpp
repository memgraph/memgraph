#include <gtest/gtest.h>
#include <filesystem>

#include "query/db_accessor.hpp"
#include "query/interpreter.hpp"
#include "query/trigger.hpp"

class TriggerTest : public ::testing::Test {
 public:
  void SetUp() override {
    db.emplace();
    interpreter_context.emplace(&*db, data_directory);
  }

  void TearDown() override {
    interpreter_context.reset();
    db.reset();
  }

 protected:
  const std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_trigger";

  std::optional<storage::Storage> db;
  std::optional<query::InterpreterContext> interpreter_context;
};

TEST(TriggerContext, ValidObjectsTest) {
  storage::Storage db;
  query::TriggerContext trigger_context;

  const auto check_typed_values_size = [&](const query::trigger::IdentifierTag tag, const size_t expected_size,
                                           query::DbAccessor *dba) {
    auto typed_values = trigger_context.GetTypedValue(tag, dba);
    ASSERT_TRUE(typed_values.IsList());
    ASSERT_EQ(typed_values.ValueList().size(), expected_size);
  };

  const auto check_typed_values = [&](const size_t vertex_count, const size_t edge_count,
                                      const size_t deleted_vertex_count, const size_t deleted_edge_count,
                                      query::DbAccessor &dba) {
    check_typed_values_size(query::trigger::IdentifierTag::CREATED_VERTICES, vertex_count, &dba);
    check_typed_values_size(query::trigger::IdentifierTag::CREATED_EDGES, edge_count, &dba);
    check_typed_values_size(query::trigger::IdentifierTag::CREATED_OBJECTS, vertex_count + edge_count, &dba);

    check_typed_values_size(query::trigger::IdentifierTag::DELETED_VERTICES, deleted_vertex_count, &dba);
    check_typed_values_size(query::trigger::IdentifierTag::DELETED_EDGES, deleted_edge_count, &dba);
    check_typed_values_size(query::trigger::IdentifierTag::DELETED_OBJECTS, deleted_vertex_count + deleted_edge_count,
                            &dba);

    check_typed_values_size(query::trigger::IdentifierTag::SET_VERTEX_PROPERTIES, vertex_count, &dba);
    check_typed_values_size(query::trigger::IdentifierTag::SET_EDGE_PROPERTIES, edge_count, &dba);

    check_typed_values_size(query::trigger::IdentifierTag::REMOVED_VERTEX_PROPERTIES, vertex_count, &dba);
    check_typed_values_size(query::trigger::IdentifierTag::REMOVED_EDGE_PROPERTIES, edge_count, &dba);

    const auto check_label_map = [&](const query::trigger::IdentifierTag tag, const size_t expected) {
      auto typed_values = trigger_context.GetTypedValue(tag, &dba);
      ASSERT_TRUE(typed_values.IsMap());
      auto &typed_values_map = typed_values.ValueMap();
      size_t value_count = 0;
      for (const auto &[label, values] : typed_values_map) {
        ASSERT_TRUE(values.IsList());
        value_count += values.ValueList().size();
      }
      ASSERT_EQ(value_count, expected);
    };

    check_label_map(query::trigger::IdentifierTag::SET_VERTEX_LABELS, vertex_count);
    check_label_map(query::trigger::IdentifierTag::REMOVED_VERTEX_LABELS, vertex_count);

    check_typed_values_size(query::trigger::IdentifierTag::UPDATED_VERTICES, 4 * vertex_count, &dba);
    check_typed_values_size(query::trigger::IdentifierTag::UPDATED_EDGES, 2 * edge_count, &dba);
    check_typed_values_size(query::trigger::IdentifierTag::UPDATED_OBJECTS, 4 * vertex_count + 2 * edge_count, &dba);
  };

  auto initial_storage_acc = db.Access();
  {
    query::DbAccessor dba{&initial_storage_acc};

    auto create_vertex = [&] {
      auto created_vertex = dba.InsertVertex();
      trigger_context.RegisterCreatedObject(created_vertex);
      trigger_context.RegisterSetObjectProperty(created_vertex, dba.NameToProperty("PROPERTY1"),
                                                query::TypedValue("Value"), query::TypedValue("ValueNew"));
      trigger_context.RegisterRemovedObjectProperty(created_vertex, dba.NameToProperty("PROPERTY2"),
                                                    query::TypedValue("Value"));
      trigger_context.RegisterSetVertexLabel(created_vertex, dba.NameToLabel("LABEL1"));
      trigger_context.RegisterRemovedVertexLabel(created_vertex, dba.NameToLabel("LABEL2"));
      return created_vertex;
    };

    std::vector<query::VertexAccessor> vertices;
    for (size_t i = 0; i < 4; ++i) {
      vertices.push_back(create_vertex());
    }

    auto create_edge = [&](auto &from, auto &to) {
      auto maybe_edge = dba.InsertEdge(&from, &to, dba.NameToEdgeType("EDGE"));
      ASSERT_FALSE(maybe_edge.HasError());
      trigger_context.RegisterCreatedObject(*maybe_edge);
      trigger_context.RegisterSetObjectProperty(*maybe_edge, dba.NameToProperty("PROPERTY1"),
                                                query::TypedValue("Value"), query::TypedValue("ValueNew"));
      trigger_context.RegisterRemovedObjectProperty(*maybe_edge, dba.NameToProperty("PROPERTY2"),
                                                    query::TypedValue("Value"));
    };

    create_edge(vertices[0], vertices[1]);
    create_edge(vertices[1], vertices[2]);
    create_edge(vertices[2], vertices[3]);

    dba.AdvanceCommand();

    check_typed_values(4, 3, 0, 0, dba);

    // we delete one of the vertices and edges in the same transaction
    const auto maybe_values = dba.DetachRemoveVertex(&vertices[3]);
    ASSERT_TRUE(maybe_values.HasValue());
    ASSERT_TRUE(maybe_values.GetValue());
    const auto &[deleted_vertex, deleted_edges] = *maybe_values.GetValue();

    trigger_context.RegisterDeletedObject(deleted_vertex);
    for (const auto &edge : deleted_edges) {
      trigger_context.RegisterDeletedObject(edge);
    }

    dba.AdvanceCommand();

    check_typed_values(3, 2, 1, 1, dba);

    // we can modify the objects, and read its values
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    auto storage_acc = db.Access();
    query::DbAccessor dba{&storage_acc};

    auto vertices = dba.Vertices(storage::View::OLD);
    for (auto vertex : vertices) {
      // delete the first vertex with its edges
      ASSERT_TRUE(dba.DetachRemoveVertex(&vertex).HasValue());
      break;
    }
    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    auto storage_acc = db.Access();
    query::DbAccessor dba{&storage_acc};

    trigger_context.AdaptForAccessor(&dba);

    // we deleted 1 vertex and 1 edge, rest should be okay
    check_typed_values(2, 1, 1, 1, dba);
  }
}

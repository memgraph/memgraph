#include <gtest/gtest.h>
#include <filesystem>

#include "query/db_accessor.hpp"
#include "query/interpreter.hpp"
#include "query/trigger.hpp"
#include "query/typed_value.hpp"
#include "utils/memory.hpp"

class TriggerContextTest : public ::testing::Test {
 public:
  void SetUp() override { db.emplace(); }

  void TearDown() override {
    accessors.clear();
    db.reset();
  }

  storage::Storage::Accessor &StartTransaction() {
    accessors.push_back(db->Access());
    return accessors.back();
  }

 protected:
  std::optional<storage::Storage> db;
  std::list<storage::Storage::Accessor> accessors;
};

namespace {
void CheckTypedValueSize(query::TriggerContext &trigger_context, const query::trigger::IdentifierTag tag,
                         const size_t expected_size, query::DbAccessor &dba) {
  auto typed_values = trigger_context.GetTypedValue(tag, &dba);
  ASSERT_TRUE(typed_values.IsList());
  ASSERT_EQ(typed_values.ValueList().size(), expected_size);
};

void CheckLabelMap(query::TriggerContext &trigger_context, const query::trigger::IdentifierTag tag,
                   const size_t expected, query::DbAccessor &dba) {
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
}  // namespace

// Ensure that TriggerContext returns only valid objects.
// Returned TypedValue should always contain only objects
// that exist (unless its explicitly created for the deleted object)
TEST_F(TriggerContextTest, ValidObjectsTest) {
  std::optional<query::TriggerContext> trigger_context;
  trigger_context.emplace();

  size_t vertex_count = 0;
  size_t edge_count = 0;
  {
    query::DbAccessor dba{&StartTransaction()};

    auto create_vertex = [&] {
      auto created_vertex = dba.InsertVertex();
      trigger_context->RegisterCreatedObject(created_vertex);
      ++vertex_count;
      return created_vertex;
    };

    // Create vertices and add them to the trigger context as created
    std::vector<query::VertexAccessor> vertices;
    for (size_t i = 0; i < 4; ++i) {
      vertices.push_back(create_vertex());
    }

    auto create_edge = [&](auto &from, auto &to) {
      auto maybe_edge = dba.InsertEdge(&from, &to, dba.NameToEdgeType("EDGE"));
      ASSERT_FALSE(maybe_edge.HasError());
      trigger_context->RegisterCreatedObject(*maybe_edge);
      ++edge_count;
    };

    // Create edges and add them to the trigger context as created
    create_edge(vertices[0], vertices[1]);
    create_edge(vertices[1], vertices[2]);
    create_edge(vertices[2], vertices[3]);

    dba.AdvanceCommand();

    // Should have all the created objects
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::CREATED_VERTICES, vertex_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::CREATED_EDGES, edge_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::CREATED_OBJECTS, vertex_count + edge_count,
                        dba);

    // we delete one of the vertices and edges in the same transaction
    ASSERT_TRUE(dba.DetachRemoveVertex(&vertices[0]).HasValue());
    --vertex_count;
    --edge_count;

    dba.AdvanceCommand();

    // Should have one less created object for vertex and edge
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::CREATED_VERTICES, vertex_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::CREATED_EDGES, edge_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::CREATED_OBJECTS, vertex_count + edge_count,
                        dba);

    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    query::DbAccessor dba{&StartTransaction()};
    trigger_context->AdaptForAccessor(&dba);

    // Should have one less created object for vertex and edge
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::CREATED_VERTICES, vertex_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::CREATED_EDGES, edge_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::CREATED_OBJECTS, vertex_count + edge_count,
                        dba);
  }

  // created vertex event disables registering each future event registration for the same object so we
  // need to reset the trigger_context
  trigger_context.emplace();

  size_t deleted_vertex_count = 0;
  size_t deleted_edge_count = 0;
  {
    query::DbAccessor dba{&StartTransaction()};

    // register each type of change for each object
    {
      auto vertices = dba.Vertices(storage::View::OLD);
      for (auto vertex : vertices) {
        trigger_context->RegisterSetObjectProperty(vertex, dba.NameToProperty("PROPERTY1"), query::TypedValue("Value"),
                                                   query::TypedValue("ValueNew"));
        trigger_context->RegisterRemovedObjectProperty(vertex, dba.NameToProperty("PROPERTY2"),
                                                       query::TypedValue("Value"));
        trigger_context->RegisterSetVertexLabel(vertex, dba.NameToLabel("LABEL1"));
        trigger_context->RegisterRemovedVertexLabel(vertex, dba.NameToLabel("LABEL2"));

        auto out_edges = vertex.OutEdges(storage::View::OLD);
        ASSERT_TRUE(out_edges.HasValue());

        for (auto edge : *out_edges) {
          trigger_context->RegisterSetObjectProperty(edge, dba.NameToProperty("PROPERTY1"), query::TypedValue("Value"),
                                                     query::TypedValue("ValueNew"));
          trigger_context->RegisterRemovedObjectProperty(edge, dba.NameToProperty("PROPERTY2"),
                                                         query::TypedValue("Value"));
        }
      }
    }

    // Delete the first vertex with its edge and register the deleted object
    {
      auto vertices = dba.Vertices(storage::View::OLD);
      for (auto vertex : vertices) {
        const auto maybe_values = dba.DetachRemoveVertex(&vertex);
        ASSERT_TRUE(maybe_values.HasValue());
        ASSERT_TRUE(maybe_values.GetValue());
        const auto &[deleted_vertex, deleted_edges] = *maybe_values.GetValue();

        trigger_context->RegisterDeletedObject(deleted_vertex);
        ++deleted_vertex_count;
        --vertex_count;
        for (const auto &edge : deleted_edges) {
          trigger_context->RegisterDeletedObject(edge);
          ++deleted_edge_count;
          --edge_count;
        }

        break;
      }
    }

    dba.AdvanceCommand();
    ASSERT_FALSE(dba.Commit().HasError());

    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::SET_VERTEX_PROPERTIES, vertex_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::SET_EDGE_PROPERTIES, edge_count, dba);

    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::REMOVED_VERTEX_PROPERTIES, vertex_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::REMOVED_EDGE_PROPERTIES, edge_count, dba);

    CheckLabelMap(*trigger_context, query::trigger::IdentifierTag::SET_VERTEX_LABELS, vertex_count, dba);
    CheckLabelMap(*trigger_context, query::trigger::IdentifierTag::REMOVED_VERTEX_LABELS, vertex_count, dba);

    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::UPDATED_VERTICES, 4 * vertex_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::UPDATED_EDGES, 2 * edge_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::UPDATED_OBJECTS,
                        4 * vertex_count + 2 * edge_count, dba);

    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::DELETED_VERTICES, deleted_vertex_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::DELETED_EDGES, deleted_edge_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::DELETED_OBJECTS,
                        deleted_vertex_count + deleted_edge_count, dba);
  }

  // delete a single vertex with its edges, it should reduce number of typed values returned by the trigger context
  // for each update event.
  // TypedValue of the deleted objects stay the same as they're bound to the transaction which deleted them.
  {
    query::DbAccessor dba{&StartTransaction()};
    trigger_context->AdaptForAccessor(&dba);

    auto vertices = dba.Vertices(storage::View::OLD);
    for (auto vertex : vertices) {
      ASSERT_TRUE(dba.DetachRemoveVertex(&vertex).HasValue());
      break;
    }
    --vertex_count;
    --edge_count;

    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    query::DbAccessor dba{&StartTransaction()};
    trigger_context->AdaptForAccessor(&dba);

    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::SET_VERTEX_PROPERTIES, vertex_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::SET_EDGE_PROPERTIES, edge_count, dba);

    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::REMOVED_VERTEX_PROPERTIES, vertex_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::REMOVED_EDGE_PROPERTIES, edge_count, dba);

    CheckLabelMap(*trigger_context, query::trigger::IdentifierTag::SET_VERTEX_LABELS, vertex_count, dba);
    CheckLabelMap(*trigger_context, query::trigger::IdentifierTag::REMOVED_VERTEX_LABELS, vertex_count, dba);

    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::UPDATED_VERTICES, 4 * vertex_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::UPDATED_EDGES, 2 * edge_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::UPDATED_OBJECTS,
                        4 * vertex_count + 2 * edge_count, dba);

    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::DELETED_VERTICES, deleted_vertex_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::DELETED_EDGES, deleted_edge_count, dba);
    CheckTypedValueSize(*trigger_context, query::trigger::IdentifierTag::DELETED_OBJECTS,
                        deleted_vertex_count + deleted_edge_count, dba);
  }
}

// If the trigger context registered a created object, each future event on the same object will be ignored.
// Binding the trigger context to transaction will mean that creating and updating an object in the same transaction
// will return only the CREATE event.
TEST_F(TriggerContextTest, ReturnCreateOnlyEvent) {
  query::TriggerContext trigger_context;

  query::DbAccessor dba{&StartTransaction()};

  auto create_vertex = [&] {
    auto vertex = dba.InsertVertex();
    trigger_context.RegisterCreatedObject(vertex);
    trigger_context.RegisterSetObjectProperty(vertex, dba.NameToProperty("PROPERTY1"), query::TypedValue("Value"),
                                              query::TypedValue("ValueNew"));
    trigger_context.RegisterRemovedObjectProperty(vertex, dba.NameToProperty("PROPERTY2"), query::TypedValue("Value"));
    trigger_context.RegisterSetVertexLabel(vertex, dba.NameToLabel("LABEL1"));
    trigger_context.RegisterRemovedVertexLabel(vertex, dba.NameToLabel("LABEL2"));
    return vertex;
  };

  auto v1 = create_vertex();
  auto v2 = create_vertex();
  auto maybe_edge = dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("EDGE"));
  ASSERT_FALSE(maybe_edge.HasError());
  trigger_context.RegisterCreatedObject(*maybe_edge);
  trigger_context.RegisterSetObjectProperty(*maybe_edge, dba.NameToProperty("PROPERTY1"), query::TypedValue("Value"),
                                            query::TypedValue("ValueNew"));
  trigger_context.RegisterRemovedObjectProperty(*maybe_edge, dba.NameToProperty("PROPERTY2"),
                                                query::TypedValue("Value"));

  dba.AdvanceCommand();

  CheckTypedValueSize(trigger_context, query::trigger::IdentifierTag::CREATED_VERTICES, 2, dba);
  CheckTypedValueSize(trigger_context, query::trigger::IdentifierTag::CREATED_EDGES, 1, dba);
  CheckTypedValueSize(trigger_context, query::trigger::IdentifierTag::CREATED_OBJECTS, 3, dba);

  CheckTypedValueSize(trigger_context, query::trigger::IdentifierTag::SET_VERTEX_PROPERTIES, 0, dba);
  CheckTypedValueSize(trigger_context, query::trigger::IdentifierTag::SET_EDGE_PROPERTIES, 0, dba);

  CheckTypedValueSize(trigger_context, query::trigger::IdentifierTag::REMOVED_VERTEX_PROPERTIES, 0, dba);
  CheckTypedValueSize(trigger_context, query::trigger::IdentifierTag::REMOVED_EDGE_PROPERTIES, 0, dba);

  CheckLabelMap(trigger_context, query::trigger::IdentifierTag::SET_VERTEX_LABELS, 0, dba);
  CheckLabelMap(trigger_context, query::trigger::IdentifierTag::REMOVED_VERTEX_LABELS, 0, dba);

  CheckTypedValueSize(trigger_context, query::trigger::IdentifierTag::UPDATED_VERTICES, 0, dba);
  CheckTypedValueSize(trigger_context, query::trigger::IdentifierTag::UPDATED_EDGES, 0, dba);
  CheckTypedValueSize(trigger_context, query::trigger::IdentifierTag::UPDATED_OBJECTS, 0, dba);
}

namespace {
void EXPECT_PROP_TRUE(const query::TypedValue &a) {
  EXPECT_TRUE(a.type() == query::TypedValue::Type::Bool && a.ValueBool());
}

void EXPECT_PROP_EQ(const query::TypedValue &a, const query::TypedValue &b) { EXPECT_PROP_TRUE(a == b); }
}  // namespace

// During a transaction, same property for the same object can change multiple times. TriggerContext should ensure
// that only the change on the global value is returned (value before the transaction + latest value after the
// transaction) everything inbetween should be ignored.
TEST_F(TriggerContextTest, GlobalPropertyChange) {
  query::DbAccessor dba{&StartTransaction()};

  auto v = dba.InsertVertex();
  dba.AdvanceCommand();

  {
    SPDLOG_DEBUG("SET -> SET");
    query::TriggerContext trigger_context;
    trigger_context.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("Value"),
                                              query::TypedValue("ValueNew"));
    trigger_context.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("ValueNew"),
                                              query::TypedValue("ValueNewer"));
    auto updated_vertices = trigger_context.GetTypedValue(query::trigger::IdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 1);
    auto &update = updated_vertices_list[0];
    ASSERT_TRUE(update.IsMap());
    EXPECT_PROP_EQ(update, query::TypedValue{std::map<std::string, query::TypedValue>{
                               {"event_type", query::TypedValue{"set_vertex_property"}},
                               {"vertex", query::TypedValue{v}},
                               {"key", query::TypedValue{"PROPERTY"}},
                               {"old", query::TypedValue{"Value"}},
                               {"new", query::TypedValue{"ValueNewer"}}}});
  }

  {
    SPDLOG_DEBUG("SET -> REMOVE");
    query::TriggerContext trigger_context;
    trigger_context.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("Value"),
                                              query::TypedValue("ValueNew"));
    trigger_context.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("ValueNew"));
    auto updated_vertices = trigger_context.GetTypedValue(query::trigger::IdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 1);
    auto &update = updated_vertices_list[0];
    ASSERT_TRUE(update.IsMap());
    EXPECT_PROP_EQ(update, query::TypedValue{std::map<std::string, query::TypedValue>{
                               {"event_type", query::TypedValue{"removed_vertex_property"}},
                               {"vertex", query::TypedValue{v}},
                               {"key", query::TypedValue{"PROPERTY"}},
                               {"old", query::TypedValue{"Value"}}}});
  }

  {
    SPDLOG_DEBUG("REMOVE -> SET");
    query::TriggerContext trigger_context;
    trigger_context.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("Value"));
    trigger_context.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue(),
                                              query::TypedValue("ValueNew"));
    auto updated_vertices = trigger_context.GetTypedValue(query::trigger::IdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 1);
    auto &update = updated_vertices_list[0];
    ASSERT_TRUE(update.IsMap());
    EXPECT_PROP_EQ(update, query::TypedValue{std::map<std::string, query::TypedValue>{
                               {"event_type", query::TypedValue{"set_vertex_property"}},
                               {"vertex", query::TypedValue{v}},
                               {"key", query::TypedValue{"PROPERTY"}},
                               {"old", query::TypedValue{"Value"}},
                               {"new", query::TypedValue{"ValueNew"}}}});
  }

  {
    SPDLOG_DEBUG("REMOVE -> REMOVE");
    query::TriggerContext trigger_context;
    trigger_context.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("Value"));
    trigger_context.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue());
    auto updated_vertices = trigger_context.GetTypedValue(query::trigger::IdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 1);
    auto &update = updated_vertices_list[0];
    ASSERT_TRUE(update.IsMap());
    EXPECT_PROP_EQ(update, query::TypedValue{std::map<std::string, query::TypedValue>{
                               {"event_type", query::TypedValue{"removed_vertex_property"}},
                               {"vertex", query::TypedValue{v}},
                               {"key", query::TypedValue{"PROPERTY"}},
                               {"old", query::TypedValue{"Value"}}}});
  }

  {
    SPDLOG_DEBUG("SET -> SET (no change on transaction level)");
    query::TriggerContext trigger_context;
    trigger_context.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("Value"),
                                              query::TypedValue("ValueNew"));
    trigger_context.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("ValueNew"),
                                              query::TypedValue("Value"));
    auto updated_vertices = trigger_context.GetTypedValue(query::trigger::IdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("SET -> REMOVE (no change on transaction level)");
    query::TriggerContext trigger_context;
    trigger_context.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue(),
                                              query::TypedValue("ValueNew"));
    trigger_context.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("ValueNew"));
    auto updated_vertices = trigger_context.GetTypedValue(query::trigger::IdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("REMOVE -> SET (no change on transaction level)");
    query::TriggerContext trigger_context;
    trigger_context.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("Value"));
    trigger_context.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue(),
                                              query::TypedValue("Value"));
    auto updated_vertices = trigger_context.GetTypedValue(query::trigger::IdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("REMOVE -> REMOVE (no change on transaction level)");
    query::TriggerContext trigger_context;
    trigger_context.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue());
    trigger_context.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue());
    auto updated_vertices = trigger_context.GetTypedValue(query::trigger::IdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("SET -> REMOVE -> SET -> REMOVE -> SET");
    query::TriggerContext trigger_context;
    trigger_context.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("Value0"),
                                              query::TypedValue("Value1"));
    trigger_context.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("Value1"));
    trigger_context.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue(),
                                              query::TypedValue("Value2"));
    trigger_context.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("Value2"));
    trigger_context.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue(),
                                              query::TypedValue("Value3"));
    auto updated_vertices = trigger_context.GetTypedValue(query::trigger::IdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 1);
    auto &update = updated_vertices_list[0];
    ASSERT_TRUE(update.IsMap());
    EXPECT_PROP_EQ(update, query::TypedValue{std::map<std::string, query::TypedValue>{
                               {"event_type", query::TypedValue{"set_vertex_property"}},
                               {"vertex", query::TypedValue{v}},
                               {"key", query::TypedValue{"PROPERTY"}},
                               {"old", query::TypedValue{"Value0"}},
                               {"new", query::TypedValue{"Value3"}}}});
  }
}

// Same as above, but for label changes
TEST_F(TriggerContextTest, GlobalLabelChange) {
  query::DbAccessor dba{&StartTransaction()};

  auto v = dba.InsertVertex();
  dba.AdvanceCommand();

  const auto label_id = dba.NameToLabel("LABEL");
  // You cannot add the same label multiple times and you cannot remove non existing labels
  // so REMOVE -> REMOVE and SET -> SET doesn't make sense
  {
    SPDLOG_DEBUG("SET -> REMOVE");
    query::TriggerContext trigger_context;
    trigger_context.RegisterSetVertexLabel(v, label_id);
    trigger_context.RegisterRemovedVertexLabel(v, label_id);
    auto updated_vertices = trigger_context.GetTypedValue(query::trigger::IdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("REMOVE -> SET");
    query::TriggerContext trigger_context;
    trigger_context.RegisterRemovedVertexLabel(v, label_id);
    trigger_context.RegisterSetVertexLabel(v, label_id);
    auto updated_vertices = trigger_context.GetTypedValue(query::trigger::IdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("SET -> REMOVE -> SET -> REMOVE -> SET");
    query::TriggerContext trigger_context;
    trigger_context.RegisterSetVertexLabel(v, label_id);
    trigger_context.RegisterRemovedVertexLabel(v, label_id);
    trigger_context.RegisterSetVertexLabel(v, label_id);
    trigger_context.RegisterRemovedVertexLabel(v, label_id);
    trigger_context.RegisterSetVertexLabel(v, label_id);
    auto updated_vertices = trigger_context.GetTypedValue(query::trigger::IdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 1);
    auto &update = updated_vertices_list[0];
    ASSERT_TRUE(update.IsMap());
    EXPECT_PROP_EQ(update, query::TypedValue{std::map<std::string, query::TypedValue>{
                               {"event_type", query::TypedValue{"set_vertex_label"}},
                               {"vertex", query::TypedValue{v}},
                               {"label", query::TypedValue{"LABEL"}}}});
  }

  {
    SPDLOG_DEBUG("REMOVE -> SET -> REMOVE -> SET -> REMOVE");
    query::TriggerContext trigger_context;
    trigger_context.RegisterRemovedVertexLabel(v, label_id);
    trigger_context.RegisterSetVertexLabel(v, label_id);
    trigger_context.RegisterRemovedVertexLabel(v, label_id);
    trigger_context.RegisterSetVertexLabel(v, label_id);
    trigger_context.RegisterRemovedVertexLabel(v, label_id);
    auto updated_vertices = trigger_context.GetTypedValue(query::trigger::IdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 1);
    auto &update = updated_vertices_list[0];
    ASSERT_TRUE(update.IsMap());
    EXPECT_PROP_EQ(update, query::TypedValue{std::map<std::string, query::TypedValue>{
                               {"event_type", query::TypedValue{"removed_vertex_label"}},
                               {"vertex", query::TypedValue{v}},
                               {"label", query::TypedValue{"LABEL"}}}});
  }
}

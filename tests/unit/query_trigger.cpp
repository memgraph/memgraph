#include <gtest/gtest.h>
#include <filesystem>

#include "query/db_accessor.hpp"
#include "query/interpreter.hpp"
#include "query/trigger.hpp"
#include "query/typed_value.hpp"
#include "utils/memory.hpp"

namespace {
const std::unordered_set<query::TriggerEventType> kAllEventTypes{
    query::TriggerEventType::ANY,    query::TriggerEventType::VERTEX_CREATE, query::TriggerEventType::EDGE_CREATE,
    query::TriggerEventType::CREATE, query::TriggerEventType::VERTEX_DELETE, query::TriggerEventType::EDGE_DELETE,
    query::TriggerEventType::DELETE, query::TriggerEventType::VERTEX_UPDATE, query::TriggerEventType::EDGE_UPDATE,
    query::TriggerEventType::UPDATE,
};
}  // namespace

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
void CheckTypedValueSize(const query::TriggerContext &trigger_context, const query::TriggerIdentifierTag tag,
                         const size_t expected_size, query::DbAccessor &dba) {
  auto typed_values = trigger_context.GetTypedValue(tag, &dba);
  ASSERT_TRUE(typed_values.IsList());
  ASSERT_EQ(typed_values.ValueList().size(), expected_size);
};

void CheckLabelList(const query::TriggerContext &trigger_context, const query::TriggerIdentifierTag tag,
                    const size_t expected, query::DbAccessor &dba) {
  auto typed_values = trigger_context.GetTypedValue(tag, &dba);
  ASSERT_TRUE(typed_values.IsList());
  const auto &label_maps = typed_values.ValueList();
  size_t value_count = 0;
  for (const auto &label_map : label_maps) {
    ASSERT_TRUE(label_map.IsMap());
    const auto &typed_values_map = label_map.ValueMap();
    ASSERT_EQ(typed_values_map.size(), 2);
    const auto label_it = typed_values_map.find("label");
    ASSERT_NE(label_it, typed_values_map.end());
    ASSERT_TRUE(label_it->second.IsString());
    const auto vertices_it = typed_values_map.find("vertices");
    ASSERT_NE(vertices_it, typed_values_map.end());
    ASSERT_TRUE(vertices_it->second.IsList());
    value_count += vertices_it->second.ValueList().size();
  }
  ASSERT_EQ(value_count, expected);
};
}  // namespace

// Ensure that TriggerContext returns only valid objects.
// Returned TypedValue should always contain only objects
// that exist (unless its explicitly created for the deleted object)
TEST_F(TriggerContextTest, ValidObjectsTest) {
  query::TriggerContext trigger_context;
  query::TriggerContextCollector trigger_context_collector{kAllEventTypes};

  size_t vertex_count = 0;
  size_t edge_count = 0;
  {
    query::DbAccessor dba{&StartTransaction()};

    auto create_vertex = [&] {
      auto created_vertex = dba.InsertVertex();
      trigger_context_collector.RegisterCreatedObject(created_vertex);
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
      trigger_context_collector.RegisterCreatedObject(*maybe_edge);
      ++edge_count;
    };

    // Create edges and add them to the trigger context as created
    create_edge(vertices[0], vertices[1]);
    create_edge(vertices[1], vertices[2]);
    create_edge(vertices[2], vertices[3]);

    dba.AdvanceCommand();
    trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    trigger_context_collector = query::TriggerContextCollector{kAllEventTypes};

    // Should have all the created objects
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::CREATED_VERTICES, vertex_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::CREATED_EDGES, edge_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::CREATED_OBJECTS, vertex_count + edge_count, dba);

    // we delete one of the vertices and edges in the same transaction
    ASSERT_TRUE(dba.DetachRemoveVertex(&vertices[0]).HasValue());
    --vertex_count;
    --edge_count;

    dba.AdvanceCommand();

    // Should have one less created object for vertex and edge
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::CREATED_VERTICES, vertex_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::CREATED_EDGES, edge_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::CREATED_OBJECTS, vertex_count + edge_count, dba);

    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    query::DbAccessor dba{&StartTransaction()};
    trigger_context.AdaptForAccessor(&dba);

    // Should have one less created object for vertex and edge
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::CREATED_VERTICES, vertex_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::CREATED_EDGES, edge_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::CREATED_OBJECTS, vertex_count + edge_count, dba);
  }

  size_t deleted_vertex_count = 0;
  size_t deleted_edge_count = 0;
  {
    query::DbAccessor dba{&StartTransaction()};

    // register each type of change for each object
    {
      auto vertices = dba.Vertices(storage::View::OLD);
      for (auto vertex : vertices) {
        trigger_context_collector.RegisterSetObjectProperty(vertex, dba.NameToProperty("PROPERTY1"),
                                                            query::TypedValue("Value"), query::TypedValue("ValueNew"));
        trigger_context_collector.RegisterRemovedObjectProperty(vertex, dba.NameToProperty("PROPERTY2"),
                                                                query::TypedValue("Value"));
        trigger_context_collector.RegisterSetVertexLabel(vertex, dba.NameToLabel("LABEL1"));
        trigger_context_collector.RegisterRemovedVertexLabel(vertex, dba.NameToLabel("LABEL2"));

        auto out_edges = vertex.OutEdges(storage::View::OLD);
        ASSERT_TRUE(out_edges.HasValue());

        for (auto edge : *out_edges) {
          trigger_context_collector.RegisterSetObjectProperty(
              edge, dba.NameToProperty("PROPERTY1"), query::TypedValue("Value"), query::TypedValue("ValueNew"));
          trigger_context_collector.RegisterRemovedObjectProperty(edge, dba.NameToProperty("PROPERTY2"),
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

        trigger_context_collector.RegisterDeletedObject(deleted_vertex);
        ++deleted_vertex_count;
        --vertex_count;
        for (const auto &edge : deleted_edges) {
          trigger_context_collector.RegisterDeletedObject(edge);
          ++deleted_edge_count;
          --edge_count;
        }

        break;
      }
    }

    dba.AdvanceCommand();
    ASSERT_FALSE(dba.Commit().HasError());

    trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    trigger_context_collector = query::TriggerContextCollector{kAllEventTypes};

    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::SET_VERTEX_PROPERTIES, vertex_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::SET_EDGE_PROPERTIES, edge_count, dba);

    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::REMOVED_VERTEX_PROPERTIES, vertex_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::REMOVED_EDGE_PROPERTIES, edge_count, dba);

    CheckLabelList(trigger_context, query::TriggerIdentifierTag::SET_VERTEX_LABELS, vertex_count, dba);
    CheckLabelList(trigger_context, query::TriggerIdentifierTag::REMOVED_VERTEX_LABELS, vertex_count, dba);

    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::UPDATED_VERTICES, 4 * vertex_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::UPDATED_EDGES, 2 * edge_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::UPDATED_OBJECTS,
                        4 * vertex_count + 2 * edge_count, dba);

    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::DELETED_VERTICES, deleted_vertex_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::DELETED_EDGES, deleted_edge_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::DELETED_OBJECTS,
                        deleted_vertex_count + deleted_edge_count, dba);
  }

  // delete a single vertex with its edges, it should reduce number of typed values returned by the trigger context
  // for each update event.
  // TypedValue of the deleted objects stay the same as they're bound to the transaction which deleted them.
  {
    query::DbAccessor dba{&StartTransaction()};
    trigger_context.AdaptForAccessor(&dba);

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
    trigger_context.AdaptForAccessor(&dba);

    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::SET_VERTEX_PROPERTIES, vertex_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::SET_EDGE_PROPERTIES, edge_count, dba);

    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::REMOVED_VERTEX_PROPERTIES, vertex_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::REMOVED_EDGE_PROPERTIES, edge_count, dba);

    CheckLabelList(trigger_context, query::TriggerIdentifierTag::SET_VERTEX_LABELS, vertex_count, dba);
    CheckLabelList(trigger_context, query::TriggerIdentifierTag::REMOVED_VERTEX_LABELS, vertex_count, dba);

    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::UPDATED_VERTICES, 4 * vertex_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::UPDATED_EDGES, 2 * edge_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::UPDATED_OBJECTS,
                        4 * vertex_count + 2 * edge_count, dba);

    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::DELETED_VERTICES, deleted_vertex_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::DELETED_EDGES, deleted_edge_count, dba);
    CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::DELETED_OBJECTS,
                        deleted_vertex_count + deleted_edge_count, dba);
  }
}

// If the trigger context registered a created object, each future event on the same object will be ignored.
// Binding the trigger context to transaction will mean that creating and updating an object in the same transaction
// will return only the CREATE event.
TEST_F(TriggerContextTest, ReturnCreateOnlyEvent) {
  query::TriggerContextCollector trigger_context_collector{kAllEventTypes};

  query::DbAccessor dba{&StartTransaction()};

  auto create_vertex = [&] {
    auto vertex = dba.InsertVertex();
    trigger_context_collector.RegisterCreatedObject(vertex);
    trigger_context_collector.RegisterSetObjectProperty(vertex, dba.NameToProperty("PROPERTY1"),
                                                        query::TypedValue("Value"), query::TypedValue("ValueNew"));
    trigger_context_collector.RegisterRemovedObjectProperty(vertex, dba.NameToProperty("PROPERTY2"),
                                                            query::TypedValue("Value"));
    trigger_context_collector.RegisterSetVertexLabel(vertex, dba.NameToLabel("LABEL1"));
    trigger_context_collector.RegisterRemovedVertexLabel(vertex, dba.NameToLabel("LABEL2"));
    return vertex;
  };

  auto v1 = create_vertex();
  auto v2 = create_vertex();
  auto maybe_edge = dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("EDGE"));
  ASSERT_FALSE(maybe_edge.HasError());
  trigger_context_collector.RegisterCreatedObject(*maybe_edge);
  trigger_context_collector.RegisterSetObjectProperty(*maybe_edge, dba.NameToProperty("PROPERTY1"),
                                                      query::TypedValue("Value"), query::TypedValue("ValueNew"));
  trigger_context_collector.RegisterRemovedObjectProperty(*maybe_edge, dba.NameToProperty("PROPERTY2"),
                                                          query::TypedValue("Value"));

  dba.AdvanceCommand();

  const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();

  CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::CREATED_VERTICES, 2, dba);
  CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::CREATED_EDGES, 1, dba);
  CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::CREATED_OBJECTS, 3, dba);

  CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::SET_VERTEX_PROPERTIES, 0, dba);
  CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::SET_EDGE_PROPERTIES, 0, dba);

  CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::REMOVED_VERTEX_PROPERTIES, 0, dba);
  CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::REMOVED_EDGE_PROPERTIES, 0, dba);

  CheckLabelList(trigger_context, query::TriggerIdentifierTag::SET_VERTEX_LABELS, 0, dba);
  CheckLabelList(trigger_context, query::TriggerIdentifierTag::REMOVED_VERTEX_LABELS, 0, dba);

  CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::UPDATED_VERTICES, 0, dba);
  CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::UPDATED_EDGES, 0, dba);
  CheckTypedValueSize(trigger_context, query::TriggerIdentifierTag::UPDATED_OBJECTS, 0, dba);
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
  const std::unordered_set<query::TriggerEventType> event_types{query::TriggerEventType::VERTEX_UPDATE};

  auto v = dba.InsertVertex();
  dba.AdvanceCommand();

  {
    SPDLOG_DEBUG("SET -> SET");
    query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("Value"),
                                                        query::TypedValue("ValueNew"));
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                        query::TypedValue("ValueNew"), query::TypedValue("ValueNewer"));
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices = trigger_context.GetTypedValue(query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
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
    query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("Value"),
                                                        query::TypedValue("ValueNew"));
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            query::TypedValue("ValueNew"));
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices = trigger_context.GetTypedValue(query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
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
    query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            query::TypedValue("Value"));
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue(),
                                                        query::TypedValue("ValueNew"));
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices = trigger_context.GetTypedValue(query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
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
    query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            query::TypedValue("Value"));
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue());
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices = trigger_context.GetTypedValue(query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
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
    query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("Value"),
                                                        query::TypedValue("ValueNew"));
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                        query::TypedValue("ValueNew"), query::TypedValue("Value"));
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices = trigger_context.GetTypedValue(query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("SET -> REMOVE (no change on transaction level)");
    query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue(),
                                                        query::TypedValue("ValueNew"));
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            query::TypedValue("ValueNew"));
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices = trigger_context.GetTypedValue(query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("REMOVE -> SET (no change on transaction level)");
    query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            query::TypedValue("Value"));
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue(),
                                                        query::TypedValue("Value"));
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices = trigger_context.GetTypedValue(query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("REMOVE -> REMOVE (no change on transaction level)");
    query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue());
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue());
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices = trigger_context.GetTypedValue(query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("SET -> REMOVE -> SET -> REMOVE -> SET");
    query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue("Value0"),
                                                        query::TypedValue("Value1"));
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            query::TypedValue("Value1"));
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue(),
                                                        query::TypedValue("Value2"));
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            query::TypedValue("Value2"));
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"), query::TypedValue(),
                                                        query::TypedValue("Value3"));
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices = trigger_context.GetTypedValue(query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
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
  const std::unordered_set<query::TriggerEventType> event_types{query::TriggerEventType::VERTEX_UPDATE};

  auto v = dba.InsertVertex();
  dba.AdvanceCommand();

  const auto label_id = dba.NameToLabel("LABEL");
  // You cannot add the same label multiple times and you cannot remove non existing labels
  // so REMOVE -> REMOVE and SET -> SET doesn't make sense
  {
    SPDLOG_DEBUG("SET -> REMOVE");
    query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterSetVertexLabel(v, label_id);
    trigger_context_collector.RegisterRemovedVertexLabel(v, label_id);
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices = trigger_context.GetTypedValue(query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("REMOVE -> SET");
    query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterRemovedVertexLabel(v, label_id);
    trigger_context_collector.RegisterSetVertexLabel(v, label_id);
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices = trigger_context.GetTypedValue(query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("SET -> REMOVE -> SET -> REMOVE -> SET");
    query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterSetVertexLabel(v, label_id);
    trigger_context_collector.RegisterRemovedVertexLabel(v, label_id);
    trigger_context_collector.RegisterSetVertexLabel(v, label_id);
    trigger_context_collector.RegisterRemovedVertexLabel(v, label_id);
    trigger_context_collector.RegisterSetVertexLabel(v, label_id);
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices = trigger_context.GetTypedValue(query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
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
    query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterRemovedVertexLabel(v, label_id);
    trigger_context_collector.RegisterSetVertexLabel(v, label_id);
    trigger_context_collector.RegisterRemovedVertexLabel(v, label_id);
    trigger_context_collector.RegisterSetVertexLabel(v, label_id);
    trigger_context_collector.RegisterRemovedVertexLabel(v, label_id);
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices = trigger_context.GetTypedValue(query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
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

class TriggerStoreTest : public ::testing::Test {
 protected:
  const std::filesystem::path testing_directory{std::filesystem::temp_directory_path() / "MG_test_unit_query_trigger"};

  void SetUp() override {
    Clear();

    storage_accessor.emplace(storage.Access());
    dba.emplace(&*storage_accessor);
  }

  void TearDown() override {
    Clear();

    dba.reset();
    storage_accessor.reset();
  }

  std::optional<query::DbAccessor> dba;

  utils::SkipList<query::QueryCacheEntry> ast_cache;
  utils::SpinLock antlr_lock;

 private:
  void Clear() {
    if (!std::filesystem::exists(testing_directory)) return;
    std::filesystem::remove_all(testing_directory);
  }

  storage::Storage storage;
  std::optional<storage::Storage::Accessor> storage_accessor;
};

TEST_F(TriggerStoreTest, Load) {
  std::optional<query::TriggerStore> store;

  store.emplace(testing_directory, &ast_cache, &*dba, &antlr_lock);

  const auto check_empty = [&] {
    ASSERT_EQ(store->GetTriggerInfo().size(), 0);
    ASSERT_EQ(store->BeforeCommitTriggers().size(), 0);
    ASSERT_EQ(store->AfterCommitTriggers().size(), 0);
  };

  check_empty();

  const auto *trigger_name_before = "trigger";
  const auto *trigger_name_after = "trigger_after";
  const auto *trigger_statement = "RETURN $parameter";
  const auto event_type = query::TriggerEventType::VERTEX_CREATE;
  store->AddTrigger(trigger_name_before, trigger_statement,
                    std::map<std::string, storage::PropertyValue>{{"parameter", storage::PropertyValue{1}}}, event_type,
                    query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba, &antlr_lock);
  store->AddTrigger(trigger_name_after, trigger_statement,
                    std::map<std::string, storage::PropertyValue>{{"parameter", storage::PropertyValue{"value"}}},
                    event_type, query::TriggerPhase::AFTER_COMMIT, &ast_cache, &*dba, &antlr_lock);

  const auto check_triggers = [&] {
    ASSERT_EQ(store->GetTriggerInfo().size(), 2);

    const auto verify_trigger = [&](const auto &trigger, const auto &name) {
      ASSERT_EQ(trigger.Name(), name);
      ASSERT_EQ(trigger.OriginalStatement(), trigger_statement);
      ASSERT_EQ(trigger.EventType(), event_type);
    };

    const auto before_commit_triggers = store->BeforeCommitTriggers().access();
    ASSERT_EQ(before_commit_triggers.size(), 1);
    for (const auto &trigger : before_commit_triggers) {
      verify_trigger(trigger, trigger_name_before);
    }

    const auto after_commit_triggers = store->AfterCommitTriggers().access();
    ASSERT_EQ(after_commit_triggers.size(), 1);
    for (const auto &trigger : after_commit_triggers) {
      verify_trigger(trigger, trigger_name_after);
    }
  };

  check_triggers();

  // recreate trigger store, this should reload everything from the disk
  store.emplace(testing_directory, &ast_cache, &*dba, &antlr_lock);
  check_triggers();

  ASSERT_NO_THROW(store->DropTrigger(trigger_name_after));
  ASSERT_NO_THROW(store->DropTrigger(trigger_name_before));

  check_empty();

  store.emplace(testing_directory, &ast_cache, &*dba, &antlr_lock);

  check_empty();
}

TEST_F(TriggerStoreTest, AddTrigger) {
  query::TriggerStore store{testing_directory, &ast_cache, &*dba, &antlr_lock};

  // Invalid query in statements
  ASSERT_THROW(store.AddTrigger("trigger", "RETUR 1", {}, query::TriggerEventType::VERTEX_CREATE,
                                query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba, &antlr_lock),
               utils::BasicException);
  ASSERT_THROW(store.AddTrigger("trigger", "RETURN createdEdges", {}, query::TriggerEventType::VERTEX_CREATE,
                                query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba, &antlr_lock),
               utils::BasicException);

  ASSERT_THROW(store.AddTrigger("trigger", "RETURN $parameter", {}, query::TriggerEventType::VERTEX_CREATE,
                                query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba, &antlr_lock),
               utils::BasicException);

  ASSERT_NO_THROW(store.AddTrigger(
      "trigger", "RETURN $parameter",
      std::map<std::string, storage::PropertyValue>{{"parameter", storage::PropertyValue{1}}},
      query::TriggerEventType::VERTEX_CREATE, query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba, &antlr_lock));

  // Inserting with the same name
  ASSERT_THROW(store.AddTrigger("trigger", "RETURN 1", {}, query::TriggerEventType::VERTEX_CREATE,
                                query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba, &antlr_lock),
               utils::BasicException);
  ASSERT_THROW(store.AddTrigger("trigger", "RETURN 1", {}, query::TriggerEventType::VERTEX_CREATE,
                                query::TriggerPhase::AFTER_COMMIT, &ast_cache, &*dba, &antlr_lock),
               utils::BasicException);

  ASSERT_EQ(store.GetTriggerInfo().size(), 1);
  ASSERT_EQ(store.BeforeCommitTriggers().size(), 1);
  ASSERT_EQ(store.AfterCommitTriggers().size(), 0);
}

TEST_F(TriggerStoreTest, DropTrigger) {
  query::TriggerStore store{testing_directory, &ast_cache, &*dba, &antlr_lock};

  ASSERT_THROW(store.DropTrigger("Unknown"), utils::BasicException);

  const auto *trigger_name = "trigger";
  store.AddTrigger(trigger_name, "RETURN 1", {}, query::TriggerEventType::VERTEX_CREATE,
                   query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba, &antlr_lock);

  ASSERT_THROW(store.DropTrigger("Unknown"), utils::BasicException);
  ASSERT_NO_THROW(store.DropTrigger(trigger_name));
  ASSERT_EQ(store.GetTriggerInfo().size(), 0);
}

TEST_F(TriggerStoreTest, TriggerInfo) {
  query::TriggerStore store{testing_directory, &ast_cache, &*dba, &antlr_lock};

  std::vector<query::TriggerStore::TriggerInfo> expected_info;
  store.AddTrigger("trigger", "RETURN 1", {}, query::TriggerEventType::VERTEX_CREATE,
                   query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba, &antlr_lock);
  expected_info.push_back(
      {"trigger", "RETURN 1", query::TriggerEventType::VERTEX_CREATE, query::TriggerPhase::BEFORE_COMMIT});

  const auto check_trigger_info = [&] {
    const auto trigger_info = store.GetTriggerInfo();
    ASSERT_EQ(expected_info.size(), trigger_info.size());
    // ensure all of the expected trigger infos can be found in the retrieved infos
    ASSERT_TRUE(std::all_of(expected_info.begin(), expected_info.end(), [&](const auto &info) {
      return std::find_if(trigger_info.begin(), trigger_info.end(), [&](const auto &other) {
               return info.name == other.name && info.statement == other.statement &&
                      info.event_type == other.event_type && info.phase == other.phase;
             }) != trigger_info.end();
    }));
  };

  check_trigger_info();

  store.AddTrigger("edge_update_trigger", "RETURN 1", {}, query::TriggerEventType::EDGE_UPDATE,
                   query::TriggerPhase::AFTER_COMMIT, &ast_cache, &*dba, &antlr_lock);
  expected_info.push_back(
      {"edge_update_trigger", "RETURN 1", query::TriggerEventType::EDGE_UPDATE, query::TriggerPhase::AFTER_COMMIT});

  check_trigger_info();

  store.DropTrigger("edge_update_trigger");
  const auto erase_from_expected = [&](const std::string_view name) {
    const auto erase_count = std::erase_if(expected_info, [name](const auto &info) { return info.name == name; });
    ASSERT_EQ(erase_count, 1);
  };
  erase_from_expected("edge_update_trigger");

  check_trigger_info();

  store.DropTrigger("trigger");
  erase_from_expected("trigger");

  check_trigger_info();
}

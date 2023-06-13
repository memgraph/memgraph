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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <filesystem>

#include <fmt/format.h>
#include "glue/auth_checker.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpreter.hpp"
#include "query/trigger.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/id_types.hpp"
#include "utils/exceptions.hpp"
#include "utils/memory.hpp"

namespace {
const std::unordered_set<memgraph::query::TriggerEventType> kAllEventTypes{
    memgraph::query::TriggerEventType::ANY,           memgraph::query::TriggerEventType::VERTEX_CREATE,
    memgraph::query::TriggerEventType::EDGE_CREATE,   memgraph::query::TriggerEventType::CREATE,
    memgraph::query::TriggerEventType::VERTEX_DELETE, memgraph::query::TriggerEventType::EDGE_DELETE,
    memgraph::query::TriggerEventType::DELETE,        memgraph::query::TriggerEventType::VERTEX_UPDATE,
    memgraph::query::TriggerEventType::EDGE_UPDATE,   memgraph::query::TriggerEventType::UPDATE,
};

class MockAuthChecker : public memgraph::query::AuthChecker {
 public:
  MOCK_CONST_METHOD2(IsUserAuthorized, bool(const std::optional<std::string> &username,
                                            const std::vector<memgraph::query::AuthQuery::Privilege> &privileges));
#ifdef MG_ENTERPRISE
  MOCK_CONST_METHOD2(GetFineGrainedAuthChecker,
                     std::unique_ptr<memgraph::query::FineGrainedAuthChecker>(
                         const std::string &username, const memgraph::query::DbAccessor *db_accessor));
#endif
};
}  // namespace

class TriggerContextTest : public ::testing::Test {
 public:
  void SetUp() override { db.emplace(); }

  void TearDown() override {
    accessors.clear();
    db.reset();
  }

  memgraph::storage::Storage::Accessor &StartTransaction() {
    accessors.push_back(db->Access());
    return accessors.back();
  }

 protected:
  std::optional<memgraph::storage::Storage> db;
  std::list<memgraph::storage::Storage::Accessor> accessors;
};

namespace {
void CheckTypedValueSize(const memgraph::query::TriggerContext &trigger_context,
                         const memgraph::query::TriggerIdentifierTag tag, const size_t expected_size,
                         memgraph::query::DbAccessor &dba) {
  auto typed_values = trigger_context.GetTypedValue(tag, &dba);
  ASSERT_TRUE(typed_values.IsList());
  ASSERT_EQ(expected_size, typed_values.ValueList().size());
};

void CheckLabelList(const memgraph::query::TriggerContext &trigger_context,
                    const memgraph::query::TriggerIdentifierTag tag, const size_t expected,
                    memgraph::query::DbAccessor &dba) {
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
  memgraph::query::TriggerContext trigger_context;
  memgraph::query::TriggerContextCollector trigger_context_collector{kAllEventTypes};

  size_t vertex_count = 0;
  size_t edge_count = 0;
  {
    memgraph::query::DbAccessor dba{&StartTransaction()};

    auto create_vertex = [&] {
      auto created_vertex = dba.InsertVertex();
      trigger_context_collector.RegisterCreatedObject(created_vertex);
      ++vertex_count;
      return created_vertex;
    };

    // Create vertices and add them to the trigger context as created
    std::vector<memgraph::query::VertexAccessor> vertices;
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
    trigger_context_collector = memgraph::query::TriggerContextCollector{kAllEventTypes};

    // Should have all the created objects
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::CREATED_VERTICES, vertex_count, dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::CREATED_EDGES, edge_count, dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::CREATED_OBJECTS,
                        vertex_count + edge_count, dba);

    // we delete one of the vertices and edges in the same transaction
    ASSERT_TRUE(dba.DetachRemoveVertex(&vertices[0]).HasValue());
    --vertex_count;
    --edge_count;

    dba.AdvanceCommand();

    // Should have one less created object for vertex and edge
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::CREATED_VERTICES, vertex_count, dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::CREATED_EDGES, edge_count, dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::CREATED_OBJECTS,
                        vertex_count + edge_count, dba);

    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    memgraph::query::DbAccessor dba{&StartTransaction()};
    trigger_context.AdaptForAccessor(&dba);

    // Should have one less created object for vertex and edge
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::CREATED_VERTICES, vertex_count, dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::CREATED_EDGES, edge_count, dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::CREATED_OBJECTS,
                        vertex_count + edge_count, dba);
  }

  size_t deleted_vertex_count = 0;
  size_t deleted_edge_count = 0;
  {
    memgraph::query::DbAccessor dba{&StartTransaction()};

    // register each type of change for each object
    {
      auto vertices = dba.Vertices(memgraph::storage::View::OLD);
      for (auto vertex : vertices) {
        trigger_context_collector.RegisterSetObjectProperty(vertex, dba.NameToProperty("PROPERTY1"),
                                                            memgraph::query::TypedValue("Value"),
                                                            memgraph::query::TypedValue("ValueNew"));
        trigger_context_collector.RegisterRemovedObjectProperty(vertex, dba.NameToProperty("PROPERTY2"),
                                                                memgraph::query::TypedValue("Value"));
        trigger_context_collector.RegisterSetVertexLabel(vertex, dba.NameToLabel("LABEL1"));
        trigger_context_collector.RegisterRemovedVertexLabel(vertex, dba.NameToLabel("LABEL2"));

        auto out_edges = vertex.OutEdges(memgraph::storage::View::OLD);
        ASSERT_TRUE(out_edges.HasValue());

        for (auto edge : *out_edges) {
          trigger_context_collector.RegisterSetObjectProperty(edge, dba.NameToProperty("PROPERTY1"),
                                                              memgraph::query::TypedValue("Value"),
                                                              memgraph::query::TypedValue("ValueNew"));
          trigger_context_collector.RegisterRemovedObjectProperty(edge, dba.NameToProperty("PROPERTY2"),
                                                                  memgraph::query::TypedValue("Value"));
        }
      }
    }

    // Delete the first vertex with its edge and register the deleted object
    {
      auto vertices = dba.Vertices(memgraph::storage::View::OLD);
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
    trigger_context_collector = memgraph::query::TriggerContextCollector{kAllEventTypes};

    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::SET_VERTEX_PROPERTIES, vertex_count,
                        dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::SET_EDGE_PROPERTIES, edge_count, dba);

    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::REMOVED_VERTEX_PROPERTIES, vertex_count,
                        dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::REMOVED_EDGE_PROPERTIES, edge_count,
                        dba);

    CheckLabelList(trigger_context, memgraph::query::TriggerIdentifierTag::SET_VERTEX_LABELS, vertex_count, dba);
    CheckLabelList(trigger_context, memgraph::query::TriggerIdentifierTag::REMOVED_VERTEX_LABELS, vertex_count, dba);

    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, 4 * vertex_count,
                        dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::UPDATED_EDGES, 2 * edge_count, dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::UPDATED_OBJECTS,
                        4 * vertex_count + 2 * edge_count, dba);

    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::DELETED_VERTICES, deleted_vertex_count,
                        dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::DELETED_EDGES, deleted_edge_count, dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::DELETED_OBJECTS,
                        deleted_vertex_count + deleted_edge_count, dba);
  }

  // delete a single vertex with its edges, it should reduce number of typed values returned by the trigger context
  // for each update event.
  // TypedValue of the deleted objects stay the same as they're bound to the transaction which deleted them.
  {
    memgraph::query::DbAccessor dba{&StartTransaction()};
    trigger_context.AdaptForAccessor(&dba);

    auto vertices = dba.Vertices(memgraph::storage::View::OLD);
    for (auto vertex : vertices) {
      ASSERT_TRUE(dba.DetachRemoveVertex(&vertex).HasValue());
      break;
    }
    --vertex_count;
    --edge_count;

    ASSERT_FALSE(dba.Commit().HasError());
  }

  {
    memgraph::query::DbAccessor dba{&StartTransaction()};
    trigger_context.AdaptForAccessor(&dba);

    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::SET_VERTEX_PROPERTIES, vertex_count,
                        dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::SET_EDGE_PROPERTIES, edge_count, dba);

    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::REMOVED_VERTEX_PROPERTIES, vertex_count,
                        dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::REMOVED_EDGE_PROPERTIES, edge_count,
                        dba);

    CheckLabelList(trigger_context, memgraph::query::TriggerIdentifierTag::SET_VERTEX_LABELS, vertex_count, dba);
    CheckLabelList(trigger_context, memgraph::query::TriggerIdentifierTag::REMOVED_VERTEX_LABELS, vertex_count, dba);

    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, 4 * vertex_count,
                        dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::UPDATED_EDGES, 2 * edge_count, dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::UPDATED_OBJECTS,
                        4 * vertex_count + 2 * edge_count, dba);

    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::DELETED_VERTICES, deleted_vertex_count,
                        dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::DELETED_EDGES, deleted_edge_count, dba);
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::DELETED_OBJECTS,
                        deleted_vertex_count + deleted_edge_count, dba);
  }
}

// If the trigger context registered a created object, each future event on the same object will be ignored.
// Binding the trigger context to transaction will mean that creating and updating an object in the same transaction
// will return only the CREATE event.
TEST_F(TriggerContextTest, ReturnCreateOnlyEvent) {
  memgraph::query::TriggerContextCollector trigger_context_collector{kAllEventTypes};

  memgraph::query::DbAccessor dba{&StartTransaction()};

  auto create_vertex = [&] {
    auto vertex = dba.InsertVertex();
    trigger_context_collector.RegisterCreatedObject(vertex);
    trigger_context_collector.RegisterSetObjectProperty(vertex, dba.NameToProperty("PROPERTY1"),
                                                        memgraph::query::TypedValue("Value"),
                                                        memgraph::query::TypedValue("ValueNew"));
    trigger_context_collector.RegisterRemovedObjectProperty(vertex, dba.NameToProperty("PROPERTY2"),
                                                            memgraph::query::TypedValue("Value"));
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
                                                      memgraph::query::TypedValue("Value"),
                                                      memgraph::query::TypedValue("ValueNew"));
  trigger_context_collector.RegisterRemovedObjectProperty(*maybe_edge, dba.NameToProperty("PROPERTY2"),
                                                          memgraph::query::TypedValue("Value"));

  dba.AdvanceCommand();

  const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();

  CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::CREATED_VERTICES, 2, dba);
  CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::CREATED_EDGES, 1, dba);
  CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::CREATED_OBJECTS, 3, dba);

  CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::SET_VERTEX_PROPERTIES, 0, dba);
  CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::SET_EDGE_PROPERTIES, 0, dba);

  CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::REMOVED_VERTEX_PROPERTIES, 0, dba);
  CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::REMOVED_EDGE_PROPERTIES, 0, dba);

  CheckLabelList(trigger_context, memgraph::query::TriggerIdentifierTag::SET_VERTEX_LABELS, 0, dba);
  CheckLabelList(trigger_context, memgraph::query::TriggerIdentifierTag::REMOVED_VERTEX_LABELS, 0, dba);

  CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, 0, dba);
  CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::UPDATED_EDGES, 0, dba);
  CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::UPDATED_OBJECTS, 0, dba);
}

namespace {
void EXPECT_PROP_TRUE(const memgraph::query::TypedValue &a) {
  EXPECT_TRUE(a.type() == memgraph::query::TypedValue::Type::Bool && a.ValueBool());
}

void EXPECT_PROP_EQ(const memgraph::query::TypedValue &a, const memgraph::query::TypedValue &b) {
  EXPECT_PROP_TRUE(a == b);
}
}  // namespace

// During a transaction, same property for the same object can change multiple times. TriggerContext should ensure
// that only the change on the global value is returned (value before the transaction + latest value after the
// transaction) everything inbetween should be ignored.
TEST_F(TriggerContextTest, GlobalPropertyChange) {
  memgraph::query::DbAccessor dba{&StartTransaction()};
  const std::unordered_set<memgraph::query::TriggerEventType> event_types{
      memgraph::query::TriggerEventType::VERTEX_UPDATE};

  auto v = dba.InsertVertex();
  dba.AdvanceCommand();

  {
    SPDLOG_DEBUG("SET -> SET");
    memgraph::query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                        memgraph::query::TypedValue("Value"),
                                                        memgraph::query::TypedValue("ValueNew"));
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                        memgraph::query::TypedValue("ValueNew"),
                                                        memgraph::query::TypedValue("ValueNewer"));
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices =
        trigger_context.GetTypedValue(memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 1);
    auto &update = updated_vertices_list[0];
    ASSERT_TRUE(update.IsMap());
    EXPECT_PROP_EQ(update, memgraph::query::TypedValue{std::map<std::string, memgraph::query::TypedValue>{
                               {"event_type", memgraph::query::TypedValue{"set_vertex_property"}},
                               {"vertex", memgraph::query::TypedValue{v}},
                               {"key", memgraph::query::TypedValue{"PROPERTY"}},
                               {"old", memgraph::query::TypedValue{"Value"}},
                               {"new", memgraph::query::TypedValue{"ValueNewer"}}}});
  }

  {
    SPDLOG_DEBUG("SET -> REMOVE");
    memgraph::query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                        memgraph::query::TypedValue("Value"),
                                                        memgraph::query::TypedValue("ValueNew"));
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            memgraph::query::TypedValue("ValueNew"));
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices =
        trigger_context.GetTypedValue(memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 1);
    auto &update = updated_vertices_list[0];
    ASSERT_TRUE(update.IsMap());
    EXPECT_PROP_EQ(update, memgraph::query::TypedValue{std::map<std::string, memgraph::query::TypedValue>{
                               {"event_type", memgraph::query::TypedValue{"removed_vertex_property"}},
                               {"vertex", memgraph::query::TypedValue{v}},
                               {"key", memgraph::query::TypedValue{"PROPERTY"}},
                               {"old", memgraph::query::TypedValue{"Value"}}}});
  }

  {
    SPDLOG_DEBUG("REMOVE -> SET");
    memgraph::query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            memgraph::query::TypedValue("Value"));
    trigger_context_collector.RegisterSetObjectProperty(
        v, dba.NameToProperty("PROPERTY"), memgraph::query::TypedValue(), memgraph::query::TypedValue("ValueNew"));
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices =
        trigger_context.GetTypedValue(memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 1);
    auto &update = updated_vertices_list[0];
    ASSERT_TRUE(update.IsMap());
    EXPECT_PROP_EQ(update, memgraph::query::TypedValue{std::map<std::string, memgraph::query::TypedValue>{
                               {"event_type", memgraph::query::TypedValue{"set_vertex_property"}},
                               {"vertex", memgraph::query::TypedValue{v}},
                               {"key", memgraph::query::TypedValue{"PROPERTY"}},
                               {"old", memgraph::query::TypedValue{"Value"}},
                               {"new", memgraph::query::TypedValue{"ValueNew"}}}});
  }

  {
    SPDLOG_DEBUG("REMOVE -> REMOVE");
    memgraph::query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            memgraph::query::TypedValue("Value"));
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            memgraph::query::TypedValue());
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices =
        trigger_context.GetTypedValue(memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 1);
    auto &update = updated_vertices_list[0];
    ASSERT_TRUE(update.IsMap());
    EXPECT_PROP_EQ(update, memgraph::query::TypedValue{std::map<std::string, memgraph::query::TypedValue>{
                               {"event_type", memgraph::query::TypedValue{"removed_vertex_property"}},
                               {"vertex", memgraph::query::TypedValue{v}},
                               {"key", memgraph::query::TypedValue{"PROPERTY"}},
                               {"old", memgraph::query::TypedValue{"Value"}}}});
  }

  {
    SPDLOG_DEBUG("SET -> SET (no change on transaction level)");
    memgraph::query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                        memgraph::query::TypedValue("Value"),
                                                        memgraph::query::TypedValue("ValueNew"));
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                        memgraph::query::TypedValue("ValueNew"),
                                                        memgraph::query::TypedValue("Value"));
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices =
        trigger_context.GetTypedValue(memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("SET -> REMOVE (no change on transaction level)");
    memgraph::query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterSetObjectProperty(
        v, dba.NameToProperty("PROPERTY"), memgraph::query::TypedValue(), memgraph::query::TypedValue("ValueNew"));
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            memgraph::query::TypedValue("ValueNew"));
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices =
        trigger_context.GetTypedValue(memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("REMOVE -> SET (no change on transaction level)");
    memgraph::query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            memgraph::query::TypedValue("Value"));
    trigger_context_collector.RegisterSetObjectProperty(
        v, dba.NameToProperty("PROPERTY"), memgraph::query::TypedValue(), memgraph::query::TypedValue("Value"));
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices =
        trigger_context.GetTypedValue(memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("REMOVE -> REMOVE (no change on transaction level)");
    memgraph::query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            memgraph::query::TypedValue());
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            memgraph::query::TypedValue());
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices =
        trigger_context.GetTypedValue(memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("SET -> REMOVE -> SET -> REMOVE -> SET");
    memgraph::query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterSetObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                        memgraph::query::TypedValue("Value0"),
                                                        memgraph::query::TypedValue("Value1"));
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            memgraph::query::TypedValue("Value1"));
    trigger_context_collector.RegisterSetObjectProperty(
        v, dba.NameToProperty("PROPERTY"), memgraph::query::TypedValue(), memgraph::query::TypedValue("Value2"));
    trigger_context_collector.RegisterRemovedObjectProperty(v, dba.NameToProperty("PROPERTY"),
                                                            memgraph::query::TypedValue("Value2"));
    trigger_context_collector.RegisterSetObjectProperty(
        v, dba.NameToProperty("PROPERTY"), memgraph::query::TypedValue(), memgraph::query::TypedValue("Value3"));
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices =
        trigger_context.GetTypedValue(memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 1);
    auto &update = updated_vertices_list[0];
    ASSERT_TRUE(update.IsMap());
    EXPECT_PROP_EQ(update, memgraph::query::TypedValue{std::map<std::string, memgraph::query::TypedValue>{
                               {"event_type", memgraph::query::TypedValue{"set_vertex_property"}},
                               {"vertex", memgraph::query::TypedValue{v}},
                               {"key", memgraph::query::TypedValue{"PROPERTY"}},
                               {"old", memgraph::query::TypedValue{"Value0"}},
                               {"new", memgraph::query::TypedValue{"Value3"}}}});
  }
}

// Same as above, but for label changes
TEST_F(TriggerContextTest, GlobalLabelChange) {
  memgraph::query::DbAccessor dba{&StartTransaction()};
  const std::unordered_set<memgraph::query::TriggerEventType> event_types{
      memgraph::query::TriggerEventType::VERTEX_UPDATE};

  auto v = dba.InsertVertex();
  dba.AdvanceCommand();

  const auto label_id = dba.NameToLabel("LABEL");
  // You cannot add the same label multiple times and you cannot remove nonexistent labels
  // so REMOVE -> REMOVE and SET -> SET doesn't make sense
  {
    SPDLOG_DEBUG("SET -> REMOVE");
    memgraph::query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterSetVertexLabel(v, label_id);
    trigger_context_collector.RegisterRemovedVertexLabel(v, label_id);
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices =
        trigger_context.GetTypedValue(memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("REMOVE -> SET");
    memgraph::query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterRemovedVertexLabel(v, label_id);
    trigger_context_collector.RegisterSetVertexLabel(v, label_id);
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices =
        trigger_context.GetTypedValue(memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 0);
  }

  {
    SPDLOG_DEBUG("SET -> REMOVE -> SET -> REMOVE -> SET");
    memgraph::query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterSetVertexLabel(v, label_id);
    trigger_context_collector.RegisterRemovedVertexLabel(v, label_id);
    trigger_context_collector.RegisterSetVertexLabel(v, label_id);
    trigger_context_collector.RegisterRemovedVertexLabel(v, label_id);
    trigger_context_collector.RegisterSetVertexLabel(v, label_id);
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices =
        trigger_context.GetTypedValue(memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 1);
    auto &update = updated_vertices_list[0];
    ASSERT_TRUE(update.IsMap());
    EXPECT_PROP_EQ(update, memgraph::query::TypedValue{std::map<std::string, memgraph::query::TypedValue>{
                               {"event_type", memgraph::query::TypedValue{"set_vertex_label"}},
                               {"vertex", memgraph::query::TypedValue{v}},
                               {"label", memgraph::query::TypedValue{"LABEL"}}}});
  }

  {
    SPDLOG_DEBUG("REMOVE -> SET -> REMOVE -> SET -> REMOVE");
    memgraph::query::TriggerContextCollector trigger_context_collector{event_types};
    trigger_context_collector.RegisterRemovedVertexLabel(v, label_id);
    trigger_context_collector.RegisterSetVertexLabel(v, label_id);
    trigger_context_collector.RegisterRemovedVertexLabel(v, label_id);
    trigger_context_collector.RegisterSetVertexLabel(v, label_id);
    trigger_context_collector.RegisterRemovedVertexLabel(v, label_id);
    const auto trigger_context = std::move(trigger_context_collector).TransformToTriggerContext();
    auto updated_vertices =
        trigger_context.GetTypedValue(memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES, &dba);
    ASSERT_TRUE(updated_vertices.IsList());
    auto &updated_vertices_list = updated_vertices.ValueList();
    ASSERT_EQ(updated_vertices_list.size(), 1);
    auto &update = updated_vertices_list[0];
    ASSERT_TRUE(update.IsMap());
    EXPECT_PROP_EQ(update, memgraph::query::TypedValue{std::map<std::string, memgraph::query::TypedValue>{
                               {"event_type", memgraph::query::TypedValue{"removed_vertex_label"}},
                               {"vertex", memgraph::query::TypedValue{v}},
                               {"label", memgraph::query::TypedValue{"LABEL"}}}});
  }
}

namespace {
struct ShouldRegisterExpectation {
  bool creation{false};
  bool deletion{false};
  bool update{false};
};

template <typename TAccessor>
void CheckRegisterInfo(const memgraph::query::TriggerContextCollector &collector,
                       const ShouldRegisterExpectation &expectation) {
  EXPECT_EQ(expectation.creation, collector.ShouldRegisterCreatedObject<TAccessor>());
  EXPECT_EQ(expectation.deletion, collector.ShouldRegisterDeletedObject<TAccessor>());
  EXPECT_EQ(expectation.update, collector.ShouldRegisterObjectPropertyChange<TAccessor>());
}

size_t BoolToSize(const bool value) { return value ? 1 : 0; }

void CheckFilters(const std::unordered_set<memgraph::query::TriggerEventType> &event_types,
                  const ShouldRegisterExpectation &vertex_expectation,
                  const ShouldRegisterExpectation &edge_expectation, memgraph::storage::Storage::Accessor *accessor) {
  memgraph::query::TriggerContextCollector collector{event_types};
  {
    SCOPED_TRACE("Checking vertex");
    CheckRegisterInfo<memgraph::query::VertexAccessor>(collector, vertex_expectation);
  }
  {
    SCOPED_TRACE("Checking edge");
    CheckRegisterInfo<memgraph::query::EdgeAccessor>(collector, edge_expectation);
  }
  EXPECT_EQ(collector.ShouldRegisterVertexLabelChange(), vertex_expectation.update);

  memgraph::query::DbAccessor dba{accessor};

  auto vertex_to_delete = dba.InsertVertex();
  auto vertex_to_modify = dba.InsertVertex();

  auto from_vertex = dba.InsertVertex();
  auto to_vertex = dba.InsertVertex();
  auto maybe_edge_to_delete = dba.InsertEdge(&from_vertex, &to_vertex, dba.NameToEdgeType("EDGE"));
  auto maybe_edge_to_modify = dba.InsertEdge(&from_vertex, &to_vertex, dba.NameToEdgeType("EDGE"));
  auto &edge_to_delete = maybe_edge_to_delete.GetValue();
  auto &edge_to_modify = maybe_edge_to_modify.GetValue();

  dba.AdvanceCommand();

  const auto created_vertex = dba.InsertVertex();
  const auto maybe_created_edge = dba.InsertEdge(&from_vertex, &to_vertex, dba.NameToEdgeType("EDGE"));
  const auto created_edge = maybe_created_edge.GetValue();
  collector.RegisterCreatedObject(created_vertex);
  collector.RegisterCreatedObject(created_edge);
  collector.RegisterDeletedObject(dba.RemoveEdge(&edge_to_delete).GetValue().value());
  collector.RegisterDeletedObject(dba.RemoveVertex(&vertex_to_delete).GetValue().value());
  collector.RegisterSetObjectProperty(vertex_to_modify, dba.NameToProperty("UPDATE"), memgraph::query::TypedValue{1},
                                      memgraph::query::TypedValue{2});
  collector.RegisterRemovedObjectProperty(vertex_to_modify, dba.NameToProperty("REMOVE"),
                                          memgraph::query::TypedValue{1});
  collector.RegisterSetObjectProperty(edge_to_modify, dba.NameToProperty("UPDATE"), memgraph::query::TypedValue{1},
                                      memgraph::query::TypedValue{2});
  collector.RegisterRemovedObjectProperty(edge_to_modify, dba.NameToProperty("REMOVE"), memgraph::query::TypedValue{1});
  collector.RegisterSetVertexLabel(vertex_to_modify, dba.NameToLabel("SET"));
  collector.RegisterRemovedVertexLabel(vertex_to_modify, dba.NameToLabel("REMOVE"));
  dba.AdvanceCommand();

  const auto trigger_context = std::move(collector).TransformToTriggerContext();
  const auto created_vertices = BoolToSize(vertex_expectation.creation);
  {
    SCOPED_TRACE("CREATED_VERTICES");
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::CREATED_VERTICES, created_vertices,
                        dba);
  }
  const auto created_edges = BoolToSize(edge_expectation.creation);
  {
    SCOPED_TRACE("CREATED_EDGES");
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::CREATED_EDGES, created_edges, dba);
  }
  {
    SCOPED_TRACE("CREATED_OBJECTS");
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::CREATED_OBJECTS,
                        created_vertices + created_edges, dba);
  }
  const auto deleted_vertices = BoolToSize(vertex_expectation.deletion);
  {
    SCOPED_TRACE("DELETED_VERTICES");
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::DELETED_VERTICES, deleted_vertices,
                        dba);
  }
  const auto deleted_edges = BoolToSize(edge_expectation.deletion);
  {
    SCOPED_TRACE("DELETED_EDGES");
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::DELETED_EDGES, deleted_edges, dba);
  }
  {
    SCOPED_TRACE("DELETED_OBJECTS");
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::DELETED_OBJECTS,
                        deleted_vertices + deleted_edges, dba);
  }
  {
    SCOPED_TRACE("SET_VERTEX_PROPERTIES");
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::SET_VERTEX_PROPERTIES,
                        BoolToSize(vertex_expectation.update), dba);
  }
  {
    SCOPED_TRACE("SET_EDGE_PROPERTIES");
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::SET_EDGE_PROPERTIES,
                        BoolToSize(edge_expectation.update), dba);
  }
  {
    SCOPED_TRACE("REMOVED_VERTEX_PROPERTIES");
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::REMOVED_VERTEX_PROPERTIES,
                        BoolToSize(vertex_expectation.update), dba);
  }
  {
    SCOPED_TRACE("REMOVED_EDGE_PROPERTIES");
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::REMOVED_EDGE_PROPERTIES,
                        BoolToSize(edge_expectation.update), dba);
  }
  const auto set_and_removed_vertex_props_and_labels = BoolToSize(vertex_expectation.update) * 4;
  {
    SCOPED_TRACE("UPDATED_VERTICES");
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::UPDATED_VERTICES,
                        set_and_removed_vertex_props_and_labels, dba);
  }
  const auto set_and_removed_edge_props = BoolToSize(edge_expectation.update) * 2;
  {
    SCOPED_TRACE("UPDATED_EDGES");
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::UPDATED_EDGES,
                        set_and_removed_edge_props, dba);
  }
  // sum of the previous
  {
    SCOPED_TRACE("UPDATED_OBJECTS");
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::UPDATED_OBJECTS,
                        set_and_removed_vertex_props_and_labels + set_and_removed_edge_props, dba);
  }
  {
    SCOPED_TRACE("SET_VERTEX_LABELS");
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::SET_VERTEX_LABELS,
                        BoolToSize(vertex_expectation.update), dba);
  }
  {
    SCOPED_TRACE("REMOVED_VERTEX_LABELS");
    CheckTypedValueSize(trigger_context, memgraph::query::TriggerIdentifierTag::REMOVED_VERTEX_LABELS,
                        BoolToSize(vertex_expectation.update), dba);
  }

  dba.Abort();
}
}  // namespace

TEST_F(TriggerContextTest, Filtering) {
  using TET = memgraph::query::TriggerEventType;
  // Check all event type individually
  {
    SCOPED_TRACE("TET::ANY");
    CheckFilters({TET::ANY}, ShouldRegisterExpectation{true, true, true}, ShouldRegisterExpectation{true, true, true},
                 &StartTransaction());
  }
  {
    SCOPED_TRACE("TET::VERTEX_CREATE");
    CheckFilters({TET::VERTEX_CREATE}, ShouldRegisterExpectation{true, false, false},
                 ShouldRegisterExpectation{false, false, false}, &StartTransaction());
  }
  {
    SCOPED_TRACE("TET::EDGE_CREATE");
    CheckFilters({TET::EDGE_CREATE}, ShouldRegisterExpectation{false, false, false},
                 ShouldRegisterExpectation{true, false, false}, &StartTransaction());
  }
  {
    SCOPED_TRACE("TET::CREATE");
    CheckFilters({TET::CREATE}, ShouldRegisterExpectation{true, false, false},
                 ShouldRegisterExpectation{true, false, false}, &StartTransaction());
  }
  {
    SCOPED_TRACE("TET::VERTEX_DELETE");
    CheckFilters({TET::VERTEX_DELETE}, ShouldRegisterExpectation{true, true, false},
                 ShouldRegisterExpectation{false, false, false}, &StartTransaction());
  }
  {
    SCOPED_TRACE("TET::EDGE_DELETE");
    CheckFilters({TET::EDGE_DELETE}, ShouldRegisterExpectation{false, false, false},
                 ShouldRegisterExpectation{true, true, false}, &StartTransaction());
  }
  {
    SCOPED_TRACE("TET::DELETE");
    CheckFilters({TET::DELETE}, ShouldRegisterExpectation{true, true, false},
                 ShouldRegisterExpectation{true, true, false}, &StartTransaction());
  }
  {
    SCOPED_TRACE("TET::VERTEX_UPDATE");
    CheckFilters({TET::VERTEX_UPDATE}, ShouldRegisterExpectation{true, false, true},
                 ShouldRegisterExpectation{false, false, false}, &StartTransaction());
  }
  {
    SCOPED_TRACE("TET::EDGE_UPDATE");
    CheckFilters({TET::EDGE_UPDATE}, ShouldRegisterExpectation{false, false, false},
                 ShouldRegisterExpectation{true, false, true}, &StartTransaction());
  }
  {
    SCOPED_TRACE("TET::UPDATE");
    CheckFilters({TET::UPDATE}, ShouldRegisterExpectation{true, false, true},
                 ShouldRegisterExpectation{true, false, true}, &StartTransaction());
  }
  // Some combined versions
  {
    SCOPED_TRACE("TET::VERTEX_UPDATE, TET::EDGE_UPDATE");
    CheckFilters({TET::VERTEX_UPDATE, TET::EDGE_UPDATE}, ShouldRegisterExpectation{true, false, true},
                 ShouldRegisterExpectation{true, false, true}, &StartTransaction());
  }
  {
    SCOPED_TRACE("TET::VERTEX_UPDATE, TET::EDGE_UPDATE, TET::DELETE");
    CheckFilters({TET::VERTEX_UPDATE, TET::EDGE_UPDATE, TET::DELETE}, ShouldRegisterExpectation{true, true, true},
                 ShouldRegisterExpectation{true, true, true}, &StartTransaction());
  }
  {
    SCOPED_TRACE("TET::UPDATE, TET::VERTEX_DELETE, TET::EDGE_DELETE");
    CheckFilters({TET::UPDATE, TET::VERTEX_DELETE, TET::EDGE_DELETE}, ShouldRegisterExpectation{true, true, true},
                 ShouldRegisterExpectation{true, true, true}, &StartTransaction());
  }
  {
    SCOPED_TRACE("TET::VERTEX_CREATE, TET::VERTEX_UPDATE");
    CheckFilters({TET::VERTEX_CREATE, TET::VERTEX_UPDATE}, ShouldRegisterExpectation{true, false, true},
                 ShouldRegisterExpectation{false, false, false}, &StartTransaction());
  }
  {
    SCOPED_TRACE("TET::EDGE_CREATE, TET::EDGE_UPDATE");
    CheckFilters({TET::EDGE_CREATE, TET::EDGE_UPDATE}, ShouldRegisterExpectation{false, false, false},
                 ShouldRegisterExpectation{true, false, true}, &StartTransaction());
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

  std::optional<memgraph::query::DbAccessor> dba;

  memgraph::utils::SkipList<memgraph::query::QueryCacheEntry> ast_cache;
  memgraph::query::AllowEverythingAuthChecker auth_checker;

 private:
  void Clear() {
    if (!std::filesystem::exists(testing_directory)) return;
    std::filesystem::remove_all(testing_directory);
  }

  memgraph::storage::Storage storage;
  std::optional<memgraph::storage::Storage::Accessor> storage_accessor;
};

TEST_F(TriggerStoreTest, Restore) {
  std::optional<memgraph::query::TriggerStore> store;

  const auto reset_store = [&] {
    store.emplace(testing_directory);
    store->RestoreTriggers(&ast_cache, &*dba, memgraph::query::InterpreterConfig::Query{}, &auth_checker);
  };

  reset_store();

  const auto check_empty = [&] {
    ASSERT_EQ(store->GetTriggerInfo().size(), 0);
    ASSERT_EQ(store->BeforeCommitTriggers().size(), 0);
    ASSERT_EQ(store->AfterCommitTriggers().size(), 0);
  };

  check_empty();

  const auto *trigger_name_before = "trigger";
  const auto *trigger_name_after = "trigger_after";
  const auto *trigger_statement = "RETURN $parameter";
  const auto event_type = memgraph::query::TriggerEventType::VERTEX_CREATE;
  const std::string owner{"owner"};
  store->AddTrigger(
      trigger_name_before, trigger_statement,
      std::map<std::string, memgraph::storage::PropertyValue>{{"parameter", memgraph::storage::PropertyValue{1}}},
      event_type, memgraph::query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba,
      memgraph::query::InterpreterConfig::Query{}, std::nullopt, &auth_checker);
  store->AddTrigger(
      trigger_name_after, trigger_statement,
      std::map<std::string, memgraph::storage::PropertyValue>{{"parameter", memgraph::storage::PropertyValue{"value"}}},
      event_type, memgraph::query::TriggerPhase::AFTER_COMMIT, &ast_cache, &*dba,
      memgraph::query::InterpreterConfig::Query{}, {owner}, &auth_checker);

  const auto check_triggers = [&] {
    ASSERT_EQ(store->GetTriggerInfo().size(), 2);

    const auto verify_trigger = [&](const auto &trigger, const auto &name, const std::string *owner) {
      ASSERT_EQ(trigger.Name(), name);
      ASSERT_EQ(trigger.OriginalStatement(), trigger_statement);
      ASSERT_EQ(trigger.EventType(), event_type);
      if (owner != nullptr) {
        ASSERT_EQ(*trigger.Owner(), *owner);
      } else {
        ASSERT_FALSE(trigger.Owner().has_value());
      }
    };

    const auto before_commit_triggers = store->BeforeCommitTriggers().access();
    ASSERT_EQ(before_commit_triggers.size(), 1);
    for (const auto &trigger : before_commit_triggers) {
      verify_trigger(trigger, trigger_name_before, nullptr);
    }

    const auto after_commit_triggers = store->AfterCommitTriggers().access();
    ASSERT_EQ(after_commit_triggers.size(), 1);
    for (const auto &trigger : after_commit_triggers) {
      verify_trigger(trigger, trigger_name_after, &owner);
    }
  };

  check_triggers();

  // recreate trigger store, this should reload everything from the disk
  reset_store();
  check_triggers();

  ASSERT_NO_THROW(store->DropTrigger(trigger_name_after));
  ASSERT_NO_THROW(store->DropTrigger(trigger_name_before));

  check_empty();

  reset_store();

  check_empty();
}

TEST_F(TriggerStoreTest, AddTrigger) {
  memgraph::query::TriggerStore store{testing_directory};

  // Invalid query in statements
  ASSERT_THROW(store.AddTrigger("trigger", "RETUR 1", {}, memgraph::query::TriggerEventType::VERTEX_CREATE,
                                memgraph::query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba,
                                memgraph::query::InterpreterConfig::Query{}, std::nullopt, &auth_checker),
               memgraph::utils::BasicException);
  ASSERT_THROW(store.AddTrigger("trigger", "RETURN createdEdges", {}, memgraph::query::TriggerEventType::VERTEX_CREATE,
                                memgraph::query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba,
                                memgraph::query::InterpreterConfig::Query{}, std::nullopt, &auth_checker),
               memgraph::utils::BasicException);

  ASSERT_THROW(store.AddTrigger("trigger", "RETURN $parameter", {}, memgraph::query::TriggerEventType::VERTEX_CREATE,
                                memgraph::query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba,
                                memgraph::query::InterpreterConfig::Query{}, std::nullopt, &auth_checker),
               memgraph::utils::BasicException);

  ASSERT_NO_THROW(store.AddTrigger(
      "trigger", "RETURN $parameter",
      std::map<std::string, memgraph::storage::PropertyValue>{{"parameter", memgraph::storage::PropertyValue{1}}},
      memgraph::query::TriggerEventType::VERTEX_CREATE, memgraph::query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba,
      memgraph::query::InterpreterConfig::Query{}, std::nullopt, &auth_checker));

  // Inserting with the same name
  ASSERT_THROW(store.AddTrigger("trigger", "RETURN 1", {}, memgraph::query::TriggerEventType::VERTEX_CREATE,
                                memgraph::query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba,
                                memgraph::query::InterpreterConfig::Query{}, std::nullopt, &auth_checker),
               memgraph::utils::BasicException);
  ASSERT_THROW(store.AddTrigger("trigger", "RETURN 1", {}, memgraph::query::TriggerEventType::VERTEX_CREATE,
                                memgraph::query::TriggerPhase::AFTER_COMMIT, &ast_cache, &*dba,
                                memgraph::query::InterpreterConfig::Query{}, std::nullopt, &auth_checker),
               memgraph::utils::BasicException);

  ASSERT_EQ(store.GetTriggerInfo().size(), 1);
  ASSERT_EQ(store.BeforeCommitTriggers().size(), 1);
  ASSERT_EQ(store.AfterCommitTriggers().size(), 0);
}

TEST_F(TriggerStoreTest, DropTrigger) {
  memgraph::query::TriggerStore store{testing_directory};

  ASSERT_THROW(store.DropTrigger("Unknown"), memgraph::utils::BasicException);

  const auto *trigger_name = "trigger";
  store.AddTrigger(trigger_name, "RETURN 1", {}, memgraph::query::TriggerEventType::VERTEX_CREATE,
                   memgraph::query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba,
                   memgraph::query::InterpreterConfig::Query{}, std::nullopt, &auth_checker);

  ASSERT_THROW(store.DropTrigger("Unknown"), memgraph::utils::BasicException);
  ASSERT_NO_THROW(store.DropTrigger(trigger_name));
  ASSERT_EQ(store.GetTriggerInfo().size(), 0);
}

TEST_F(TriggerStoreTest, TriggerInfo) {
  memgraph::query::TriggerStore store{testing_directory};

  std::vector<memgraph::query::TriggerStore::TriggerInfo> expected_info;
  store.AddTrigger("trigger", "RETURN 1", {}, memgraph::query::TriggerEventType::VERTEX_CREATE,
                   memgraph::query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba,
                   memgraph::query::InterpreterConfig::Query{}, std::nullopt, &auth_checker);
  expected_info.push_back({"trigger", "RETURN 1", memgraph::query::TriggerEventType::VERTEX_CREATE,
                           memgraph::query::TriggerPhase::BEFORE_COMMIT});

  const auto check_trigger_info = [&] {
    const auto trigger_info = store.GetTriggerInfo();
    ASSERT_EQ(expected_info.size(), trigger_info.size());
    // ensure all of the expected trigger infos can be found in the retrieved infos
    ASSERT_TRUE(std::all_of(expected_info.begin(), expected_info.end(), [&](const auto &info) {
      return std::find_if(trigger_info.begin(), trigger_info.end(), [&](const auto &other) {
               return info.name == other.name && info.statement == other.statement &&
                      info.event_type == other.event_type && info.phase == other.phase && !info.owner.has_value();
             }) != trigger_info.end();
    }));
  };

  check_trigger_info();

  store.AddTrigger("edge_update_trigger", "RETURN 1", {}, memgraph::query::TriggerEventType::EDGE_UPDATE,
                   memgraph::query::TriggerPhase::AFTER_COMMIT, &ast_cache, &*dba,
                   memgraph::query::InterpreterConfig::Query{}, std::nullopt, &auth_checker);
  expected_info.push_back({"edge_update_trigger", "RETURN 1", memgraph::query::TriggerEventType::EDGE_UPDATE,
                           memgraph::query::TriggerPhase::AFTER_COMMIT});

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

TEST_F(TriggerStoreTest, AnyTriggerAllKeywords) {
  memgraph::query::TriggerStore store{testing_directory};

  using namespace std::literals;

  const auto created_vertices = "createdVertices"sv;
  const auto created_edges = "createdEdges"sv;
  const auto created_objects = "createdObjects"sv;
  const auto deleted_vertices = "deletedVertices"sv;
  const auto deleted_edges = "deletedEdges"sv;
  const auto deleted_objects = "deletedObjects"sv;
  const auto set_vertex_properties = "setVertexProperties"sv;
  const auto set_edge_properties = "setEdgeProperties"sv;
  const auto removed_vertex_properties = "removedVertexProperties"sv;
  const auto removed_edge_properties = "removedEdgeProperties"sv;
  const auto set_vertex_labels = "setVertexLabels"sv;
  const auto removed_vertex_labels = "removedVertexLabels"sv;
  const auto updated_vertices = "updatedVertices"sv;
  const auto updated_edges = "updatedEdges"sv;
  const auto updates_objects = "updatedObjects"sv;

  std::array event_types_to_test = {
      std::make_pair(memgraph::query::TriggerEventType::CREATE,
                     std::vector{created_vertices, created_edges, created_objects}),
      std::make_pair(memgraph::query::TriggerEventType::VERTEX_CREATE, std::vector{created_vertices}),
      std::make_pair(memgraph::query::TriggerEventType::EDGE_CREATE, std::vector{created_edges}),
      std::make_pair(memgraph::query::TriggerEventType::UPDATE,
                     std::vector{
                         set_vertex_properties,
                         set_edge_properties,
                         removed_vertex_properties,
                         removed_edge_properties,
                         set_vertex_labels,
                         removed_vertex_labels,
                         updated_vertices,
                         updated_edges,
                         updates_objects,
                     }),
      std::make_pair(memgraph::query::TriggerEventType::VERTEX_UPDATE,
                     std::vector{
                         set_vertex_properties,
                         removed_vertex_properties,
                         set_vertex_labels,
                         removed_vertex_labels,
                         updated_vertices,
                     }),
      std::make_pair(memgraph::query::TriggerEventType::EDGE_UPDATE,
                     std::vector{
                         set_edge_properties,
                         removed_edge_properties,
                         updated_edges,
                     }),
      std::make_pair(memgraph::query::TriggerEventType::DELETE,
                     std::vector{
                         deleted_vertices,
                         deleted_edges,
                         deleted_objects,
                     }),
      std::make_pair(memgraph::query::TriggerEventType::VERTEX_DELETE,
                     std::vector{
                         deleted_vertices,
                     }),
      std::make_pair(memgraph::query::TriggerEventType::EDGE_DELETE,
                     std::vector{
                         deleted_edges,
                     }),
      std::make_pair(memgraph::query::TriggerEventType::ANY,
                     std::vector{
                         created_vertices,
                         created_edges,
                         created_objects,
                         deleted_vertices,
                         deleted_edges,
                         deleted_objects,
                         set_vertex_properties,
                         set_edge_properties,
                         removed_vertex_properties,
                         removed_edge_properties,
                         set_vertex_labels,
                         removed_vertex_labels,
                         updated_vertices,
                         updated_edges,
                         updates_objects,
                     }),
  };

  const auto trigger_name = "trigger"s;
  for (const auto &[event_type, keywords] : event_types_to_test) {
    SCOPED_TRACE(memgraph::query::TriggerEventTypeToString(event_type));
    for (const auto keyword : keywords) {
      SCOPED_TRACE(keyword);
      EXPECT_NO_THROW(store.AddTrigger(trigger_name, fmt::format("RETURN {}", keyword), {}, event_type,
                                       memgraph::query::TriggerPhase::BEFORE_COMMIT, &ast_cache, &*dba,
                                       memgraph::query::InterpreterConfig::Query{}, std::nullopt, &auth_checker));
      store.DropTrigger(trigger_name);
    }
  }
}

TEST_F(TriggerStoreTest, AuthCheckerUsage) {
  using Privilege = memgraph::query::AuthQuery::Privilege;
  using ::testing::_;
  using ::testing::ElementsAre;
  using ::testing::Return;
  std::optional<memgraph::query::TriggerStore> store{testing_directory};
  const std::optional<std::string> owner{"testing_owner"};
  MockAuthChecker mock_checker;

  ::testing::InSequence s;

  EXPECT_CALL(mock_checker, IsUserAuthorized(std::optional<std::string>{}, ElementsAre(Privilege::CREATE)))
      .Times(1)
      .WillOnce(Return(true));
  EXPECT_CALL(mock_checker, IsUserAuthorized(owner, ElementsAre(Privilege::CREATE))).Times(1).WillOnce(Return(true));

  ASSERT_NO_THROW(store->AddTrigger("successfull_trigger_1", "CREATE (n:VERTEX) RETURN n", {},
                                    memgraph::query::TriggerEventType::EDGE_UPDATE,
                                    memgraph::query::TriggerPhase::AFTER_COMMIT, &ast_cache, &*dba,
                                    memgraph::query::InterpreterConfig::Query{}, std::nullopt, &mock_checker));

  ASSERT_NO_THROW(store->AddTrigger("successfull_trigger_2", "CREATE (n:VERTEX) RETURN n", {},
                                    memgraph::query::TriggerEventType::EDGE_UPDATE,
                                    memgraph::query::TriggerPhase::AFTER_COMMIT, &ast_cache, &*dba,
                                    memgraph::query::InterpreterConfig::Query{}, owner, &mock_checker));

  EXPECT_CALL(mock_checker, IsUserAuthorized(std::optional<std::string>{}, ElementsAre(Privilege::MATCH)))
      .Times(1)
      .WillOnce(Return(false));

  ASSERT_THROW(
      store->AddTrigger("unprivileged_trigger", "MATCH (n:VERTEX) RETURN n", {},
                        memgraph::query::TriggerEventType::EDGE_UPDATE, memgraph::query::TriggerPhase::AFTER_COMMIT,
                        &ast_cache, &*dba, memgraph::query::InterpreterConfig::Query{}, std::nullopt, &mock_checker);
      , memgraph::utils::BasicException);

  store.emplace(testing_directory);
  EXPECT_CALL(mock_checker, IsUserAuthorized(std::optional<std::string>{}, ElementsAre(Privilege::CREATE)))
      .Times(1)
      .WillOnce(Return(false));
  EXPECT_CALL(mock_checker, IsUserAuthorized(owner, ElementsAre(Privilege::CREATE))).Times(1).WillOnce(Return(true));

  ASSERT_NO_THROW(
      store->RestoreTriggers(&ast_cache, &*dba, memgraph::query::InterpreterConfig::Query{}, &mock_checker));

  const auto triggers = store->GetTriggerInfo();
  ASSERT_EQ(triggers.size(), 1);
  ASSERT_EQ(triggers.front().owner, owner);
}

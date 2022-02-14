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

#include <string>
#include <string_view>

#include <gflags/gflags.h>
#include <mgclient.hpp>
#include "common.hpp"
#include "utils/logging.hpp"

constexpr std::string_view kTriggerUpdatedVertexLabel{"UPDATED_VERTEX"};
constexpr std::string_view kTriggerUpdatedEdgeLabel{"UPDATED_EDGE"};
constexpr std::string_view kTriggerUpdatedObjectLabel{"UPDATED_OBJECT"};
constexpr std::string_view kTriggerSetVertexPropertyLabel{"SET_VERTEX_PROPERTY"};
constexpr std::string_view kTriggerRemovedVertexPropertyLabel{"REMOVED_VERTEX_PROPERTY"};
constexpr std::string_view kTriggerSetVertexLabelLabel{"SET_VERTEX_LABEL"};
constexpr std::string_view kTriggerRemovedVertexLabelLabel{"REMOVED_VERTEX_LABEL"};
constexpr std::string_view kTriggerSetEdgePropertyLabel{"SET_EDGE_PROPERTY"};
constexpr std::string_view kTriggerRemovedEdgePropertyLabel{"REMOVED_EDGE_PROPERTY"};

void SetVertexProperty(mg::Client &client, int vertex_id, std::string_view property_name, mg::Value value) {
  mg::Map parameters{
      {"id", mg::Value{vertex_id}},
      {"value", std::move(value)},
  };
  client.Execute(fmt::format("MATCH (n: {} {{id: $id}}) "
                             "SET n.{} = $value",
                             kVertexLabel, property_name),
                 mg::ConstMap{parameters.ptr()});
  client.DiscardAll();
}

void SetEdgeProperty(mg::Client &client, int edge_id, std::string_view property_name, mg::Value value) {
  mg::Map parameters{
      {"id", mg::Value{edge_id}},
      {"value", std::move(value)},
  };
  client.Execute(fmt::format("MATCH ()-[r: {} {{id: $id}}]->() "
                             "SET r.{} = $value",
                             kEdgeLabel, property_name),
                 mg::ConstMap{parameters.ptr()});
  client.DiscardAll();
}

void DoVertexLabelOperation(mg::Client &client, int vertex_id, std::string_view label, std::string_view operation) {
  mg::Map parameters{{"id", mg::Value{vertex_id}}};
  client.Execute(fmt::format("MATCH (n: {} {{id: $id}}) "
                             "{} n:{}",
                             kVertexLabel, operation, label),
                 mg::ConstMap{parameters.ptr()});
  client.DiscardAll();
}

void AddVertexLabel(mg::Client &client, int vertex_id, std::string_view label) {
  DoVertexLabelOperation(client, vertex_id, label, "SET");
}

void RemoveVertexLabel(mg::Client &client, int vertex_id, std::string_view label) {
  DoVertexLabelOperation(client, vertex_id, label, "REMOVE");
}

void CheckVertexProperty(mg::Client &client, std::string_view label, int vertex_id, std::string_view property_name,
                         const mg::Value &value) {
  const auto vertex = GetVertex(client, label, vertex_id);
  MG_ASSERT(vertex, "Cannot check property of not existing vertex with label {} and id {}", label, vertex_id);

  const auto properties = vertex->ValueNode().properties();
  const auto prop_it = properties.find(property_name);
  MG_ASSERT(prop_it != properties.end(), "Vertex with label {} and id {} doesn't have expected property {}!", label,
            vertex_id, property_name);
  MG_ASSERT((*prop_it).second == value, "Property {} of vertex with label {} and id {} doesn't have expected value!",
            property_name, label, vertex_id);
}

void CreateOnUpdateTriggers(mg::Client &client, bool is_before) {
  const std::string_view before_or_after = is_before ? "BEFORE" : "AFTER";
  client.Execute(
      fmt::format("CREATE TRIGGER UpdatedVerticesTrigger ON () UPDATE "
                  "{} COMMIT "
                  "EXECUTE "
                  "UNWIND updatedVertices as updateVertexEvent "
                  "CREATE (n: {} {{ id: updateVertexEvent.vertex.id , event_type: updateVertexEvent.event_type }})",
                  before_or_after, kTriggerUpdatedVertexLabel));
  client.DiscardAll();
  client.Execute(
      fmt::format("CREATE TRIGGER UpdatedEdgesTrigger ON --> UPDATE "
                  "{} COMMIT "
                  "EXECUTE "
                  "UNWIND updatedEdges as updatedEdgeEvent "
                  "CREATE (n: {} {{ id: updatedEdgeEvent.edge.id, event_type: updatedEdgeEvent.event_type }})",
                  before_or_after, kTriggerUpdatedEdgeLabel));
  client.DiscardAll();
  client.Execute(
      fmt::format("CREATE TRIGGER UpdatedObjectsTrigger ON UPDATE "
                  "{} COMMIT "
                  "EXECUTE "
                  "UNWIND updatedObjects as updatedObject "
                  "WITH CASE updatedObject.event_type "
                  "WHEN \"set_edge_property\" THEN updatedObject.edge.id "
                  "WHEN \"removed_edge_property\" THEN updatedObject.edge.id "
                  "ELSE updatedObject.vertex.id END as id, updatedObject "
                  "CREATE (n: {} {{ id: id, event_type: updatedObject.event_type }})",
                  before_or_after, kTriggerUpdatedObjectLabel));
  client.DiscardAll();

  client.Execute(
      fmt::format("CREATE TRIGGER SetVertexPropertiesTrigger ON () UPDATE "
                  "{} COMMIT "
                  "EXECUTE "
                  "UNWIND setVertexProperties as assignedVertexProperty "
                  "CREATE (n: {} {{ id: assignedVertexProperty.vertex.id }})",
                  before_or_after, kTriggerSetVertexPropertyLabel));
  client.DiscardAll();
  client.Execute(
      fmt::format("CREATE TRIGGER RemovedVertexPropertiesTrigger ON () UPDATE "
                  "{} COMMIT "
                  "EXECUTE "
                  "UNWIND removedVertexProperties as removedVertexProperty "
                  "CREATE (n: {} {{ id: removedVertexProperty.vertex.id }})",
                  before_or_after, kTriggerRemovedVertexPropertyLabel));
  client.DiscardAll();
  client.Execute(
      fmt::format("CREATE TRIGGER SetVertexLabelsTrigger ON () UPDATE "
                  "{} COMMIT "
                  "EXECUTE "
                  "UNWIND setVertexLabels as assignedVertexLabel "
                  "UNWIND assignedVertexLabel.vertices as vertex "
                  "CREATE (n: {} {{ id: vertex.id }})",
                  before_or_after, kTriggerSetVertexLabelLabel));
  client.DiscardAll();
  client.Execute(
      fmt::format("CREATE TRIGGER RemovedVertexLabelTrigger ON () UPDATE "
                  "{} COMMIT "
                  "EXECUTE "
                  "UNWIND removedVertexLabels as removedVertexLabel "
                  "UNWIND removedVertexLabel.vertices as vertex "
                  "CREATE (n: {} {{ id: vertex.id }})",
                  before_or_after, kTriggerRemovedVertexLabelLabel));
  client.DiscardAll();

  client.Execute(
      fmt::format("CREATE TRIGGER SetEdgePropertiesTrigger ON --> UPDATE "
                  "{} COMMIT "
                  "EXECUTE "
                  "UNWIND setEdgeProperties as assignedEdgeProperty "
                  "CREATE (n: {} {{ id: assignedEdgeProperty.edge.id }})",
                  before_or_after, kTriggerSetEdgePropertyLabel));
  client.DiscardAll();
  client.Execute(
      fmt::format("CREATE TRIGGER RemovedEdgePropertiesTrigger ON --> UPDATE "
                  "{} COMMIT "
                  "EXECUTE "
                  "UNWIND removedEdgeProperties as removedEdgeProperty "
                  "CREATE (n: {} {{ id: removedEdgeProperty.edge.id }})",
                  before_or_after, kTriggerRemovedEdgePropertyLabel));
  client.DiscardAll();
}

void DropOnUpdateTriggers(mg::Client &client) {
  client.Execute("DROP TRIGGER UpdatedVerticesTrigger");
  client.DiscardAll();
  client.Execute("DROP TRIGGER UpdatedEdgesTrigger");
  client.DiscardAll();
  client.Execute("DROP TRIGGER UpdatedObjectsTrigger");
  client.DiscardAll();
  client.Execute("DROP TRIGGER SetVertexPropertiesTrigger");
  client.DiscardAll();
  client.Execute("DROP TRIGGER RemovedVertexPropertiesTrigger");
  client.DiscardAll();
  client.Execute("DROP TRIGGER SetVertexLabelsTrigger");
  client.DiscardAll();
  client.Execute("DROP TRIGGER RemovedVertexLabelTrigger");
  client.DiscardAll();
  client.Execute("DROP TRIGGER SetEdgePropertiesTrigger");
  client.DiscardAll();
  client.Execute("DROP TRIGGER RemovedEdgePropertiesTrigger");
  client.DiscardAll();
}

struct EdgeInfo {
  int from_vertex;
  int to_vertex;
  int edge_id;
};

int main(int argc, char **argv) {
  constexpr std::string_view kExtraLabel = "EXTRA_LABEL";
  constexpr std::string_view kUpdatedProperty = "updateProperty";
  gflags::SetUsageMessage("Memgraph E2E ON UPDATE Triggers");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  logging::RedirectToStderr();

  mg::Client::Init();

  auto client = Connect();

  const auto run_update_trigger_tests = [&](bool is_before) {
    const std::array vertex_ids{1, 2, 3, 4};
    const std::array edges{EdgeInfo{vertex_ids[0], vertex_ids[1], 5}, EdgeInfo{vertex_ids[2], vertex_ids[3], 6}};
    {
      CreateOnUpdateTriggers(*client, is_before);

      client->BeginTransaction();
      for (const auto vertex_id : vertex_ids) {
        CreateVertex(*client, vertex_id);
        SetVertexProperty(*client, vertex_id, kUpdatedProperty, mg::Value(vertex_id));
        AddVertexLabel(*client, vertex_id, kExtraLabel);
      }
      for (const auto &edge : edges) {
        CreateEdge(*client, edge.from_vertex, edge.to_vertex, edge.edge_id);
        SetEdgeProperty(*client, edge.edge_id, kUpdatedProperty, mg::Value(edge.edge_id));
      }
      client->CommitTransaction();
      CheckNumberOfAllVertices(*client, vertex_ids.size());

      client->BeginTransaction();
      SetVertexProperty(*client, vertex_ids[0], kUpdatedProperty, mg::Value(-1));
      SetVertexProperty(*client, vertex_ids[1], kUpdatedProperty, mg::Value());
      AddVertexLabel(*client, vertex_ids[2], "NEW_LABEL");
      RemoveVertexLabel(*client, vertex_ids[3], kExtraLabel);
      SetEdgeProperty(*client, edges[0].edge_id, kUpdatedProperty, mg::Value(-1));
      SetEdgeProperty(*client, edges[1].edge_id, kUpdatedProperty, mg::Value());
      CheckNumberOfAllVertices(*client, vertex_ids.size());
      client->CommitTransaction();

      // :VERTEX                  x 4
      // :UPDATED_VERTEX          x 4
      // :UPDATED_EDGE            x 2
      // :UPDATED_OBJECT          x 6
      // :SET_VERTEX_PROPERTY     x 1
      // :REMOVED_VERTEX_PROPERTY x 1
      // :SET_VERTEX_LABEL        x 1
      // :REMOVED_VERTEX_LABEL    x 1
      // :SET_EDGE_PROPERTY       x 1
      // :REMOVED_EDGE_PROPERTY   x 1
      constexpr auto kNumberOfExpectedVertices = 22;

      if (is_before) {
        CheckNumberOfAllVertices(*client, kNumberOfExpectedVertices);
      } else {
        WaitForNumberOfAllVertices(*client, kNumberOfExpectedVertices);
      }

      CheckVertexProperty(*client, kTriggerUpdatedVertexLabel, vertex_ids[0], "event_type",
                          mg::Value{"set_vertex_property"});
      CheckVertexProperty(*client, kTriggerUpdatedVertexLabel, vertex_ids[1], "event_type",
                          mg::Value{"removed_vertex_property"});
      CheckVertexProperty(*client, kTriggerUpdatedVertexLabel, vertex_ids[2], "event_type",
                          mg::Value{"set_vertex_label"});
      CheckVertexProperty(*client, kTriggerUpdatedVertexLabel, vertex_ids[3], "event_type",
                          mg::Value{"removed_vertex_label"});
      CheckVertexProperty(*client, kTriggerUpdatedEdgeLabel, edges[0].edge_id, "event_type",
                          mg::Value{"set_edge_property"});
      CheckVertexProperty(*client, kTriggerUpdatedEdgeLabel, edges[1].edge_id, "event_type",
                          mg::Value{"removed_edge_property"});

      CheckVertexProperty(*client, kTriggerUpdatedObjectLabel, vertex_ids[0], "event_type",
                          mg::Value{"set_vertex_property"});
      CheckVertexProperty(*client, kTriggerUpdatedObjectLabel, vertex_ids[1], "event_type",
                          mg::Value{"removed_vertex_property"});
      CheckVertexProperty(*client, kTriggerUpdatedObjectLabel, vertex_ids[2], "event_type",
                          mg::Value{"set_vertex_label"});
      CheckVertexProperty(*client, kTriggerUpdatedObjectLabel, vertex_ids[3], "event_type",
                          mg::Value{"removed_vertex_label"});
      CheckVertexProperty(*client, kTriggerUpdatedObjectLabel, edges[0].edge_id, "event_type",
                          mg::Value{"set_edge_property"});
      CheckVertexProperty(*client, kTriggerUpdatedObjectLabel, edges[1].edge_id, "event_type",
                          mg::Value{"removed_edge_property"});

      CheckVertexExists(*client, kTriggerSetVertexPropertyLabel, vertex_ids[0]);
      CheckVertexExists(*client, kTriggerRemovedVertexPropertyLabel, vertex_ids[1]);
      CheckVertexExists(*client, kTriggerSetVertexLabelLabel, vertex_ids[2]);
      CheckVertexExists(*client, kTriggerRemovedVertexLabelLabel, vertex_ids[3]);
      CheckVertexExists(*client, kTriggerSetEdgePropertyLabel, edges[0].edge_id);
      CheckVertexExists(*client, kTriggerRemovedEdgePropertyLabel, edges[1].edge_id);

      DropOnUpdateTriggers(*client);
      client->Execute("MATCH (n) DETACH DELETE n;");
      client->DiscardAll();
    }
  };
  constexpr bool kBeforeCommit = true;
  constexpr bool kAfterCommit = false;
  run_update_trigger_tests(kBeforeCommit);
  run_update_trigger_tests(kAfterCommit);

  const auto run_update_trigger_write_procedure_vertex_set_property_test = [&]() {
    CreateOnUpdateTriggers(*client, true);
    ExecuteCreateVertex(*client, 1);
    client->Execute("MATCH (n) CALL write.set_property(n)");
    client->DiscardAll();
    constexpr auto kNumberOfExpectedVertices = 4;
    constexpr int expected_updated_id = 2;
    CheckNumberOfAllVertices(*client, kNumberOfExpectedVertices);
    CheckVertexExists(*client, kTriggerUpdatedVertexLabel, expected_updated_id);
    CheckVertexExists(*client, kTriggerUpdatedObjectLabel, expected_updated_id);
    CheckVertexExists(*client, kTriggerSetVertexPropertyLabel, expected_updated_id);
    client->Execute(fmt::format("MATCH (n1:{}), (n2:{}), (n3:{}) DELETE n1, n2, n3", kTriggerUpdatedVertexLabel,
                                kTriggerUpdatedObjectLabel, kTriggerSetVertexPropertyLabel));
    client->DiscardAll();
    client->Execute("MATCH (n) CALL write.remove_property(n)");
    client->DiscardAll();
    CheckNumberOfAllVertices(*client, kNumberOfExpectedVertices);
    CheckVertexExists(*client, kTriggerUpdatedVertexLabel, expected_updated_id);
    CheckVertexExists(*client, kTriggerUpdatedObjectLabel, expected_updated_id);
    CheckVertexExists(*client, kTriggerRemovedVertexPropertyLabel, 2);
    DropOnUpdateTriggers(*client);
    client->Execute("MATCH (n) DETACH DELETE n;");
    client->DiscardAll();
  };
  run_update_trigger_write_procedure_vertex_set_property_test();

  const auto run_update_trigger_write_procedure_edge_set_property_test = [&]() {
    CreateOnUpdateTriggers(*client, true);
    ExecuteCreateVertex(*client, 1);
    ExecuteCreateVertex(*client, 3);
    client->Execute("MATCH (n {id:1}), (m {id:3}) CALL write.create_edge(n, m, 'edge') YIELD e RETURN e");
    client->DiscardAll();
    client->Execute("MATCH ()-[e]->() CALL write.set_property(e)");
    client->DiscardAll();
    constexpr auto kNumberOfExpectedVertices = 5;
    constexpr int expected_updated_id = 2;
    CheckNumberOfAllVertices(*client, kNumberOfExpectedVertices);
    CheckVertexExists(*client, kTriggerSetEdgePropertyLabel, expected_updated_id);
    CheckVertexExists(*client, kTriggerUpdatedObjectLabel, expected_updated_id);
    CheckVertexExists(*client, kTriggerUpdatedEdgeLabel, expected_updated_id);
    client->Execute(fmt::format("MATCH (n1:{}), (n2:{}), (n3:{}) DELETE n1, n2, n3", kTriggerSetEdgePropertyLabel,
                                kTriggerUpdatedObjectLabel, kTriggerUpdatedEdgeLabel));
    client->DiscardAll();
    client->Execute("MATCH ()-[e]->() CALL write.remove_property(e)");
    client->DiscardAll();
    CheckNumberOfAllVertices(*client, kNumberOfExpectedVertices);
    CheckVertexExists(*client, kTriggerUpdatedObjectLabel, expected_updated_id);
    CheckVertexExists(*client, kTriggerUpdatedEdgeLabel, expected_updated_id);
    CheckVertexExists(*client, kTriggerRemovedEdgePropertyLabel, 2);
    DropOnUpdateTriggers(*client);
    client->Execute("MATCH (n) DETACH DELETE n;");
    client->DiscardAll();
  };
  run_update_trigger_write_procedure_edge_set_property_test();

  const auto run_update_trigger_write_procedure_set_label_test = [&]() {
    ExecuteCreateVertex(*client, 1);
    client->Execute("MATCH (n) CALL write.add_label(n, 'label') YIELD o RETURN o");
    client->DiscardAll();
    CreateOnUpdateTriggers(*client, true);
    client->Execute("MATCH (n) CALL write.add_label(n, 'new') YIELD o RETURN o");
    client->DiscardAll();
    constexpr auto kNumberOfExpectedVertices = 4;
    CheckNumberOfAllVertices(*client, kNumberOfExpectedVertices);
    CheckVertexExists(*client, kTriggerSetVertexLabelLabel, 1);
    CheckVertexExists(*client, kTriggerUpdatedVertexLabel, 1);
    CheckVertexExists(*client, kTriggerUpdatedObjectLabel, 1);
    client->Execute(fmt::format("MATCH (n1:{}), (n2:{}), (n3:{}) DELETE n1, n2, n3", kTriggerSetVertexLabelLabel,
                                kTriggerUpdatedVertexLabel, kTriggerUpdatedObjectLabel));
    client->DiscardAll();
    client->Execute("MATCH (n:new) CALL write.remove_label(n, 'new') YIELD o RETURN o");
    client->DiscardAll();
    CheckNumberOfAllVertices(*client, kNumberOfExpectedVertices);
    CheckVertexExists(*client, kTriggerRemovedVertexLabelLabel, 1);
    CheckVertexExists(*client, kTriggerUpdatedVertexLabel, 1);
    CheckVertexExists(*client, kTriggerUpdatedObjectLabel, 1);
    DropOnUpdateTriggers(*client);
    client->Execute("MATCH (n) DETACH DELETE n;");
    client->DiscardAll();
  };
  run_update_trigger_write_procedure_set_label_test();

  return 0;
}

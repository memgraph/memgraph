#include <string>
#include <string_view>

#include <gflags/gflags.h>
#include <mgclient.hpp>
#include "common.hpp"
#include "utils/logging.hpp"

constexpr std::string_view kTriggerUpdatedVertexLabel{"UPDATED_VERTEX"};
constexpr std::string_view kTriggerUpdatedEdgeLabel{"UPDATED_EDGE"};
constexpr std::string_view kTriggerUpdatedObjectLabel{"UPDATED_OBJECT"};

void SetVertexProperty(mg::Client &client, int vertex_id, std::string_view property_name, mg::Value value) {
  mg::Map parameters{
      {"id", mg::Value{vertex_id}},
      {"value", std::move(value)},
  };
  client.Execute(Concat("MATCH (n: ", kVertexLabel, " {id: $id}) SET n.", property_name, " = $value"),
                 mg::ConstMap{parameters.ptr()});
  client.DiscardAll();
}

void SetEdgeProperty(mg::Client &client, int edge_id, std::string_view property_name, mg::Value value) {
  mg::Map parameters{
      {"id", mg::Value{edge_id}},
      {"value", std::move(value)},
  };
  client.Execute(Concat("MATCH ()-[r: ", kEdgeLabel, " {id: $id}]->() SET r.", property_name, " = $value"),
                 mg::ConstMap{parameters.ptr()});
  client.DiscardAll();
}

void DoVertexLabelOperation(mg::Client &client, int vertex_id, std::string_view label, std::string_view operation) {
  mg::Map parameters{{"id", mg::Value{vertex_id}}};
  client.Execute(Concat("MATCH (n: ", kVertexLabel, " {id: $id}) ", operation, " n:", label),
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
  if (!vertex) {
    LOG_FATAL("Cannot check property of not existing vertex with label {} and id {}", label, vertex_id);
  }
  const auto properties = vertex->ValueNode().properties();
  const auto prop_it = properties.find(property_name);
  if (prop_it == properties.end()) {
    LOG_FATAL("Vertex with label {} and id {} doesn't have expected property {}!", label, vertex_id, property_name);
  }
  if ((*prop_it).second != value) {
    LOG_FATAL("Property {} of vertex with label {} and id {} doesn't have expected value!", property_name, label,
              vertex_id);
  }
}

void CreateOnUpdateTriggers(mg::Client &client, std::string_view before_or_after) {
  client.Execute(Concat("CREATE TRIGGER UpdatedVerticesTrigger ON () UPDATE ", before_or_after,
                        " COMMIT "
                        "EXECUTE "
                        "UNWIND updatedVertices as updateVertexEvent "
                        "CREATE (n: ",
                        kTriggerUpdatedVertexLabel,
                        " { id: updateVertexEvent.vertex.id , event_type: updateVertexEvent.event_type })"));
  client.DiscardAll();
  client.Execute(Concat("CREATE TRIGGER UpdatedEdgesTrigger ON --> UPDATE ", before_or_after,
                        " COMMIT "
                        "EXECUTE "
                        "UNWIND updatedEdges as updatedEdgeEvent "
                        "CREATE (n: ",
                        kTriggerUpdatedEdgeLabel,
                        " { id: updatedEdgeEvent.edge.id, event_type: updatedEdgeEvent.event_type })"));
  client.DiscardAll();
  client.Execute(Concat("CREATE TRIGGER UpdatedObjectsTrigger ON UPDATE ", before_or_after,
                        " COMMIT "
                        "EXECUTE "
                        "UNWIND updatedObjects as updatedObject "
                        "WITH CASE updatedObject.event_type "
                        "WHEN \"set_edge_property\" THEN updatedObject.edge.id "
                        "WHEN \"removed_edge_property\" THEN updatedObject.edge.id "
                        "ELSE updatedObject.vertex.id END as id, updatedObject "
                        "CREATE (n: ",
                        kTriggerUpdatedObjectLabel, " { id: id, event_type: updatedObject.event_type })"));
  client.DiscardAll();
}

void DropOnUpdateTriggers(mg::Client &client) {
  client.Execute("DROP TRIGGER UpdatedVerticesTrigger");
  client.DiscardAll();
  client.Execute("DROP TRIGGER UpdatedEdgesTrigger");
  client.DiscardAll();
  client.Execute("DROP TRIGGER UpdatedObjectsTrigger");
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

  const auto run_update_trigger_tests = [&](std::string_view before_or_after) {
    const std::array<int, 4> vertex_ids{1, 2, 3, 4};
    const std::array<EdgeInfo, 2> edges{EdgeInfo{vertex_ids[0], vertex_ids[1], 5},
                                        EdgeInfo{vertex_ids[2], vertex_ids[3], 6}};
    {
      CreateOnUpdateTriggers(*client, before_or_after);

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

      // explicitly created vertex          x 4
      // set/removed vertex property vertex x 2
      // set/removed vertex label vertex    x 2
      // set/removed edge property vertex   x 2
      // updated object vertex              x 6
      constexpr auto kNumberOfExpectedVertices = 16;
      WaitForNumberOfAllVertices(*client, kNumberOfExpectedVertices);

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

      DropOnUpdateTriggers(*client);
      client->Execute("MATCH (n) DETACH DELETE n;");
      client->DiscardAll();
    }
  };
  run_update_trigger_tests("BEFORE");
  run_update_trigger_tests("AFTER");

  return 0;
}

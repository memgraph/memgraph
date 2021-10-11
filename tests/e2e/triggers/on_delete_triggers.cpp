#include <string>
#include <string_view>

#include <gflags/gflags.h>
#include <mgclient.hpp>
#include "common.hpp"
#include "utils/logging.hpp"

constexpr std::string_view kTriggerDeletedVertexLabel{"DELETED_VERTEX"};
constexpr std::string_view kTriggerDeletedEdgeLabel{"DELETED_EDGE"};
constexpr std::string_view kTriggerDeletedObjectLabel{"DELETED_OBJECT"};

void DetachDeleteVertex(mg::Client &client, int vertex_id) {
  mg::Map parameters{{"id", mg::Value{vertex_id}}};
  client.Execute(fmt::format("MATCH (n: {} {{id: $id}}) DETACH DELETE n", kVertexLabel),
                 mg::ConstMap{parameters.ptr()});
  client.DiscardAll();
}

void DeleteEdge(mg::Client &client, int edge_id) {
  mg::Map parameters{{"id", mg::Value{edge_id}}};
  client.Execute(fmt::format("MATCH ()-[r: {} {{id: $id}}]->() DELETE r", kEdgeLabel), mg::ConstMap{parameters.ptr()});
  client.DiscardAll();
}

void CreateOnDeleteTriggers(mg::Client &client, bool is_before) {
  const std::string_view before_or_after = is_before ? "BEFORE" : "AFTER";
  client.Execute(
      fmt::format("CREATE TRIGGER DeletedVerticesTrigger ON () DELETE "
                  "{} COMMIT "
                  "EXECUTE "
                  "UNWIND deletedVertices as deletedVertex "
                  "CREATE (n: {} {{ id: deletedVertex.id }})",
                  before_or_after, kTriggerDeletedVertexLabel));
  client.DiscardAll();
  client.Execute(
      fmt::format("CREATE TRIGGER DeletedEdgesTrigger ON --> DELETE "
                  "{} COMMIT "
                  "EXECUTE "
                  "UNWIND deletedEdges as deletedEdge "
                  "CREATE (n: {} {{ id: deletedEdge.id }})",
                  before_or_after, kTriggerDeletedEdgeLabel));
  client.DiscardAll();
  client.Execute(
      fmt::format("CREATE TRIGGER DeletedObjectsTrigger ON DELETE "
                  "{} COMMIT "
                  "EXECUTE "
                  "UNWIND deletedObjects as deletedObjectEvent "
                  "WITH CASE deletedObjectEvent.event_type WHEN \"deleted_vertex\" THEN deletedObjectEvent.vertex.id "
                  "ELSE deletedObjectEvent.edge.id END as id "
                  "CREATE (n: {} {{ id: id }})",
                  before_or_after, kTriggerDeletedObjectLabel));
  client.DiscardAll();
}

void DropOnDeleteTriggers(mg::Client &client) {
  client.Execute("DROP TRIGGER DeletedVerticesTrigger");
  client.DiscardAll();
  client.Execute("DROP TRIGGER DeletedEdgesTrigger");
  client.DiscardAll();
  client.Execute("DROP TRIGGER DeletedObjectsTrigger");
  client.DiscardAll();
}

struct EdgeInfo {
  int from_vertex;
  int to_vertex;
  int edge_id;
};

int main(int argc, char **argv) {
  gflags::SetUsageMessage("Memgraph E2E ON DELETE Triggers");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  logging::RedirectToStderr();

  mg::Client::Init();

  auto client = Connect();

  const auto run_delete_trigger_tests = [&](bool is_before) {
    const std::array vertex_ids{1, 2, 3, 4};
    const std::array edges{EdgeInfo{vertex_ids[0], vertex_ids[1], 5}, EdgeInfo{vertex_ids[2], vertex_ids[3], 6}};
    {
      CreateOnDeleteTriggers(*client, is_before);

      client->BeginTransaction();
      for (const auto vertex_id : vertex_ids) {
        CreateVertex(*client, vertex_id);
      }
      for (const auto &edge : edges) {
        CreateEdge(*client, edge.from_vertex, edge.to_vertex, edge.edge_id);
      }
      client->CommitTransaction();
      CheckNumberOfAllVertices(*client, vertex_ids.size());

      client->BeginTransaction();
      DetachDeleteVertex(*client, vertex_ids[0]);
      DeleteEdge(*client, edges[1].edge_id);
      client->CommitTransaction();

      // :VERTEX         x  4
      // deleted :VERTEX x -1
      // :DELETED_VERTEX x  1
      // :DELETED_EDGE   x  2
      // :DELETED_OBJECT x  3
      constexpr auto kNumberOfExpectedVertices = 9;

      if (is_before) {
        CheckNumberOfAllVertices(*client, kNumberOfExpectedVertices);
      } else {
        WaitForNumberOfAllVertices(*client, kNumberOfExpectedVertices);
      }

      CheckVertexExists(*client, kTriggerDeletedVertexLabel, vertex_ids[0]);
      CheckVertexExists(*client, kTriggerDeletedObjectLabel, vertex_ids[0]);

      for (const auto &edge : edges) {
        CheckVertexExists(*client, kTriggerDeletedEdgeLabel, edge.edge_id);
        CheckVertexExists(*client, kTriggerDeletedObjectLabel, edge.edge_id);
      }

      DropOnDeleteTriggers(*client);
      client->Execute("MATCH (n) DETACH DELETE n;");
      client->DiscardAll();
    }
  };
  constexpr bool kBeforeCommit = true;
  constexpr bool kAfterCommit = false;
  run_delete_trigger_tests(kBeforeCommit);
  run_delete_trigger_tests(kAfterCommit);

  const auto run_delete_trigger_write_procedure_tests = [&]() {
    ExecuteCreateVertex(*client, 2);
    ExecuteCreateVertex(*client, 3);
    client->Execute("MATCH (n {id:2}), (m {id:3}) CALL write.create_edge(n, m, 'edge') YIELD e RETURN e");
    client->DiscardAll();
    CreateOnDeleteTriggers(*client, true);
    client->Execute("MATCH ()-[e]->() CALL write.delete_edge(e)");
    client->DiscardAll();
    client->Execute("MATCH (n {id:2}) CALL write.delete_vertex(n)");
    client->DiscardAll();
    constexpr auto kNumberOfExpectedVertices = 5;
    CheckNumberOfAllVertices(*client, kNumberOfExpectedVertices);
    CheckVertexExists(*client, kTriggerDeletedEdgeLabel, 1);
    CheckVertexExists(*client, kTriggerDeletedObjectLabel, 1);
    CheckVertexExists(*client, kTriggerDeletedVertexLabel, 2);
    CheckVertexExists(*client, kTriggerDeletedObjectLabel, 2);
    DropOnDeleteTriggers(*client);
    client->Execute("MATCH (n) DETACH DELETE n;");
    client->DiscardAll();
  };
  run_delete_trigger_write_procedure_tests();

  const auto run_delete_trigger_write_procedure_delete_detach_test = [&]() {
    ExecuteCreateVertex(*client, 2);
    ExecuteCreateVertex(*client, 3);
    client->Execute("MATCH (n {id:2}), (m {id:3}) CALL write.create_edge(n, m, 'edge') YIELD e RETURN e");
    client->DiscardAll();
    CreateOnDeleteTriggers(*client, true);
    client->Execute("MATCH (v {id:2}) CALL write.detach_delete_vertex(v)");
    client->DiscardAll();
    constexpr auto kNumberOfExpectedVertices = 5;
    CheckNumberOfAllVertices(*client, kNumberOfExpectedVertices);
    CheckVertexExists(*client, kTriggerDeletedEdgeLabel, 1);
    CheckVertexExists(*client, kTriggerDeletedObjectLabel, 1);
    CheckVertexExists(*client, kTriggerDeletedVertexLabel, 2);
    CheckVertexExists(*client, kTriggerDeletedObjectLabel, 2);
    DropOnDeleteTriggers(*client);
    client->Execute("MATCH (n) DETACH DELETE n;");
    client->DiscardAll();
  };
  run_delete_trigger_write_procedure_delete_detach_test();
  return 0;
}

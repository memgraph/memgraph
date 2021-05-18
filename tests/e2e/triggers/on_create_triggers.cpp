#include <string>
#include <string_view>

#include <gflags/gflags.h>
#include <mgclient.hpp>
#include "common.hpp"
#include "utils/logging.hpp"

constexpr std::string_view kTriggerCreatedVertexLabel{"CREATED_VERTEX"};
constexpr std::string_view kTriggerCreatedEdgeLabel{"CREATED_EDGE"};
constexpr std::string_view kTriggerCreatedObjectLabel{"CREATED_OBJECT"};

void CreateOnCreateTriggers(mg::Client &client, std::string_view before_or_after) {
  client.Execute(Concat("CREATE TRIGGER CreatedVerticesTrigger ON () CREATE ", before_or_after,
                        " COMMIT "
                        "EXECUTE "
                        "UNWIND createdVertices as createdVertex "
                        "CREATE (n: ",
                        kTriggerCreatedVertexLabel, " { id: createdVertex.id })"));
  client.DiscardAll();
  client.Execute(Concat("CREATE TRIGGER CreatedEdgesTrigger ON --> CREATE ", before_or_after,
                        " COMMIT "
                        "EXECUTE "
                        "UNWIND createdEdges as createdEdge "
                        "CREATE (n: ",
                        kTriggerCreatedEdgeLabel, " { id: createdEdge.id })"));
  client.DiscardAll();
  client.Execute(
      Concat("CREATE TRIGGER CreatedObjectsTrigger ON CREATE ", before_or_after,
             " COMMIT "
             "EXECUTE "
             "UNWIND createdObjects as createdObjectEvent "
             "WITH CASE createdObjectEvent.event_type WHEN \"created_vertex\" THEN createdObjectEvent.vertex.id ELSE "
             "createdObjectEvent.edge.id END as id "
             "CREATE (n: ",
             kTriggerCreatedObjectLabel, " { id: id })"));
  client.DiscardAll();
}

void DropOnCreateTriggers(mg::Client &client) {
  client.Execute("DROP TRIGGER CreatedVerticesTrigger");
  client.DiscardAll();
  client.Execute("DROP TRIGGER CreatedEdgesTrigger");
  client.DiscardAll();
  client.Execute("DROP TRIGGER CreatedObjectsTrigger");
  client.DiscardAll();
}

int main(int argc, char **argv) {
  gflags::SetUsageMessage("Memgraph E2E ON CREATE Triggers");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  logging::RedirectToStderr();

  mg::Client::Init();

  auto client = Connect();

  const auto run_create_trigger_tests = [&](std::string_view before_or_after) {
    const std::array<int, 2> vertex_ids{1, 2};
    const int edge_id = 3;
    {
      CreateOnCreateTriggers(*client, before_or_after);
      client->BeginTransaction();
      for (const auto vertex_id : vertex_ids) {
        CreateVertex(*client, vertex_id);
        CheckVertexExists(*client, kVertexLabel, vertex_id);
        CheckVertexMissing(*client, kTriggerCreatedVertexLabel, vertex_id);
        CheckVertexMissing(*client, kTriggerCreatedObjectLabel, vertex_id);
      }
      CreateEdge(*client, vertex_ids[0], vertex_ids[1], edge_id);
      CheckVertexMissing(*client, kTriggerCreatedEdgeLabel, edge_id);
      CheckVertexMissing(*client, kTriggerCreatedObjectLabel, edge_id);
      client->CommitTransaction();

      // explicitly created vertex x 2
      // created object vertex     x 3
      // created vertex vertex     x 2
      // created edge vertex       x 1
      constexpr auto kNumberOfExpectedVertices = 8;
      WaitForNumberOfAllVertices(*client, kNumberOfExpectedVertices);

      for (const auto vertex_id : vertex_ids) {
        CheckVertexExists(*client, kTriggerCreatedVertexLabel, vertex_id);
        CheckVertexExists(*client, kTriggerCreatedObjectLabel, vertex_id);
      }
      CheckVertexExists(*client, kTriggerCreatedEdgeLabel, edge_id);
      CheckVertexExists(*client, kTriggerCreatedObjectLabel, edge_id);
      DropOnCreateTriggers(*client);
      client->Execute("MATCH (n) DETACH DELETE n;");
      client->DiscardAll();
    }
  };
  run_create_trigger_tests("BEFORE");
  run_create_trigger_tests("AFTER");

  return 0;
}

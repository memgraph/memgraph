#include <cstdint>
#include <string>
#include <string_view>
#include <thread>

#include <gflags/gflags.h>
#include <mgclient.hpp>
#include "utils/logging.hpp"
#include "utils/timer.hpp"

DEFINE_uint64(bolt_port, 7687, "Bolt port");
DEFINE_uint64(timeout, 120, "Timeout seconds");

constexpr std::string_view kVertexLabel{"VERTEX"};
constexpr std::string_view kEdgeLabel{"EDGE"};
constexpr std::string_view kTriggerCreatedVertexLabel{"CREATED_VERTEX"};
constexpr std::string_view kTriggerCreatedEdgeLabel{"CREATED_EDGE"};
constexpr std::string_view kTriggerCreatedObjectLabel{"CREATED_OBJECT"};

template <typename TInitArg, typename... TArgs>
std::string Concat(TInitArg &&init, TArgs &&...args) {
  std::string result{std::forward<TInitArg>(init)};
  (result.append(args), ...);
  return result;
}

void CreateVertex(mg::Client &client, int vertex_id) {
  mg::Map parameters{
      {"label", mg::Value{kVertexLabel}},
      {"id", mg::Value{vertex_id}},
  };
  std::string query = Concat("CREATE (n: ", kVertexLabel, " { id: $id })");
  client.Execute(query, mg::ConstMap{parameters.ptr()});
  client.DiscardAll();
}

bool IsVertexExists(mg::Client &client, std::string_view label, int vertex_id) {
  mg::Map parameters{
      {"id", mg::Value{vertex_id}},
  };

  std::string query = Concat("MATCH (n: ", label, " {id: $id}) RETURN n.id");
  client.Execute(query, mg::ConstMap{parameters.ptr()});
  const auto result = client.FetchAll();
  if (!result) {
    LOG_FATAL("Vertex with label {} and id {} cannot be found!", label, vertex_id);
  }
  const auto &rows = *result;
  if (rows.size() > 1) {
    LOG_FATAL("Unexpected number of vertices with label {} and id {}, found {} vertices", label, vertex_id,
              rows.size());
  }
  if (rows.empty()) {
    return false;
  }

  if (rows[0][0].ValueInt() != vertex_id) {
    LOG_FATAL("Query returned invalid result, this shouldn't happen!");
  }
  return true;
}

void CheckVertexMissing(mg::Client &client, std::string_view label, int vertex_id) {
  if (IsVertexExists(client, label, vertex_id)) {
    LOG_FATAL("Not expected vertex exist with label {} and id {}!", label, vertex_id);
  }
}

void CheckVertexExists(mg::Client &client, std::string_view label, int vertex_id) {
  if (!IsVertexExists(client, label, vertex_id)) {
    LOG_FATAL("Expected vertex doesn't exist with label {} and id {}!", label, vertex_id);
  }
}

void CreateEdge(mg::Client &client, int from_vertex, int to_vertex, int edge_id) {
  mg::Map parameters{
      {"from", mg::Value{from_vertex}},
      {"to", mg::Value{to_vertex}},
      {"id", mg::Value{edge_id}},
  };
  std::string query = Concat("MATCH (from: ", kVertexLabel, " { id: $from }), (to: ", kVertexLabel,
                             " {id: $to }) "
                             "CREATE (from)-[r: ",
                             kEdgeLabel, " {id: $id}]->(to)");
  client.Execute(query, mg::ConstMap{parameters.ptr()});
  client.DiscardAll();
}

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
  google::SetUsageMessage("Memgraph E2E ON CREATE Triggers");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  logging::RedirectToStderr();

  mg::Client::Init();

  auto client =
      mg::Client::Connect({.host = "127.0.0.1", .port = static_cast<uint16_t>(FLAGS_bolt_port), .use_ssl = false});
  if (!client) {
    LOG_FATAL("Failed to connect!");
  }

  const auto run_create_trigger_tests = [&](std::string_view before_or_after) {
    std::array<int, 2> vertex_ids{1, 2};
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

      utils::Timer timer{};
      bool should_stop = false;
      while (!should_stop) {
        should_stop = true;
        for (const auto vertex_id : vertex_ids) {
          should_stop &= IsVertexExists(*client, kTriggerCreatedVertexLabel, vertex_id);
          should_stop &= IsVertexExists(*client, kTriggerCreatedObjectLabel, vertex_id);
        }
        should_stop &= IsVertexExists(*client, kTriggerCreatedEdgeLabel, edge_id);
        should_stop &= IsVertexExists(*client, kTriggerCreatedObjectLabel, edge_id);
        should_stop |= timer.Elapsed().count() >= 0.5;
      }

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

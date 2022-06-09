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

inline constexpr std::string_view kTriggerCreatedVertexLabel{"CREATED_VERTEX"};
inline constexpr std::string_view kTriggerCreatedEdgeLabel{"CREATED_EDGE"};
inline constexpr std::string_view kTriggerCreatedObjectLabel{"CREATED_OBJECT"};

void CreateOnCreateTriggers(mg::Client &client, bool is_before) {
  const std::string_view before_or_after = is_before ? "BEFORE" : "AFTER";
  client.Execute(
      fmt::format("CREATE TRIGGER CreatedVerticesTrigger ON () CREATE "
                  "{} COMMIT "
                  "EXECUTE "
                  "UNWIND createdVertices as createdVertex "
                  "CREATE (n: {} {{ id: createdVertex.id }})",
                  before_or_after, kTriggerCreatedVertexLabel));
  client.DiscardAll();
  client.Execute(
      fmt::format("CREATE TRIGGER CreatedEdgesTrigger ON --> CREATE "
                  "{} COMMIT "
                  "EXECUTE "
                  "UNWIND createdEdges as createdEdge "
                  "CREATE (n: {} {{ id: createdEdge.id }})",
                  before_or_after, kTriggerCreatedEdgeLabel));
  client.DiscardAll();
  client.Execute(
      fmt::format("CREATE TRIGGER CreatedObjectsTrigger ON CREATE "
                  "{} COMMIT "
                  "EXECUTE "
                  "UNWIND createdObjects as createdObjectEvent "
                  "WITH CASE createdObjectEvent.event_type WHEN \"created_vertex\" THEN createdObjectEvent.vertex.id "
                  "ELSE createdObjectEvent.edge.id END as id "
                  "CREATE (n: {} {{ id: id }})",
                  before_or_after, kTriggerCreatedObjectLabel));
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
  memgraph::logging::RedirectToStderr();

  mg::Client::Init();

  auto client = Connect();

  const auto run_create_trigger_tests = [&](bool is_before) {
    const std::array vertex_ids{1, 2};
    const int edge_id = 3;
    {
      CreateOnCreateTriggers(*client, is_before);
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

      // :VERTEX         x 2
      // :CREATED_VERTEX x 2
      // :CREATED_EDGE   x 1
      // :CREATED_OBJECT x 3
      static constexpr auto kNumberOfExpectedVertices = 8;

      if (is_before) {
        CheckNumberOfAllVertices(*client, kNumberOfExpectedVertices);
      } else {
        WaitForNumberOfAllVertices(*client, kNumberOfExpectedVertices);
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
  static constexpr bool kBeforeCommit = true;
  static constexpr bool kAfterCommit = false;
  run_create_trigger_tests(kBeforeCommit);
  run_create_trigger_tests(kAfterCommit);

  const auto run_create_trigger_write_proc_create_vertex_test = [&]() {
    CreateOnCreateTriggers(*client, true);
    ExecuteCreateVertex(*client, 1);
    static constexpr auto kNumberOfExpectedVertices = 3;
    CheckNumberOfAllVertices(*client, kNumberOfExpectedVertices);
    CheckVertexExists(*client, kTriggerCreatedVertexLabel, 1);
    CheckVertexExists(*client, kTriggerCreatedObjectLabel, 1);
    DropOnCreateTriggers(*client);
    client->Execute("MATCH (n) DETACH DELETE n;");
    client->DiscardAll();
  };
  run_create_trigger_write_proc_create_vertex_test();

  const auto run_create_trigger_write_proc_create_edge_test = [&]() {
    ExecuteCreateVertex(*client, 1);
    ExecuteCreateVertex(*client, 2);
    CreateOnCreateTriggers(*client, true);
    client->Execute("MATCH (n {id:1}), (m {id:2}) CALL write.create_edge(n, m, 'edge') YIELD e RETURN e");
    client->DiscardAll();
    static constexpr auto kNumberOfExpectedVertices = 4;
    CheckNumberOfAllVertices(*client, kNumberOfExpectedVertices);
    CheckVertexExists(*client, kTriggerCreatedEdgeLabel, 1);
    CheckVertexExists(*client, kTriggerCreatedObjectLabel, 1);
    DropOnCreateTriggers(*client);
    client->Execute("MATCH (n) DETACH DELETE n;");
    client->DiscardAll();
  };
  run_create_trigger_write_proc_create_edge_test();

  return 0;
}

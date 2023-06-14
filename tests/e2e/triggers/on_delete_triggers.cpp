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
#include <unordered_set>

#include <gflags/gflags.h>
#include <mgclient.hpp>
#include "common.hpp"
#include "utils/logging.hpp"

inline constexpr std::string_view kTriggerDeletedVertexLabel{"DELETED_VERTEX"};
inline constexpr std::string_view kTriggerDeletedEdgeLabel{"DELETED_EDGE"};
inline constexpr std::string_view kTriggerDeletedObjectLabel{"DELETED_OBJECT"};

enum class AllowedTriggerType : uint8_t {
  VERTEX,
  EDGE,
  OBJECT,
};

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

void CreateOnDeleteTriggers(mg::Client &client, bool is_before,
                            const std::unordered_set<AllowedTriggerType> &allowed_trigger_types) {
  const std::string_view before_or_after = is_before ? "BEFORE" : "AFTER";

  const auto create_on_vertex_delete_trigger = [&, before_or_after] {
    client.Execute(
        fmt::format("CREATE TRIGGER DeletedVerticesTrigger ON () DELETE "
                    "{} COMMIT "
                    "EXECUTE "
                    "UNWIND deletedVertices as deletedVertex "
                    "CREATE (n: {} {{ id: deletedVertex.id }})",
                    before_or_after, kTriggerDeletedVertexLabel));
    client.DiscardAll();
  };

  const auto create_on_edge_delete_trigger = [&, before_or_after] {
    client.Execute(
        fmt::format("CREATE TRIGGER DeletedEdgesTrigger ON --> DELETE "
                    "{} COMMIT "
                    "EXECUTE "
                    "UNWIND deletedEdges as deletedEdge "
                    "CREATE (n: {} {{ id: deletedEdge.id }})",
                    before_or_after, kTriggerDeletedEdgeLabel));
    client.DiscardAll();
  };

  const auto create_on_object_delete_trigger = [&, before_or_after] {
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
  };

  for (const auto allowed_trigger_type : allowed_trigger_types) {
    switch (allowed_trigger_type) {
      case AllowedTriggerType::VERTEX:
        create_on_vertex_delete_trigger();
        break;
      case AllowedTriggerType::EDGE:
        create_on_edge_delete_trigger();
        break;
      case AllowedTriggerType::OBJECT:
        create_on_object_delete_trigger();
        break;
    }
  }
}

void DropOnDeleteTriggers(mg::Client &client, const std::unordered_set<AllowedTriggerType> &allowed_trigger_types) {
  for (const auto allowed_trigger_type : allowed_trigger_types) {
    switch (allowed_trigger_type) {
      case AllowedTriggerType::VERTEX: {
        client.Execute("DROP TRIGGER DeletedVerticesTrigger");
        client.DiscardAll();
        break;
      }
      case AllowedTriggerType::EDGE: {
        client.Execute("DROP TRIGGER DeletedEdgesTrigger");
        client.DiscardAll();
        break;
      }
      case AllowedTriggerType::OBJECT: {
        client.Execute("DROP TRIGGER DeletedObjectsTrigger");
        client.DiscardAll();
        break;
      }
    }
  }
}

struct EdgeInfo {
  int from_vertex;
  int to_vertex;
  int edge_id;
};

void ValidateVertexExistence(mg::Client &client, const bool should_exist, const std::string_view label, const int id) {
  should_exist ? CheckVertexExists(client, label, id) : CheckVertexMissing(client, label, id);
};

int main(int argc, char **argv) {
  gflags::SetUsageMessage("Memgraph E2E ON DELETE Triggers");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memgraph::logging::RedirectToStderr();

  mg::Client::Init();

  auto client = Connect();

  const auto run_delete_trigger_tests = [&](const bool is_before,
                                            const std::unordered_set<AllowedTriggerType> &allowed_trigger_types) {
    static constexpr std::array vertex_ids{1, 2, 3, 4};
    static constexpr std::array edges{EdgeInfo{vertex_ids[0], vertex_ids[1], 5},
                                      EdgeInfo{vertex_ids[2], vertex_ids[3], 6}};
    {
      CreateOnDeleteTriggers(*client, is_before, allowed_trigger_types);

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
      auto number_of_expected_vertices = 3;
      for (const auto allowed_trigger_type : allowed_trigger_types) {
        switch (allowed_trigger_type) {
          case AllowedTriggerType::VERTEX:
            number_of_expected_vertices += 1;
            break;
          case AllowedTriggerType::EDGE:
            number_of_expected_vertices += 2;
            break;
          case AllowedTriggerType::OBJECT:
            number_of_expected_vertices += 3;
            break;
        }
      }

      if (is_before) {
        CheckNumberOfAllVertices(*client, number_of_expected_vertices);
      } else {
        WaitForNumberOfAllVertices(*client, number_of_expected_vertices);
      }

      ValidateVertexExistence(*client, allowed_trigger_types.contains(AllowedTriggerType::VERTEX),
                              kTriggerDeletedVertexLabel, vertex_ids[0]);
      ValidateVertexExistence(*client, allowed_trigger_types.contains(AllowedTriggerType::OBJECT),
                              kTriggerDeletedObjectLabel, vertex_ids[0]);

      for (const auto &edge : edges) {
        ValidateVertexExistence(*client, allowed_trigger_types.contains(AllowedTriggerType::EDGE),
                                kTriggerDeletedEdgeLabel, edge.edge_id);
        ValidateVertexExistence(*client, allowed_trigger_types.contains(AllowedTriggerType::OBJECT),
                                kTriggerDeletedObjectLabel, edge.edge_id);
      }

      DropOnDeleteTriggers(*client, allowed_trigger_types);
      client->Execute("MATCH (n) DETACH DELETE n;");
      client->DiscardAll();
    }
  };

  const auto run_delete_trigger_write_procedure_tests =
      [&](const std::unordered_set<AllowedTriggerType> &allowed_trigger_types) {
        ExecuteCreateVertex(*client, 2);
        ExecuteCreateVertex(*client, 3);
        client->Execute("MATCH (n {id:2}), (m {id:3}) CALL write.create_edge(n, m, 'edge') YIELD e RETURN e");
        client->DiscardAll();
        CreateOnDeleteTriggers(*client, true, allowed_trigger_types);
        client->Execute("MATCH ()-[e]->() CALL write.delete_edge(e)");
        client->DiscardAll();
        client->Execute("MATCH (n {id:2}) CALL write.delete_vertex(n)");
        client->DiscardAll();

        auto number_of_expected_vertices = 1;
        for (const auto allowed_trigger_type : allowed_trigger_types) {
          switch (allowed_trigger_type) {
            case AllowedTriggerType::VERTEX:
              number_of_expected_vertices += 1;
              break;
            case AllowedTriggerType::EDGE:
              number_of_expected_vertices += 1;
              break;
            case AllowedTriggerType::OBJECT:
              number_of_expected_vertices += 2;
              break;
          }
        }

        CheckNumberOfAllVertices(*client, number_of_expected_vertices);

        ValidateVertexExistence(*client, allowed_trigger_types.contains(AllowedTriggerType::EDGE),
                                kTriggerDeletedEdgeLabel, 1);
        ValidateVertexExistence(*client, allowed_trigger_types.contains(AllowedTriggerType::OBJECT),
                                kTriggerDeletedObjectLabel, 1);
        ValidateVertexExistence(*client, allowed_trigger_types.contains(AllowedTriggerType::VERTEX),
                                kTriggerDeletedVertexLabel, 2);
        ValidateVertexExistence(*client, allowed_trigger_types.contains(AllowedTriggerType::OBJECT),
                                kTriggerDeletedObjectLabel, 2);

        DropOnDeleteTriggers(*client, allowed_trigger_types);
        client->Execute("MATCH (n) DETACH DELETE n;");
        client->DiscardAll();
      };

  const auto run_delete_trigger_write_procedure_delete_detach_test =
      [&](const std::unordered_set<AllowedTriggerType> &allowed_trigger_types) {
        ExecuteCreateVertex(*client, 2);
        ExecuteCreateVertex(*client, 3);
        client->Execute("MATCH (n {id:2}), (m {id:3}) CALL write.create_edge(n, m, 'edge') YIELD e RETURN e");
        client->DiscardAll();

        CreateOnDeleteTriggers(*client, true, allowed_trigger_types);

        client->Execute("MATCH (v {id:2}) CALL write.detach_delete_vertex(v)");
        client->DiscardAll();

        auto number_of_expected_vertices = 1;
        for (const auto allowed_trigger_type : allowed_trigger_types) {
          switch (allowed_trigger_type) {
            case AllowedTriggerType::VERTEX:
              number_of_expected_vertices += 1;
              break;
            case AllowedTriggerType::EDGE:
              number_of_expected_vertices += 1;
              break;
            case AllowedTriggerType::OBJECT:
              number_of_expected_vertices += 2;
              break;
          }
        }

        CheckNumberOfAllVertices(*client, number_of_expected_vertices);

        ValidateVertexExistence(*client, allowed_trigger_types.contains(AllowedTriggerType::EDGE),
                                kTriggerDeletedEdgeLabel, 1);
        ValidateVertexExistence(*client, allowed_trigger_types.contains(AllowedTriggerType::OBJECT),
                                kTriggerDeletedObjectLabel, 1);
        ValidateVertexExistence(*client, allowed_trigger_types.contains(AllowedTriggerType::VERTEX),
                                kTriggerDeletedVertexLabel, 2);
        ValidateVertexExistence(*client, allowed_trigger_types.contains(AllowedTriggerType::OBJECT),
                                kTriggerDeletedObjectLabel, 2);

        DropOnDeleteTriggers(*client, allowed_trigger_types);
        client->Execute("MATCH (n) DETACH DELETE n;");
        client->DiscardAll();
      };

  const auto run_for_trigger_combinations = [&](const bool is_before) {
    const std::array trigger_type_combinations{
        std::unordered_set{AllowedTriggerType::VERTEX},
        std::unordered_set{AllowedTriggerType::EDGE},
        std::unordered_set{AllowedTriggerType::OBJECT},
        std::unordered_set{AllowedTriggerType::VERTEX, AllowedTriggerType::EDGE},
        std::unordered_set{AllowedTriggerType::VERTEX, AllowedTriggerType::OBJECT},
        std::unordered_set{AllowedTriggerType::EDGE, AllowedTriggerType::OBJECT},
        std::unordered_set{AllowedTriggerType::VERTEX, AllowedTriggerType::EDGE, AllowedTriggerType::OBJECT},
    };

    for (const auto &allowed_trigger_types : trigger_type_combinations) {
      run_delete_trigger_tests(is_before, allowed_trigger_types);
      run_delete_trigger_write_procedure_tests(allowed_trigger_types);
      run_delete_trigger_write_procedure_delete_detach_test(allowed_trigger_types);
    }
  };

  static constexpr bool kBeforeCommit = true;
  static constexpr bool kAfterCommit = false;
  run_for_trigger_combinations(kBeforeCommit);
  run_for_trigger_combinations(kAfterCommit);

  return 0;
}

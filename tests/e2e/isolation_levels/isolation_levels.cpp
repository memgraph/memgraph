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

#include <fmt/core.h>
#include <gflags/gflags.h>
#include <mgclient.hpp>

#include "query/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/timer.hpp"

#include <iostream>

DEFINE_uint64(bolt_port, 7687, "Bolt port");
DEFINE_uint64(timeout, 120, "Timeout seconds");

namespace {

auto GetClient() {
  auto client =
      mg::Client::Connect({.host = "127.0.0.1", .port = static_cast<uint16_t>(FLAGS_bolt_port), .use_ssl = false});
  MG_ASSERT(client, "Failed to connect!");

  return client;
}

auto GetVertexCount(std::unique_ptr<mg::Client> &client) {
  MG_ASSERT(client->Execute("MATCH (n) RETURN count(n)"));
  auto maybe_row = client->FetchOne();
  MG_ASSERT(maybe_row, "Failed to fetch vertex count");

  const auto &row = *maybe_row;
  MG_ASSERT(row.size() == 1, "Got invalid result for vertex count");

  client->FetchOne();
  return row[0].ValueInt();
}

bool IsDiskStorageMode(std::unique_ptr<mg::Client> &client) {
  MG_ASSERT(client->Execute("SHOW STORAGE INFO"));
  auto maybe_rows = client->FetchAll();
  MG_ASSERT(maybe_rows, "Failed to fetch storage info");

  for (auto &row : *maybe_rows) {
    if (row[0].ValueString() == "storage_mode") {
      return row[1].ValueString() == "ON_DISK_TRANSACTIONAL";
    }
  }
  return false;
}

void CleanDatabase(std::unique_ptr<mg::Client> &client) {
  MG_ASSERT(client->Execute("MATCH (n) DETACH DELETE n;"));
  client->DiscardAll();
}

void SetupCleanDB() {
  auto client = GetClient();
  MG_ASSERT(client->Execute("USE DATABASE memgraph;"));
  client->DiscardAll();
  try {
    client->Execute("DROP DATABASE clean;");
    client->DiscardAll();
  } catch (const mg::ClientException &) {
    // In case clean doesn't exist
  }
  MG_ASSERT(client->Execute("CREATE DATABASE clean;"));
  client->DiscardAll();
  MG_ASSERT(client->Execute("USE DATABASE clean;"));
  client->DiscardAll();
  CleanDatabase(client);
}

void SwitchToDB(const std::string &name, std::unique_ptr<mg::Client> &client) {
  MG_ASSERT(client->Execute(fmt::format("USE DATABASE {};", name)));
  client->DiscardAll();
}

void SwitchToCleanDB(std::unique_ptr<mg::Client> &client) { SwitchToDB("clean", client); }

void SwitchToSameDB(std::unique_ptr<mg::Client> &main, std::unique_ptr<mg::Client> &client) {
  MG_ASSERT(main->Execute("SHOW DATABASES;"));
  auto dbs = main->FetchAll();
  MG_ASSERT(dbs, "Failed to show databases");
  for (const auto &elem : *dbs) {
    MG_ASSERT(elem.size(), "Show databases wrong output");
    const auto &active = elem[1].ValueString();
    if (active == "*") {
      const auto &name = elem[0].ValueString();
      SwitchToDB(std::string(name), client);
      break;
    }
  }
}

void TestSnapshotIsolation(std::unique_ptr<mg::Client> &client) {
  spdlog::info("Verifying SNAPSHOT ISOLATION");

  auto creator = GetClient();
  SwitchToSameDB(client, creator);

  MG_ASSERT(client->BeginTransaction());
  MG_ASSERT(creator->BeginTransaction());

  static constexpr auto vertex_count = 10;
  for (size_t i = 0; i < vertex_count; ++i) {
    MG_ASSERT(creator->Execute("CREATE ()"));
    creator->DiscardAll();

    auto current_vertex_count = GetVertexCount(client);
    MG_ASSERT(current_vertex_count == 0,
              "Invalid number of vertices found for SNAPSHOT ISOLATION (found {}, expected {}). Read vertices from a "
              "transaction which started "
              "at a later point.",
              current_vertex_count, 0);
  }

  MG_ASSERT(creator->CommitTransaction());

  auto current_vertex_count = GetVertexCount(client);
  MG_ASSERT(current_vertex_count == 0,
            "Invalid number of vertices found for SNAPSHOT ISOLATION (found {}, expected {}). Read vertices from a "
            "transaction which started "
            "at a later point.",
            current_vertex_count, 0);
  MG_ASSERT(client->CommitTransaction());
  CleanDatabase(creator);
}

void TestReadCommitted(std::unique_ptr<mg::Client> &client) {
  spdlog::info("Verifying READ COMMITTED");

  auto creator = GetClient();
  SwitchToSameDB(client, creator);

  MG_ASSERT(client->BeginTransaction());
  MG_ASSERT(creator->BeginTransaction());

  static constexpr auto vertex_count = 10;
  for (size_t i = 0; i < vertex_count; ++i) {
    MG_ASSERT(creator->Execute("CREATE ()"));
    creator->DiscardAll();

    auto current_vertex_count = GetVertexCount(client);
    MG_ASSERT(current_vertex_count == 0,
              "Invalid number of vertices found for READ COMMITTED (found {}, expected {}. Read vertices from a "
              "transaction which is not "
              "committed.",
              current_vertex_count, 0);
  }

  MG_ASSERT(creator->CommitTransaction());

  auto current_vertex_count = GetVertexCount(client);
  MG_ASSERT(current_vertex_count == vertex_count,
            "Invalid number of vertices found for READ COMMITTED (found {}, expected {}). Failed to read vertices "
            "from a committed transaction",
            current_vertex_count, vertex_count);
  MG_ASSERT(client->CommitTransaction());
  CleanDatabase(creator);
}

void TestReadUncommitted(std::unique_ptr<mg::Client> &client) {
  spdlog::info("Verifying READ UNCOMMITTED");

  auto creator = GetClient();
  SwitchToSameDB(client, creator);

  MG_ASSERT(client->BeginTransaction());
  MG_ASSERT(creator->BeginTransaction());

  static constexpr auto vertex_count = 10;
  for (size_t i = 1; i <= vertex_count; ++i) {
    MG_ASSERT(creator->Execute("CREATE ()"));
    creator->DiscardAll();

    auto current_vertex_count = GetVertexCount(client);
    MG_ASSERT(current_vertex_count == i,
              "Invalid number of vertices found for READ UNCOMMITTED (found {}, expected {}). Failed to read vertices "
              "from a different transaction.",
              current_vertex_count, i);
  }

  MG_ASSERT(creator->CommitTransaction());

  auto current_vertex_count = GetVertexCount(client);
  MG_ASSERT(current_vertex_count == vertex_count,
            "Invalid number of vertices found for READ UNCOMMITTED (found {}, expected {}). Failed to read vertices "
            "from a different transaction",
            current_vertex_count, vertex_count);
  MG_ASSERT(client->CommitTransaction());
  CleanDatabase(creator);
}

inline constexpr std::array isolation_levels{std::pair{"SNAPSHOT ISOLATION", &TestSnapshotIsolation},
                                             std::pair{"READ COMMITTED", &TestReadCommitted},
                                             std::pair{"READ UNCOMMITTED", &TestReadUncommitted}};

void TestGlobalIsolationLevel(bool isDiskStorage, bool mdb = false) {
  spdlog::info("\n\n----Test global isolation levels----\n");
  auto first_client = GetClient();
  auto second_client = GetClient();

  if (mdb) {
    SwitchToCleanDB(first_client);
    SwitchToCleanDB(second_client);
  }

  for (const auto &[isolation_level, verification_function] : isolation_levels) {
    spdlog::info("--------------------------");

    if (isDiskStorage && strcmp(isolation_level, "SNAPSHOT ISOLATION") != 0) {
      spdlog::info("Skipping for disk storage unsupported isolation level {}", isolation_level);
      continue;
    }

    spdlog::info("Setting global isolation level to {}", isolation_level);
    MG_ASSERT(first_client->Execute(fmt::format("SET GLOBAL TRANSACTION ISOLATION LEVEL {}", isolation_level)));

    first_client->DiscardAll();

    verification_function(first_client);
    verification_function(second_client);
    spdlog::info("--------------------------\n");
  }
}

void TestSessionIsolationLevel(bool isDiskStorage, bool mdb = false) {
  spdlog::info("\n\n----Test session isolation levels----\n");

  auto global_client = GetClient();
  auto session_client = GetClient();

  if (mdb) {
    SwitchToCleanDB(global_client);
    SwitchToCleanDB(session_client);
  }

  for (const auto &[global_isolation_level, global_verification_function] : isolation_levels) {
    if (isDiskStorage && strcmp(global_isolation_level, "SNAPSHOT ISOLATION") != 0) {
      spdlog::info("Skipping for disk storage unsupported global isolation level {}", global_isolation_level);
      continue;
    }
    spdlog::info("Setting global isolation level to {}", global_isolation_level);
    MG_ASSERT(global_client->Execute(fmt::format("SET GLOBAL TRANSACTION ISOLATION LEVEL {}", global_isolation_level)));
    global_client->DiscardAll();

    for (const auto &[session_isolation_level, session_verification_function] : isolation_levels) {
      spdlog::info("--------------------------");
      if (isDiskStorage && strcmp(session_isolation_level, "SNAPSHOT ISOLATION") != 0) {
        spdlog::info("Skipping for disk storage unsupported session isolation level {}", session_isolation_level);
        continue;
      }
      spdlog::info("Setting session isolation level to {}", session_isolation_level);
      MG_ASSERT(
          session_client->Execute(fmt::format("SET SESSION TRANSACTION ISOLATION LEVEL {}", session_isolation_level)));
      session_client->DiscardAll();

      spdlog::info("Verifying client which is using global isolation level");
      global_verification_function(global_client);
      spdlog::info("Verifying client which is using session isolation level");
      session_verification_function(session_client);
      spdlog::info("--------------------------\n");
    }
  }
}

// Priority of applying the isolation level from highest priority NEXT -> SESSION -> GLOBAL
void TestNextIsolationLevel(bool isDiskStorage, bool mdb = false) {
  spdlog::info("\n\n----Test next isolation levels----\n");

  auto global_client = GetClient();
  auto session_client = GetClient();

  if (mdb) {
    SwitchToCleanDB(global_client);
    SwitchToCleanDB(session_client);
  }

  for (const auto &[global_isolation_level, global_verification_function] : isolation_levels) {
    if (isDiskStorage && strcmp(global_isolation_level, "SNAPSHOT ISOLATION") != 0) {
      spdlog::info("Skipping for disk storage unsupported global isolation level {}", global_isolation_level);
      continue;
    }
    spdlog::info("Setting global isolation level to {}", global_isolation_level);

    MG_ASSERT(global_client->Execute(fmt::format("SET GLOBAL TRANSACTION ISOLATION LEVEL {}", global_isolation_level)));
    global_client->DiscardAll();

    for (const auto &[session_isolation_level, session_verification_function] : isolation_levels) {
      if (isDiskStorage && strcmp(session_isolation_level, "SNAPSHOT ISOLATION") != 0) {
        spdlog::info("Skipping for disk storage unsupported session isolation level {}", session_isolation_level);
        continue;
      }
      spdlog::info("Setting session isolation level to {}", session_isolation_level);
      MG_ASSERT(
          session_client->Execute(fmt::format("SET SESSION TRANSACTION ISOLATION LEVEL {}", session_isolation_level)));
      session_client->DiscardAll();

      for (const auto &[next_isolation_level, next_verification_function] : isolation_levels) {
        spdlog::info("--------------------------");
        spdlog::info("Verifying client which is using global isolation level");
        global_verification_function(global_client);
        spdlog::info("Verifying client which is using session isolation level");
        session_verification_function(session_client);

        if (isDiskStorage && strcmp(next_isolation_level, "SNAPSHOT ISOLATION") != 0) {
          spdlog::info("Skipping for disk storage unsupported next transaction isolation level {}",
                       next_isolation_level);
          continue;
        }
        spdlog::info("Setting isolation level of the next transaction to {}", next_isolation_level);
        MG_ASSERT(global_client->Execute(fmt::format("SET NEXT TRANSACTION ISOLATION LEVEL {}", next_isolation_level)));
        global_client->DiscardAll();
        MG_ASSERT(
            session_client->Execute(fmt::format("SET NEXT TRANSACTION ISOLATION LEVEL {}", next_isolation_level)));
        session_client->DiscardAll();

        spdlog::info("Verifying client which is using global isolation level while next isolation level is set");
        next_verification_function(global_client);
        spdlog::info("Verifying client which is using session isolation level while next isolation level is set");
        next_verification_function(session_client);

        spdlog::info("Verifying client which is using global isolation level after the next isolation level was used");
        global_verification_function(global_client);
        spdlog::info("Verifying client which is using session isolation level after the next isolation level was used");
        session_verification_function(session_client);
        spdlog::info("--------------------------\n");
      }
    }
  }
}

}  // namespace

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E Isolation Levels");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memgraph::logging::RedirectToStderr();

  mg::Client::Init();

  auto client = GetClient();
  bool isDiskStorage = IsDiskStorageMode(client);
  client->DiscardAll();
  bool multiDB = false;

  TestGlobalIsolationLevel(isDiskStorage);
  TestSessionIsolationLevel(isDiskStorage);
  TestNextIsolationLevel(isDiskStorage);

  // MultiDB tests
  multiDB = true;
  spdlog::info("--------------------------");
  spdlog::info("---- RUNNING MULTI DB ----");
  spdlog::info("--------------------------");
  SetupCleanDB();
  TestGlobalIsolationLevel(isDiskStorage, multiDB);
  TestSessionIsolationLevel(isDiskStorage, multiDB);
  TestNextIsolationLevel(isDiskStorage, multiDB);

  return 0;
}

// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <chrono>
#include <string>
#include <string_view>
#include <thread>

#include <gflags/gflags.h>
#include <mgclient.hpp>
#include "common.hpp"
#include "utils/logging.hpp"

namespace {

inline constexpr int kVertexId{42};
inline constexpr std::string_view kDefinerUser{"DEFINER_USER"};
inline constexpr std::string_view kInvokerUser{"INVOKER_USER"};
inline constexpr std::string_view kInvokerWithoutSet{"INVOKER_WITHOUT_SET"};
inline constexpr std::string_view kDefinerWithoutSet{"DEFINER_WITHOUT_SET"};
inline constexpr std::string_view kTriggerProperty{"triggered"};

template <typename TException>
bool FunctionThrows(const auto &function) {
  try {
    function();
  } catch (const TException & /*unused*/) {
    return true;
  }
  return false;
}

void CreateTrigger(mg::Client &client, std::string_view trigger_name, const std::string &phase,
                   std::string_view security_mode = "") {
  std::string security_clause = security_mode.empty() ? "" : fmt::format(" SECURITY {}", security_mode);
  client.Execute(
      fmt::format("CREATE TRIGGER {}{} ON CREATE "
                  "{} COMMIT EXECUTE "
                  "UNWIND createdVertices as createdVertex "
                  "SET createdVertex.{} = true",
                  trigger_name, security_clause, phase, kTriggerProperty));
  client.DiscardAll();
}

void DropTrigger(mg::Client &client, std::string_view trigger_name) {
  client.Execute(fmt::format("DROP TRIGGER {};", trigger_name));
  client.DiscardAll();
}

void CreateUser(mg::Client &client, std::string_view username) {
  client.Execute(fmt::format("CREATE USER {};", username));
  client.DiscardAll();
}

void DropUser(mg::Client &client, std::string_view username) {
  client.Execute(fmt::format("DROP USER {};", username));
  client.DiscardAll();
}

void GrantAllPrivileges(mg::Client &client, std::string_view username) {
  // Grant all global privileges
  client.Execute(fmt::format("GRANT ALL PRIVILEGES TO {};", username));
  client.DiscardAll();
  // Grant all label-based privileges
  client.Execute(fmt::format("GRANT * ON NODES CONTAINING LABELS * TO {};", username));
  client.DiscardAll();
}

void DenySet(mg::Client &client, std::string_view username) {
  client.Execute(fmt::format("DENY SET TO {};", username));
  client.DiscardAll();
}

void CleanupVertices(mg::Client &client) {
  client.Execute("MATCH (n) DETACH DELETE n;");
  client.DiscardAll();
  CheckNumberOfAllVertices(client, 0);
}

bool VertexHasProperty(mg::Client &client, int vertex_id, std::string_view property_name) {
  mg::Map parameters{{"id", mg::Value{vertex_id}}, {"prop", mg::Value{std::string{property_name}}}};
  client.Execute(fmt::format("MATCH (n: {} {{id: $id}}) RETURN n.{} as prop", kVertexLabel, property_name),
                 mg::ConstMap{parameters.ptr()});
  auto result = client.FetchAll();
  if (!result || result->empty()) {
    return false;
  }
  const auto &value = result->at(0)[0];
  return value.type() == mg::Value::Type::Bool && value.ValueBool();
}

// Test 1: Default DEFINER mode - definer needs SET permission
void TestDefaultInvoker(mg::Client &admin_client, const std::string &phase) {
  auto definer_client = ConnectWithUser(kDefinerUser);
  auto invoker_client = ConnectWithUser(kInvokerUser);
  auto invoker_without_set_client = ConnectWithUser(kInvokerWithoutSet);

  const bool is_after = (phase == "AFTER");

  // Definer creates trigger (default is DEFINER)
  CreateTrigger(*definer_client, "DefaultInvoker", phase, "");

  // Any invoker can trigger it successfully (definer's permissions are used)
  CreateVertex(*invoker_client, kVertexId);
  if (is_after) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  CheckNumberOfAllVertices(*invoker_client, 1);
  MG_ASSERT(VertexHasProperty(*invoker_client, kVertexId, kTriggerProperty),
            "Vertex should have trigger property set (definer's permissions)");

  CleanupVertices(*invoker_client);

  // Invoker without SET can also trigger it successfully (definer's permissions are used)
  CreateVertex(*invoker_without_set_client, kVertexId);
  if (is_after) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  CheckNumberOfAllVertices(*invoker_without_set_client, 1);
  MG_ASSERT(VertexHasProperty(*invoker_without_set_client, kVertexId, kTriggerProperty),
            "Vertex should have trigger property set (definer's permissions)");

  CleanupVertices(*invoker_without_set_client);
  DropTrigger(admin_client, "DefaultInvoker");
}

// Test 2: Explicit INVOKER mode - tests that explicit SECURITY INVOKER grammar works
void TestExplicitInvoker(mg::Client &admin_client, const std::string &phase) {
  auto definer_client = ConnectWithUser(kDefinerUser);
  auto invoker_client = ConnectWithUser(kInvokerUser);
  auto invoker_without_set_client = ConnectWithUser(kInvokerWithoutSet);

  const bool is_after = (phase == "AFTER");

  // Definer creates trigger with explicit INVOKER
  CreateTrigger(*definer_client, "ExplicitInvoker", phase, "INVOKER");

  // Invoker with SET can trigger it successfully
  CreateVertex(*invoker_client, kVertexId);
  if (is_after) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  CheckNumberOfAllVertices(*invoker_client, 1);
  MG_ASSERT(VertexHasProperty(*invoker_client, kVertexId, kTriggerProperty), "Vertex should have trigger property set");

  CleanupVertices(*invoker_client);

  // Invoker without SET - behavior differs for BEFORE vs AFTER COMMIT
  if (!is_after) {
    // BEFORE COMMIT: transaction should fail
    MG_ASSERT(FunctionThrows<mg::TransientException>([&] { CreateVertex(*invoker_without_set_client, kVertexId); }),
              "BEFORE COMMIT trigger should fail transaction when invoker lacks SET privilege");
    CheckNumberOfAllVertices(*invoker_without_set_client, 0);
  } else {
    // AFTER COMMIT: transaction succeeds but trigger fails, property not set
    CreateVertex(*invoker_without_set_client, kVertexId);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    CheckNumberOfAllVertices(*invoker_without_set_client, 1);
    MG_ASSERT(!VertexHasProperty(*invoker_without_set_client, kVertexId, kTriggerProperty),
              "Vertex should not have trigger property set when invoker lacks SET privilege");
  }

  CleanupVertices(*invoker_without_set_client);
  DropTrigger(admin_client, "ExplicitInvoker");
}

// Test 3: DEFINER mode - definer needs SET permission
void TestDefiner(mg::Client &admin_client, const std::string &phase) {
  auto definer_client = ConnectWithUser(kDefinerUser);
  auto definer_without_set_client = ConnectWithUser(kDefinerWithoutSet);
  auto invoker_client = ConnectWithUser(kInvokerUser);
  auto invoker_without_set_client = ConnectWithUser(kInvokerWithoutSet);

  // Definer with SET creates trigger with DEFINER mode
  CreateTrigger(*definer_client, "Definer", phase, "DEFINER");

  // Any invoker can trigger it successfully (definer's permissions are used)
  CreateVertex(*invoker_client, kVertexId);

  const bool is_after = (phase == "AFTER");
  if (is_after) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  CheckNumberOfAllVertices(*invoker_client, 1);
  MG_ASSERT(VertexHasProperty(*invoker_client, kVertexId, kTriggerProperty),
            "Vertex should have trigger property set (definer's permissions)");

  CleanupVertices(*invoker_client);

  CreateVertex(*invoker_without_set_client, kVertexId);
  if (is_after) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  CheckNumberOfAllVertices(*invoker_without_set_client, 1);
  MG_ASSERT(VertexHasProperty(*invoker_without_set_client, kVertexId, kTriggerProperty),
            "Vertex should have trigger property set (definer's permissions)");

  CleanupVertices(*invoker_without_set_client);
  DropTrigger(admin_client, "Definer");
}

// Test 4: DEFINER mode creation fails if definer lacks SET permission
void TestDefinerCreationFails(const std::string &phase) {
  auto definer_without_set_client = ConnectWithUser(kDefinerWithoutSet);

  // Definer without SET cannot create trigger with DEFINER mode
  MG_ASSERT(FunctionThrows<mg::TransientException>(
                [&] { CreateTrigger(*definer_without_set_client, "DefinerFail", phase, "DEFINER"); }),
            "Creating DEFINER trigger without SET privilege should fail");
}

}  // namespace

int main(int argc, char **argv) {
  gflags::SetUsageMessage("Memgraph E2E Triggers privilege check");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memgraph::logging::RedirectToStderr();

  mg::Client::Init();

  auto admin_client = Connect();

  CreateUser(*admin_client, kDefinerUser);
  CreateUser(*admin_client, kDefinerWithoutSet);
  CreateUser(*admin_client, kInvokerUser);
  CreateUser(*admin_client, kInvokerWithoutSet);

  GrantAllPrivileges(*admin_client, kDefinerUser);
  GrantAllPrivileges(*admin_client, kDefinerWithoutSet);
  GrantAllPrivileges(*admin_client, kInvokerUser);
  GrantAllPrivileges(*admin_client, kInvokerWithoutSet);

  DenySet(*admin_client, kDefinerWithoutSet);
  DenySet(*admin_client, kInvokerWithoutSet);

  // Run tests for both BEFORE COMMIT and AFTER COMMIT triggers
  TestDefaultInvoker(*admin_client, "BEFORE");
  TestDefaultInvoker(*admin_client, "AFTER");
  TestExplicitInvoker(*admin_client, "BEFORE");
  TestExplicitInvoker(*admin_client, "AFTER");
  TestDefiner(*admin_client, "BEFORE");
  TestDefiner(*admin_client, "AFTER");
  TestDefinerCreationFails("BEFORE");
  TestDefinerCreationFails("AFTER");

  DropUser(*admin_client, kDefinerUser);
  DropUser(*admin_client, kDefinerWithoutSet);
  DropUser(*admin_client, kInvokerUser);
  DropUser(*admin_client, kInvokerWithoutSet);

  return 0;
}

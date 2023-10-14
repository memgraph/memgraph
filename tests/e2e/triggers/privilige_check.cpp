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

#include <string>
#include <string_view>

#include <gflags/gflags.h>
#include <mgclient.hpp>
#include "common.hpp"
#include "utils/logging.hpp"

inline constexpr std::string_view kTriggerPrefix{"CreatedVerticesTrigger"};

template <typename TException>
bool FunctionThrows(const auto &function) {
  try {
    function();
  } catch (const TException & /*unused*/) {
    return true;
  }
  return false;
}

int main(int argc, char **argv) {
  gflags::SetUsageMessage("Memgraph E2E Triggers privilege check");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memgraph::logging::RedirectToStderr();

  static constexpr int kVertexId{42};
  static constexpr std::string_view kUserlessLabel{"USERLESS"};
  static constexpr std::string_view kAdminUser{"ADMIN"};
  static constexpr std::string_view kUserWithCreate{"USER_WITH_CREATE"};
  static constexpr std::string_view kUserWithoutCreate{"USER_WITHOUT_CREATE"};

  mg::Client::Init();

  auto userless_client = Connect();

  const auto get_number_of_triggers = [&userless_client] {
    userless_client->Execute("SHOW TRIGGERS");
    auto result = userless_client->FetchAll();
    MG_ASSERT(result.has_value());
    return result->size();
  };

  auto create_trigger = [&get_number_of_triggers](mg::Client &client, const std::string_view vertexLabel,
                                                  bool should_succeed = true) {
    const auto number_of_triggers_before = get_number_of_triggers();
    client.Execute(
        fmt::format("CREATE TRIGGER {}{} ON () CREATE "
                    "AFTER COMMIT "
                    "EXECUTE "
                    "UNWIND createdVertices as createdVertex "
                    "CREATE (n: {} {{ id: createdVertex.id }})",
                    kTriggerPrefix, vertexLabel, vertexLabel));
    const bool succeeded = !FunctionThrows<mg::TransientException>([&] { client.DiscardAll(); });

    MG_ASSERT(succeeded == should_succeed, "Unexpected outcome from creating triggers: expected {}, actual {}",
              should_succeed, succeeded);
    const auto number_of_triggers_after = get_number_of_triggers();
    if (should_succeed) {
      MG_ASSERT(number_of_triggers_after == number_of_triggers_before + 1);
    } else {
      MG_ASSERT(number_of_triggers_after == number_of_triggers_before);
    }
  };

  auto delete_vertices = [&userless_client] {
    userless_client->Execute("MATCH (n) DETACH DELETE n;");
    userless_client->DiscardAll();
    CheckNumberOfAllVertices(*userless_client, 0);
  };

  auto create_user = [&userless_client](const std::string_view username) {
    userless_client->Execute(fmt::format("CREATE USER {};", username));
    userless_client->DiscardAll();
    userless_client->Execute(fmt::format("GRANT TRIGGER TO {};", username));
    userless_client->DiscardAll();
  };

  auto drop_user = [&userless_client](const std::string_view username) {
    userless_client->Execute(fmt::format("DROP USER {};", username));
    userless_client->DiscardAll();
  };

  auto drop_trigger_of_user = [&userless_client](const std::string_view username) {
    userless_client->Execute(fmt::format("DROP TRIGGER {}{};", kTriggerPrefix, username));
    userless_client->DiscardAll();
  };

  // Single trigger created without user, there is no existing users
  create_trigger(*userless_client, kUserlessLabel);
  CreateVertex(*userless_client, kVertexId);

  WaitForNumberOfAllVertices(*userless_client, 2);
  CheckVertexExists(*userless_client, kVertexLabel, kVertexId);
  CheckVertexExists(*userless_client, kUserlessLabel, kVertexId);

  delete_vertices();

  // Single trigger created without user, there is an existing user
  // The trigger fails because there is no owner
  create_user(kAdminUser);
  CreateVertex(*userless_client, kVertexId);

  CheckVertexExists(*userless_client, kVertexLabel, kVertexId);
  CheckNumberOfAllVertices(*userless_client, 1);

  delete_vertices();

  // Three triggers: without an owner, an owner with CREATE privilege, an owner without CREATE privilege; there are
  // existing users
  // Only the trigger which owner has CREATE privilege will succeed
  create_user(kUserWithCreate);
  userless_client->Execute(fmt::format("GRANT CREATE TO {};", kUserWithCreate));
  userless_client->DiscardAll();
  create_user(kUserWithoutCreate);
  auto client_with_create = ConnectWithUser(kUserWithCreate);
  auto client_without_create = ConnectWithUser(kUserWithoutCreate);
  create_trigger(*client_with_create, kUserWithCreate);
  create_trigger(*client_without_create, kUserWithoutCreate, false);

  // Grant CREATE to be able to create the trigger than revoke it
  userless_client->Execute(fmt::format("GRANT CREATE TO {};", kUserWithoutCreate));
  userless_client->DiscardAll();
  create_trigger(*client_without_create, kUserWithoutCreate);
  userless_client->Execute(fmt::format("REVOKE CREATE FROM {};", kUserWithoutCreate));
  userless_client->DiscardAll();

  CreateVertex(*userless_client, kVertexId);

  WaitForNumberOfAllVertices(*userless_client, 2);
  CheckVertexExists(*userless_client, kVertexLabel, kVertexId);
  CheckVertexExists(*userless_client, kUserWithCreate, kVertexId);

  delete_vertices();

  // Three triggers: without an owner, an owner with CREATE privilege, an owner without CREATE privilege; there is no
  // existing user
  // All triggers will succeed, as there is no authorization is done when there are no users
  drop_user(kAdminUser);
  drop_user(kUserWithCreate);
  drop_user(kUserWithoutCreate);
  CreateVertex(*userless_client, kVertexId);

  WaitForNumberOfAllVertices(*userless_client, 4);
  CheckVertexExists(*userless_client, kVertexLabel, kVertexId);
  CheckVertexExists(*userless_client, kUserlessLabel, kVertexId);
  CheckVertexExists(*userless_client, kUserWithCreate, kVertexId);
  CheckVertexExists(*userless_client, kUserWithoutCreate, kVertexId);

  delete_vertices();
  drop_trigger_of_user(kUserlessLabel);
  drop_trigger_of_user(kUserWithCreate);
  drop_trigger_of_user(kUserWithoutCreate);

  // The BEFORE COMMIT trigger without proper privileges make the transaction fail
  create_user(kUserWithoutCreate);
  userless_client->Execute(fmt::format("GRANT CREATE TO {};", kUserWithoutCreate));
  userless_client->DiscardAll();
  client_without_create->Execute(
      fmt::format("CREATE TRIGGER {}{} ON () CREATE "
                  "BEFORE COMMIT "
                  "EXECUTE "
                  "UNWIND createdVertices as createdVertex "
                  "CREATE (n: {} {{ id: createdVertex.id }})",
                  kTriggerPrefix, kUserWithoutCreate, kUserWithoutCreate));
  client_without_create->DiscardAll();

  userless_client->Execute(fmt::format("REVOKE CREATE FROM {};", kUserWithoutCreate));
  userless_client->DiscardAll();

  MG_ASSERT(FunctionThrows<mg::TransientException>([&] { CreateVertex(*userless_client, kVertexId); }),
            "Create should have thrown because user doesn't have privilege for CREATE");
  CheckNumberOfAllVertices(*userless_client, 0);

  return 0;
}

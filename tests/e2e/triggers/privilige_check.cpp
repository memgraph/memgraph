#include <string>
#include <string_view>

#include <gflags/gflags.h>
#include <spdlog/fmt/bundled/core.h>
#include <mgclient.hpp>
#include "common.hpp"
#include "utils/logging.hpp"

constexpr std::string_view kTriggerPrefix{"CreatedVerticesTrigger"};
void CreateOnCreateTrigger(mg::Client &client, const std::string_view vertexLabel) {
  client.Execute(
      fmt::format("CREATE TRIGGER {}{} ON () CREATE "
                  "AFTER COMMIT "
                  "EXECUTE "
                  "UNWIND createdVertices as createdVertex "
                  "CREATE (n: {} {{ id: createdVertex.id }})",
                  kTriggerPrefix, vertexLabel, vertexLabel));
  client.DiscardAll();
}

int main(int argc, char **argv) {
  gflags::SetUsageMessage("Memgraph E2E Triggers privilege check");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  logging::RedirectToStderr();

  constexpr int kVertexId{42};
  constexpr std::string_view kUserlessLabel{"USERLESS"};
  constexpr std::string_view kAdminUser{"ADMIN"};
  constexpr std::string_view kUserWithCreate{"USER_WITH_CREATE"};
  constexpr std::string_view kUserWithoutCreate{"USER_WITHOUT_CREATE"};

  mg::Client::Init();

  auto userless_client = Connect();

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
  CreateOnCreateTrigger(*userless_client, kUserlessLabel);
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
  CreateOnCreateTrigger(*client_with_create, kUserWithCreate);
  CreateOnCreateTrigger(*client_without_create, kUserWithoutCreate);
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

  // The trigger without proper privileges make the transaction fail
  create_user(kUserWithoutCreate);
  client_with_create->Execute(
      fmt::format("CREATE TRIGGER {}{} ON () CREATE "
                  "BEFORE COMMIT "
                  "EXECUTE "
                  "UNWIND createdVertices as createdVertex "
                  "CREATE (n: {} {{ id: createdVertex.id }})",
                  kTriggerPrefix, kUserWithoutCreate, kUserWithoutCreate));
  client_with_create->DiscardAll();

  CreateVertex(*userless_client, kVertexId);
  CheckNumberOfAllVertices(*userless_client, 0);

  return 0;
}

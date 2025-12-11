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

inline constexpr std::string_view kTriggerPrefix{"CreatedVerticesTrigger"};
inline constexpr std::string_view kSecurityTestTriggerPrefix{"SecurityTestTrigger"};
inline constexpr int kVertexId{42};
inline constexpr std::chrono::milliseconds kAfterCommitWaitTime{200};

namespace {
template <typename TException>
bool FunctionThrows(const auto &function) {
  try {
    function();
  } catch (const TException & /*unused*/) {
    return true;
  }
  return false;
}

// Helper class for managing database operations
class DatabaseHelper {
 public:
  explicit DatabaseHelper(mg::Client &client) : client_(client) {}

  void DeleteAllVertices() {
    client_.Execute("MATCH (n) DETACH DELETE n;");
    client_.DiscardAll();
    CheckNumberOfAllVertices(client_, 0);
  }

  void CreateUser(const std::string_view username) {
    client_.Execute(fmt::format("CREATE USER {};", username));
    client_.DiscardAll();
  }

  void DropUser(const std::string_view username) {
    client_.Execute(fmt::format("DROP USER {};", username));
    client_.DiscardAll();
  }

  void GrantPrivilege(const std::string_view privilege, const std::string_view username) {
    client_.Execute(fmt::format("GRANT {} TO {};", privilege, username));
    client_.DiscardAll();
  }

  void RevokePrivilege(const std::string_view privilege, const std::string_view username) {
    client_.Execute(fmt::format("REVOKE {} FROM {};", privilege, username));
    client_.DiscardAll();
  }

  void CreateUserWithTriggerPrivilege(const std::string_view username) {
    CreateUser(username);
    GrantPrivilege("TRIGGER", username);
  }

  size_t GetNumberOfTriggers() {
    client_.Execute("SHOW TRIGGERS");
    auto result = client_.FetchAll();
    MG_ASSERT(result.has_value());
    return result->size();
  }

  void DropTrigger(const std::string_view trigger_name) {
    client_.Execute(fmt::format("DROP TRIGGER {};", trigger_name));
    client_.DiscardAll();
  }

 private:
  mg::Client &client_;
};

// Helper function to create a trigger with a specific label
void CreateTriggerWithLabel(mg::Client &client, const std::string_view trigger_name, const std::string_view label,
                            const std::string_view prefix = kTriggerPrefix) {
  client.Execute(
      fmt::format("CREATE TRIGGER {}{} ON () CREATE "
                  "AFTER COMMIT "
                  "EXECUTE "
                  "UNWIND createdVertices as createdVertex "
                  "CREATE (n: {} {{ id: createdVertex.id }})",
                  prefix, trigger_name, label));
  client.DiscardAll();
}

// Helper function to create a trigger with security mode
void CreateTriggerWithSecurity(mg::Client &client, const std::string_view trigger_name, const std::string_view label,
                               const std::string_view phase, const std::string_view security_mode,
                               const std::string_view prefix) {
  std::string security_clause = security_mode.empty() ? "" : fmt::format(" {} ", security_mode);
  client.Execute(
      fmt::format("CREATE TRIGGER {}{} ON () CREATE "
                  "{} COMMIT{}"
                  "EXECUTE "
                  "UNWIND createdVertices as createdVertex "
                  "CREATE (n: {} {{ id: createdVertex.id }})",
                  prefix, trigger_name, phase, security_clause, label));
  client.DiscardAll();
}

// Helper function to verify trigger creation
bool CreateTriggerAndVerify(DatabaseHelper &db_helper, mg::Client &client, const std::string_view vertex_label,
                            bool should_succeed = true) {
  const auto number_of_triggers_before = db_helper.GetNumberOfTriggers();
  CreateTriggerWithLabel(client, vertex_label, vertex_label);
  const bool succeeded = !FunctionThrows<mg::TransientException>([&] { client.DiscardAll(); });

  MG_ASSERT(succeeded == should_succeed, "Unexpected outcome from creating triggers: expected {}, actual {}",
            should_succeed, succeeded);
  const auto number_of_triggers_after = db_helper.GetNumberOfTriggers();
  if (should_succeed) {
    MG_ASSERT(number_of_triggers_after == number_of_triggers_before + 1);
  } else {
    MG_ASSERT(number_of_triggers_after == number_of_triggers_before);
  }
  return succeeded;
}

// Test helper for security mode testing
class SecurityTestHelper {
 public:
  SecurityTestHelper() : admin_client_(Connect()), db_helper_(*admin_client_) {}

  void SetupUsers(const std::string_view definer_user, const std::string_view invoker_user) {
    db_helper_.CreateUserWithTriggerPrivilege(definer_user);
    db_helper_.CreateUserWithTriggerPrivilege(invoker_user);
    db_helper_.GrantPrivilege("CREATE", definer_user);
    // Do NOT grant CREATE to invoker (this is the key test)
  }

  void CleanupUsers(const std::string_view definer_user, const std::string_view invoker_user) {
    db_helper_.DropUser(definer_user);
    db_helper_.DropUser(invoker_user);
  }

  void CreateTrigger(const std::string_view trigger_name, const std::string_view label, const std::string_view phase,
                     const std::string_view security_mode = "") {
    auto definer_client = ConnectWithUser(definer_user_);
    CreateTriggerWithSecurity(*definer_client, trigger_name, label, phase, security_mode, kSecurityTestTriggerPrefix);
  }

  void TestInvokerPermissions(const std::string_view trigger_name, const std::string_view label,
                              const std::string_view phase, const std::string_view security_mode = "") {
    CreateTrigger(trigger_name, label, phase, security_mode);

    // Grant CREATE temporarily so invoker can create the initial vertex
    db_helper_.GrantPrivilege("CREATE", invoker_user_);
    auto invoker_client = ConnectWithUser(invoker_user_);
    CreateVertex(*invoker_client, kVertexId);
    db_helper_.RevokePrivilege("CREATE", invoker_user_);

    if (phase == "AFTER") {
      std::this_thread::sleep_for(kAfterCommitWaitTime);
    }

    // Verify: original vertex exists, but trigger-created vertex should NOT exist
    CheckVertexExists(*admin_client_, kVertexLabel, kVertexId);
    CheckVertexMissing(*admin_client_, label, kVertexId);
    CheckNumberOfAllVertices(*admin_client_, 1);

    db_helper_.DeleteAllVertices();
    db_helper_.DropTrigger(fmt::format("{}{}", kSecurityTestTriggerPrefix, trigger_name));
  }

  void TestDefinerPermissions(const std::string_view trigger_name, const std::string_view label,
                              const std::string_view phase, const std::string_view security_mode = "SECURITY DEFINER") {
    CreateTrigger(trigger_name, label, phase, security_mode);

    // Grant CREATE temporarily so invoker can create the initial vertex
    db_helper_.GrantPrivilege("CREATE", invoker_user_);
    auto invoker_client = ConnectWithUser(invoker_user_);
    CreateVertex(*invoker_client, kVertexId);
    db_helper_.RevokePrivilege("CREATE", invoker_user_);

    if (phase == "AFTER") {
      std::this_thread::sleep_for(kAfterCommitWaitTime);
    }

    // Verify: both vertices should exist (using definer's permissions)
    CheckVertexExists(*admin_client_, kVertexLabel, kVertexId);
    CheckVertexExists(*admin_client_, label, kVertexId);
    CheckNumberOfAllVertices(*admin_client_, 2);

    db_helper_.DeleteAllVertices();
    db_helper_.DropTrigger(fmt::format("{}{}", kSecurityTestTriggerPrefix, trigger_name));
  }

  void TestBeforeCommitInvokerFailure(const std::string_view trigger_name, const std::string_view label) {
    // Grant CREATE temporarily so trigger can be created
    db_helper_.GrantPrivilege("CREATE", invoker_user_);
    CreateTrigger(trigger_name, label, "BEFORE", "");
    db_helper_.RevokePrivilege("CREATE", invoker_user_);

    // This should fail because BEFORE COMMIT triggers check permissions immediately
    auto invoker_client = ConnectWithUser(invoker_user_);
    MG_ASSERT(FunctionThrows<mg::TransientException>([&] { CreateVertex(*invoker_client, kVertexId); }),
              "Create should have thrown because invoker doesn't have CREATE privilege");
    CheckNumberOfAllVertices(*admin_client_, 0);

    db_helper_.DropTrigger(fmt::format("{}{}", kSecurityTestTriggerPrefix, trigger_name));
  }

  void SetUsers(const std::string_view definer_user, const std::string_view invoker_user) {
    definer_user_ = definer_user;
    invoker_user_ = invoker_user;
  }

  DatabaseHelper &GetDbHelper() { return db_helper_; }

 private:
  std::unique_ptr<mg::Client> admin_client_;
  DatabaseHelper db_helper_;
  std::string_view definer_user_;
  std::string_view invoker_user_;
};

// Test functions for basic privilege checks
void TestTriggerWithoutUser(DatabaseHelper &db_helper) {
  auto userless_client = Connect();
  CreateTriggerWithLabel(*userless_client, "USERLESS", "USERLESS");
  CreateVertex(*userless_client, kVertexId);

  WaitForNumberOfAllVertices(*userless_client, 2);
  CheckVertexExists(*userless_client, kVertexLabel, kVertexId);
  CheckVertexExists(*userless_client, "USERLESS", kVertexId);
  db_helper.DeleteAllVertices();
}

void TestTriggerWithExistingUser(DatabaseHelper &db_helper, const std::string_view admin_user) {
  db_helper.CreateUserWithTriggerPrivilege(admin_user);
  auto userless_client = Connect();
  CreateVertex(*userless_client, kVertexId);

  CheckVertexExists(*userless_client, kVertexLabel, kVertexId);
  CheckNumberOfAllVertices(*userless_client, 1);
  db_helper.DeleteAllVertices();
}

void TestTriggersWithPrivileges(DatabaseHelper &db_helper, const std::string_view user_with_create,
                                const std::string_view user_without_create) {
  db_helper.CreateUserWithTriggerPrivilege(user_with_create);
  db_helper.GrantPrivilege("CREATE", user_with_create);
  db_helper.CreateUserWithTriggerPrivilege(user_without_create);

  auto client_with_create = ConnectWithUser(user_with_create);
  auto client_without_create = ConnectWithUser(user_without_create);

  CreateTriggerAndVerify(db_helper, *client_with_create, user_with_create, true);
  CreateTriggerAndVerify(db_helper, *client_without_create, user_without_create, false);

  // Grant CREATE to be able to create the trigger then revoke it
  db_helper.GrantPrivilege("CREATE", user_without_create);
  CreateTriggerAndVerify(db_helper, *client_without_create, user_without_create, true);
  db_helper.RevokePrivilege("CREATE", user_without_create);

  auto userless_client = Connect();
  CreateVertex(*userless_client, kVertexId);

  WaitForNumberOfAllVertices(*userless_client, 2);
  CheckVertexExists(*userless_client, kVertexLabel, kVertexId);
  CheckVertexExists(*userless_client, user_with_create, kVertexId);
  db_helper.DeleteAllVertices();
}

void TestTriggersWithoutUsers(DatabaseHelper &db_helper, const std::string_view userless_label,
                              const std::string_view user_with_create, const std::string_view user_without_create) {
  auto userless_client = Connect();
  CreateVertex(*userless_client, kVertexId);

  WaitForNumberOfAllVertices(*userless_client, 4);
  CheckVertexExists(*userless_client, kVertexLabel, kVertexId);
  CheckVertexExists(*userless_client, userless_label, kVertexId);
  CheckVertexExists(*userless_client, user_with_create, kVertexId);
  CheckVertexExists(*userless_client, user_without_create, kVertexId);
  db_helper.DeleteAllVertices();
}

void TestBeforeCommitTriggerFailure(DatabaseHelper &db_helper, const std::string_view user_without_create) {
  db_helper.CreateUserWithTriggerPrivilege(user_without_create);
  db_helper.GrantPrivilege("CREATE", user_without_create);

  auto client_without_create = ConnectWithUser(user_without_create);
  CreateTriggerWithLabel(*client_without_create, user_without_create, user_without_create);
  // Change to BEFORE COMMIT
  client_without_create->Execute(fmt::format("DROP TRIGGER {}{};", kTriggerPrefix, user_without_create));
  client_without_create->DiscardAll();
  client_without_create->Execute(
      fmt::format("CREATE TRIGGER {}{} ON () CREATE "
                  "BEFORE COMMIT "
                  "EXECUTE "
                  "UNWIND createdVertices as createdVertex "
                  "CREATE (n: {} {{ id: createdVertex.id }})",
                  kTriggerPrefix, user_without_create, user_without_create));
  client_without_create->DiscardAll();

  db_helper.RevokePrivilege("CREATE", user_without_create);

  auto userless_client = Connect();
  MG_ASSERT(FunctionThrows<mg::TransientException>([&] { CreateVertex(*userless_client, kVertexId); }),
            "Create should have thrown because user doesn't have privilege for CREATE");
  CheckNumberOfAllVertices(*userless_client, 0);
}

}  // namespace

int main(int argc, char **argv) {
  gflags::SetUsageMessage("Memgraph E2E Triggers privilege check");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memgraph::logging::RedirectToStderr();

  static constexpr std::string_view kUserlessLabel{"USERLESS"};
  static constexpr std::string_view kAdminUser{"ADMIN"};
  static constexpr std::string_view kUserWithCreate{"USER_WITH_CREATE"};
  static constexpr std::string_view kUserWithoutCreate{"USER_WITHOUT_CREATE"};

  mg::Client::Init();

  auto userless_client = Connect();
  DatabaseHelper db_helper(*userless_client);

  // Test 1: Single trigger created without user, there is no existing users
  TestTriggerWithoutUser(db_helper);

  // Test 2: Single trigger created without user, there is an existing user
  // The trigger fails because there is no owner
  TestTriggerWithExistingUser(db_helper, kAdminUser);

  // Test 3: Three triggers: without an owner, an owner with CREATE privilege, an owner without CREATE privilege;
  // there are existing users. Only the trigger which owner has CREATE privilege will succeed
  TestTriggersWithPrivileges(db_helper, kUserWithCreate, kUserWithoutCreate);

  // Test 4: Three triggers: without an owner, an owner with CREATE privilege, an owner without CREATE privilege;
  // there is no existing user. All triggers will succeed, as there is no authorization when there are no users
  db_helper.DropUser(kAdminUser);
  db_helper.DropUser(kUserWithCreate);
  db_helper.DropUser(kUserWithoutCreate);

  // Clean up old triggers
  db_helper.DropTrigger(fmt::format("{}{}", kTriggerPrefix, kUserlessLabel));
  db_helper.DropTrigger(fmt::format("{}{}", kTriggerPrefix, kUserWithCreate));
  db_helper.DropTrigger(fmt::format("{}{}", kTriggerPrefix, kUserWithoutCreate));

  TestTriggersWithoutUsers(db_helper, kUserlessLabel, kUserWithCreate, kUserWithoutCreate);

  // Clean up triggers
  db_helper.DropTrigger(fmt::format("{}{}", kTriggerPrefix, kUserlessLabel));
  db_helper.DropTrigger(fmt::format("{}{}", kTriggerPrefix, kUserWithCreate));
  db_helper.DropTrigger(fmt::format("{}{}", kTriggerPrefix, kUserWithoutCreate));

  // Test 5: The BEFORE COMMIT trigger without proper privileges makes the transaction fail
  TestBeforeCommitTriggerFailure(db_helper, kUserWithoutCreate);
  db_helper.DropUser(kUserWithoutCreate);

  // ========== SECURITY INVOKER / DEFINER TESTS ==========
  // Test that triggers respect SECURITY INVOKER (default) and SECURITY DEFINER modes
  static constexpr std::string_view kTriggerDefinerUser{"TRIGGER_DEFINER"};
  static constexpr std::string_view kTriggerInvokerUser{"TRIGGER_INVOKER"};
  static constexpr std::string_view kInvokerLabel{"INVOKER_CREATED"};
  static constexpr std::string_view kDefinerLabel{"DEFINER_CREATED"};

  SecurityTestHelper security_helper;
  security_helper.SetupUsers(kTriggerDefinerUser, kTriggerInvokerUser);

  // Test 1: Default behavior (no SECURITY clause = SECURITY INVOKER)
  security_helper.TestInvokerPermissions("DefaultInvoker", kInvokerLabel, "AFTER", "");

  // Test 2: Explicit SECURITY INVOKER - same behavior as default
  security_helper.TestInvokerPermissions("InvokerExplicit", kInvokerLabel, "AFTER", "SECURITY INVOKER");

  // Test 3: SECURITY DEFINER - trigger should use definer's permissions
  security_helper.TestDefinerPermissions("Definer", kDefinerLabel, "AFTER", "SECURITY DEFINER");

  // Test 4: BEFORE COMMIT trigger with INVOKER permissions
  security_helper.TestBeforeCommitInvokerFailure("BeforeCommitInvoker", kInvokerLabel);

  // Test 5: BEFORE COMMIT trigger with DEFINER permissions
  security_helper.TestDefinerPermissions("BeforeCommitDefiner", kDefinerLabel, "BEFORE", "SECURITY DEFINER");

  // Cleanup
  security_helper.CleanupUsers(kTriggerDefinerUser, kTriggerInvokerUser);

  return 0;
}

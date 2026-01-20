// Copyright 2026 Memgraph Ltd.
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

#include <gtest/gtest.h>
#include <mgclient.hpp>
#include "common.hpp"
#include "utils/logging.hpp"

namespace {

inline constexpr int kVertexId{42};
inline constexpr std::string_view kDefinerUser{"DEFINER_USER"};
inline constexpr std::string_view kInvokerUser{"INVOKER_USER"};
inline constexpr std::string_view kInvokerWithoutSet{"INVOKER_WITHOUT_SET"};
inline constexpr std::string_view kDefinerWithoutSet{"DEFINER_WITHOUT_SET"};
inline constexpr std::string_view kInvokerWithFineGrainedSet{"INVOKER_FG_SET"};
inline constexpr std::string_view kInvokerWithoutFineGrainedSet{"INVOKER_FG_NO_SET"};
inline constexpr std::string_view kDefinerWithFineGrainedSet{"DEFINER_FG_SET"};
inline constexpr std::string_view kDefinerWithoutFineGrainedSet{"DEFINER_FG_NO_SET"};
inline constexpr std::string_view kTriggerProperty{"triggered"};

void CreateUser(mg::Client &client, std::string_view username) {
  client.Execute(fmt::format("CREATE USER {};", username));
  client.DiscardAll();
}

void DropUser(mg::Client &client, std::string_view username) {
  client.Execute(fmt::format("DROP USER {};", username));
  client.DiscardAll();
}

void GrantAllPrivileges(mg::Client &client, std::string_view username) {
  // Grant all role based privileges
  client.Execute(fmt::format("GRANT ALL PRIVILEGES TO {};", username));
  client.DiscardAll();
  // Grant all label-based privileges
  client.Execute(fmt::format("GRANT * ON NODES CONTAINING LABELS * TO {};", username));
  client.DiscardAll();
}

void GrantAllRoleBasedPrivileges(mg::Client &client, std::string_view username) {
  client.Execute(fmt::format("GRANT ALL PRIVILEGES TO {};", username));
  client.DiscardAll();
}

void GrantFineGrainedPrivileges(mg::Client &client, std::string_view username, std::string_view privilege,
                                std::string_view label) {
  client.Execute(fmt::format("GRANT {} ON NODES CONTAINING LABELS :{} TO {};", privilege, label, username));
  client.DiscardAll();
}

void DenySet(mg::Client &client, std::string_view username) {
  client.Execute(fmt::format("DENY SET TO {};", username));
  client.DiscardAll();
}

void CreateTrigger(mg::Client &client, std::string_view trigger_name, const std::string &phase,
                   std::string_view security_mode = "") {
  std::string security_clause = security_mode.empty() ? "" : fmt::format("SECURITY {}", security_mode);
  client.Execute(
      fmt::format("CREATE TRIGGER {} {} ON CREATE "
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

}  // namespace

// Test fixture for privilege check tests
class PrivilegeCheckTest : public ::testing::TestWithParam<std::string> {
 protected:
  static void SetUpTestSuite() {
    memgraph::logging::RedirectToStderr();
    mg::Client::Init();

    admin_client_ = Connect();

    // role based auth tests
    CreateUser(*admin_client_, kDefinerUser);
    CreateUser(*admin_client_, kDefinerWithoutSet);
    CreateUser(*admin_client_, kInvokerUser);
    CreateUser(*admin_client_, kInvokerWithoutSet);

    GrantAllPrivileges(*admin_client_, kDefinerUser);
    GrantAllPrivileges(*admin_client_, kDefinerWithoutSet);
    GrantAllPrivileges(*admin_client_, kInvokerUser);
    GrantAllPrivileges(*admin_client_, kInvokerWithoutSet);

    DenySet(*admin_client_, kDefinerWithoutSet);
    DenySet(*admin_client_, kInvokerWithoutSet);

    // fine grained auth tests
    CreateUser(*admin_client_, kInvokerWithFineGrainedSet);
    CreateUser(*admin_client_, kInvokerWithoutFineGrainedSet);
    CreateUser(*admin_client_, kDefinerWithFineGrainedSet);
    CreateUser(*admin_client_, kDefinerWithoutFineGrainedSet);

    GrantAllRoleBasedPrivileges(*admin_client_, kInvokerWithFineGrainedSet);
    GrantAllRoleBasedPrivileges(*admin_client_, kInvokerWithoutFineGrainedSet);
    GrantAllRoleBasedPrivileges(*admin_client_, kDefinerWithFineGrainedSet);
    GrantAllRoleBasedPrivileges(*admin_client_, kDefinerWithoutFineGrainedSet);

    GrantFineGrainedPrivileges(*admin_client_, kInvokerWithFineGrainedSet, "*", kVertexLabel);
    GrantFineGrainedPrivileges(*admin_client_, kInvokerWithoutFineGrainedSet, "READ, CREATE", kVertexLabel);
    GrantFineGrainedPrivileges(*admin_client_, kDefinerWithFineGrainedSet, "*", kVertexLabel);
    GrantFineGrainedPrivileges(*admin_client_, kDefinerWithoutFineGrainedSet, "READ, CREATE", kVertexLabel);
  }

  static void TearDownTestSuite() {
    DropUser(*admin_client_, kDefinerUser);
    DropUser(*admin_client_, kDefinerWithoutSet);
    DropUser(*admin_client_, kInvokerUser);
    DropUser(*admin_client_, kInvokerWithoutSet);
    DropUser(*admin_client_, kInvokerWithFineGrainedSet);
    DropUser(*admin_client_, kInvokerWithoutFineGrainedSet);
    DropUser(*admin_client_, kDefinerWithFineGrainedSet);

    admin_client_.reset();
  }

  static std::unique_ptr<mg::Client> admin_client_;
};

std::unique_ptr<mg::Client> PrivilegeCheckTest::admin_client_ = nullptr;

TEST_P(PrivilegeCheckTest, Default) {
  const std::string &phase = GetParam();
  const bool is_after = (phase == "AFTER");

  auto definer_client = ConnectWithUser(kDefinerUser);
  auto invoker_client = ConnectWithUser(kInvokerUser);
  auto invoker_without_set_client = ConnectWithUser(kInvokerWithoutSet);

  // default privilege context is DEFINER
  CreateTrigger(*definer_client, "DefaultDefiner", phase, "");

  // any invoker can trigger successfully
  CreateVertex(*invoker_client, kVertexId);
  CheckNumberOfAllVertices(*invoker_client, 1);
  if (is_after) {
    EXPECT_TRUE(PollUntilTrue([&]() { return VertexHasProperty(*invoker_client, kVertexId, kTriggerProperty); }))
        << "After commit trigger should set property within timeout";
  } else {
    EXPECT_TRUE(VertexHasProperty(*invoker_client, kVertexId, kTriggerProperty))
        << "Vertex should have trigger property set (definer's permissions)";
  }

  CleanupVertices(*admin_client_);

  // any invoker can trigger successfully
  CreateVertex(*invoker_without_set_client, kVertexId);
  CheckNumberOfAllVertices(*invoker_without_set_client, 1);
  if (is_after) {
    EXPECT_TRUE(PollUntilTrue([&]() {
      return VertexHasProperty(*invoker_without_set_client, kVertexId, kTriggerProperty);
    })) << "After commit trigger should set property within timeout";
  } else {
    EXPECT_TRUE(VertexHasProperty(*invoker_without_set_client, kVertexId, kTriggerProperty))
        << "Vertex should have trigger property set (definer's permissions)";
  }

  CleanupVertices(*admin_client_);
  DropTrigger(*admin_client_, "DefaultDefiner");
}

TEST_P(PrivilegeCheckTest, ExplicitInvoker) {
  const std::string &phase = GetParam();
  const bool is_after = (phase == "AFTER");

  auto definer_client = ConnectWithUser(kDefinerUser);
  auto invoker_client = ConnectWithUser(kInvokerUser);
  auto invoker_without_set_client = ConnectWithUser(kInvokerWithoutSet);

  CreateTrigger(*definer_client, "ExplicitInvoker", phase, "INVOKER");

  // invoker with SET can trigger successfully
  CreateVertex(*invoker_client, kVertexId);
  CheckNumberOfAllVertices(*invoker_client, 1);
  if (is_after) {
    EXPECT_TRUE(PollUntilTrue([&]() { return VertexHasProperty(*invoker_client, kVertexId, kTriggerProperty); }))
        << "After commit trigger should set property within timeout";
  } else {
    EXPECT_TRUE(VertexHasProperty(*invoker_client, kVertexId, kTriggerProperty))
        << "Vertex should have trigger property set";
  }

  CleanupVertices(*admin_client_);

  // invoker without SET can't trigger successfully
  if (!is_after) {
    // BEFORE COMMIT: transaction should fail
    EXPECT_THROW({ CreateVertex(*invoker_without_set_client, kVertexId); }, mg::TransientException)
        << "BEFORE COMMIT trigger should fail transaction when invoker lacks SET privilege";
    CheckNumberOfAllVertices(*invoker_without_set_client, 0);
  } else {
    // AFTER COMMIT: transaction succeeds but trigger fails, property not set
    CreateVertex(*invoker_without_set_client, kVertexId);
    CheckNumberOfAllVertices(*invoker_without_set_client, 1);
    EXPECT_FALSE(VertexHasProperty(*invoker_without_set_client, kVertexId, kTriggerProperty))
        << "After commit trigger should not set property when invoker lacks SET privilege";
  }

  CleanupVertices(*admin_client_);
  DropTrigger(*admin_client_, "ExplicitInvoker");
}

TEST_P(PrivilegeCheckTest, Definer) {
  const std::string &phase = GetParam();
  const bool is_after = (phase == "AFTER");
  auto definer_client = ConnectWithUser(kDefinerUser);
  auto invoker_client = ConnectWithUser(kInvokerUser);
  auto invoker_without_set_client = ConnectWithUser(kInvokerWithoutSet);

  CreateTrigger(*definer_client, "Definer", phase, "DEFINER");

  // any invoker can trigger successfully
  CreateVertex(*invoker_client, kVertexId);
  CheckNumberOfAllVertices(*invoker_client, 1);
  if (is_after) {
    EXPECT_TRUE(PollUntilTrue([&]() { return VertexHasProperty(*invoker_client, kVertexId, kTriggerProperty); }))
        << "After commit trigger should set property within timeout";
  } else {
    EXPECT_TRUE(VertexHasProperty(*invoker_client, kVertexId, kTriggerProperty))
        << "Vertex should have trigger property set (definer's permissions)";
  }

  CleanupVertices(*admin_client_);

  // any invoker can trigger successfully
  CreateVertex(*invoker_without_set_client, kVertexId);
  CheckNumberOfAllVertices(*invoker_without_set_client, 1);
  if (is_after) {
    EXPECT_TRUE(PollUntilTrue([&]() {
      return VertexHasProperty(*invoker_without_set_client, kVertexId, kTriggerProperty);
    })) << "After commit trigger should set property within timeout";
  } else {
    EXPECT_TRUE(VertexHasProperty(*invoker_without_set_client, kVertexId, kTriggerProperty))
        << "Vertex should have trigger property set (definer's permissions)";
  }

  CleanupVertices(*admin_client_);
  DropTrigger(*admin_client_, "Definer");
}

TEST_P(PrivilegeCheckTest, DefinerCreationFails) {
  const std::string &phase = GetParam();
  auto definer_without_set_client = ConnectWithUser(kDefinerWithoutSet);

  // definer without SET cannot create trigger with DEFINER mode
  EXPECT_THROW({ CreateTrigger(*definer_without_set_client, "DefinerFail", phase, "DEFINER"); }, mg::TransientException)
      << "Creating DEFINER trigger without SET privilege should fail";
}

TEST_P(PrivilegeCheckTest, InvokerFineGrainedSet) {
  const std::string &phase = GetParam();
  const bool is_after = (phase == "AFTER");

  auto definer_client = ConnectWithUser(kDefinerUser);
  auto invoker_fg_set_client = ConnectWithUser(kInvokerWithFineGrainedSet);

  CreateTrigger(*definer_client, "InvokerFGSet", phase, "INVOKER");

  // invoker with SET on label can trigger successfully
  CreateVertex(*invoker_fg_set_client, kVertexId);
  CheckNumberOfAllVertices(*invoker_fg_set_client, 1);
  if (is_after) {
    EXPECT_TRUE(PollUntilTrue([&]() { return VertexHasProperty(*invoker_fg_set_client, kVertexId, kTriggerProperty); }))
        << "After commit trigger should set property within timeout";
  } else {
    EXPECT_TRUE(VertexHasProperty(*invoker_fg_set_client, kVertexId, kTriggerProperty))
        << "Vertex should have trigger property set when invoker has SET on VERTEX label";
  }

  CleanupVertices(*admin_client_);
  DropTrigger(*admin_client_, "InvokerFGSet");
}

TEST_P(PrivilegeCheckTest, InvokerFineGrainedNoSet) {
  const std::string &phase = GetParam();
  const bool is_after = (phase == "AFTER");

  auto definer_client = ConnectWithUser(kDefinerUser);
  auto invoker_fg_no_set_client = ConnectWithUser(kInvokerWithoutFineGrainedSet);

  CreateTrigger(*definer_client, "InvokerFGNoSet", phase, "INVOKER");

  // invoker without SET on label can't trigger successfully
  if (!is_after) {
    // BEFORE COMMIT: transaction should fail
    EXPECT_THROW({ CreateVertex(*invoker_fg_no_set_client, kVertexId); }, mg::TransientException)
        << "BEFORE COMMIT trigger should fail transaction when invoker lacks SET privilege";
    CheckNumberOfAllVertices(*invoker_fg_no_set_client, 0);
  } else {
    // AFTER COMMIT: transaction succeeds but trigger fails, property not set
    CreateVertex(*invoker_fg_no_set_client, kVertexId);
    CheckNumberOfAllVertices(*invoker_fg_no_set_client, 1);
    EXPECT_FALSE(VertexHasProperty(*invoker_fg_no_set_client, kVertexId, kTriggerProperty))
        << "After commit trigger should not set property when invoker lacks SET on VERTEX label";
  }

  CleanupVertices(*admin_client_);
  DropTrigger(*admin_client_, "InvokerFGNoSet");
}

TEST_P(PrivilegeCheckTest, DefinerFineGrainedSet) {
  const std::string &phase = GetParam();
  const bool is_after = (phase == "AFTER");

  auto definer_fg_set_client = ConnectWithUser(kDefinerWithFineGrainedSet);
  auto invoker_fg_no_set_client = ConnectWithUser(kInvokerWithoutFineGrainedSet);

  // Definer with SET on VERTEX label creates trigger with DEFINER mode
  CreateTrigger(*definer_fg_set_client, "DefinerFGSet", phase, "DEFINER");

  // any invoker can trigger successfully
  CreateVertex(*invoker_fg_no_set_client, kVertexId);
  CheckNumberOfAllVertices(*invoker_fg_no_set_client, 1);
  if (is_after) {
    EXPECT_TRUE(PollUntilTrue([&]() {
      return VertexHasProperty(*invoker_fg_no_set_client, kVertexId, kTriggerProperty);
    })) << "After commit trigger should set property within timeout";
  } else {
    EXPECT_TRUE(VertexHasProperty(*invoker_fg_no_set_client, kVertexId, kTriggerProperty))
        << "Vertex should have trigger property set (definer's fine-grained SET permissions)";
  }

  CleanupVertices(*admin_client_);
  DropTrigger(*admin_client_, "DefinerFGSet");
}

TEST_P(PrivilegeCheckTest, DefinerFineGrainedNoSet) {
  const std::string &phase = GetParam();
  const bool is_after = (phase == "AFTER");

  auto definer_fg_no_set_client = ConnectWithUser(kDefinerWithoutFineGrainedSet);
  auto invoker_client = ConnectWithUser(kInvokerUser);

  // Definer without SET on label creates trigger with DEFINER mode (creation succeeds)
  CreateTrigger(*definer_fg_no_set_client, "DefinerFGNoSet", phase, "DEFINER");

  // Trigger execution fails because definer lacks SET privilege
  if (!is_after) {
    // BEFORE COMMIT: transaction should fail
    EXPECT_THROW({ CreateVertex(*invoker_client, kVertexId); }, mg::TransientException)
        << "BEFORE COMMIT trigger should fail transaction when definer lacks SET privilege";
    CheckNumberOfAllVertices(*invoker_client, 0);
  } else {
    // AFTER COMMIT: transaction succeeds but trigger fails, property not set
    CreateVertex(*invoker_client, kVertexId);
    CheckNumberOfAllVertices(*invoker_client, 1);
    EXPECT_FALSE(VertexHasProperty(*invoker_client, kVertexId, kTriggerProperty))
        << "After commit trigger should not set property when definer lacks SET on VERTEX label";
  }

  CleanupVertices(*admin_client_);
  DropTrigger(*admin_client_, "DefinerFGNoSet");
}

INSTANTIATE_TEST_SUITE_P(TriggerPhases, PrivilegeCheckTest, testing::Values("BEFORE", "AFTER"),
                         [](const testing::TestParamInfo<std::string> &info) {
                           return info.param == "BEFORE" ? "BeforeCommit" : "AfterCommit";
                         });

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

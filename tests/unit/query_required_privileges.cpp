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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <unordered_map>

#include "license/license.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/ast/query/tenant_profile.hpp"
#include "query/frontend/ast/query/user_profile.hpp"
#include "query/frontend/semantic/required_privileges.hpp"

#include "plan/operator.hpp"
#include "query_common.hpp"

using namespace memgraph::query;

class FakeDbAccessor {};

const std::string EDGE_TYPE = "0";
const std::string LABEL_0 = "label0";
const std::string LABEL_1 = "label1";
const std::string PROP_0 = "prop0";

using ::testing::UnorderedElementsAre;

class TestPrivilegeExtractor : public ::testing::Test {
 protected:
  AstStorage storage;
  FakeDbAccessor dba;
};

TEST_F(TestPrivilegeExtractor, CreateNode) {
  auto *query = QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n")))));
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::CREATE));
}

TEST_F(TestPrivilegeExtractor, MatchNodeDelete) {
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), DELETE(IDENT("n"))));
  EXPECT_THAT(GetRequiredPrivileges(query),
              UnorderedElementsAre(AuthQuery::Privilege::MATCH, AuthQuery::Privilege::DELETE));
}

TEST_F(TestPrivilegeExtractor, MatchNodeReturn) {
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), RETURN("n")));
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::MATCH));
}

TEST_F(TestPrivilegeExtractor, MatchCreateExpand) {
  auto *query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                         CREATE(PATTERN(NODE("n"), EDGE("r", EdgeAtom::Direction::OUT, {EDGE_TYPE}), NODE("m")))));
  EXPECT_THAT(GetRequiredPrivileges(query),
              UnorderedElementsAre(AuthQuery::Privilege::MATCH, AuthQuery::Privilege::CREATE));
}

TEST_F(TestPrivilegeExtractor, MatchNodeSetLabels) {
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), SET("n", {LABEL_0, LABEL_1})));
  EXPECT_THAT(GetRequiredPrivileges(query),
              UnorderedElementsAre(AuthQuery::Privilege::MATCH, AuthQuery::Privilege::SET));
}

TEST_F(TestPrivilegeExtractor, MatchNodeSetProperty) {
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))),
                                   SET(PROPERTY_LOOKUP(dba, storage.Create<Identifier>("n"), PROP_0), LITERAL(42))));
  EXPECT_THAT(GetRequiredPrivileges(query),
              UnorderedElementsAre(AuthQuery::Privilege::MATCH, AuthQuery::Privilege::SET));
}

TEST_F(TestPrivilegeExtractor, MatchNodeSetProperties) {
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), SET("n", LIST())));
  EXPECT_THAT(GetRequiredPrivileges(query),
              UnorderedElementsAre(AuthQuery::Privilege::MATCH, AuthQuery::Privilege::SET));
}

TEST_F(TestPrivilegeExtractor, MatchNodeRemoveLabels) {
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), REMOVE("n", {LABEL_0, LABEL_1})));
  EXPECT_THAT(GetRequiredPrivileges(query),
              UnorderedElementsAre(AuthQuery::Privilege::MATCH, AuthQuery::Privilege::REMOVE));
}

TEST_F(TestPrivilegeExtractor, MatchNodeRemoveProperty) {
  auto *query = QUERY(
      SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), REMOVE(PROPERTY_LOOKUP(dba, storage.Create<Identifier>("n"), PROP_0))));
  EXPECT_THAT(GetRequiredPrivileges(query),
              UnorderedElementsAre(AuthQuery::Privilege::MATCH, AuthQuery::Privilege::REMOVE));
}

TEST_F(TestPrivilegeExtractor, CreateIndex) {
  auto *query = CREATE_INDEX_ON(storage.GetLabelIx(LABEL_0), storage.GetPropertyIx(PROP_0));
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::INDEX));
}
#ifdef MG_ENTERPRISE
TEST_F(TestPrivilegeExtractor, AuthQuery) {
  memgraph::license::global_license_checker.EnableTesting();
  auto label_privileges = std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>{};
  auto label_matching_modes = std::vector<AuthQuery::LabelMatchingMode>{};
  auto edge_type_privileges =
      std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>{};
  auto *query = AUTH_QUERY(AuthQuery::Action::CREATE_ROLE,
                           "",
                           std::vector<std::string>{"role"},
                           "",
                           false,
                           nullptr,
                           "",
                           std::vector<AuthQuery::Privilege>{},
                           label_privileges,
                           label_matching_modes,
                           edge_type_privileges,
                           std::vector<std::string>{});
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::AUTH));
}
#endif

TEST_F(TestPrivilegeExtractor, ShowIndexInfo) {
  auto *db_query = storage.Create<DatabaseInfoQuery>();
  db_query->info_type_ = DatabaseInfoQuery::InfoType::INDEX;
  EXPECT_THAT(GetRequiredPrivileges(db_query), UnorderedElementsAre(AuthQuery::Privilege::INDEX));
}

TEST_F(TestPrivilegeExtractor, ShowStatsInfo) {
  auto *db_query = storage.Create<SystemInfoQuery>();
  db_query->info_type_ = SystemInfoQuery::InfoType::STORAGE;
  EXPECT_THAT(GetRequiredPrivileges(db_query), UnorderedElementsAre(AuthQuery::Privilege::STATS));
}

TEST_F(TestPrivilegeExtractor, ShowConstraintInfo) {
  auto *db_query = storage.Create<DatabaseInfoQuery>();
  db_query->info_type_ = DatabaseInfoQuery::InfoType::CONSTRAINT;
  EXPECT_THAT(GetRequiredPrivileges(db_query), UnorderedElementsAre(AuthQuery::Privilege::CONSTRAINT));
}

TEST_F(TestPrivilegeExtractor, CreateConstraint) {
  auto *query = storage.Create<ConstraintQuery>();
  query->action_type_ = ConstraintQuery::ActionType::CREATE;
  query->constraint_.label = storage.GetLabelIx("label");
  query->constraint_.properties.push_back(storage.GetPropertyIx("prop0"));
  query->constraint_.properties.push_back(storage.GetPropertyIx("prop1"));
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::CONSTRAINT));
}

TEST_F(TestPrivilegeExtractor, DropConstraint) {
  auto *query = storage.Create<ConstraintQuery>();
  query->action_type_ = ConstraintQuery::ActionType::DROP;
  query->constraint_.label = storage.GetLabelIx("label");
  query->constraint_.properties.push_back(storage.GetPropertyIx("prop0"));
  query->constraint_.properties.push_back(storage.GetPropertyIx("prop1"));
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::CONSTRAINT));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(TestPrivilegeExtractor, DumpDatabase) {
  auto *query = storage.Create<DumpQuery>();
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::DUMP));
}

// Parametrized test for both LoadCsv and LoadParquet
class TestReadFilePrivilege : public TestPrivilegeExtractor, public ::testing::WithParamInterface<std::string> {};

TEST_P(TestReadFilePrivilege, ReadFile) {
  const std::string &file_type = GetParam();

  Clause *load_clause = nullptr;
  if (file_type == "CSV") {
    auto *load_csv = storage.Create<LoadCsv>();
    load_csv->row_var_ = IDENT("row");
    load_clause = load_csv;
  } else if (file_type == "Parquet") {
    auto *load_parquet = storage.Create<LoadParquet>();
    load_parquet->row_var_ = IDENT("row");
    load_clause = load_parquet;
  }

  auto *query = QUERY(SINGLE_QUERY(load_clause));
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::READ_FILE));
}

INSTANTIATE_TEST_SUITE_P(FileFormats, TestReadFilePrivilege, ::testing::Values("CSV", "Parquet"));

TEST_F(TestPrivilegeExtractor, LockPathQuery) {
  auto *query = storage.Create<LockPathQuery>();
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::DURABILITY));
}

TEST_F(TestPrivilegeExtractor, FreeMemoryQuery) {
  auto *query = storage.Create<FreeMemoryQuery>();
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::FREE_MEMORY));
}

TEST_F(TestPrivilegeExtractor, TriggerQuery) {
  auto *query = storage.Create<TriggerQuery>();
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::TRIGGER));
}

TEST_F(TestPrivilegeExtractor, SetIsolationLevelQuery) {
  auto *query = storage.Create<IsolationLevelQuery>();
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::CONFIG));
}

TEST_F(TestPrivilegeExtractor, CreateSnapshotQuery) {
  auto *query = storage.Create<CreateSnapshotQuery>();
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::DURABILITY));
}

TEST_F(TestPrivilegeExtractor, RecoverSnapshotQuery) {
  auto *query = storage.Create<RecoverSnapshotQuery>();
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::DURABILITY));
}

TEST_F(TestPrivilegeExtractor, ShowSnapshotsQuery) {
  auto *query = storage.Create<ShowSnapshotsQuery>();
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::DURABILITY));
}

TEST_F(TestPrivilegeExtractor, CoordinatorQuery) {
  // Read-only coordinator introspection requires COORDINATOR_READ.
  auto *show_instances = storage.Create<CoordinatorQuery>();
  show_instances->action_ = CoordinatorQuery::Action::SHOW_INSTANCES;
  EXPECT_THAT(GetRequiredPrivileges(show_instances), UnorderedElementsAre(AuthQuery::Privilege::COORDINATOR_READ));

  // Mutating/admin coordinator queries require COORDINATOR_WRITE.
  auto *register_instance = storage.Create<CoordinatorQuery>();
  register_instance->action_ = CoordinatorQuery::Action::REGISTER_INSTANCE;
  EXPECT_THAT(GetRequiredPrivileges(register_instance), UnorderedElementsAre(AuthQuery::Privilege::COORDINATOR_WRITE));
}

TEST_F(TestPrivilegeExtractor, StreamQuery) {
  auto *query = storage.Create<StreamQuery>();
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::STREAM));
}

TEST_F(TestPrivilegeExtractor, SettingQuery) {
  auto *query = storage.Create<SettingQuery>();
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::CONFIG));
}

TEST_F(TestPrivilegeExtractor, ShowVersion) {
  auto *query = storage.Create<VersionQuery>();
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::STATS));
}

TEST_F(TestPrivilegeExtractor, CallProcedureQuery) {
  {
    auto *query = QUERY(SINGLE_QUERY(CALL_PROCEDURE("mg.get_module_files")));
    EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::MODULE_READ));
  }
  {
    auto *query = QUERY(SINGLE_QUERY(CALL_PROCEDURE("mg.create_module_file", {LITERAL("some_name.py")})));
    EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::MODULE_WRITE));
  }
  {
    auto *query = QUERY(
        SINGLE_QUERY(CALL_PROCEDURE("mg.update_module_file", {LITERAL("some_name.py"), LITERAL("some content")})));
    EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::MODULE_WRITE));
  }
  {
    auto *query = QUERY(SINGLE_QUERY(CALL_PROCEDURE("mg.get_module_file", {LITERAL("some_name.py")})));
    EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::MODULE_READ));
  }
  {
    auto *query = QUERY(SINGLE_QUERY(CALL_PROCEDURE("mg.delete_module_file", {LITERAL("some_name.py")})));
    EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::MODULE_WRITE));
  }
}

TEST_F(TestPrivilegeExtractor, UserProfile) {
  auto *query = storage.Create<UserProfileQuery>();
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::PROFILE_RESTRICTION));
}

TEST_F(TestPrivilegeExtractor, TenantProfile) {
  auto *query = storage.Create<TenantProfileQuery>();
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::PROFILE_RESTRICTION));
}

TEST_F(TestPrivilegeExtractor, ParallelQuery) {
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), RETURN("n")));
  EXPECT_THAT(GetRequiredPrivileges(query),
              UnorderedElementsAre(AuthQuery::Privilege::MATCH, AuthQuery::Privilege::PARALLEL_EXECUTION));
}

TEST_F(TestPrivilegeExtractor, ParallelQueryWithThreads) {
  auto *query = PARALLEL_QUERY_WITH_THREADS(LITERAL(4), SINGLE_QUERY(MATCH(PATTERN(NODE("n"))), RETURN("n")));
  EXPECT_THAT(GetRequiredPrivileges(query),
              UnorderedElementsAre(AuthQuery::Privilege::MATCH, AuthQuery::Privilege::PARALLEL_EXECUTION));
}

TEST_F(TestPrivilegeExtractor, ParallelQueryCreate) {
  auto *query = PARALLEL_QUERY(SINGLE_QUERY(CREATE(PATTERN(NODE("n")))));
  EXPECT_THAT(GetRequiredPrivileges(query),
              UnorderedElementsAre(AuthQuery::Privilege::CREATE, AuthQuery::Privilege::PARALLEL_EXECUTION));
}

TEST_F(TestPrivilegeExtractor, ParameterQuery) {
  auto *query = storage.Create<ParameterQuery>();
  query->action_ = ParameterQuery::Action::SHOW_PARAMETERS;
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::SERVER_SIDE_PARAMETERS));
}

TEST_F(TestPrivilegeExtractor, DescriptionQuery) {
  auto *query = storage.Create<DescriptionQuery>();
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::SERVER_SIDE_DESCRIPTIONS));
}

TEST_F(TestPrivilegeExtractor, ShowQueryCallableMappingsQuery) {
  auto *query = storage.Create<ShowQueryCallableMappingsQuery>();
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::CONFIG));
}

// The coordinator auth-query gate permits exactly CREATE/DROP/SHOW ROLE, GRANT/REVOKE of the coordinator READ/WRITE
// privileges on a role, and SHOW PRIVILEGES FOR a role; every other auth query (and any grant of a non-coordinator
// privilege or against a USER) is rejected on a coordinator. This exercises the predicate the interpreter gate uses.
TEST(CoordinatorAuthQueryGate, PermitsExactlyRoleAndCoordinatorPrivilegeQueries) {
  AstStorage storage;
  auto make = [&storage](AuthQuery::Action action,
                         std::vector<AuthQuery::Privilege> privileges = {},
                         AuthQuery::UserOrRoleType entity_type = AuthQuery::UserOrRoleType::UNSPECIFIED) {
    auto *query = storage.Create<AuthQuery>();
    query->action_ = action;
    query->privileges_ = std::move(privileges);
    query->entity_type_ = entity_type;
    return query;
  };

  // Role management is always permitted.
  EXPECT_TRUE(IsCoordinatorPermittedAuthQuery(*make(AuthQuery::Action::CREATE_ROLE)));
  EXPECT_TRUE(IsCoordinatorPermittedAuthQuery(*make(AuthQuery::Action::DROP_ROLE)));
  EXPECT_TRUE(IsCoordinatorPermittedAuthQuery(*make(AuthQuery::Action::SHOW_ROLES)));

  // GRANT/REVOKE of coordinator READ/WRITE on a role (or unspecified target) is permitted.
  EXPECT_TRUE(IsCoordinatorPermittedAuthQuery(
      *make(AuthQuery::Action::GRANT_PRIVILEGE, {AuthQuery::Privilege::COORDINATOR_READ})));
  EXPECT_TRUE(IsCoordinatorPermittedAuthQuery(*make(AuthQuery::Action::REVOKE_PRIVILEGE,
                                                    {AuthQuery::Privilege::COORDINATOR_WRITE},
                                                    AuthQuery::UserOrRoleType::ROLE)));
  EXPECT_TRUE(IsCoordinatorPermittedAuthQuery(
      *make(AuthQuery::Action::GRANT_PRIVILEGE,
            {AuthQuery::Privilege::COORDINATOR_READ, AuthQuery::Privilege::COORDINATOR_WRITE})));

  // GRANT/REVOKE ALL PRIVILEGES is permitted (maps to both coordinator privileges downstream).
  {
    auto *grant_all = make(AuthQuery::Action::GRANT_PRIVILEGE, kPrivilegesAll);
    grant_all->all_privileges_ = true;
    EXPECT_TRUE(IsCoordinatorPermittedAuthQuery(*grant_all));
    auto *revoke_all = make(AuthQuery::Action::REVOKE_PRIVILEGE, kPrivilegesAll);
    revoke_all->all_privileges_ = true;
    EXPECT_TRUE(IsCoordinatorPermittedAuthQuery(*revoke_all));
  }

  // Fine-grained access control (label/edge-type entity privileges) is rejected even on GRANT_PRIVILEGE.
  {
    auto *fgac_labels = make(AuthQuery::Action::GRANT_PRIVILEGE);
    fgac_labels->label_privileges_.emplace_back(
        std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>{
            {AuthQuery::FineGrainedPrivilege::CREATE, {"*"}}});
    EXPECT_FALSE(IsCoordinatorPermittedAuthQuery(*fgac_labels));
    auto *fgac_edges = make(AuthQuery::Action::GRANT_PRIVILEGE);
    fgac_edges->edge_type_privileges_.emplace_back(
        std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>{
            {AuthQuery::FineGrainedPrivilege::UPDATE, {"*"}}});
    EXPECT_FALSE(IsCoordinatorPermittedAuthQuery(*fgac_edges));
  }

  // SHOW PRIVILEGES FOR a role (or unspecified target) is permitted.
  EXPECT_TRUE(IsCoordinatorPermittedAuthQuery(*make(AuthQuery::Action::SHOW_PRIVILEGES)));
  EXPECT_TRUE(
      IsCoordinatorPermittedAuthQuery(*make(AuthQuery::Action::SHOW_PRIVILEGES, {}, AuthQuery::UserOrRoleType::ROLE)));

  // Rejected: granting a non-coordinator privilege, an empty privilege list, or targeting a USER.
  EXPECT_FALSE(
      IsCoordinatorPermittedAuthQuery(*make(AuthQuery::Action::GRANT_PRIVILEGE, {AuthQuery::Privilege::MATCH})));
  EXPECT_FALSE(IsCoordinatorPermittedAuthQuery(*make(AuthQuery::Action::GRANT_PRIVILEGE, {})));
  EXPECT_FALSE(IsCoordinatorPermittedAuthQuery(*make(
      AuthQuery::Action::GRANT_PRIVILEGE, {AuthQuery::Privilege::COORDINATOR_READ}, AuthQuery::UserOrRoleType::USER)));
  EXPECT_FALSE(
      IsCoordinatorPermittedAuthQuery(*make(AuthQuery::Action::SHOW_PRIVILEGES, {}, AuthQuery::UserOrRoleType::USER)));

  // Every other auth action is rejected on a coordinator.
  for (auto const action : {AuthQuery::Action::CREATE_USER,
                            AuthQuery::Action::SET_PASSWORD,
                            AuthQuery::Action::CHANGE_PASSWORD,
                            AuthQuery::Action::DROP_USER,
                            AuthQuery::Action::SHOW_CURRENT_USER,
                            AuthQuery::Action::SHOW_CURRENT_ROLE,
                            AuthQuery::Action::SHOW_USERS,
                            AuthQuery::Action::SET_ROLE,
                            AuthQuery::Action::CLEAR_ROLE,
                            AuthQuery::Action::GRANT_ROLE,
                            AuthQuery::Action::REVOKE_ROLE,
                            AuthQuery::Action::DENY_PRIVILEGE,
                            AuthQuery::Action::SHOW_ROLE_FOR_USER,
                            AuthQuery::Action::SHOW_USERS_FOR_ROLE,
                            AuthQuery::Action::GRANT_DATABASE_TO_USER,
                            AuthQuery::Action::DENY_DATABASE_FROM_USER,
                            AuthQuery::Action::REVOKE_DATABASE_FROM_USER,
                            AuthQuery::Action::SHOW_DATABASE_PRIVILEGES,
                            AuthQuery::Action::SET_MAIN_DATABASE,
                            AuthQuery::Action::GRANT_IMPERSONATE_USER,
                            AuthQuery::Action::DENY_IMPERSONATE_USER,
                            AuthQuery::Action::GRANT_PROPERTY_PERMISSION,
                            AuthQuery::Action::DENY_PROPERTY_PERMISSION,
                            AuthQuery::Action::REVOKE_PROPERTY_PERMISSION}) {
    EXPECT_FALSE(IsCoordinatorPermittedAuthQuery(*make(action)));
  }
}

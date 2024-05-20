// Copyright 2024 Memgraph Ltd.
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
#include "query/frontend/semantic/required_privileges.hpp"
#include "storage/v2/id_types.hpp"

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
  auto edge_type_privileges =
      std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>{};
  auto *query = AUTH_QUERY(AuthQuery::Action::CREATE_ROLE, "", "role", "", false, nullptr, "",
                           std::vector<AuthQuery::Privilege>{}, label_privileges, edge_type_privileges);
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

TEST_F(TestPrivilegeExtractor, ReadFile) {
  auto *load_csv = storage.Create<LoadCsv>();
  load_csv->row_var_ = IDENT("row");
  auto *query = QUERY(SINGLE_QUERY(load_csv));
  EXPECT_THAT(GetRequiredPrivileges(query), UnorderedElementsAre(AuthQuery::Privilege::READ_FILE));
}

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

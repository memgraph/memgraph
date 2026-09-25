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

#include "query/storage_access.hpp"

#include <gtest/gtest.h>

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage_mode.hpp"

using memgraph::query::AstStorage;
using memgraph::query::RequiredStorageAccess;
using memgraph::query::StorageAccessRequirement;
using memgraph::storage::IsolationLevel;
using memgraph::storage::StorageMode;
using enum memgraph::storage::StorageAccessType;

namespace {

constexpr auto kModes = std::array{
    StorageMode::IN_MEMORY_TRANSACTIONAL, StorageMode::IN_MEMORY_ANALYTICAL, StorageMode::ON_DISK_TRANSACTIONAL};

// A kind whose policy settles the answer is asked under every input that could disturb it.
template <typename TQuery>
void ExpectSettledBy(StorageAccessRequirement const &expected) {
  AstStorage storage;
  auto *query = storage.Create<TQuery>();
  for (auto const cypher_access : {NO_ACCESS, READ, WRITE, UNIQUE}) {
    EXPECT_EQ(RequiredStorageAccess(*query, cypher_access, std::nullopt), expected);
    for (auto const mode : kModes) {
      EXPECT_EQ(RequiredStorageAccess(*query, cypher_access, mode), expected);
    }
  }
}

}  // namespace

TEST(QueryStorageAccess, NoPolicyNeedsNoAccessor) {
  ExpectSettledBy<memgraph::query::FreeMemoryQuery>({});
  ExpectSettledBy<memgraph::query::ShowConfigQuery>({});
}

TEST(QueryStorageAccess, ReadPolicyReads) {
  ExpectSettledBy<memgraph::query::DumpQuery>({.access = READ});
  ExpectSettledBy<memgraph::query::ExplainQuery>({.access = READ});
}

TEST(QueryStorageAccess, UniquePolicyTakesTheGraph) {
  ExpectSettledBy<memgraph::query::DropGraphQuery>({.access = UNIQUE});
  ExpectSettledBy<memgraph::query::PointIndexQuery>({.access = UNIQUE});
}

TEST(QueryStorageAccess, CypherTakesTheShapeItWasPlannedFor) {
  AstStorage storage;
  auto *query = storage.Create<memgraph::query::CypherQuery>();
  EXPECT_EQ(RequiredStorageAccess(*query, READ, std::nullopt),
            (StorageAccessRequirement{.access = READ, .could_commit = true}));
  EXPECT_EQ(RequiredStorageAccess(*query, WRITE, std::nullopt),
            (StorageAccessRequirement{.access = WRITE, .could_commit = true}));
  EXPECT_EQ(RequiredStorageAccess(*query, UNIQUE, std::nullopt),
            (StorageAccessRequirement{.access = UNIQUE, .could_commit = true}));
}

TEST(QueryStorageAccess, GraphFreeCypherOpensNoTransaction) {
  AstStorage storage;
  auto *query = storage.Create<memgraph::query::CypherQuery>();
  // NO_ACCESS opens no storage transaction, so there is nothing to commit either.
  EXPECT_EQ(RequiredStorageAccess(*query, NO_ACCESS, std::nullopt), StorageAccessRequirement{});
  EXPECT_EQ(RequiredStorageAccess(*query, NO_ACCESS, StorageMode::IN_MEMORY_TRANSACTIONAL), StorageAccessRequirement{});
}

TEST(QueryStorageAccess, ProfileTakesTheSameShapeButCommitsNothing) {
  AstStorage storage;
  auto *query = storage.Create<memgraph::query::ProfileQuery>();
  EXPECT_EQ(RequiredStorageAccess(*query, READ, std::nullopt), (StorageAccessRequirement{.access = READ}));
  EXPECT_EQ(RequiredStorageAccess(*query, WRITE, std::nullopt), (StorageAccessRequirement{.access = WRITE}));
}

TEST(QueryStorageAccess, IndexCreationNeedsWritersOutUnderTransactionalMode) {
  AstStorage storage;
  auto *query = storage.Create<memgraph::query::IndexQuery>();
  query->action_ = memgraph::query::IndexQuery::Action::CREATE;
  EXPECT_EQ(
      RequiredStorageAccess(*query, WRITE, StorageMode::IN_MEMORY_TRANSACTIONAL),
      (StorageAccessRequirement{
          .access = READ_ONLY, .mode_dependent = true, .isolation_override = IsolationLevel::SNAPSHOT_ISOLATION}));
}

TEST(QueryStorageAccess, IndexDropOnlyReadsUnderTransactionalMode) {
  AstStorage storage;
  auto *query = storage.Create<memgraph::query::IndexQuery>();
  query->action_ = memgraph::query::IndexQuery::Action::DROP;
  EXPECT_EQ(RequiredStorageAccess(*query, WRITE, StorageMode::IN_MEMORY_TRANSACTIONAL),
            (StorageAccessRequirement{
                .access = READ, .mode_dependent = true, .isolation_override = IsolationLevel::SNAPSHOT_ISOLATION}));
}

TEST(QueryStorageAccess, AnalyticalIndexDdlHoldsReadOnlyEitherWay) {
  AstStorage storage;
  for (auto const action : {memgraph::query::IndexQuery::Action::CREATE, memgraph::query::IndexQuery::Action::DROP}) {
    auto *query = storage.Create<memgraph::query::IndexQuery>();
    query->action_ = action;
    EXPECT_EQ(RequiredStorageAccess(*query, WRITE, StorageMode::IN_MEMORY_ANALYTICAL),
              (StorageAccessRequirement{.access = READ_ONLY, .mode_dependent = true}));
  }
}

TEST(QueryStorageAccess, OnDiskIndexDdlTakesTheGraph) {
  AstStorage storage;
  auto *query = storage.Create<memgraph::query::IndexQuery>();
  query->action_ = memgraph::query::IndexQuery::Action::CREATE;
  EXPECT_EQ(RequiredStorageAccess(*query, WRITE, StorageMode::ON_DISK_TRANSACTIONAL),
            (StorageAccessRequirement{.access = UNIQUE, .mode_dependent = true}));
}

TEST(QueryStorageAccess, EdgeIndexDdlFollowsTheSameRuleAsIndexDdl) {
  AstStorage storage;
  auto *create = storage.Create<memgraph::query::EdgeIndexQuery>();
  create->action_ = memgraph::query::EdgeIndexQuery::Action::CREATE;
  EXPECT_EQ(
      RequiredStorageAccess(*create, WRITE, StorageMode::IN_MEMORY_TRANSACTIONAL),
      (StorageAccessRequirement{
          .access = READ_ONLY, .mode_dependent = true, .isolation_override = IsolationLevel::SNAPSHOT_ISOLATION}));
  auto *drop = storage.Create<memgraph::query::EdgeIndexQuery>();
  drop->action_ = memgraph::query::EdgeIndexQuery::Action::DROP;
  EXPECT_EQ(RequiredStorageAccess(*drop, WRITE, StorageMode::ON_DISK_TRANSACTIONAL),
            (StorageAccessRequirement{.access = UNIQUE, .mode_dependent = true}));
}

TEST(QueryStorageAccess, ConstraintDdlIsReadOnlyExceptOnDisk) {
  AstStorage storage;
  auto *query = storage.Create<memgraph::query::ConstraintQuery>();
  EXPECT_EQ(RequiredStorageAccess(*query, WRITE, StorageMode::IN_MEMORY_TRANSACTIONAL),
            (StorageAccessRequirement{.access = READ_ONLY, .mode_dependent = true}));
  EXPECT_EQ(RequiredStorageAccess(*query, WRITE, StorageMode::IN_MEMORY_ANALYTICAL),
            (StorageAccessRequirement{.access = READ_ONLY, .mode_dependent = true}));
  EXPECT_EQ(RequiredStorageAccess(*query, WRITE, StorageMode::ON_DISK_TRANSACTIONAL),
            (StorageAccessRequirement{.access = UNIQUE, .mode_dependent = true}));
}

TEST(QueryStorageAccess, DdlWithoutADatabaseIsRefused) {
  AstStorage storage;
  auto *index = storage.Create<memgraph::query::IndexQuery>();
  index->action_ = memgraph::query::IndexQuery::Action::CREATE;
  EXPECT_THROW(RequiredStorageAccess(*index, WRITE, std::nullopt), memgraph::query::DatabaseContextRequiredException);

  auto *edge_index = storage.Create<memgraph::query::EdgeIndexQuery>();
  edge_index->action_ = memgraph::query::EdgeIndexQuery::Action::CREATE;
  EXPECT_THROW(RequiredStorageAccess(*edge_index, WRITE, std::nullopt),
               memgraph::query::DatabaseContextRequiredException);

  auto *constraint = storage.Create<memgraph::query::ConstraintQuery>();
  EXPECT_THROW(RequiredStorageAccess(*constraint, WRITE, std::nullopt),
               memgraph::query::DatabaseContextRequiredException);
}

TEST(QueryStorageAccess, DescribingTakesTheGraphOnlyToChangeADescription) {
  AstStorage storage;
  using Action = memgraph::query::DescriptionQuery::Action;
  for (auto const action : {Action::SET, Action::DELETE}) {
    auto *query = storage.Create<memgraph::query::DescriptionQuery>();
    query->action_ = action;
    EXPECT_EQ(RequiredStorageAccess(*query, WRITE, StorageMode::IN_MEMORY_TRANSACTIONAL),
              (StorageAccessRequirement{.access = UNIQUE}));
  }
  auto *show = storage.Create<memgraph::query::DescriptionQuery>();
  show->action_ = Action::SHOW_ALL;
  EXPECT_EQ(RequiredStorageAccess(*show, WRITE, StorageMode::IN_MEMORY_TRANSACTIONAL),
            (StorageAccessRequirement{.access = READ}));
}

TEST(QueryStorageAccess, OnlyTriggerCreationNeedsAnAccessor) {
  AstStorage storage;
  using Action = memgraph::query::TriggerQuery::Action;
  auto *create = storage.Create<memgraph::query::TriggerQuery>();
  create->action_ = Action::CREATE_TRIGGER;
  EXPECT_EQ(RequiredStorageAccess(*create, WRITE, StorageMode::IN_MEMORY_TRANSACTIONAL),
            (StorageAccessRequirement{.access = READ}));

  for (auto const action : {Action::DROP_TRIGGER, Action::SHOW_TRIGGERS}) {
    auto *query = storage.Create<memgraph::query::TriggerQuery>();
    query->action_ = action;
    EXPECT_EQ(RequiredStorageAccess(*query, WRITE, StorageMode::IN_MEMORY_TRANSACTIONAL), StorageAccessRequirement{});
  }
}

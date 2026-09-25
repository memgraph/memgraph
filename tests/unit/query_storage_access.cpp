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

#include <array>
#include <optional>
#include <set>
#include <string>
#include <string_view>

#include <fmt/format.h>

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/query/auth_query.hpp"
#include "query/frontend/ast/query/tenant_profile.hpp"
#include "query/frontend/ast/query/user_profile.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage_mode.hpp"
#include "utils/variant_helpers.hpp"

using memgraph::query::AstStorage;
using memgraph::query::ConstraintDdl;
using memgraph::query::FixedAccess;
using memgraph::query::IndexDdl;
using memgraph::query::NoAccess;
using memgraph::query::PlannerShaped;
using memgraph::query::RequiredStorageAccess;
using memgraph::query::StorageAccessPolicy;
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

// Every query states what it needs, and this is the whole table of those answers. The expected
// values are the ones the interpreter's visitor produced before the answers moved onto the queries,
// so a row disagreeing means a query changed what it asks for.
std::set<std::string_view> covered;

// Named locally rather than through the stream operator in storage.hpp, which is a heavy header to
// include for four words.
std::string_view Name(memgraph::storage::StorageAccessType access) {
  switch (access) {
    case NO_ACCESS:
      return "NO_ACCESS";
    case READ:
      return "READ";
    case WRITE:
      return "WRITE";
    case UNIQUE:
      return "UNIQUE";
    case READ_ONLY:
      return "READ_ONLY";
  }
  return "?";
}

// gtest prints a variant as its bytes, which names neither the case nor the access when a row fails.
std::string Describe(StorageAccessPolicy const &policy) {
  return std::visit(
      memgraph::utils::Overloaded{
          [](NoAccess) { return std::string{"NoAccess"}; },
          [](FixedAccess fixed) { return fmt::format("FixedAccess({})", Name(fixed.access)); },
          [](PlannerShaped shaped) { return fmt::format("PlannerShaped(commits={})", shaped.commits); },
          [](IndexDdl ddl) { return fmt::format("IndexDdl(creating={}, on_edges={})", ddl.creating, ddl.on_edges); },
          [](ConstraintDdl) { return std::string{"ConstraintDdl"}; },
      },
      policy);
}

template <typename TQuery>
void Check(StorageAccessPolicy const &policy, bool operates_on_graph_data, TQuery const &query) {
  EXPECT_EQ(query.AccessPolicy(), policy)
      << TQuery::kType.name << " states " << Describe(query.AccessPolicy()) << ", expected " << Describe(policy);
  EXPECT_EQ(query.OperatesOnGraphData(), operates_on_graph_data) << TQuery::kType.name;
  covered.insert(TQuery::kType.name);
}

template <typename TQuery>
void Declares(StorageAccessPolicy const &policy, bool operates_on_graph_data) {
  AstStorage storage;
  Check(policy, operates_on_graph_data, *storage.Create<TQuery>());
}

template <typename TQuery, typename TAction>
void DeclaresWithAction(TAction action, StorageAccessPolicy const &policy, bool operates_on_graph_data) {
  AstStorage storage;
  auto *query = storage.Create<TQuery>();
  query->action_ = action;
  Check(policy, operates_on_graph_data, *query);
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
  EXPECT_EQ(RequiredStorageAccess(*query, NO_ACCESS, std::nullopt), StorageAccessRequirement{});
  EXPECT_EQ(RequiredStorageAccess(*query, NO_ACCESS, StorageMode::IN_MEMORY_TRANSACTIONAL), StorageAccessRequirement{});
}

TEST(QueryStorageAccess, ProfileTakesTheSameShapeButCommitsNothing) {
  AstStorage storage;
  auto *query = storage.Create<memgraph::query::ProfileQuery>();
  EXPECT_EQ(RequiredStorageAccess(*query, READ, std::nullopt), (StorageAccessRequirement{.access = READ}));
  EXPECT_EQ(RequiredStorageAccess(*query, WRITE, std::nullopt), (StorageAccessRequirement{.access = WRITE}));
}

TEST(QueryStorageAccess, NoAccessIsNeverAskedForAsAnAccessor) {
  AstStorage storage;
  // NO_ACCESS names the absence of a hold, and taking an accessor under that name aborts, so it has
  // to answer as no accessor rather than as one of that type. Both planner-shaped kinds are asked
  // because only one of them can be handed it today, and that is not a property of the rule.
  EXPECT_EQ(RequiredStorageAccess(*storage.Create<memgraph::query::CypherQuery>(), NO_ACCESS, std::nullopt),
            StorageAccessRequirement{});
  EXPECT_EQ(RequiredStorageAccess(*storage.Create<memgraph::query::ProfileQuery>(), NO_ACCESS, std::nullopt),
            StorageAccessRequirement{});
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

TEST(QueryStorageAccess, WorkingOnTheGraphIsTheDefaultAnswer) {
  AstStorage storage;
  // Anything that reads or writes the current database's data, including the metadata a broken
  // database would report as a clean empty result.
  EXPECT_TRUE(storage.Create<memgraph::query::CypherQuery>()->OperatesOnGraphData());
  EXPECT_TRUE(storage.Create<memgraph::query::IndexQuery>()->OperatesOnGraphData());
  EXPECT_TRUE(storage.Create<memgraph::query::DumpQuery>()->OperatesOnGraphData());
  EXPECT_TRUE(storage.Create<memgraph::query::DatabaseInfoQuery>()->OperatesOnGraphData());
  EXPECT_TRUE(storage.Create<memgraph::query::CreateSnapshotQuery>()->OperatesOnGraphData());
  // The cure works on that data too, by replacing it, so its availability while a database is
  // broken is the gate's own exception rather than a claim made here.
  EXPECT_TRUE(storage.Create<memgraph::query::RecoverSnapshotQuery>()->OperatesOnGraphData());
}

TEST(QueryStorageAccess, InstanceAndSessionQueriesStayAvailable) {
  AstStorage storage;
  EXPECT_FALSE(storage.Create<memgraph::query::AuthQuery>()->OperatesOnGraphData());
  EXPECT_FALSE(storage.Create<memgraph::query::ReplicationQuery>()->OperatesOnGraphData());
  EXPECT_FALSE(storage.Create<memgraph::query::ShowDatabasesQuery>()->OperatesOnGraphData());
  EXPECT_FALSE(storage.Create<memgraph::query::FreeMemoryQuery>()->OperatesOnGraphData());
  EXPECT_FALSE(storage.Create<memgraph::query::SessionQuery>()->OperatesOnGraphData());
  EXPECT_FALSE(storage.Create<memgraph::query::SystemInfoQuery>()->OperatesOnGraphData());
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

TEST(QueryStorageAccess, EveryQueryStatesWhatItNeeds) {
  using namespace memgraph::query;  // NOLINT: 59 query types are named below

  Declares<CypherQuery>(PlannerShaped{.commits = true}, true);
  Declares<ExplainQuery>(FixedAccess{.access = READ}, true);
  Declares<ProfileQuery>(PlannerShaped{.commits = false}, true);
  DeclaresWithAction<IndexQuery>(IndexQuery::Action::CREATE, IndexDdl{.creating = true, .on_edges = false}, true);
  DeclaresWithAction<IndexQuery>(IndexQuery::Action::DROP, IndexDdl{.creating = false, .on_edges = false}, true);
  DeclaresWithAction<EdgeIndexQuery>(
      EdgeIndexQuery::Action::CREATE, IndexDdl{.creating = true, .on_edges = true}, true);
  DeclaresWithAction<EdgeIndexQuery>(EdgeIndexQuery::Action::DROP, IndexDdl{.creating = false, .on_edges = true}, true);
  Declares<PointIndexQuery>(FixedAccess{.access = UNIQUE}, true);
  Declares<TextIndexQuery>(FixedAccess{.access = UNIQUE}, true);
  Declares<CreateTextEdgeIndexQuery>(FixedAccess{.access = UNIQUE}, true);
  Declares<VectorIndexQuery>(FixedAccess{.access = UNIQUE}, true);
  Declares<CreateVectorEdgeIndexQuery>(FixedAccess{.access = UNIQUE}, true);
  Declares<AuthQuery>(NoAccess{}, false);
  Declares<DatabaseInfoQuery>(NoAccess{}, true);
  Declares<SystemInfoQuery>(NoAccess{}, false);
  Declares<ConstraintQuery>(ConstraintDdl{}, true);
  Declares<DumpQuery>(FixedAccess{.access = READ}, true);
  Declares<ReplicationQuery>(NoAccess{}, false);
  Declares<ReplicationInfoQuery>(NoAccess{}, false);
  Declares<LockPathQuery>(NoAccess{}, false);
  Declares<FreeMemoryQuery>(NoAccess{}, false);
  DeclaresWithAction<TriggerQuery>(TriggerQuery::Action::CREATE_TRIGGER, FixedAccess{.access = READ}, true);
  DeclaresWithAction<TriggerQuery>(TriggerQuery::Action::DROP_TRIGGER, NoAccess{}, true);
  DeclaresWithAction<TriggerQuery>(TriggerQuery::Action::SHOW_TRIGGERS, NoAccess{}, true);
  Declares<IsolationLevelQuery>(NoAccess{}, true);
  Declares<CreateSnapshotQuery>(NoAccess{}, true);
  Declares<RecoverSnapshotQuery>(FixedAccess{.access = UNIQUE}, true);
  Declares<ShowSnapshotsQuery>(NoAccess{}, true);
  Declares<ShowNextSnapshotQuery>(NoAccess{}, true);
  Declares<StreamQuery>(NoAccess{}, true);
  Declares<SettingQuery>(NoAccess{}, false);
  Declares<VersionQuery>(NoAccess{}, false);
  Declares<ShowConfigQuery>(NoAccess{}, false);
  Declares<ShowQueryCallableMappingsQuery>(NoAccess{}, false);
  Declares<TransactionQueueQuery>(NoAccess{}, false);
  Declares<SessionQuery>(NoAccess{}, false);
  Declares<StorageModeQuery>(NoAccess{}, true);
  Declares<AnalyzeGraphQuery>(FixedAccess{.access = READ}, true);
  Declares<MultiDatabaseQuery>(NoAccess{}, false);
  Declares<UseDatabaseQuery>(NoAccess{}, false);
  Declares<ShowDatabaseQuery>(NoAccess{}, false);
  Declares<ShowDatabasesQuery>(NoAccess{}, false);
  Declares<EdgeImportModeQuery>(NoAccess{}, true);
  Declares<CoordinatorQuery>(NoAccess{}, false);
  Declares<DropAllIndexesQuery>(FixedAccess{.access = UNIQUE}, true);
  Declares<DropAllConstraintsQuery>(FixedAccess{.access = UNIQUE}, true);
  Declares<DropGraphQuery>(FixedAccess{.access = UNIQUE}, true);
  Declares<CreateEnumQuery>(FixedAccess{.access = UNIQUE}, true);
  Declares<ShowEnumsQuery>(FixedAccess{.access = READ}, true);
  Declares<AlterEnumAddValueQuery>(FixedAccess{.access = UNIQUE}, true);
  Declares<AlterEnumUpdateValueQuery>(FixedAccess{.access = UNIQUE}, true);
  Declares<AlterEnumRemoveValueQuery>(NoAccess{}, true);
  Declares<DropEnumQuery>(NoAccess{}, true);
  Declares<ShowSchemaInfoQuery>(FixedAccess{.access = READ}, true);
  Declares<TtlQuery>(FixedAccess{.access = UNIQUE}, true);
  Declares<SessionTraceQuery>(NoAccess{}, false);
  Declares<SessionSettingQuery>(NoAccess{}, false);
  Declares<UserProfileQuery>(NoAccess{}, false);
  Declares<TenantProfileQuery>(NoAccess{}, false);
  Declares<ParameterQuery>(NoAccess{}, false);
  DeclaresWithAction<DescriptionQuery>(DescriptionQuery::Action::SET, FixedAccess{.access = UNIQUE}, true);
  DeclaresWithAction<DescriptionQuery>(DescriptionQuery::Action::DELETE, FixedAccess{.access = UNIQUE}, true);
  DeclaresWithAction<DescriptionQuery>(DescriptionQuery::Action::SHOW_ALL, FixedAccess{.access = READ}, true);
  Declares<ReloadSSLQuery>(NoAccess{}, false);
  Declares<ShowMemoryInfoQuery>(NoAccess{}, false);

  // The count comes from the visitor's own list of query types rather than from a literal, so a new
  // type of query reaches this test as a missing row instead of passing unnoticed.
  EXPECT_EQ(covered.size(), memgraph::query::QueryVisitor<void>::kVisitableCount);
}

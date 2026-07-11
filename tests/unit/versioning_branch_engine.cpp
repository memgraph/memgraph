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

// Graph Versioning: BranchEngine (materialize-per-checkout substrate) unit tests.
//
// BranchEngine::BuildFromFork is exercised directly against a real InMemoryStorage main +
// InMemoryStorage::RegisterForkPin (mirrors versioning_historical_access.cpp's and
// versioning_merge.cpp's own fixture style) -- it is not wired into the interpreter yet (a later
// unit owns running ordinary Cypher against the seeded engine), only the seeding step itself.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "versioning/branch_engine.hpp"

import memgraph.storage.property_value;

namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;

namespace {

class VersioningBranchEngineTest : public testing::Test {
 protected:
  void SetUp() override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    storage_ = std::make_unique<ms::InMemoryStorage>(config);
    mem_storage_ = static_cast<ms::InMemoryStorage *>(storage_.get());
  }

  void TearDown() override {
    branch_engine_.reset();
    if (fork_ts_.has_value()) mem_storage_->ReleaseForkPin(*fork_ts_);
    storage_.reset();
  }

  // Registers a fork pin at the CURRENT tip and remembers it for TearDown's release.
  uint64_t ForkNow() {
    fork_ts_ = mem_storage_->RegisterForkPin();
    return *fork_ts_;
  }

  std::unique_ptr<ms::Storage> storage_;
  ms::InMemoryStorage *mem_storage_{};
  std::optional<uint64_t> fork_ts_;
  std::unique_ptr<mv::BranchEngine> branch_engine_;
};

// Builds main's `(s:S)-[:R {w:5}]->(a:A {x:1})` plus an isolated `(:B)`, forks, and verifies
// BuildFromFork produces a full, gid-preserving copy: right vertex count, labels (translated by
// NAME into the branch engine's OWN mapper), properties, and the edge with its type + property.
TEST_F(VersioningBranchEngineTest, SeedsFullForkStateCopy) {
  const auto label_s = storage_->NameToLabel("S");
  const auto label_a = storage_->NameToLabel("A");
  const auto label_b = storage_->NameToLabel("B");
  const auto prop_x = storage_->NameToProperty("x");
  const auto prop_w = storage_->NameToProperty("w");

  ms::Gid s_gid, a_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto s = acc->CreateVertex();
    s_gid = s.Gid();
    ASSERT_TRUE(s.AddLabel(label_s).has_value());

    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    ASSERT_TRUE(a.AddLabel(label_a).has_value());
    ASSERT_TRUE(a.SetProperty(prop_x, ms::PropertyValue(1)).has_value());

    auto r = acc->CreateEdge(&s, &a, acc->NameToEdgeType("R"));
    ASSERT_TRUE(r.has_value());
    ASSERT_TRUE(r->SetProperty(prop_w, ms::PropertyValue(5)).has_value());

    auto b = acc->CreateVertex();  // isolated
    ASSERT_TRUE(b.AddLabel(label_b).has_value());

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();

  auto result = mv::BranchEngine::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value());
  branch_engine_ = std::move(*result);

  auto acc = branch_engine_->storage().Access(ms::READ);

  size_t vertex_count = 0;
  for (auto v : acc->Vertices(ms::View::OLD)) {
    (void)v;
    ++vertex_count;
  }
  EXPECT_EQ(vertex_count, 3u) << "S, A, B must all have been copied";

  // gid preservation: A must be findable at its ORIGINAL gid, with its label + property translated
  // by name into the branch engine's own (numerically distinct) mapper.
  auto branch_label_a = acc->NameToLabel("A");
  auto branch_prop_x = acc->NameToProperty("x");
  auto a = acc->FindVertex(a_gid, ms::View::OLD);
  ASSERT_TRUE(a.has_value());
  EXPECT_TRUE(a->HasLabel(branch_label_a, ms::View::OLD).value_or(false));
  auto x_val = a->GetProperty(branch_prop_x, ms::View::OLD);
  ASSERT_TRUE(x_val.has_value());
  EXPECT_EQ(*x_val, ms::PropertyValue(1));

  // S must exist at its original gid too, with its label.
  auto branch_label_s = acc->NameToLabel("S");
  auto s = acc->FindVertex(s_gid, ms::View::OLD);
  ASSERT_TRUE(s.has_value());
  EXPECT_TRUE(s->HasLabel(branch_label_s, ms::View::OLD).value_or(false));

  // (:B) must be present, isolated, with its label.
  auto branch_label_b = acc->NameToLabel("B");
  bool found_b = false;
  for (auto v : acc->Vertices(ms::View::OLD)) {
    if (v.HasLabel(branch_label_b, ms::View::OLD).value_or(false)) found_b = true;
  }
  EXPECT_TRUE(found_b);

  // The S-[:R]->A edge must exist, with its type and property.
  auto out_edges = s->OutEdges(ms::View::OLD);
  ASSERT_TRUE(out_edges.has_value());
  ASSERT_EQ(out_edges->edges.size(), 1u);
  const auto &r_edge = out_edges->edges.front();
  EXPECT_EQ(r_edge.EdgeType(), acc->NameToEdgeType("R"));
  EXPECT_EQ(r_edge.ToVertex().Gid(), a_gid);
  auto w_val = r_edge.GetProperty(acc->NameToProperty("w"), ms::View::OLD);
  ASSERT_TRUE(w_val.has_value());
  EXPECT_EQ(*w_val, ms::PropertyValue(5));
}

// R35-style isolation: the branch engine is a physically independent COPY, not a live view --
// writes on either side after the fork must never be observed by the other.
TEST_F(VersioningBranchEngineTest, BranchEngineIsIndependentOfMain) {
  const auto prop_x = storage_->NameToProperty("x");
  const auto label_a = storage_->NameToLabel("A");

  ms::Gid a_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    ASSERT_TRUE(a.AddLabel(label_a).has_value());
    ASSERT_TRUE(a.SetProperty(prop_x, ms::PropertyValue(1)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();

  auto result = mv::BranchEngine::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value());
  branch_engine_ = std::move(*result);

  // Mutate the BRANCH engine's copy: SET a.x = 2.
  {
    auto acc = branch_engine_->storage().Access(ms::WRITE);
    auto branch_prop_x = acc->NameToProperty("x");
    auto a = acc->FindVertex(a_gid, ms::View::OLD);
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(a->SetProperty(branch_prop_x, ms::PropertyValue(2)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Main must still read x == 1 -- the branch engine's write must not leak back.
  {
    auto acc = storage_->Access(ms::READ);
    auto a = acc->FindVertex(a_gid, ms::View::OLD);
    ASSERT_TRUE(a.has_value());
    auto x_val = a->GetProperty(prop_x, ms::View::OLD);
    ASSERT_TRUE(x_val.has_value());
    EXPECT_EQ(*x_val, ms::PropertyValue(1));
  }

  // Mutate MAIN after the branch engine was built: SET a.x = 99 on main.
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->FindVertex(a_gid, ms::View::OLD);
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(a->SetProperty(prop_x, ms::PropertyValue(99)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // The branch engine must still see its OWN copy's value (2, from the branch-side write above),
  // unaffected by main's later edit.
  {
    auto acc = branch_engine_->storage().Access(ms::READ);
    auto branch_prop_x = acc->NameToProperty("x");
    auto a = acc->FindVertex(a_gid, ms::View::OLD);
    ASSERT_TRUE(a.has_value());
    auto x_val = a->GetProperty(branch_prop_x, ms::View::OLD);
    ASSERT_TRUE(x_val.has_value());
    EXPECT_EQ(*x_val, ms::PropertyValue(2));
  }
}

// Defensive misuse guard (mirrors versioning_historical_access.cpp's
// RejectsUnpinnedForkTimestamp): a fork_ts that was never registered via RegisterForkPin must be
// reported as BranchEngineBuildError::kForkStateUnavailable, rather than build a bogus or partial
// engine.
TEST_F(VersioningBranchEngineTest, PropagatesHistoricalAccessErrorForUnpinnedTs) {
  {
    auto acc = storage_->Access(ms::WRITE);
    (void)acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Never pinned via RegisterForkPin -- must be refused rather than approximated.
  constexpr uint64_t kNeverPinnedForkTs = 1;
  auto result =
      mv::BranchEngine::BuildFromFork(*mem_storage_, kNeverPinnedForkTs, memgraph::tests::MakeMainCommitArgs());
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, mv::BranchEngineBuildError::Kind::kForkStateUnavailable);
}

// HIGH: an Enum property at the TOP LEVEL of a fork-state vertex must be rejected loudly
// (kUnsupportedEnumProperty), never silently dropped -- an Enum's embedded enum-type-id is main's
// and meaningless (or, worse, accidentally valid as some unrelated type) under the branch engine's
// own numbering.
TEST_F(VersioningBranchEngineTest, RejectsForkStateWithTopLevelEnumProperty) {
  const auto label_a = storage_->NameToLabel("A");
  const auto prop_e = storage_->NameToProperty("e");

  {
    auto unique_acc = storage_->UniqueAccess();
    ASSERT_TRUE(unique_acc->CreateEnum("Color", std::vector<std::string>{"Red", "Green"}).has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    ASSERT_TRUE(a.AddLabel(label_a).has_value());
    auto enum_val = acc->GetEnumValue("Color", "Red");
    ASSERT_TRUE(enum_val.has_value());
    ASSERT_TRUE(a.SetProperty(prop_e, ms::PropertyValue(*enum_val)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();

  auto result = mv::BranchEngine::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, mv::BranchEngineBuildError::Kind::kUnsupportedEnumProperty);
}

// HIGH: the case the top-level `IsEnum()` check alone would MISS -- an Enum nested inside a List
// property. ContainsEnum must recurse into the list to catch it; without that recursion this would
// previously have been silently copied verbatim (corruption), not just silently dropped.
TEST_F(VersioningBranchEngineTest, RejectsForkStateWithEnumNestedInList) {
  const auto label_a = storage_->NameToLabel("A");
  const auto prop_l = storage_->NameToProperty("l");

  {
    auto unique_acc = storage_->UniqueAccess();
    ASSERT_TRUE(unique_acc->CreateEnum("Color", std::vector<std::string>{"Red", "Green"}).has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    ASSERT_TRUE(a.AddLabel(label_a).has_value());
    auto enum_val = acc->GetEnumValue("Color", "Red");
    ASSERT_TRUE(enum_val.has_value());
    auto list_val =
        ms::PropertyValue(std::vector<ms::PropertyValue>{ms::PropertyValue(1), ms::PropertyValue(*enum_val)});
    ASSERT_TRUE(a.SetProperty(prop_l, list_val).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();

  auto result = mv::BranchEngine::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, mv::BranchEngineBuildError::Kind::kUnsupportedEnumProperty);
}

}  // namespace

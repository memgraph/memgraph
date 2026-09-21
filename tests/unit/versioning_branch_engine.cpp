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

// Graph Versioning v1, slice E-1: BranchContext (lazy diff-context substrate) unit tests.
//
// Supersedes the earlier materialize-per-checkout BranchEngine tests: BuildFromFork no longer
// copies main's fork-state graph at all (O(1), not O(main's size)) -- it just opens an EMPTY
// private diff engine plus a HistoricalAccess(fork_ts) reader. CowVertex/ResolveVertex/Vertices
// are exercised directly against a real InMemoryStorage main + InMemoryStorage::RegisterForkPin
// (mirrors versioning_historical_access.cpp's and versioning_merge.cpp's own fixture style) -- not
// wired into the interpreter here (see versioning_interpreter.cpp for that end-to-end coverage).
//
// ONE SHARED NameIdMapper: BuildFromFork constructs the diff engine sharing `mem_storage_`'s own
// NameIdMapper (see branch_engine.cpp/hpp's own doc-comments) -- so `storage_->NameToLabel(...)` /
// `storage_->NameToProperty(...)` and the SAME calls against `branch_context_->diff_engine()`'s
// own accessor return IDENTICAL ids below, not merely translated-to-equivalent ones. Tests use a
// single `label_a`/`prop_x` (main-side) variable throughout rather than a separate "diff_*" one,
// since there is no longer a second id space to keep straight.
//
// VERTEX-ONLY (this slice's own scope): no edge coverage here.
//
// perf/versioning-branch-read D1 ADDENDUM: one edge-focused test was added at the bottom of this
// file (EdgeBranchedBitDeleteViewNewRaisesViewOldFallsBackToForkValue) to pin the exact
// FindDiffEdge/IsEdgeTombstoned View::OLD-vs-View::NEW split the D1 edge-bit value-read gate
// (query::EdgeAccessor::Properties/GetProperty/GetPropertySize, edge_accessor.cpp) depends on for
// its delete-safety guarantee, exercised directly against query::EdgeAccessor::GetProperty itself.
// It is here rather than in versioning_interpreter.cpp because constructing an independent, never-
// COW'd binding to an already-branch-deleted edge (to reach the tombstone-guard branch rather than
// the diff-resident short-circuit) is not reachable through a single Cypher statement. The query-
// layer NEW-view half of the same contract (raising through the interpreter) is covered directly in
// versioning_interpreter.cpp's BranchEdgeBitFastPathDeleteAfterModifyStillRaisesOnNewView.

#include <cstdint>
#include <filesystem>
#include <map>
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
    ClearBranchWalRoot();
  }

  void TearDown() override {
    branch_context_.reset();
    if (fork_ts_.has_value()) mem_storage_->ReleaseForkPin(*fork_ts_);
    storage_.reset();
    ClearBranchWalRoot();
  }

  // Registers a fork pin at the CURRENT tip and remembers it for TearDown's release.
  uint64_t ForkNow() {
    fork_ts_ = mem_storage_->RegisterForkPin();
    return *fork_ts_;
  }

  // Durable-capture slice (design slices A+B+C): BuildFromFork now also opens this checkout
  // session's own BranchLog under a caller-supplied root directory (a fresh per-call UUID
  // subdirectory beneath it -- see BranchContext::BuildFromFork's own doc-comment), which means it
  // genuinely touches disk now, unlike the rest of this file's in-RAM-only diff engine. A real,
  // throwaway temp directory, mirroring versioning_branch_log.cpp's own convention -- this file
  // does not otherwise care about (or assert on) the branch log's contents.
  std::filesystem::path BranchWalRoot() const {
    return std::filesystem::temp_directory_path() / "MG_test_unit_versioning_branch_engine";
  }

  std::unique_ptr<ms::Storage> storage_;
  ms::InMemoryStorage *mem_storage_{};
  std::optional<uint64_t> fork_ts_;
  std::unique_ptr<mv::BranchContext> branch_context_;

 private:
  void ClearBranchWalRoot() const {
    if (std::filesystem::exists(BranchWalRoot())) std::filesystem::remove_all(BranchWalRoot());
  }
};

// BuildFromFork must be O(1): the diff engine starts genuinely empty (module the internal
// gid-watermark reservation, which leaves zero LIVE vertices -- see kBranchNativeGidWatermark's
// doc-comment in branch_engine.cpp), while historical_ still sees main's full fork-state graph.
TEST_F(VersioningBranchEngineTest, BuildFromForkOpensEmptyDiffEngineAndLiveHistoricalBase) {
  const auto label_a = storage_->NameToLabel("A");
  ms::Gid a_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    ASSERT_TRUE(a.AddLabel(label_a).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_,
                                                 fork_ts,
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 BranchWalRoot(),
                                                 {});
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  // The diff engine has NOTHING in it yet -- nothing was copied.
  auto diff_acc = branch_context_->diff_engine().Access(ms::READ);
  size_t diff_vertex_count = 0;
  for (auto v : diff_acc->Vertices(ms::View::OLD)) {
    (void)v;
    ++diff_vertex_count;
  }
  EXPECT_EQ(diff_vertex_count, 0u);

  // historical_ still sees main's fork-state graph directly (no copy needed to read it).
  auto hist_vertex = branch_context_->historical().FindVertex(a_gid, ms::View::OLD);
  ASSERT_TRUE(hist_vertex.has_value());
  EXPECT_TRUE(hist_vertex->HasLabel(storage_->NameToLabel("A"), ms::View::OLD).value_or(false));
}

// The core COW contract: CowVertex(A) must copy ONLY A (labels+properties, ids copied directly --
// see the file's own note on the shared NameIdMapper) into the diff engine -- B, untouched, must
// NOT appear there, and must still resolve via the historical fallback.
TEST_F(VersioningBranchEngineTest, CowVertexCopiesOnlyTheTouchedVertex) {
  const auto label_a = storage_->NameToLabel("A");
  const auto label_b = storage_->NameToLabel("B");
  const auto prop_x = storage_->NameToProperty("x");

  ms::Gid a_gid, b_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    ASSERT_TRUE(a.AddLabel(label_a).has_value());
    ASSERT_TRUE(a.SetProperty(prop_x, ms::PropertyValue(1)).has_value());

    auto b = acc->CreateVertex();
    b_gid = b.Gid();
    ASSERT_TRUE(b.AddLabel(label_b).has_value());

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_,
                                                 fork_ts,
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 BranchWalRoot(),
                                                 {});
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  // Single-slot collapse (mgp_vertex size budget, R6): CowVertex/ResolveVertex/Vertices no longer
  // take an accessor parameter -- they read it from this one per-query slot instead (safe: a
  // checked-out branch is exclusive single-writer, so at most one query/accessor is ever live).
  branch_context_->set_current_diff_txn(diff_acc.get());

  auto cowed = branch_context_->CowVertex(a_gid);
  ASSERT_TRUE(cowed.has_value());
  EXPECT_EQ(cowed->Gid(), a_gid);

  // Labels/properties copied with their ids UNCHANGED -- label_a/prop_x (main's own ids) are
  // valid directly against the diff engine's copy too, since diff_engine_ shares main's mapper.
  EXPECT_TRUE(cowed->HasLabel(label_a, ms::View::NEW).value_or(false));
  auto x_val = cowed->GetProperty(prop_x, ms::View::NEW);
  ASSERT_TRUE(x_val.has_value());
  EXPECT_EQ(*x_val, ms::PropertyValue(1));

  // Only ONE vertex resident in the diff engine -- B was never touched.
  size_t diff_vertex_count = 0;
  for (auto v : diff_acc->Vertices(ms::View::NEW)) {
    (void)v;
    ++diff_vertex_count;
  }
  EXPECT_EQ(diff_vertex_count, 1u);
  EXPECT_FALSE(diff_acc->FindVertex(b_gid, ms::View::NEW).has_value());

  ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// CowVertex is idempotent: a second call for the same gid must return the SAME diff-engine copy,
// not create a duplicate.
TEST_F(VersioningBranchEngineTest, CowVertexIsIdempotent) {
  const auto label_a = storage_->NameToLabel("A");
  ms::Gid a_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    ASSERT_TRUE(a.AddLabel(label_a).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_,
                                                 fork_ts,
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 BranchWalRoot(),
                                                 {});
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  // Single-slot collapse (mgp_vertex size budget, R6): CowVertex/ResolveVertex/Vertices no longer
  // take an accessor parameter -- they read it from this one per-query slot instead (safe: a
  // checked-out branch is exclusive single-writer, so at most one query/accessor is ever live).
  branch_context_->set_current_diff_txn(diff_acc.get());

  auto first = branch_context_->CowVertex(a_gid);
  ASSERT_TRUE(first.has_value());
  auto prop_x = storage_->NameToProperty("x");
  ASSERT_TRUE(first->SetProperty(prop_x, ms::PropertyValue(42)).has_value());

  auto second = branch_context_->CowVertex(a_gid);
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(second->Gid(), a_gid);
  auto x_val = second->GetProperty(prop_x, ms::View::NEW);
  ASSERT_TRUE(x_val.has_value());
  EXPECT_EQ(*x_val, ms::PropertyValue(42)) << "second CowVertex must resolve to the SAME (already-mutated) copy";

  size_t diff_vertex_count = 0;
  for (auto v : diff_acc->Vertices(ms::View::NEW)) {
    (void)v;
    ++diff_vertex_count;
  }
  EXPECT_EQ(diff_vertex_count, 1u) << "idempotent COW must not create a duplicate";

  ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// ResolveVertex: diff-engine-first, falling back to historical_ on a miss.
TEST_F(VersioningBranchEngineTest, ResolveVertexPrefersDiffEngineOverHistorical) {
  const auto prop_x = storage_->NameToProperty("x");
  ms::Gid a_gid, b_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    ASSERT_TRUE(a.SetProperty(prop_x, ms::PropertyValue(1)).has_value());
    auto b = acc->CreateVertex();
    b_gid = b.Gid();
    ASSERT_TRUE(b.SetProperty(prop_x, ms::PropertyValue(9)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_,
                                                 fork_ts,
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 BranchWalRoot(),
                                                 {});
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  // Single-slot collapse (mgp_vertex size budget, R6): CowVertex/ResolveVertex/Vertices no longer
  // take an accessor parameter -- they read it from this one per-query slot instead (safe: a
  // checked-out branch is exclusive single-writer, so at most one query/accessor is ever live).
  branch_context_->set_current_diff_txn(diff_acc.get());

  // COW A and change it to 2 -- ResolveVertex(A) must now see the DIFF copy (2), not historical's
  // (1). `prop_x` is valid against BOTH sides -- diff_engine_ shares main's own NameIdMapper.
  auto cowed = branch_context_->CowVertex(a_gid);
  ASSERT_TRUE(cowed.has_value());
  ASSERT_TRUE(cowed->SetProperty(prop_x, ms::PropertyValue(2)).has_value());

  auto resolved_a = branch_context_->ResolveVertex(a_gid, ms::View::NEW);
  ASSERT_TRUE(resolved_a.has_value());
  auto a_val = resolved_a->GetProperty(prop_x, ms::View::NEW);
  ASSERT_TRUE(a_val.has_value());
  EXPECT_EQ(*a_val, ms::PropertyValue(2));

  // B was never COW'd -- ResolveVertex(B) must fall back to historical_'s value (9).
  auto resolved_b = branch_context_->ResolveVertex(b_gid, ms::View::NEW);
  ASSERT_TRUE(resolved_b.has_value());
  auto b_val = resolved_b->GetProperty(prop_x, ms::View::OLD);
  ASSERT_TRUE(b_val.has_value());
  EXPECT_EQ(*b_val, ms::PropertyValue(9));

  ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// The gid-ordered UNION scan: a COW'd+modified vertex (diff wins the tie), an untouched historical
// vertex, and a branch-native create (diff-only) must each be yielded EXACTLY once.
TEST_F(VersioningBranchEngineTest, VerticesUnionMergesDiffAndHistoricalByGid) {
  const auto prop_x = storage_->NameToProperty("x");
  ms::Gid a_gid, b_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    ASSERT_TRUE(a.SetProperty(prop_x, ms::PropertyValue(1)).has_value());
    auto b = acc->CreateVertex();
    b_gid = b.Gid();
    ASSERT_TRUE(b.SetProperty(prop_x, ms::PropertyValue(9)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_,
                                                 fork_ts,
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 BranchWalRoot(),
                                                 {});
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  // Single-slot collapse (mgp_vertex size budget, R6): CowVertex/ResolveVertex/Vertices no longer
  // take an accessor parameter -- they read it from this one per-query slot instead (safe: a
  // checked-out branch is exclusive single-writer, so at most one query/accessor is ever live).
  branch_context_->set_current_diff_txn(diff_acc.get());

  // COW A, change it to 2. `prop_x` (main's own id) is valid directly against the diff engine's
  // copy too -- diff_engine_ shares main's own NameIdMapper (one id space, no by-name translation).
  auto cowed_a = branch_context_->CowVertex(a_gid);
  ASSERT_TRUE(cowed_a.has_value());
  ASSERT_TRUE(cowed_a->SetProperty(prop_x, ms::PropertyValue(2)).has_value());

  // A branch-native create -- gid not present in historical_ at all.
  auto c = diff_acc->CreateVertex();
  const auto c_gid = c.Gid();
  ASSERT_TRUE(c.SetProperty(prop_x, ms::PropertyValue(7)).has_value());

  // Collect (gid -> value) from the union scan directly -- with ONE shared mapper, `prop_x` reads
  // correctly no matter which side (historical_ or the diff engine) actually yielded the vertex,
  // so this also exercises the tie-break (A must show 2, the diff engine's COW'd value, not
  // historical's stale 1) inline, not as a separate ResolveVertex follow-up check.
  std::map<uint64_t, int64_t> seen;
  for (auto v : branch_context_->Vertices(ms::View::NEW)) {
    auto val = v.GetProperty(prop_x, ms::View::NEW);
    ASSERT_TRUE(val.has_value());
    seen.emplace(v.Gid().AsUint(), val->ValueInt());
  }
  ASSERT_EQ(seen.size(), 3u) << "A, B, C must each be yielded exactly once";
  EXPECT_EQ(seen.at(a_gid.AsUint()), 2) << "A must yield the diff engine's COW'd copy, not historical's stale 1";
  EXPECT_EQ(seen.at(b_gid.AsUint()), 9) << "B must yield historical_'s untouched value";
  EXPECT_EQ(seen.at(c_gid.AsUint()), 7) << "C (branch-native) must be yielded too";

  ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// GID COLLISION FIX regression: a branch-native CreateVertex() (auto-gid path) issued BEFORE any
// COW must land in a gid range disjoint from anything historical_ could contain -- see
// kBranchNativeGidWatermark's doc-comment in branch_engine.cpp. This does not pin the exact
// watermark value (an implementation detail), only that it is comfortably clear of any gid a
// small, freshly-forked main could have used.
TEST_F(VersioningBranchEngineTest, BranchNativeCreateGidIsDisjointFromHistoricalRange) {
  {
    auto acc = storage_->Access(ms::WRITE);
    for (int i = 0; i < 5; ++i) (void)acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_,
                                                 fork_ts,
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 BranchWalRoot(),
                                                 {});
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  // Single-slot collapse (mgp_vertex size budget, R6): CowVertex/ResolveVertex/Vertices no longer
  // take an accessor parameter -- they read it from this one per-query slot instead (safe: a
  // checked-out branch is exclusive single-writer, so at most one query/accessor is ever live).
  branch_context_->set_current_diff_txn(diff_acc.get());
  auto native = diff_acc->CreateVertex();
  EXPECT_GE(native.Gid().AsUint(), 1ULL << 61) << "a branch-native create must not reuse a low (main-plausible) gid";
  ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// Defensive misuse guard (mirrors versioning_historical_access.cpp's
// RejectsUnpinnedForkTimestamp): a fork_ts that was never registered via RegisterForkPin must be
// reported as a BuildError, rather than build a bogus context.
TEST_F(VersioningBranchEngineTest, PropagatesHistoricalAccessErrorForUnpinnedTs) {
  {
    auto acc = storage_->Access(ms::WRITE);
    (void)acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Never pinned via RegisterForkPin -- must be refused rather than approximated.
  constexpr uint64_t kNeverPinnedForkTs = 1;
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_,
                                                 kNeverPinnedForkTs,
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 BranchWalRoot(),
                                                 {});
  ASSERT_FALSE(result.has_value());
  EXPECT_FALSE(result.error().message.empty());
}

// HIGH: an Enum property at the TOP LEVEL of a historical vertex must be rejected loudly by
// CowVertex (not BuildFromFork -- with lazy COW, BuildFromFork never even looks at this vertex
// unless/until something touches it), and must leave the diff engine with NO partial state.
TEST_F(VersioningBranchEngineTest, CowVertexRejectsTopLevelEnumProperty) {
  const auto prop_e = storage_->NameToProperty("e");
  ms::Gid a_gid;
  {
    auto unique_acc = storage_->UniqueAccess();
    ASSERT_TRUE(unique_acc->CreateEnum("Color", std::vector<std::string>{"Red", "Green"}).has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    auto enum_val = acc->GetEnumValue("Color", "Red");
    ASSERT_TRUE(enum_val.has_value());
    ASSERT_TRUE(a.SetProperty(prop_e, ms::PropertyValue(*enum_val)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_,
                                                 fork_ts,
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 BranchWalRoot(),
                                                 {});
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  // Single-slot collapse (mgp_vertex size budget, R6): CowVertex/ResolveVertex/Vertices no longer
  // take an accessor parameter -- they read it from this one per-query slot instead (safe: a
  // checked-out branch is exclusive single-writer, so at most one query/accessor is ever live).
  branch_context_->set_current_diff_txn(diff_acc.get());
  auto cowed = branch_context_->CowVertex(a_gid);
  ASSERT_FALSE(cowed.has_value());
  EXPECT_FALSE(cowed.error().message.empty());

  // No partial state left behind.
  EXPECT_FALSE(diff_acc->FindVertex(a_gid, ms::View::NEW).has_value());
  size_t diff_vertex_count = 0;
  for (auto v : diff_acc->Vertices(ms::View::NEW)) {
    (void)v;
    ++diff_vertex_count;
  }
  EXPECT_EQ(diff_vertex_count, 0u);

  ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// HIGH: the case a top-level-only check would MISS -- an Enum nested inside a List property.
// ContainsEnum must recurse into the list to catch it.
TEST_F(VersioningBranchEngineTest, CowVertexRejectsEnumNestedInList) {
  const auto prop_l = storage_->NameToProperty("l");
  ms::Gid a_gid;
  {
    auto unique_acc = storage_->UniqueAccess();
    ASSERT_TRUE(unique_acc->CreateEnum("Color", std::vector<std::string>{"Red", "Green"}).has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    auto enum_val = acc->GetEnumValue("Color", "Red");
    ASSERT_TRUE(enum_val.has_value());
    auto list_val =
        ms::PropertyValue(std::vector<ms::PropertyValue>{ms::PropertyValue(1), ms::PropertyValue(*enum_val)});
    ASSERT_TRUE(a.SetProperty(prop_l, list_val).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_,
                                                 fork_ts,
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 BranchWalRoot(),
                                                 {});
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  // Single-slot collapse (mgp_vertex size budget, R6): CowVertex/ResolveVertex/Vertices no longer
  // take an accessor parameter -- they read it from this one per-query slot instead (safe: a
  // checked-out branch is exclusive single-writer, so at most one query/accessor is ever live).
  branch_context_->set_current_diff_txn(diff_acc.get());
  auto cowed = branch_context_->CowVertex(a_gid);
  ASSERT_FALSE(cowed.has_value());
  EXPECT_FALSE(diff_acc->FindVertex(a_gid, ms::View::NEW).has_value());

  ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// perf/versioning-branch-read D1 -- delete-safety, View::OLD vs View::NEW, exercised directly
// against `query::EdgeAccessor::GetProperty` itself (see this file's own header addendum above for
// why this lives here instead of versioning_interpreter.cpp). This test does NOT link mg-query (this
// file intentionally stays storage+versioning-only, see the header addendum above), so rather than
// going through query::EdgeAccessor::GetProperty itself, it replicates that function's documented
// gate VERBATIM (query/edge_accessor.cpp's own doc-comment) against `fork_edge` -- the UNTOUCHED FORK
// copy (never itself COW'd in this test) -- using only the storage-level primitives that gate is
// built from: `storage::EdgeAccessor::MaybeBranchedHint()`, `BranchContext::FindDiffEdge`, and
// `BranchContext::IsEdgeTombstoned`. This is the exact shape a second, independent binding to the
// same logical edge would have if it never itself went through CowEdgeIfNeeded (e.g. a fresh
// traversal in a later statement). The query-layer NEW-view half of the same contract, through the
// REAL query::EdgeAccessor code, is covered end-to-end via the interpreter in
// versioning_interpreter.cpp's BranchEdgeBitFastPathDeleteAfterModifyStillRaisesOnNewView.
//
// After a DIFFERENT statement COWs, SETs (w=2) and deletes the edge on the branch:
//   - View::NEW: `fork_edge.MaybeBranchedHint()` is true (CowEdge set main's bit) -> FindDiffEdge(NEW)
//     MISSES (verified empirically: `Storage::Accessor::FindEdge` resolves purely from the FROM
//     vertex's CURRENT physical adjacency list, which the delete already removed the edge from --
//     unlike vertex-by-gid lookups, this is not reversible by view at all) -> falls through to the
//     existing S2 tombstone guard -> DELETED_OBJECT. This is the load-bearing assertion: it proves
//     D1's branched()-bit gate defers to (does not bypass) the tombstone guard.
//   - View::OLD: FindDiffEdge(OLD) MISSES for the identical reason (the adjacency-based lookup does
//     not depend on the requested view), and the View::OLD tombstone-guard condition
//     (`view == View::NEW && ...`) is false, so the gate falls through ALL THE WAY to
//     `fork_edge.GetProperty(key, view)` -- i.e. it reads the FORK edge's OWN, ORIGINAL value (1),
//     not the diff engine's last-committed pre-delete value (2). This degrade-to-fork-value behavior
//     is the PRE-EXISTING S2 contract (the FindDiffEdge-miss-at-OLD-view path is identical to what an
//     unrelated, never-branched edge's OLD-view read already does) -- D1 does not change it, it only
//     adds the `!MaybeBranchedHint()` gate ABOVE this pre-existing fallback chain.
TEST_F(VersioningBranchEngineTest, EdgeBranchedBitDeleteViewNewRaisesViewOldFallsBackToForkValue) {
  const auto prop_w = storage_->NameToProperty("w");
  const auto edge_type = storage_->NameToEdgeType("E");
  ms::Gid s_gid, edge_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto s = acc->CreateVertex();
    s_gid = s.Gid();
    auto a = acc->CreateVertex();
    auto edge = acc->CreateEdge(&s, &a, edge_type);
    ASSERT_TRUE(edge.has_value());
    edge_gid = edge->Gid();
    ASSERT_TRUE(edge->SetProperty(prop_w, ms::PropertyValue(1)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_,
                                                 fork_ts,
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 BranchWalRoot(),
                                                 {});
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  // A SEPARATE, never-touched binding to the SAME fork edge -- stays main-resident throughout this
  // test, exactly like a fresh traversal that never itself called CowEdgeIfNeeded.
  auto hist_s = branch_context_->historical().FindVertex(s_gid, ms::View::OLD);
  ASSERT_TRUE(hist_s.has_value());
  auto out_res = hist_s->OutEdges(ms::View::OLD);
  ASSERT_TRUE(out_res.has_value());
  ASSERT_EQ(out_res->edges.size(), 1u);
  const ms::EdgeAccessor fork_edge = out_res->edges[0];

  // A DIFFERENT statement: COW the fork edge, SET w=2, commit -- mirrors an earlier branch write.
  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    branch_context_->set_current_diff_txn(diff_acc.get());
    auto cowed = branch_context_->CowEdge(fork_edge);
    ASSERT_TRUE(cowed.has_value());
    ASSERT_TRUE(cowed->SetProperty(prop_w, ms::PropertyValue(2)).has_value());
    ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  EXPECT_TRUE(fork_edge.MaybeBranchedHint())
      << "sanity: CowEdge must have set main's own Edge::branched() bit -- `fork_edge` IS this same "
         "physical object (historical_ aliasing main directly)";

  // A LATER statement deletes it and tombstones it.
  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    branch_context_->set_current_diff_txn(diff_acc.get());
    auto diff_edge = branch_context_->FindDiffEdge(edge_gid, ms::View::NEW);
    ASSERT_TRUE(diff_edge.has_value()) << "the edge committed by the previous statement must already be diff-resident";
    ASSERT_TRUE(diff_acc->DeleteEdge(&*diff_edge).has_value());
    branch_context_->TombstoneEdge(edge_gid);
    ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // THE ASSERTIONS: replicate query::EdgeAccessor::GetProperty's D1 gate VERBATIM (see this test's
  // own header comment) against `fork_edge` -- still bound to the untouched fork copy -- at both
  // views, via a FRESH diff-engine transaction (mirrors a fresh statement reading after the delete
  // committed).
  auto read_acc = branch_context_->diff_engine().Access(ms::READ);
  branch_context_->set_current_diff_txn(read_acc.get());

  auto d1_gate_get_property = [&](ms::View view) -> ms::Result<ms::PropertyValue> {
    if (fork_edge.storage_ == &branch_context_->diff_engine()) return fork_edge.GetProperty(prop_w, view);
    if (!fork_edge.MaybeBranchedHint()) return fork_edge.GetProperty(prop_w, view);
    if (auto diff_edge = branch_context_->FindDiffEdge(edge_gid, view)) return diff_edge->GetProperty(prop_w, view);
    if (view == ms::View::NEW && branch_context_->IsEdgeTombstoned(edge_gid)) {
      return std::unexpected{ms::Error::DELETED_OBJECT};
    }
    return fork_edge.GetProperty(prop_w, view);
  };

  auto new_view_result = d1_gate_get_property(ms::View::NEW);
  ASSERT_FALSE(new_view_result.has_value());
  EXPECT_EQ(new_view_result.error(), ms::Error::DELETED_OBJECT)
      << "View::NEW must raise DELETED_OBJECT via the S2 tombstone guard -- D1's branched()-bit gate "
         "must defer to it, not bypass it";

  auto old_view_result = d1_gate_get_property(ms::View::OLD);
  ASSERT_TRUE(old_view_result.has_value())
      << "View::OLD must NOT raise -- the tombstone guard is gated on view == View::NEW only";
  EXPECT_EQ(*old_view_result, ms::PropertyValue(1))
      << "View::OLD falls through past the FindDiffEdge miss straight to fork_edge.GetProperty(view), "
         "i.e. the FORK edge's own original value -- this is the pre-existing S2 fallback chain, "
         "unmodified by D1";
}

// Fix H1 (abort-safety, 2026-07-18): a DELETE that then ABORTS must not permanently hide the
// deleted vertex for the rest of the checkout session. Before this fix, `TombstoneVertex` inserted
// directly into the permanent `tombstoned_vertices_` set at Pull-time (before commit/abort was
// known) -- an abort never undid that, so the vertex stayed hidden forever even though its delete
// was rolled back. Replicates the exact production sequence (query::DbAccessor::RemoveVertex,
// db_accessor.hpp: CowVertex -> accessor_->DeleteVertex -> TombstoneVertex) directly against the
// storage-level primitives this file already exercises elsewhere (no mg-query link here, same
// rationale as the D1 edge test above), then simulates the interpreter's abort path
// (`CleanupDBTransaction`, interpreter.cpp) by calling `Abort()` on the transaction instead of
// committing it, followed by `DiscardPendingTombstones()`.
TEST_F(VersioningBranchEngineTest, TombstonedVertexIsHiddenWithinTxnButAbortRestoresVisibility) {
  const auto prop_x = storage_->NameToProperty("x");
  ms::Gid a_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    ASSERT_TRUE(a.SetProperty(prop_x, ms::PropertyValue(1)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_,
                                                 fork_ts,
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 BranchWalRoot(),
                                                 {});
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  // Statement 1 (aborted): DELETE the fork vertex on the branch, then roll the transaction back
  // WITHOUT committing -- mirrors a query that deletes and then hits a runtime error/explicit
  // ROLLBACK before the interpreter's own commit boundary.
  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    branch_context_->set_current_diff_txn(diff_acc.get());

    ASSERT_FALSE(branch_context_->IsVertexTombstoned(a_gid));

    auto cowed = branch_context_->CowVertex(a_gid);
    ASSERT_TRUE(cowed.has_value());
    auto delete_res = diff_acc->DeleteVertex(&*cowed);
    ASSERT_TRUE(delete_res.has_value() && delete_res->has_value());
    branch_context_->TombstoneVertex(a_gid);

    // Same-transaction visibility: the just-deleted vertex must stay hidden to THIS transaction
    // too (the pending set, not just the committed one, gates ResolveVertex/IsVertexTombstoned).
    EXPECT_TRUE(branch_context_->IsVertexTombstoned(a_gid));
    EXPECT_FALSE(branch_context_->ResolveVertex(a_gid, ms::View::NEW).has_value());

    // Abort instead of commit -- CaptureCommitIfBranch (and therefore the pending->committed
    // promotion) is NEVER reached on this path, mirroring Interpreter::Commit only running the
    // capture hook on the success path.
    diff_acc->Abort();

    // Mirrors the interpreter's abort path (CleanupDBTransaction, interpreter.cpp): discard this
    // transaction's own pending tombstones so the rolled-back delete does not leak.
    branch_context_->DiscardPendingTombstones();
  }

  EXPECT_FALSE(branch_context_->IsVertexTombstoned(a_gid))
      << "an aborted delete's tombstone must be fully discarded, not merely left un-promoted";

  // Statement 2 (fresh transaction, as if nothing happened): the vertex must resolve again, still
  // carrying its pre-delete value -- it was never actually removed from historical_, and the diff
  // engine never durably recorded the delete either.
  {
    auto diff_acc2 = branch_context_->diff_engine().Access(ms::READ);
    branch_context_->set_current_diff_txn(diff_acc2.get());

    auto resolved = branch_context_->ResolveVertex(a_gid, ms::View::NEW);
    ASSERT_TRUE(resolved.has_value()) << "the vertex must be visible again after the delete was aborted";
    auto x_val = resolved->GetProperty(prop_x, ms::View::OLD);
    ASSERT_TRUE(x_val.has_value());
    EXPECT_EQ(*x_val, ms::PropertyValue(1));

    size_t union_count = 0;
    bool found_in_union = false;
    for (auto v : branch_context_->Vertices(ms::View::NEW)) {
      ++union_count;
      if (v.Gid() == a_gid) found_in_union = true;
    }
    EXPECT_TRUE(found_in_union) << "the gid-ordered union scan must also see the vertex again post-abort";
  }
}

// Fix H1 companion: a delete that DOES commit must become a PERMANENT tombstone -- surviving a
// subsequent (irrelevant) `DiscardPendingTombstones()` call, exactly as the interpreter's commit
// path (CaptureCommitIfBranch promotes, THEN the caller's own commit runs) followed later by
// CleanupDBTransaction's unconditional-on-every-transaction-end discard call would produce.
TEST_F(VersioningBranchEngineTest, TombstonedVertexSurvivesAfterCommitPromotesIt) {
  ms::Gid a_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_,
                                                 fork_ts,
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 memgraph::tests::MakeMainCommitArgs(),
                                                 BranchWalRoot(),
                                                 {});
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    branch_context_->set_current_diff_txn(diff_acc.get());

    auto cowed = branch_context_->CowVertex(a_gid);
    ASSERT_TRUE(cowed.has_value());
    auto delete_res = diff_acc->DeleteVertex(&*cowed);
    ASSERT_TRUE(delete_res.has_value() && delete_res->has_value());
    branch_context_->TombstoneVertex(a_gid);

    // Mirrors Interpreter::Commit's capture hook: CaptureCommitIfBranch runs (promoting the
    // pending tombstone into the permanent set) strictly BEFORE the transaction's own commit call.
    auto outcome = branch_context_->CaptureCommitIfBranch(*diff_acc->GetTransaction(), /*max_changelog_length=*/1000);
    ASSERT_TRUE(outcome.attempted);
    ASSERT_FALSE(outcome.cap_exceeded);

    ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

    // CleanupDBTransaction discards pending tombstones unconditionally at the end of EVERY
    // transaction, commit or abort alike -- must be a no-op here since promotion already emptied
    // the pending set above.
    branch_context_->DiscardPendingTombstones();
  }

  EXPECT_TRUE(branch_context_->IsVertexTombstoned(a_gid)) << "a committed delete's tombstone must remain permanent";

  auto diff_acc2 = branch_context_->diff_engine().Access(ms::READ);
  branch_context_->set_current_diff_txn(diff_acc2.get());
  EXPECT_FALSE(branch_context_->ResolveVertex(a_gid, ms::View::NEW).has_value())
      << "the committed delete must stay hidden in a later, unrelated transaction too";
}

}  // namespace

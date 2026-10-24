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
// FindDiffEdge/IsForkEdgeHidden View::OLD-vs-View::NEW split the D1 edge-bit value-read gate
// (query::EdgeAccessor::Properties/GetProperty/GetPropertySize, edge_accessor.cpp) depends on for
// its delete-safety guarantee, exercised directly against query::EdgeAccessor::GetProperty itself.
// It is here rather than in versioning_interpreter.cpp because constructing an independent, never-
// COW'd binding to an already-branch-deleted edge (to reach the delete-guard branch rather than
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

#include <chrono>
#include <thread>

#include <gtest/gtest.h>
#include <prometheus/gauge.h>

#include "memory/db_arena.hpp"
#include "metrics/metric_handles.hpp"
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
    branch_context_.reset();
    if (fork_ts_.has_value()) mem_storage_->ReleaseForkPin(*fork_ts_);
    storage_.reset();
  }

  // Registers a fork pin at the CURRENT tip and remembers it for TearDown's release.
  uint64_t ForkNow() {
    fork_ts_ = mem_storage_->RegisterForkPin();
    return *fork_ts_;
  }

  // Publishes `diff_acc` as the context's current transaction and opens the per-transaction historical accessor.
  void Begin(ms::Storage::Accessor *diff_acc) {
    ASSERT_TRUE(branch_context_->BeginTransaction(diff_acc).has_value());
  }

  std::unique_ptr<ms::Storage> storage_;
  ms::InMemoryStorage *mem_storage_{};
  std::optional<uint64_t> fork_ts_;
  std::unique_ptr<mv::BranchContext> branch_context_;
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
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
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

  // The per-transaction historical accessor sees main's fork-state graph directly (no copy needed to read it).
  Begin(diff_acc.get());
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
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  // Single-slot collapse (mgp_vertex size budget, R6): CowVertex/ResolveVertex/Vertices no longer
  // take an accessor parameter -- they read it from this one per-query slot instead (safe: a
  // checked-out branch is exclusive single-writer, so at most one query/accessor is ever live).
  Begin(diff_acc.get());

  auto cowed = branch_context_->CowVertex(a_gid);
  ASSERT_TRUE(cowed.has_value());
  EXPECT_EQ(cowed->Gid(), a_gid);

  // The COW'd vertex is an EMPTY stub: nothing is copied, the fork state stays in historical_ and
  // is overlaid on read by the query layer (query::VertexAccessor).
  EXPECT_FALSE(cowed->HasLabel(label_a, ms::View::NEW).value_or(true));
  auto x_val = cowed->GetProperty(prop_x, ms::View::NEW);
  ASSERT_TRUE(x_val.has_value());
  EXPECT_TRUE(x_val->IsNull());

  // The fork vertex (main@fork_ts) still carries the original label/property under main's own ids.
  auto fork_a = branch_context_->FindForkVertex(a_gid);
  ASSERT_TRUE(fork_a.has_value());
  EXPECT_TRUE(fork_a->HasLabel(label_a, ms::View::OLD).value_or(false));
  auto fork_x = fork_a->GetProperty(prop_x, ms::View::OLD);
  ASSERT_TRUE(fork_x.has_value());
  EXPECT_EQ(*fork_x, ms::PropertyValue(1));

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
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  // Single-slot collapse (mgp_vertex size budget, R6): CowVertex/ResolveVertex/Vertices no longer
  // take an accessor parameter -- they read it from this one per-query slot instead (safe: a
  // checked-out branch is exclusive single-writer, so at most one query/accessor is ever live).
  Begin(diff_acc.get());

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
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  // Single-slot collapse (mgp_vertex size budget, R6): CowVertex/ResolveVertex/Vertices no longer
  // take an accessor parameter -- they read it from this one per-query slot instead (safe: a
  // checked-out branch is exclusive single-writer, so at most one query/accessor is ever live).
  Begin(diff_acc.get());

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
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  // Single-slot collapse (mgp_vertex size budget, R6): CowVertex/ResolveVertex/Vertices no longer
  // take an accessor parameter -- they read it from this one per-query slot instead (safe: a
  // checked-out branch is exclusive single-writer, so at most one query/accessor is ever live).
  Begin(diff_acc.get());

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
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  // Single-slot collapse (mgp_vertex size budget, R6): CowVertex/ResolveVertex/Vertices no longer
  // take an accessor parameter -- they read it from this one per-query slot instead (safe: a
  // checked-out branch is exclusive single-writer, so at most one query/accessor is ever live).
  Begin(diff_acc.get());
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
  auto result =
      mv::BranchContext::BuildFromFork(*mem_storage_, kNeverPinnedForkTs, memgraph::tests::MakeMainCommitArgs());
  ASSERT_FALSE(result.has_value());
  EXPECT_FALSE(result.error().message.empty());
}

// HIGH: an Enum property at the TOP LEVEL of a historical vertex must be rejected loudly by
// CowVertex (not BuildFromFork -- with lazy COW, BuildFromFork never even looks at this vertex
// unless/until something touches it), and must leave the diff engine with NO partial state.
TEST_F(VersioningBranchEngineTest, CowVertexStubDoesNotCopyTopLevelEnumProperty) {
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
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  // Single-slot collapse (mgp_vertex size budget, R6): CowVertex/ResolveVertex/Vertices no longer
  // take an accessor parameter -- they read it from this one per-query slot instead (safe: a
  // checked-out branch is exclusive single-writer, so at most one query/accessor is ever live).
  Begin(diff_acc.get());
  // Fork properties are not copied into the stub, so an enum-typed fork property (whose enum-TYPE id
  // is not shared with the diff engine) can no longer make the COW fail.
  auto cowed = branch_context_->CowVertex(a_gid);
  ASSERT_TRUE(cowed.has_value());
  auto stub_e = cowed->GetProperty(prop_e, ms::View::NEW);
  ASSERT_TRUE(stub_e.has_value());
  EXPECT_TRUE(stub_e->IsNull());
  auto fork_e = branch_context_->FindForkVertex(a_gid)->GetProperty(prop_e, ms::View::OLD);
  ASSERT_TRUE(fork_e.has_value());
  EXPECT_TRUE(fork_e->IsEnum());

  ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// HIGH: the case a top-level-only check would MISS -- an Enum nested inside a List property.
// ContainsEnum must recurse into the list to catch it.
TEST_F(VersioningBranchEngineTest, CowVertexStubDoesNotCopyEnumNestedInList) {
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
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  // Single-slot collapse (mgp_vertex size budget, R6): CowVertex/ResolveVertex/Vertices no longer
  // take an accessor parameter -- they read it from this one per-query slot instead (safe: a
  // checked-out branch is exclusive single-writer, so at most one query/accessor is ever live).
  Begin(diff_acc.get());
  // See CowVertexStubDoesNotCopyTopLevelEnumProperty: the nested enum stays in the fork vertex.
  auto cowed = branch_context_->CowVertex(a_gid);
  ASSERT_TRUE(cowed.has_value());
  auto stub_l = cowed->GetProperty(prop_l, ms::View::NEW);
  ASSERT_TRUE(stub_l.has_value());
  EXPECT_TRUE(stub_l->IsNull());
  auto fork_l = branch_context_->FindForkVertex(a_gid)->GetProperty(prop_l, ms::View::OLD);
  ASSERT_TRUE(fork_l.has_value());
  EXPECT_TRUE(fork_l->IsList());

  ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// perf/versioning-branch-read D1 -- delete-safety, View::OLD vs View::NEW, exercised directly
// against the gate `query::EdgeAccessor::GetProperty` applies (see this file's own header addendum above for
// why this lives here instead of versioning_interpreter.cpp). This test does NOT link mg-query (this
// file intentionally stays storage+versioning-only, see the header addendum above), so rather than
// going through query::EdgeAccessor::GetProperty itself, it replicates that function's documented
// gate VERBATIM (query/edge_accessor.cpp) against `fork_edge` -- the UNTOUCHED FORK
// copy (never itself COW'd in this test) -- using only the storage-level primitives that gate is
// built from: `storage::EdgeAccessor::MaybeBranchedHint()`, `BranchContext::FindDiffEdge`, and
// `BranchContext::IsForkEdgeHidden`. This is the exact shape a second, independent binding to the
// same logical edge would have if it never itself went through CowEdgeIfNeeded (e.g. a fresh
// traversal in a later statement).
//
// After a DIFFERENT statement COWs, SETs (w=2) and deletes the edge on the branch (DeleteForkEdge: the diff
// copy is deleted, the gid is listed on both endpoint stubs):
//   - inside the deleting command, View::NEW is hidden -> DELETED_OBJECT, while View::OLD still reads the fork
//     edge's own original value (1): the list write belongs to the current command, as on main.
//   - in a later transaction both views see the committed delete -> DELETED_OBJECT.
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
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  // A SEPARATE, never-touched binding to the SAME fork edge -- stays main-resident throughout this
  // test, exactly like a fresh traversal that never itself called CowEdgeIfNeeded.
  // Held for the whole test: the per-transaction historical accessor inside the context is re-opened by every Begin().
  auto independent_hist = mem_storage_->HistoricalAccess(fork_ts);
  ASSERT_TRUE(independent_hist.has_value());
  auto hist_s = (*independent_hist)->FindVertex(s_gid, ms::View::OLD);
  ASSERT_TRUE(hist_s.has_value());
  auto out_res = hist_s->OutEdges(ms::View::OLD);
  ASSERT_TRUE(out_res.has_value());
  ASSERT_EQ(out_res->edges.size(), 1u);
  const ms::EdgeAccessor fork_edge = out_res->edges[0];

  // A DIFFERENT statement: COW the fork edge, SET w=2, commit -- mirrors an earlier branch write.
  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    Begin(diff_acc.get());
    auto cowed = branch_context_->CowEdge(fork_edge);
    ASSERT_TRUE(cowed.has_value());
    ASSERT_TRUE(cowed->SetProperty(prop_w, ms::PropertyValue(2)).has_value());
    ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  EXPECT_TRUE(fork_edge.MaybeBranchedHint())
      << "sanity: CowEdge must have set main's own Edge::branched() bit -- `fork_edge` IS this same "
         "physical object (historical_ aliasing main directly)";

  auto d1_gate_get_property = [&](ms::View view) -> ms::Result<ms::PropertyValue> {
    if (fork_edge.storage_ == &branch_context_->diff_engine()) return fork_edge.GetProperty(prop_w, view);
    if (!fork_edge.MaybeBranchedHint()) return fork_edge.GetProperty(prop_w, view);
    if (auto diff_edge = branch_context_->FindDiffEdge(edge_gid, view)) return diff_edge->GetProperty(prop_w, view);
    if (branch_context_->IsForkEdgeHidden(fork_edge, view)) return std::unexpected{ms::Error::DELETED_OBJECT};
    return fork_edge.GetProperty(prop_w, view);
  };

  // A LATER statement deletes it.
  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    Begin(diff_acc.get());
    auto diff_edge = branch_context_->FindDiffEdge(edge_gid, ms::View::NEW);
    ASSERT_TRUE(diff_edge.has_value()) << "the edge committed by the previous statement must already be diff-resident";
    auto deleted = branch_context_->DeleteForkEdge(fork_edge);
    ASSERT_TRUE(deleted.has_value());
    ASSERT_TRUE(*deleted);

    auto new_view_result = d1_gate_get_property(ms::View::NEW);
    ASSERT_FALSE(new_view_result.has_value());
    EXPECT_EQ(new_view_result.error(), ms::Error::DELETED_OBJECT)
        << "View::NEW must raise DELETED_OBJECT -- D1's branched()-bit gate must defer to the delete guard";

    auto old_view_result = d1_gate_get_property(ms::View::OLD);
    ASSERT_TRUE(old_view_result.has_value())
        << "View::OLD must NOT raise inside the deleting command: its own marker writes are invisible to it";
    EXPECT_EQ(*old_view_result, ms::PropertyValue(1))
        << "View::OLD falls through past the FindDiffEdge miss to the fork edge's own original value";

    ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // A fresh transaction sees the committed delete at both views.
  auto read_acc = branch_context_->diff_engine().Access(ms::READ);
  Begin(read_acc.get());
  for (auto view : {ms::View::NEW, ms::View::OLD}) {
    auto result = d1_gate_get_property(view);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ms::Error::DELETED_OBJECT);
  }
}

// A fork-vertex DELETE that then ABORTS must leave the vertex fully visible: the delete marker is an ordinary
// diff-engine write, so rolling the transaction back removes it (no side state to discard). Replicates the
// production sequence (query::DbAccessor::BranchDetachDelete -> BranchContext::DeleteForkVertex) against the
// storage-level primitives (no mg-query link here, same rationale as the D1 edge test above).
TEST_F(VersioningBranchEngineTest, DeletedVertexIsHiddenWithinTxnButAbortRestoresVisibility) {
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
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  // Statement 1 (aborted): DELETE the fork vertex on the branch, then roll the transaction back.
  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    Begin(diff_acc.get());

    ASSERT_FALSE(branch_context_->IsVertexDeleted(a_gid, ms::View::NEW));

    auto deleted = branch_context_->DeleteForkVertex(a_gid);
    ASSERT_TRUE(deleted.has_value() && deleted->has_value());

    // The transaction sees its own delete at View::NEW, not at View::OLD (same command).
    EXPECT_TRUE(branch_context_->IsVertexDeleted(a_gid, ms::View::NEW));
    EXPECT_FALSE(branch_context_->IsVertexDeleted(a_gid, ms::View::OLD));
    EXPECT_FALSE(branch_context_->ResolveVertex(a_gid, ms::View::NEW).has_value());
    EXPECT_TRUE(branch_context_->ResolveVertex(a_gid, ms::View::OLD).has_value());

    // A repeated delete is a no-op.
    auto again = branch_context_->DeleteForkVertex(a_gid);
    ASSERT_TRUE(again.has_value());
    EXPECT_FALSE(again->has_value());

    diff_acc->Abort();
  }

  // Statement 2 (fresh transaction, as if nothing happened).
  {
    auto diff_acc2 = branch_context_->diff_engine().Access(ms::READ);
    Begin(diff_acc2.get());

    EXPECT_FALSE(branch_context_->IsVertexDeleted(a_gid, ms::View::NEW))
        << "an aborted delete must leave no marker behind";
    auto resolved = branch_context_->ResolveVertex(a_gid, ms::View::NEW);
    ASSERT_TRUE(resolved.has_value()) << "the vertex must be visible again after the delete was aborted";
    auto x_val = branch_context_->FindForkVertex(a_gid)->GetProperty(prop_x, ms::View::OLD);
    ASSERT_TRUE(x_val.has_value());
    EXPECT_EQ(*x_val, ms::PropertyValue(1));

    bool found_in_union = false;
    for (auto v : branch_context_->Vertices(ms::View::NEW)) {
      if (v.Gid() == a_gid) found_in_union = true;
    }
    EXPECT_TRUE(found_in_union) << "the gid-ordered union scan must also see the vertex again post-abort";
  }
}

// A fork-vertex DELETE that COMMITS stays hidden to later transactions, from point lookups and the union scan.
TEST_F(VersioningBranchEngineTest, DeletedVertexSurvivesCommit) {
  ms::Gid a_gid;
  ms::Gid b_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    a_gid = acc->CreateVertex().Gid();
    b_gid = acc->CreateVertex().Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    Begin(diff_acc.get());

    auto deleted = branch_context_->DeleteForkVertex(a_gid);
    ASSERT_TRUE(deleted.has_value() && deleted->has_value());

    ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto diff_acc2 = branch_context_->diff_engine().Access(ms::READ);
  Begin(diff_acc2.get());
  EXPECT_TRUE(branch_context_->IsVertexDeleted(a_gid, ms::View::NEW))
      << "a committed delete must stay visible to a later transaction";
  EXPECT_FALSE(branch_context_->ResolveVertex(a_gid, ms::View::NEW).has_value());
  EXPECT_TRUE(branch_context_->ResolveVertex(b_gid, ms::View::NEW).has_value());

  std::vector<ms::Gid> scanned;
  for (auto v : branch_context_->Vertices(ms::View::NEW)) scanned.push_back(v.Gid());
  EXPECT_EQ(scanned, std::vector<ms::Gid>{b_gid}) << "the union scan must skip the deleted fork vertex";
}

// CowEdge materializes an EMPTY, delta-less stub (no fork property is copied), linked between the endpoint stubs. It
// is idempotent, adds no second edge to the branch's adjacency (the fork edge is what ResolveEdges emits), and a
// rolled-back write to it leaves the stub in place so the next write just reuses it.
TEST_F(VersioningBranchEngineTest, CowEdgeMaterializesEmptyStubThatSurvivesRollbackAndIsIdempotent) {
  const auto prop_w = storage_->NameToProperty("w");
  const auto prop_x = storage_->NameToProperty("x");
  const auto edge_type = storage_->NameToEdgeType("E");
  ms::Gid s_gid, a_gid, edge_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto s = acc->CreateVertex();
    s_gid = s.Gid();
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    auto edge = acc->CreateEdge(&s, &a, edge_type);
    ASSERT_TRUE(edge.has_value());
    edge_gid = edge->Gid();
    ASSERT_TRUE(edge->SetProperty(prop_w, ms::PropertyValue(1)).has_value());
    ASSERT_TRUE(edge->SetProperty(prop_x, ms::PropertyValue(2)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  auto fork_edge_of = [&]() {
    auto hist_s = branch_context_->historical().FindVertex(s_gid, ms::View::OLD);
    EXPECT_TRUE(hist_s.has_value());
    auto out = hist_s->OutEdges(ms::View::OLD);
    EXPECT_TRUE(out.has_value());
    EXPECT_EQ(out->edges.size(), 1u);
    return out->edges[0];
  };

  // A write that rolls back.
  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    Begin(diff_acc.get());
    auto stub = branch_context_->CowEdge(fork_edge_of());
    ASSERT_TRUE(stub.has_value());
    EXPECT_EQ(stub->Gid(), edge_gid);
    auto props = stub->Properties(ms::View::NEW);
    ASSERT_TRUE(props.has_value());
    EXPECT_TRUE(props->empty()) << "a stub copies no fork property";
    ASSERT_TRUE(stub->SetProperty(prop_w, ms::PropertyValue(2)).has_value());

    auto again = branch_context_->CowEdge(fork_edge_of());
    ASSERT_TRUE(again.has_value()) << "CowEdge is idempotent";
    EXPECT_EQ(again->edge_.ptr, stub->edge_.ptr);
    diff_acc->Abort();
  }

  // The stub is still there, still empty, and writable again.
  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    Begin(diff_acc.get());
    auto found = branch_context_->FindDiffEdge(edge_gid, ms::View::NEW);
    ASSERT_TRUE(found.has_value()) << "a rolled-back write must not take the stub with it";
    EXPECT_TRUE(found->Properties(ms::View::NEW)->empty());
    auto stub = branch_context_->CowEdge(fork_edge_of());
    ASSERT_TRUE(stub.has_value());
    ASSERT_TRUE(stub->SetProperty(prop_w, ms::PropertyValue(3)).has_value());

    // Fork identity wins in adjacency: exactly one edge, the historical one; the stub is not emitted.
    auto resolved = branch_context_->ResolveEdges(s_gid, ms::EdgeDirection::OUT, ms::View::NEW);
    ASSERT_EQ(resolved.size(), 1u);
    EXPECT_NE(resolved[0].storage_, &branch_context_->diff_engine());
    auto resolved_in = branch_context_->ResolveEdges(a_gid, ms::EdgeDirection::IN, ms::View::NEW);
    ASSERT_EQ(resolved_in.size(), 1u);
    EXPECT_NE(resolved_in[0].storage_, &branch_context_->diff_engine());

    EXPECT_TRUE(branch_context_->StubEdgeTouches(*stub, prop_w, ms::View::NEW));
    EXPECT_FALSE(branch_context_->StubEdgeTouches(*stub, prop_x, ms::View::NEW));
    ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Both fork identity and the stub resolve the same fork edge.
  auto read_acc = branch_context_->diff_engine().Access(ms::READ);
  Begin(read_acc.get());
  auto stub = branch_context_->FindDiffEdge(edge_gid, ms::View::NEW);
  ASSERT_TRUE(stub.has_value());
  EXPECT_TRUE(branch_context_->IsStub(*stub));
  auto fork = branch_context_->FindForkEdge(*stub);
  ASSERT_TRUE(fork.has_value());
  EXPECT_EQ(fork->Gid(), edge_gid);
  EXPECT_EQ(*fork->GetProperty(prop_w, ms::View::OLD), ms::PropertyValue(1));
  EXPECT_EQ(*stub->GetProperty(prop_w, ms::View::NEW), ms::PropertyValue(3));
}

// Graph Versioning v2 (S3-c): the diff engine runs its own GC with main's GC config and charges its memory to main's
// tenant (arena pool, embedding tracker, unreleased-delta gauge). Main here is built the way Database builds it:
// with a per-DB arena pool, and the calling thread inside that pool's DbArenaScope.
class VersioningBranchGcTest : public testing::Test {
 protected:
  virtual ms::Config MainConfig() const {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    return config;
  }

  void SetUp() override {
    pool_ = std::make_unique<memgraph::memory::ArenaPool>(&db_tracker_);
    scope_ = std::make_unique<memgraph::memory::DbArenaScope>(pool_.get());
    memgraph::metrics::DatabaseMetricHandles handles{};
    handles.unreleased_delta_objects = {&delta_gauge_};
    storage_ = std::make_unique<ms::InMemoryStorage>(MainConfig(),
                                                     std::nullopt,
                                                     std::make_unique<ms::PlanInvalidatorDefault>(),
                                                     handles,
                                                     nullptr,
                                                     pool_.get(),
                                                     &embedding_tracker_);
    label_ = storage_->NameToLabel("L");
    prop_x_ = storage_->NameToProperty("x");
    edge_type_ = storage_->NameToEdgeType("E");
    {
      auto acc = storage_->Access(ms::WRITE);
      auto v = acc->CreateVertex();
      v_gid_ = v.Gid();
      EXPECT_TRUE(v.AddLabel(label_).has_value());
      EXPECT_TRUE(v.SetProperty(prop_x_, ms::PropertyValue(0)).has_value());
      auto a = acc->CreateVertex();
      a_gid_ = a.Gid();
      auto e = acc->CreateEdge(&v, &a, edge_type_);
      EXPECT_TRUE(e.has_value());
      edge_gid_ = e->Gid();
      EXPECT_TRUE(e->SetProperty(prop_x_, ms::PropertyValue(1)).has_value());
      EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    fork_ts_ = storage_->RegisterForkPin();
    auto built = mv::BranchContext::BuildFromFork(*storage_, fork_ts_, memgraph::tests::MakeMainCommitArgs());
    ASSERT_TRUE(built.has_value());
    ctx_ = std::move(*built);
    Collect();
    baseline_ = Deltas();
  }

  void TearDown() override {
    ctx_.reset();
    storage_->ReleaseForkPin(fork_ts_);
    storage_.reset();
    scope_.reset();
    pool_.reset();
  }

  ms::InMemoryStorage &Diff() { return ctx_->diff_engine(); }

  int64_t Deltas() const { return static_cast<int64_t>(delta_gauge_.Value()); }

  // Explicit GC passes on the diff engine (unlinking and freeing can take separate passes).
  void Collect() {
    for (int i = 0; i < 3; ++i) Diff().FreeMemory({}, false);
  }

  void CommitStubWrite(int64_t value) {
    auto acc = Diff().Access(ms::WRITE);
    auto v = acc->FindVertex(v_gid_, ms::View::NEW);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->SetProperty(prop_x_, ms::PropertyValue(value)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  void MaterializeVertexStub() {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    ASSERT_TRUE(ctx_->CowVertex(v_gid_).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx_->EndTransaction();
  }

  prometheus::Gauge delta_gauge_;
  memgraph::utils::MemoryTracker db_tracker_;
  memgraph::utils::MemoryTracker embedding_tracker_;
  std::unique_ptr<memgraph::memory::ArenaPool> pool_;
  std::unique_ptr<memgraph::memory::DbArenaScope> scope_;
  std::unique_ptr<ms::InMemoryStorage> storage_;
  std::unique_ptr<mv::BranchContext> ctx_;
  uint64_t fork_ts_{};
  int64_t baseline_{};
  ms::LabelId label_;
  ms::PropertyId prop_x_;
  ms::EdgeTypeId edge_type_;
  ms::Gid v_gid_, a_gid_, edge_gid_;
};

class VersioningBranchPeriodicGcTest : public VersioningBranchGcTest {
 protected:
  ms::Config MainConfig() const override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::PERIODIC, .interval = std::chrono::milliseconds(20)};
    return config;
  }
};

TEST_F(VersioningBranchGcTest, DiffEngineSharesMainsArenaTrackersAndGcConfig) {
  EXPECT_EQ(Diff().DbArenaPool(), pool_.get());
  EXPECT_EQ(Diff().DbEmbeddingMemoryTracker(), &embedding_tracker_);
  EXPECT_TRUE(Diff().config_.gc == storage_->config_.gc);
}

// Another session's open transaction defeats fast-discard, so every committed branch update keeps its delta until GC.
TEST_F(VersioningBranchGcTest, DeltasRetainedBehindOpenTransactionAreFreedByGc) {
  MaterializeVertexStub();
  constexpr int64_t kCommits = 2000;
  {
    auto session_b = Diff().Access(ms::READ);
    for (int64_t i = 1; i <= kCommits; ++i) CommitStubWrite(i);
    Collect();
    EXPECT_GE(Deltas() - baseline_, kCommits) << "an older open transaction must pin the committed deltas";
  }
  Collect();
  EXPECT_LE(Deltas() - baseline_, 4);

  auto acc = Diff().Access(ms::READ);
  EXPECT_EQ(*acc->FindVertex(v_gid_, ms::View::NEW)->GetProperty(prop_x_, ms::View::NEW), ms::PropertyValue(kCommits));
}

TEST_F(VersioningBranchGcTest, RolledBackBigTransactionLeavesNoDeltasAfterGc) {
  MaterializeVertexStub();
  constexpr int64_t kWrites = 2000;
  {
    auto acc = Diff().Access(ms::WRITE);
    auto v = acc->FindVertex(v_gid_, ms::View::NEW);
    ASSERT_TRUE(v.has_value());
    for (int64_t i = 0; i < kWrites; ++i) {
      acc->CreateVertex();
      ASSERT_TRUE(v->SetProperty(prop_x_, ms::PropertyValue(i)).has_value());
    }
    EXPECT_GE(Deltas() - baseline_, 2 * kWrites);
    acc->Abort();
  }
  Collect();
  EXPECT_LE(Deltas() - baseline_, 4);

  auto acc = Diff().Access(ms::READ);
  size_t natives = 0;
  for (auto v : acc->Vertices(ms::View::NEW)) {
    if (!ctx_->IsForkGid(v.Gid())) ++natives;
  }
  EXPECT_EQ(natives, 0u);
  EXPECT_TRUE(acc->FindVertex(v_gid_, ms::View::NEW).has_value()) << "the stub is never collected";
}

// A deleted edge stub is a normal MVCC delete, so GC frees it. The delete is recorded on the endpoint stubs, so the fork
// edge stays hidden and CowEdge refuses it; the storage layer can still materialize a fresh stub under the same gid.
TEST_F(VersioningBranchGcTest, CollectedDeletedEdgeStubStaysHiddenAndCanBeRematerialized) {
  auto fork_edge_of = [&] {
    auto from = ctx_->historical().FindVertex(v_gid_, ms::View::OLD);
    EXPECT_TRUE(from.has_value());
    auto out = from->OutEdges(ms::View::OLD);
    EXPECT_TRUE(out.has_value());
    EXPECT_EQ(out->edges.size(), 1u);
    return out->edges[0];
  };
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    auto stub = ctx_->CowEdge(fork_edge_of());
    ASSERT_TRUE(stub.has_value());
    ASSERT_TRUE(stub->SetProperty(prop_x_, ms::PropertyValue(7)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx_->EndTransaction();
  }
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    auto deleted = ctx_->DeleteForkEdge(fork_edge_of());
    ASSERT_TRUE(deleted.has_value());
    EXPECT_TRUE(*deleted);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx_->EndTransaction();
  }
  Collect();
  EXPECT_LE(Deltas() - baseline_, 4) << "the stub's and the delete's deltas must be freed";

  auto acc = Diff().Access(ms::WRITE);
  ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
  auto fork_edge = fork_edge_of();
  EXPECT_FALSE(ctx_->FindDiffEdge(edge_gid_, ms::View::NEW).has_value());
  EXPECT_TRUE(ctx_->IsForkEdgeHidden(fork_edge, ms::View::NEW));
  EXPECT_TRUE(ctx_->ResolveEdges(v_gid_, ms::EdgeDirection::OUT, ms::View::NEW).empty());
  EXPECT_FALSE(ctx_->CowEdge(fork_edge).has_value());

  auto fresh =
      static_cast<ms::ReplicationAccessor *>(acc.get())->MaterializeStubEdge(edge_gid_, v_gid_, a_gid_, edge_type_);
  EXPECT_EQ(fresh.Gid(), edge_gid_);
  EXPECT_TRUE(fresh.Properties(ms::View::NEW)->empty());
  EXPECT_TRUE(ctx_->IsForkEdgeHidden(fork_edge, ms::View::NEW));
  acc->Abort();
  ctx_->EndTransaction();
}

TEST_F(VersioningBranchGcTest, BranchWritesAreChargedToMainsMemoryTracker) {
#if !USE_JEMALLOC
  GTEST_SKIP() << "per-DB arena tracking needs jemalloc";
#endif
  Collect();
  const auto before = db_tracker_.Amount();
  auto acc = Diff().Access(ms::WRITE);
  for (int i = 0; i < 40000; ++i) {
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.SetProperty(prop_x_, ms::PropertyValue(std::string(64, 'a'))).has_value());
  }
  EXPECT_GT(db_tracker_.Amount() - before, 1024 * 1024);
  acc->Abort();
}

TEST_F(VersioningBranchPeriodicGcTest, PeriodicGcThreadFreesDeltasWithoutExplicitCollect) {
  MaterializeVertexStub();
  {
    auto session_b = Diff().Access(ms::READ);
    for (int64_t i = 1; i <= 500; ++i) CommitStubWrite(i);
    EXPECT_GE(Deltas() - baseline_, 500);
  }
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (Deltas() - baseline_ > 4 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_LE(Deltas() - baseline_, 4);
}

}  // namespace

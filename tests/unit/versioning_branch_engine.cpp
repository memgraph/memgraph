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

// BranchContext tests against a real InMemoryStorage main plus RegisterForkPin; no interpreter wiring (this file does
// not link mg-query). The diff engine shares main's NameIdMapper, so one label/property id is valid on both sides.

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
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
    config.salient.items.storage_light_edge = LightEdge();
    storage_ = std::make_unique<ms::InMemoryStorage>(config);
    mem_storage_ = static_cast<ms::InMemoryStorage *>(storage_.get());
  }

  void TearDown() override {
    branch_context_.reset();
    if (fork_ts_.has_value()) mem_storage_->ReleaseForkPin(*fork_ts_);
    storage_.reset();
  }

  virtual bool LightEdge() const { return false; }

  uint64_t ForkNow() {
    fork_ts_ = mem_storage_->RegisterForkPin();
    return *fork_ts_;
  }

  void Begin(ms::Storage::Accessor *diff_acc) { ASSERT_TRUE(branch_context_->BeginTransaction(diff_acc).has_value()); }

  std::unique_ptr<ms::Storage> storage_;
  ms::InMemoryStorage *mem_storage_{};
  std::optional<uint64_t> fork_ts_;
  std::unique_ptr<mv::BranchContext> branch_context_;
};

// CowVertex materializes an empty stub for ONLY the touched vertex; untouched vertices never appear in the diff engine.
TEST_F(VersioningBranchEngineTest, CowVertexMaterializesEmptyStubOnlyForTouchedVertex) {
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

  auto count_diff_vertices = [](ms::Storage::Accessor &acc) {
    size_t n = 0;
    for (auto v : acc.Vertices(ms::View::NEW)) {
      (void)v;
      ++n;
    }
    return n;
  };

  auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
  Begin(diff_acc.get());

  ASSERT_EQ(count_diff_vertices(*diff_acc), 0u);
  auto hist_vertex = branch_context_->historical().FindVertex(a_gid, ms::View::OLD);
  ASSERT_TRUE(hist_vertex.has_value());
  EXPECT_TRUE(hist_vertex->HasLabel(label_a, ms::View::OLD).value_or(false));

  auto cowed = branch_context_->CowVertex(a_gid);
  ASSERT_TRUE(cowed.has_value());
  EXPECT_EQ(cowed->Gid(), a_gid);

  EXPECT_FALSE(cowed->HasLabel(label_a, ms::View::NEW).value_or(true));
  auto x_val = cowed->GetProperty(prop_x, ms::View::NEW);
  ASSERT_TRUE(x_val.has_value());
  EXPECT_TRUE(x_val->IsNull());

  auto fork_a = branch_context_->FindForkVertex(a_gid);
  ASSERT_TRUE(fork_a.has_value());
  EXPECT_TRUE(fork_a->HasLabel(label_a, ms::View::OLD).value_or(false));
  auto fork_x = fork_a->GetProperty(prop_x, ms::View::OLD);
  ASSERT_TRUE(fork_x.has_value());
  EXPECT_EQ(*fork_x, ms::PropertyValue(1));

  EXPECT_EQ(count_diff_vertices(*diff_acc), 1u);
  EXPECT_FALSE(diff_acc->FindVertex(b_gid, ms::View::NEW).has_value()) << "untouched vertex must not be copied";

  ASSERT_TRUE(cowed->SetProperty(prop_x, ms::PropertyValue(42)).has_value());
  auto second = branch_context_->CowVertex(a_gid);
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(second->Gid(), a_gid);
  auto x_again = second->GetProperty(prop_x, ms::View::NEW);
  ASSERT_TRUE(x_again.has_value());
  EXPECT_EQ(*x_again, ms::PropertyValue(42)) << "second CowVertex must resolve to the SAME (already-mutated) stub";
  EXPECT_EQ(count_diff_vertices(*diff_acc), 1u) << "idempotent COW must not create a duplicate";

  // Unknown gid: an error, and nothing is created.
  auto missing = branch_context_->CowVertex(ms::Gid::FromUint(1'000'000));
  ASSERT_FALSE(missing.has_value());
  EXPECT_NE(missing.error().message.find("not found"), std::string::npos) << missing.error().message;
  EXPECT_EQ(count_diff_vertices(*diff_acc), 1u);

  auto deleted_a = branch_context_->DeleteForkVertex(a_gid);
  ASSERT_TRUE(deleted_a.has_value() && deleted_a->has_value());
  auto refused = branch_context_->CowVertex(a_gid);
  ASSERT_FALSE(refused.has_value());
  EXPECT_NE(refused.error().message.find("already deleted"), std::string::npos) << refused.error().message;

  ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// ResolveVertex is diff-first with historical fallback; the gid-ordered union scan yields a COW'd vertex (diff wins),
// an untouched fork vertex and branch-native creates exactly once.
TEST_F(VersioningBranchEngineTest, ResolveAndUnionScanPreferDiffAndIncludeNatives) {
  const auto prop_x = storage_->NameToProperty("x");
  const auto edge_type = storage_->NameToEdgeType("E");
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
  Begin(diff_acc.get());

  auto cowed_a = branch_context_->CowVertex(a_gid);
  ASSERT_TRUE(cowed_a.has_value());
  ASSERT_TRUE(cowed_a->SetProperty(prop_x, ms::PropertyValue(2)).has_value());

  auto c = diff_acc->CreateVertex();
  auto d = diff_acc->CreateVertex();
  const auto c_gid = c.Gid();
  ASSERT_TRUE(c.SetProperty(prop_x, ms::PropertyValue(7)).has_value());
  auto native_edge = diff_acc->CreateEdge(&c, &d, edge_type);
  ASSERT_TRUE(native_edge.has_value());
  EXPECT_FALSE(branch_context_->IsForkGid(c_gid)) << "a native vertex must not reuse a fork-range gid";
  EXPECT_FALSE(branch_context_->IsForkGid(native_edge->Gid())) << "a native edge must not reuse a fork-range gid";

  auto resolved_a = branch_context_->ResolveVertex(a_gid, ms::View::NEW);
  ASSERT_TRUE(resolved_a.has_value());
  auto a_val = resolved_a->GetProperty(prop_x, ms::View::NEW);
  ASSERT_TRUE(a_val.has_value());
  EXPECT_EQ(*a_val, ms::PropertyValue(2));
  auto resolved_b = branch_context_->ResolveVertex(b_gid, ms::View::NEW);
  ASSERT_TRUE(resolved_b.has_value());
  auto b_val = resolved_b->GetProperty(prop_x, ms::View::OLD);
  ASSERT_TRUE(b_val.has_value());
  EXPECT_EQ(*b_val, ms::PropertyValue(9));

  std::map<uint64_t, int64_t> seen;
  size_t yielded = 0;
  std::vector<uint64_t> order;
  for (auto v : branch_context_->Vertices(ms::View::NEW)) {
    ++yielded;
    order.push_back(v.Gid().AsUint());
    auto val = v.GetProperty(prop_x, ms::View::NEW);
    ASSERT_TRUE(val.has_value());
    seen.emplace(v.Gid().AsUint(), val->IsNull() ? -1 : val->ValueInt());
  }
  EXPECT_EQ(yielded, 4u) << "A, B, C, D must each be yielded exactly once";
  ASSERT_EQ(seen.size(), 4u);
  EXPECT_TRUE(std::ranges::is_sorted(order)) << "the union merge must be gid-ordered";
  EXPECT_EQ(seen.at(a_gid.AsUint()), 2) << "A must yield the diff copy, not historical's stale 1";
  EXPECT_EQ(seen.at(b_gid.AsUint()), 9) << "B must yield historical_'s untouched value";
  EXPECT_EQ(seen.at(c_gid.AsUint()), 7) << "C (branch-native) must be yielded too";

  ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// A fork_ts never registered via RegisterForkPin is a BuildError.
TEST_F(VersioningBranchEngineTest, BuildFromForkRejectsUnpinnedTs) {
  {
    auto acc = storage_->Access(ms::WRITE);
    (void)acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // A pin exists, but on a different timestamp.
  const auto pinned_ts = ForkNow();
  const uint64_t never_pinned_ts = pinned_ts + 1000;
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_, never_pinned_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().message.find(std::to_string(never_pinned_ts)), std::string::npos) << result.error().message;
  EXPECT_EQ(mem_storage_->OldestForkPin(), std::optional<uint64_t>{pinned_ts}) << "a failed build must leak no pin";

  auto built = mv::BranchContext::BuildFromFork(*mem_storage_, pinned_ts, memgraph::tests::MakeMainCommitArgs());
  EXPECT_TRUE(built.has_value()) << "the pinned timestamp itself still builds";
}

// A fork-vertex delete is a diff-engine marker write: hidden at View::NEW but not View::OLD within the deleting
// command; abort leaves no marker; a committed delete stays hidden to later transactions.
TEST_F(VersioningBranchEngineTest, DeleteForkVertexMarkerFollowsMvcc) {
  const auto prop_x = storage_->NameToProperty("x");
  ms::Gid a_gid, b_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    ASSERT_TRUE(a.SetProperty(prop_x, ms::PropertyValue(1)).has_value());
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
    ASSERT_FALSE(branch_context_->IsVertexDeleted(a_gid, ms::View::NEW));

    auto deleted = branch_context_->DeleteForkVertex(a_gid);
    ASSERT_TRUE(deleted.has_value() && deleted->has_value());

    EXPECT_TRUE(branch_context_->IsVertexDeleted(a_gid, ms::View::NEW));
    EXPECT_FALSE(branch_context_->IsVertexDeleted(a_gid, ms::View::OLD));
    EXPECT_FALSE(branch_context_->ResolveVertex(a_gid, ms::View::NEW).has_value());
    EXPECT_TRUE(branch_context_->ResolveVertex(a_gid, ms::View::OLD).has_value());

    auto again = branch_context_->DeleteForkVertex(a_gid);
    ASSERT_TRUE(again.has_value());
    EXPECT_FALSE(again->has_value()) << "a repeated delete is a no-op";

    diff_acc->Abort();
  }

  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    Begin(diff_acc.get());
    EXPECT_FALSE(branch_context_->IsVertexDeleted(a_gid, ms::View::NEW))
        << "an aborted delete must leave no marker behind";
    ASSERT_TRUE(branch_context_->ResolveVertex(a_gid, ms::View::NEW).has_value());
    auto x_val = branch_context_->FindForkVertex(a_gid)->GetProperty(prop_x, ms::View::OLD);
    ASSERT_TRUE(x_val.has_value());
    EXPECT_EQ(*x_val, ms::PropertyValue(1));
    bool found_in_union = false;
    for (auto v : branch_context_->Vertices(ms::View::NEW)) {
      if (v.Gid() == a_gid) found_in_union = true;
    }
    EXPECT_TRUE(found_in_union) << "the union scan must see the vertex again post-abort";

    auto deleted = branch_context_->DeleteForkVertex(a_gid);
    ASSERT_TRUE(deleted.has_value() && deleted->has_value());
    ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto read_acc = branch_context_->diff_engine().Access(ms::READ);
  Begin(read_acc.get());
  EXPECT_TRUE(branch_context_->IsVertexDeleted(a_gid, ms::View::NEW))
      << "a committed delete must stay visible to a later transaction";
  EXPECT_FALSE(branch_context_->ResolveVertex(a_gid, ms::View::NEW).has_value());
  EXPECT_TRUE(branch_context_->ResolveVertex(b_gid, ms::View::NEW).has_value());
  std::vector<ms::Gid> scanned;
  for (auto v : branch_context_->Vertices(ms::View::NEW)) scanned.push_back(v.Gid());
  EXPECT_EQ(scanned, std::vector<ms::Gid>{b_gid}) << "the union scan must skip the deleted fork vertex";
}

// Fork-edge delete after an earlier COW+SET: hidden at View::NEW but not View::OLD within the deleting command, sets
// main's branched bit, hidden at both views later. A self-loop is recorded once and excluded from OUT and IN
// resolution.
TEST_F(VersioningBranchEngineTest, DeleteForkEdgeHiddenPerViewThenCommitted) {
  const auto prop_w = storage_->NameToProperty("w");
  const auto edge_type = storage_->NameToEdgeType("E");
  ms::Gid s_gid, a_gid, u_gid, m_gid, edge_gid, loop_gid;
  {
    auto acc = storage_->Access(ms::WRITE);
    auto s = acc->CreateVertex();
    s_gid = s.Gid();
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    auto u = acc->CreateVertex();
    u_gid = u.Gid();
    auto edge = acc->CreateEdge(&s, &a, edge_type);
    ASSERT_TRUE(edge.has_value());
    edge_gid = edge->Gid();
    ASSERT_TRUE(edge->SetProperty(prop_w, ms::PropertyValue(1)).has_value());
    auto m = acc->CreateVertex();
    m_gid = m.Gid();
    auto p1 = acc->CreateVertex();
    auto p2 = acc->CreateVertex();
    ASSERT_TRUE(acc->CreateEdge(&m, &p1, edge_type).has_value());
    ASSERT_TRUE(acc->CreateEdge(&m, &p2, edge_type).has_value());
    auto loop = acc->CreateEdge(&u, &u, edge_type);
    ASSERT_TRUE(loop.has_value());
    loop_gid = loop->Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const auto fork_ts = ForkNow();
  auto result = mv::BranchContext::BuildFromFork(*mem_storage_, fork_ts, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(result.has_value());
  branch_context_ = std::move(*result);

  // Fork edges bound independently of CowEdge.
  auto independent_hist = mem_storage_->HistoricalAccess(fork_ts);
  ASSERT_TRUE(independent_hist.has_value());
  auto hist_s = (*independent_hist)->FindVertex(s_gid, ms::View::OLD);
  ASSERT_TRUE(hist_s.has_value());
  auto out_res = hist_s->OutEdges(ms::View::OLD);
  ASSERT_TRUE(out_res.has_value());
  ASSERT_EQ(out_res->edges.size(), 1u);
  const ms::EdgeAccessor fork_edge = out_res->edges[0];
  auto hist_u = (*independent_hist)->FindVertex(u_gid, ms::View::OLD);
  ASSERT_TRUE(hist_u.has_value());
  auto loop_res = hist_u->OutEdges(ms::View::OLD);
  ASSERT_TRUE(loop_res.has_value());
  ASSERT_EQ(loop_res->edges.size(), 1u);
  const ms::EdgeAccessor fork_loop = loop_res->edges[0];
  ASSERT_EQ(fork_loop.Gid(), loop_gid);
  auto hist_m = (*independent_hist)->FindVertex(m_gid, ms::View::OLD);
  ASSERT_TRUE(hist_m.has_value());
  auto m_res = hist_m->OutEdges(ms::View::OLD);
  ASSERT_TRUE(m_res.has_value());
  ASSERT_EQ(m_res->edges.size(), 2u);
  std::vector<ms::EdgeAccessor> m_edges = m_res->edges;
  std::ranges::sort(m_edges, [](const auto &l, const auto &r) { return l.Gid() > r.Gid(); });
  ASSERT_TRUE(m_edges[0].Gid() > m_edges[1].Gid());
  EXPECT_FALSE(fork_loop.MaybeBranchedHint()) << "an untouched fork edge is not branched";

  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    Begin(diff_acc.get());
    auto cowed = branch_context_->CowEdge(fork_edge);
    ASSERT_TRUE(cowed.has_value());
    ASSERT_TRUE(cowed->SetProperty(prop_w, ms::PropertyValue(2)).has_value());
    ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  EXPECT_TRUE(fork_edge.MaybeBranchedHint()) << "CowEdge must set main's own Edge::branched() bit";

  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    Begin(diff_acc.get());
    ASSERT_TRUE(branch_context_->FindDiffEdge(edge_gid, ms::View::NEW).has_value())
        << "the edge committed by the previous statement must already be diff-resident";
    auto deleted = branch_context_->DeleteForkEdge(fork_edge);
    ASSERT_TRUE(deleted.has_value());
    ASSERT_TRUE(*deleted);

    EXPECT_FALSE(branch_context_->FindDiffEdge(edge_gid, ms::View::NEW).has_value());
    EXPECT_TRUE(branch_context_->IsForkEdgeHidden(fork_edge, ms::View::NEW));
    EXPECT_FALSE(branch_context_->IsForkEdgeHidden(fork_edge, ms::View::OLD))
        << "the deleting command's own marker writes are invisible at View::OLD";
    EXPECT_TRUE(fork_edge.MaybeBranchedHint());

    auto again = branch_context_->DeleteForkEdge(fork_edge);
    ASSERT_TRUE(again.has_value());
    EXPECT_FALSE(*again) << "a repeated delete is a no-op";

    auto loop_deleted = branch_context_->DeleteForkEdge(fork_loop);
    ASSERT_TRUE(loop_deleted.has_value());
    EXPECT_TRUE(*loop_deleted);
    EXPECT_TRUE(fork_loop.MaybeBranchedHint()) << "DeleteForkEdge must set main's branched bit on the fork edge";
    auto loop_again = branch_context_->DeleteForkEdge(fork_loop);
    ASSERT_TRUE(loop_again.has_value());
    EXPECT_FALSE(*loop_again) << "a self-loop is recorded once";

    EXPECT_FALSE(m_edges[0].MaybeBranchedHint());
    for (const auto &e : m_edges) {
      auto m_deleted = branch_context_->DeleteForkEdge(e);
      ASSERT_TRUE(m_deleted.has_value());
      EXPECT_TRUE(*m_deleted);
    }

    ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto read_acc = branch_context_->diff_engine().Access(ms::READ);
  Begin(read_acc.get());
  for (auto view : {ms::View::NEW, ms::View::OLD}) {
    EXPECT_TRUE(branch_context_->IsForkEdgeHidden(fork_edge, view));
    for (const auto &e : m_edges) EXPECT_TRUE(branch_context_->IsForkEdgeHidden(e, view));
  }
  EXPECT_TRUE(branch_context_->ResolveEdges(s_gid, ms::EdgeDirection::OUT, ms::View::NEW).empty());
  EXPECT_TRUE(branch_context_->ResolveEdges(a_gid, ms::EdgeDirection::IN, ms::View::NEW).empty());
  EXPECT_TRUE(branch_context_->ResolveEdges(m_gid, ms::EdgeDirection::OUT, ms::View::NEW).empty());
  EXPECT_TRUE(branch_context_->IsForkEdgeHidden(fork_loop, ms::View::NEW));
  EXPECT_TRUE(branch_context_->ResolveEdges(u_gid, ms::EdgeDirection::OUT, ms::View::NEW).empty());
  EXPECT_TRUE(branch_context_->ResolveEdges(u_gid, ms::EdgeDirection::IN, ms::View::NEW).empty());
}

// CowEdge materializes an empty, delta-less stub linked between the endpoint stubs; it is idempotent, never emitted by
// ResolveEdges (the fork edge is), and survives a rolled-back write.
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

  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::WRITE);
    Begin(diff_acc.get());
    auto found = branch_context_->FindDiffEdge(edge_gid, ms::View::NEW);
    ASSERT_TRUE(found.has_value()) << "a rolled-back write must not take the stub with it";
    EXPECT_TRUE(found->Properties(ms::View::NEW)->empty());
    auto stub = branch_context_->CowEdge(fork_edge_of());
    ASSERT_TRUE(stub.has_value());
    ASSERT_TRUE(stub->SetProperty(prop_w, ms::PropertyValue(3)).has_value());

    // Fork identity wins in adjacency.
    auto resolved = branch_context_->ResolveEdges(s_gid, ms::EdgeDirection::OUT, ms::View::NEW);
    ASSERT_EQ(resolved.size(), 1u);
    EXPECT_NE(resolved[0].storage_, &branch_context_->diff_engine());
    auto resolved_in = branch_context_->ResolveEdges(a_gid, ms::EdgeDirection::IN, ms::View::NEW);
    ASSERT_EQ(resolved_in.size(), 1u);
    EXPECT_NE(resolved_in[0].storage_, &branch_context_->diff_engine());

    // Linked into both diff-engine endpoint stubs exactly once.
    auto diff_from = diff_acc->FindVertex(s_gid, ms::View::NEW);
    ASSERT_TRUE(diff_from.has_value());
    auto diff_out = diff_from->OutEdges(ms::View::NEW);
    ASSERT_TRUE(diff_out.has_value());
    ASSERT_EQ(diff_out->edges.size(), 1u);
    EXPECT_EQ(diff_out->edges[0].Gid(), edge_gid);
    auto diff_to = diff_acc->FindVertex(a_gid, ms::View::NEW);
    ASSERT_TRUE(diff_to.has_value());
    auto diff_in = diff_to->InEdges(ms::View::NEW);
    ASSERT_TRUE(diff_in.has_value());
    ASSERT_EQ(diff_in->edges.size(), 1u);
    EXPECT_EQ(diff_in->edges[0].Gid(), edge_gid);

    EXPECT_TRUE(branch_context_->StubEdgeTouches(*stub, prop_w, ms::View::NEW));
    EXPECT_FALSE(branch_context_->StubEdgeTouches(*stub, prop_x, ms::View::NEW));
    ASSERT_TRUE(diff_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

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

class VersioningBranchLightEdgeTest : public VersioningBranchEngineTest {
 protected:
  bool LightEdge() const override { return true; }
};

// Same as the non-light-edge test, on light-edge storage.
TEST_F(VersioningBranchLightEdgeTest, CowEdgeMaterializesEmptyStubThatSurvivesRollback) {
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

  auto fork_edge_of = [&]() {
    auto hist_s = branch_context_->historical().FindVertex(s_gid, ms::View::OLD);
    EXPECT_TRUE(hist_s.has_value());
    auto out = hist_s->OutEdges(ms::View::OLD);
    EXPECT_TRUE(out.has_value());
    EXPECT_EQ(out->edges.size(), 1u);
    return out->edges[0];
  };

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
    EXPECT_EQ(again->edge_.ptr, stub->edge_.ptr) << "idempotent CowEdge must return the same stub object";
    auto diff_from = diff_acc->FindVertex(s_gid, ms::View::NEW);
    ASSERT_TRUE(diff_from.has_value());
    auto diff_out = diff_from->OutEdges(ms::View::NEW);
    ASSERT_TRUE(diff_out.has_value());
    EXPECT_EQ(diff_out->edges.size(), 1u) << "the stub is linked once into the diff from-stub";
    diff_acc->Abort();
  }

  {
    auto diff_acc = branch_context_->diff_engine().Access(ms::READ);
    Begin(diff_acc.get());
    auto found = branch_context_->FindDiffEdge(edge_gid, ms::View::NEW);
    ASSERT_TRUE(found.has_value()) << "a rolled-back write must not take the stub with it";
    EXPECT_TRUE(found->Properties(ms::View::NEW)->empty());
    EXPECT_TRUE(branch_context_->IsStub(*found));
  }
}

// The diff engine runs its own GC with main's GC config and charges memory to main's tenant (arena pool, embedding
// tracker, unreleased-delta gauge). Main is built as Database builds it, inside the pool's DbArenaScope.
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

  // Unlinking and freeing can take separate passes.
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

TEST_F(VersioningBranchPeriodicGcTest, DiffEngineInheritsNonDefaultGcInterval) {
  EXPECT_EQ(storage_->config_.gc.interval, std::chrono::milliseconds(20));
  EXPECT_EQ(Diff().config_.gc.type, ms::Config::Gc::Type::PERIODIC);
  EXPECT_EQ(Diff().config_.gc.interval, storage_->config_.gc.interval);
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
  constexpr int64_t kWrites = 2000;
  {
    auto acc = Diff().Access(ms::WRITE);
    ASSERT_TRUE(ctx_->BeginTransaction(acc.get()).has_value());
    // The stub is delta-less, so the abort cannot undo it.
    auto v = ctx_->CowVertex(v_gid_);
    ASSERT_TRUE(v.has_value());
    for (int64_t i = 0; i < kWrites; ++i) {
      acc->CreateVertex();
      ASSERT_TRUE(v->SetProperty(prop_x_, ms::PropertyValue(i)).has_value());
    }
    EXPECT_GE(Deltas() - baseline_, 2 * kWrites);
    acc->Abort();
    ctx_->EndTransaction();
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

// GC frees a deleted edge stub; the fork edge stays hidden (the delete lives on the endpoint stubs) and CowEdge refuses
// it, while the storage layer can still materialize a fresh stub under the same gid.
TEST_F(VersioningBranchGcTest, CollectedDeletedEdgeStubStaysHiddenAndCanBeRematerialized) {
  auto fork_edge_of = [&] {
    auto from = ctx_->historical().FindVertex(v_gid_, ms::View::OLD);
    EXPECT_TRUE(from.has_value());
    auto out = from->OutEdges(ms::View::OLD);
    EXPECT_TRUE(out.has_value());
    EXPECT_EQ(out->edges.size(), 1u);
    return out->edges[0];
  };
  // The older open transaction defeats fast-discard.
  auto pin_reader = Diff().Access(ms::READ);
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
  EXPECT_GT(Deltas() - baseline_, 4) << "the reader must pin the stub's and the delete's deltas";
  pin_reader.reset();
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
  auto again =
      static_cast<ms::ReplicationAccessor *>(acc.get())->MaterializeStubEdge(edge_gid_, v_gid_, a_gid_, edge_type_);
  EXPECT_EQ(again.edge_.ptr, fresh.edge_.ptr) << "re-materializing an existing stub must return it, not a second edge";
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
  // A fresh thread starts outside any arena scope; entering the diff engine's pool means a null or foreign pool in
  // Build would leave main's tracker flat.
  std::thread writer([&] {
    const memgraph::memory::DbArenaScope scope{Diff().DbArenaPool()};
    auto acc = Diff().Access(ms::WRITE);
    for (int i = 0; i < 40000; ++i) {
      auto v = acc->CreateVertex();
      if (!v.SetProperty(prop_x_, ms::PropertyValue(std::string(64, 'a'))).has_value()) {
        ADD_FAILURE() << "SetProperty failed";
        break;
      }
    }
    EXPECT_GT(db_tracker_.Amount() - before, 1024 * 1024);
    acc->Abort();
  });
  writer.join();
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

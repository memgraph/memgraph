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

// Graph Versioning v1 -- BRANCH read/traversal resolve microbenchmark.
//
// PURPOSE: measure the cost of the two hot, UN-memoized read paths a branch checkout resolves
// through on every single read -- versioning::BranchContext::ResolveEdges (branch_engine.cpp:
// 638-681, a per-call O(degree) union-recompute of historical + diff incident edges, de-duped by
// gid) and ResolveVertex (branch_engine.cpp:586-604, tombstone-probe + diff-miss + historical
// routing) -- so we can decide, profile-first, whether a resolve-cache is worth building. This is
// a MEASUREMENT tool: correctness of the harness (does it actually exercise BranchContext, does it
// print real numbers) matters more than polish.
//
// SETUP MIRRORS tests/unit/versioning_branch_engine.cpp's own VersioningBranchEngineTest fixture
// EXACTLY (main InMemoryStorage -> RegisterForkPin -> BranchContext::BuildFromFork -> diff_engine()
// .Access(WRITE) -> set_current_diff_txn) -- see BranchFixture below, whose ctor/ForkNow/Checkout
// are a line-for-line mirror of that test file's SetUp/ForkNow/every TEST_F's own opening
// boilerplate (tests/unit/versioning_branch_engine.cpp:53-134).
//
// CONFOUND CONTROLLED (T5 only): Transaction::manyDeltasCache (storage/v2/transaction.hpp:303,
// gated by FLAGS_delta_chain_cache_threshold, storage/v2/vertex_info_cache.cpp:24) already
// amortizes repeated large-delta-chain PROPERTY reads within one checkout's historical_ accessor
// (CreateHistoricalTransaction, inmemory/storage.cpp:3090, is unconditionally SNAPSHOT_ISOLATION,
// so Transaction::UseCache() is true for it) -- T5 forces the threshold to isolate that effect.
// T1-T4 (edges) are NOT amortized by it at all -- ResolveEdges rebuilds its OutEdges/InEdges union
// from scratch on every call, there is no edge-side equivalent of manyDeltasCache -- which is
// exactly why they are this file's primary focus.

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#include <benchmark/benchmark.h>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex_info_cache.hpp"  // FLAGS_delta_chain_cache_threshold (DECLARE_uint64)
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "versioning/branch_engine.hpp"

namespace {

namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;

// One shared root directory for every BranchContext::BuildFromFork call this binary ever makes.
// Safe to share across many Checkout() calls: BuildFromFork mints a FRESH per-session UUID
// subdirectory beneath whatever root it's given (branch_engine.hpp's own doc-comment on
// `branch_wal_root_directory`) -- exactly the same root-reuse pattern
// tests/unit/versioning_branch_engine.cpp's BranchWalRoot() uses across its whole test suite.
std::filesystem::path BenchWalRoot() {
  return std::filesystem::temp_directory_path() / "MG_bench_versioning_branch_resolve";
}

// Mirrors tests/unit/versioning_branch_engine.cpp's VersioningBranchEngineTest fixture: owns a
// main InMemoryStorage, a fork pin, and (after Checkout()) one checked-out BranchContext with its
// single current-diff-txn slot wired -- see BranchContext::set_current_diff_txn's own doc-comment
// (branch_engine.hpp) for why there is exactly one such accessor per checkout, held here for this
// fixture's whole lifetime (never committed -- ~InMemoryAccessor aborts an uncommitted transaction
// on destruction, inmemory/storage.cpp:768, same as any other RAII-scoped accessor in this
// codebase; the benchmarks below only ever READ through it, so there is nothing to commit).
//
// Member declaration order is DELIBERATE (reverse = destruction order): diff_acc_ must be torn
// down BEFORE branch_context_ (it is an accessor INTO branch_context_'s own diff_engine() storage
// -- destroying the storage first would use-after-free it), and fork_ts_ must be released via
// ReleaseForkPin BEFORE storage_ is destroyed. The custom destructor below makes this explicit
// rather than relying on member order alone.
struct BranchFixture {
  explicit BranchFixture(bool properties_on_edges = true, bool enable_edges_metadata = false) {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    config.salient.items.properties_on_edges = properties_on_edges;
    config.salient.items.enable_edges_metadata = enable_edges_metadata;
    storage_ = std::make_unique<ms::InMemoryStorage>(config);
    mem_storage_ = static_cast<ms::InMemoryStorage *>(storage_.get());
  }

  BranchFixture(const BranchFixture &) = delete;
  BranchFixture &operator=(const BranchFixture &) = delete;

  ~BranchFixture() {
    diff_acc_.reset();
    branch_context_.reset();
    if (fork_ts_.has_value()) mem_storage_->ReleaseForkPin(*fork_ts_);
    storage_.reset();
  }

  // Registers a fork pin at the CURRENT tip -- must be called (and any post-fork main churn
  // applied) BEFORE Checkout().
  uint64_t ForkNow() {
    fork_ts_ = mem_storage_->RegisterForkPin();
    return *fork_ts_;
  }

  // Builds the BranchContext and wires the single current-diff-txn slot. Mirrors every TEST_F's
  // own opening boilerplate in tests/unit/versioning_branch_engine.cpp (e.g. lines 111-119,
  // 159-173) exactly -- same BuildFromFork args (empty changelog, MakeMainCommitArgs() x2), same
  // Access(WRITE) + set_current_diff_txn pairing.
  void Checkout() {
    auto result = mv::BranchContext::BuildFromFork(*mem_storage_,
                                                   *fork_ts_,
                                                   memgraph::tests::MakeMainCommitArgs(),
                                                   memgraph::tests::MakeMainCommitArgs(),
                                                   BenchWalRoot(),
                                                   {});
    if (!result.has_value()) {
      std::cerr << "FATAL: BranchContext::BuildFromFork failed: " << result.error().message << "\n";
      std::abort();
    }
    branch_context_ = std::move(*result);
    diff_acc_ = branch_context_->diff_engine().Access(ms::WRITE);
    branch_context_->set_current_diff_txn(diff_acc_.get());
  }

  std::unique_ptr<ms::Storage> storage_;
  ms::InMemoryStorage *mem_storage_{};
  std::optional<uint64_t> fork_ts_;
  std::unique_ptr<mv::BranchContext> branch_context_;
  std::unique_ptr<ms::Storage::Accessor> diff_acc_;
};

// ---------------------------------------------------------------------------------------------
// T1 -- incident-edge resolution, the core cost: a hub vertex of fork-degree D, forked with NO
// branch writes at all (diff engine stays empty for the whole run). Every ResolveEdges call is
// therefore pure historical-side collection (branch_engine.cpp:673-675's `collect` on
// historical_base_) plus an empty no-op diff-side probe -- isolating the union-recompute's
// per-degree cost on its own, before T2/T3 layer the diff side back in.
// ---------------------------------------------------------------------------------------------
void BM_T1_ResolveEdges_HistoricalOnly(benchmark::State &state) {
  const auto degree = static_cast<size_t>(state.range(0));

  BranchFixture fx;
  ms::Gid hub_gid;
  {
    auto acc = fx.storage_->Access(ms::WRITE);
    auto hub = acc->CreateVertex();
    hub_gid = hub.Gid();
    auto edge_type = acc->NameToEdgeType("E");
    for (size_t i = 0; i < degree; ++i) {
      auto other = acc->CreateVertex();
      if (!acc->CreateEdge(&hub, &other, edge_type).has_value()) {
        state.SkipWithError("T1 setup: main CreateEdge failed");
        return;
      }
    }
    if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) {
      state.SkipWithError("T1 setup: main commit failed");
      return;
    }
  }

  fx.ForkNow();
  fx.Checkout();

  // Setup-only sanity check (OUTSIDE the timed region): confirm the resolve actually returns the
  // whole fork-state incident set, so we are not benchmarking an empty/broken path.
  auto sanity = fx.branch_context_->ResolveEdges(hub_gid, ms::EdgeDirection::OUT, ms::View::NEW, {});
  if (sanity.size() != degree) {
    state.SkipWithError("T1 setup sanity check failed: resolved edge count != fork degree");
    return;
  }

  for (auto _ : state) {
    auto result = fx.branch_context_->ResolveEdges(hub_gid, ms::EdgeDirection::OUT, ms::View::NEW, {});
    benchmark::DoNotOptimize(result);
  }
  state.SetItemsProcessed(state.iterations());
  state.SetLabel("D=" + std::to_string(degree));
}

BENCHMARK(BM_T1_ResolveEdges_HistoricalOnly)->Arg(4)->Arg(64)->Arg(1024)->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------------------------
// T2 -- multi-hop traversal crossing overlay<->historical: a depth-5, branching-factor-3 tree
// forked from main, with EVEN-depth vertices COW'd+touched on the branch (AddLabel forces a real
// CowVertex copy into the diff engine -- see CowVertex's own doc-comment, branch_engine.hpp) and
// ODD-depth vertices left untouched (resolve purely via historical_). A k-hop expand alternates
// crossing the two stores at every level: ResolveEdges(v) (historical+diff union, same cost shape
// as T1) followed by ResolveVertex(child) for each discovered neighbor (diff-hit for a touched
// vertex, historical fallback for an untouched one) -- measuring the per-hop re-resolve + weave
// cost a real multi-hop MATCH pays, not just a single incident-set lookup.
// ---------------------------------------------------------------------------------------------
void BM_T2_KHopExpand(benchmark::State &state) {
  const int k = static_cast<int>(state.range(0));
  constexpr int kBranchingFactor = 3;
  constexpr int kMaxDepth = 5;  // must stay > the largest k this benchmark is run with (4)

  BranchFixture fx;
  std::vector<std::vector<ms::Gid>> levels;  // levels[d] = every vertex gid at depth d
  ms::Gid root_gid;
  {
    auto acc = fx.storage_->Access(ms::WRITE);
    auto edge_type = acc->NameToEdgeType("E");
    auto root = acc->CreateVertex();
    root_gid = root.Gid();
    levels.push_back({root_gid});

    std::vector<ms::VertexAccessor> prev_level{root};
    for (int d = 1; d <= kMaxDepth; ++d) {
      std::vector<ms::Gid> cur_gids;
      std::vector<ms::VertexAccessor> cur_level;
      cur_level.reserve(prev_level.size() * kBranchingFactor);
      for (auto &parent : prev_level) {
        for (int c = 0; c < kBranchingFactor; ++c) {
          auto child = acc->CreateVertex();
          if (!acc->CreateEdge(&parent, &child, edge_type).has_value()) {
            state.SkipWithError("T2 setup: tree CreateEdge failed");
            return;
          }
          cur_gids.push_back(child.Gid());
          cur_level.push_back(child);
        }
      }
      levels.push_back(cur_gids);
      prev_level = std::move(cur_level);
    }
    if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) {
      state.SkipWithError("T2 setup: main commit failed");
      return;
    }
  }

  fx.ForkNow();
  fx.Checkout();

  // Touch every EVEN-depth vertex (COW + mutate) -- ODD depths stay historical-only.
  {
    auto label_touched = fx.mem_storage_->NameToLabel("Touched");
    for (int d = 0; d <= kMaxDepth; d += 2) {
      for (auto gid : levels[d]) {
        auto cowed = fx.branch_context_->CowVertex(gid);
        if (!cowed.has_value()) {
          state.SkipWithError("T2 setup: CowVertex failed");
          return;
        }
        if (!cowed->AddLabel(label_touched).has_value()) {
          state.SkipWithError("T2 setup: AddLabel failed");
          return;
        }
      }
    }
  }

  auto expand_once = [&]() {
    std::vector<ms::Gid> frontier{root_gid};
    for (int hop = 0; hop < k; ++hop) {
      std::vector<ms::Gid> next_frontier;
      for (auto v_gid : frontier) {
        auto edges = fx.branch_context_->ResolveEdges(v_gid, ms::EdgeDirection::OUT, ms::View::NEW, {});
        for (auto &e : edges) {
          auto neighbor_gid = e.ToVertex().Gid();
          auto resolved = fx.branch_context_->ResolveVertex(neighbor_gid, ms::View::NEW);
          if (resolved.has_value()) next_frontier.push_back(neighbor_gid);
        }
      }
      frontier = std::move(next_frontier);
    }
    return frontier;
  };

  // Setup-only sanity check: a full k-hop expand must actually reach a non-empty frontier.
  auto sanity_frontier = expand_once();
  if (sanity_frontier.empty()) {
    state.SkipWithError("T2 setup sanity check failed: k-hop expand returned an empty frontier");
    return;
  }

  for (auto _ : state) {
    auto frontier = expand_once();
    benchmark::DoNotOptimize(frontier);
  }
  state.SetItemsProcessed(state.iterations());
  state.SetLabel("k=" + std::to_string(k));
}

BENCHMARK(BM_T2_KHopExpand)->Arg(2)->Arg(4)->Unit(benchmark::kMicrosecond);

// ---------------------------------------------------------------------------------------------
// T3 -- branch edit (N added + M tombstoned) union cost: a hub vertex forked at degree
// `degree_fork`; the branch then adds N branch-native edges to it and tombstones M of its
// fork-state edges (TombstoneEdge -- see its own doc-comment, branch_engine.hpp, for why a bare
// diff-miss cannot represent "deleted" on its own). Measures ResolveEdges' union+tombstone-skip
// cost with a genuinely non-trivial diff side, not just an empty one (T1) or a same-set COW (T2).
// ---------------------------------------------------------------------------------------------
void BM_T3_ResolveEdges_BranchEditUnionTombstone(benchmark::State &state) {
  const auto degree_fork = static_cast<size_t>(state.range(0));
  const auto n_added = static_cast<size_t>(state.range(1));
  const auto m_deleted = static_cast<size_t>(state.range(2));
  if (m_deleted > degree_fork) {
    state.SkipWithError("T3 setup: m_deleted > degree_fork");
    return;
  }

  BranchFixture fx;
  ms::Gid hub_gid;
  {
    auto acc = fx.storage_->Access(ms::WRITE);
    auto hub = acc->CreateVertex();
    hub_gid = hub.Gid();
    auto edge_type = acc->NameToEdgeType("E");
    for (size_t i = 0; i < degree_fork; ++i) {
      auto other = acc->CreateVertex();
      if (!acc->CreateEdge(&hub, &other, edge_type).has_value()) {
        state.SkipWithError("T3 setup: main CreateEdge failed");
        return;
      }
    }
    if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) {
      state.SkipWithError("T3 setup: main commit failed");
      return;
    }
  }

  fx.ForkNow();
  fx.Checkout();

  // Capture the fork-state incident set BEFORE any branch edit (diff engine is still empty here,
  // so this is a pure historical read, same shape as T1) -- picks the M gids to tombstone below.
  auto fork_edges = fx.branch_context_->ResolveEdges(hub_gid, ms::EdgeDirection::OUT, ms::View::NEW, {});
  if (fork_edges.size() != degree_fork) {
    state.SkipWithError("T3 setup: fork-state edge count mismatch");
    return;
  }
  for (size_t i = 0; i < m_deleted; ++i) {
    fx.branch_context_->TombstoneEdge(fork_edges[i].Gid());
  }

  auto cowed_hub_result = fx.branch_context_->CowVertex(hub_gid);
  if (!cowed_hub_result.has_value()) {
    state.SkipWithError("T3 setup: CowVertex(hub) failed");
    return;
  }
  auto cowed_hub = *cowed_hub_result;
  auto diff_edge_type = fx.diff_acc_->NameToEdgeType("E");
  for (size_t i = 0; i < n_added; ++i) {
    auto native = fx.diff_acc_->CreateVertex();
    if (!fx.diff_acc_->CreateEdge(&cowed_hub, &native, diff_edge_type).has_value()) {
      state.SkipWithError("T3 setup: branch-native CreateEdge failed");
      return;
    }
  }

  const auto expected = degree_fork - m_deleted + n_added;
  auto sanity = fx.branch_context_->ResolveEdges(hub_gid, ms::EdgeDirection::OUT, ms::View::NEW, {});
  if (sanity.size() != expected) {
    state.SkipWithError("T3 setup sanity check failed: resolved incident-set size != expected");
    return;
  }

  for (auto _ : state) {
    auto result = fx.branch_context_->ResolveEdges(hub_gid, ms::EdgeDirection::OUT, ms::View::NEW, {});
    benchmark::DoNotOptimize(result);
  }
  state.SetItemsProcessed(state.iterations());
  std::ostringstream label;
  label << "Dfork=" << degree_fork << " N=" << n_added << " M=" << m_deleted;
  state.SetLabel(label.str());
}

BENCHMARK(BM_T3_ResolveEdges_BranchEditUnionTombstone)
    ->Args({64, 16, 16})
    ->Args({1024, 128, 128})
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------------------------
// T4 -- T1 (historical-only ResolveEdges) repeated under THREE edge-storage configs:
//   mode 0: properties_on_edges=true              (heavy edges -- storage::Config's own default)
//   mode 1: properties_on_edges=false, enable_edges_metadata=true   (light edges, metadata ON --
//           the configuration branch_engine.cpp's own doc-comment (:330-342) says is needed for a
//           stable EdgeAccessor::Gid())
//   mode 2: properties_on_edges=false, enable_edges_metadata=false (light edges, NO metadata --
//           the exact combination the task calls out as an open light-edge-gid-stability question)
//
// CORRECTNESS PROBE, not a crash guard: EdgeAccessor::Gid() (edge_accessor.cpp:580-584) reads a
// plain EdgeRef-embedded field unconditionally when properties_on_edges is false -- it is always
// *defined*, so this does not crash. What CAN go silently wrong is ResolveEdges' de-dupe-by-gid
// (branch_engine.cpp:664, `seen.try_emplace(edge.Gid(), ...)`): if that light-edge gid is not
// actually a stable, collision-free identity across the historical+diff union, DIFFERENT edges
// could collide and get silently DROPPED (resolved count < fork degree) rather than erroring. We
// check for exactly that (count + distinct-gid count, both against the known fork degree) and, if
// it fires, report it to stderr as a FINDING and skip only that benchmark arm (state.SkipWithError)
// -- never assert/abort on it.
// ---------------------------------------------------------------------------------------------
void BM_T4_ResolveEdges_EdgeMode(benchmark::State &state) {
  const auto degree = static_cast<size_t>(state.range(0));
  const auto mode = state.range(1);
  const bool properties_on_edges = (mode == 0);
  const bool enable_edges_metadata = (mode == 1);

  BranchFixture fx(properties_on_edges, enable_edges_metadata);
  ms::Gid hub_gid;
  {
    auto acc = fx.storage_->Access(ms::WRITE);
    auto hub = acc->CreateVertex();
    hub_gid = hub.Gid();
    auto edge_type = acc->NameToEdgeType("E");
    for (size_t i = 0; i < degree; ++i) {
      auto other = acc->CreateVertex();
      if (!acc->CreateEdge(&hub, &other, edge_type).has_value()) {
        state.SkipWithError("T4 setup: main CreateEdge failed");
        return;
      }
    }
    if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) {
      state.SkipWithError("T4 setup: main commit failed");
      return;
    }
  }

  fx.ForkNow();
  fx.Checkout();

  auto resolved = fx.branch_context_->ResolveEdges(hub_gid, ms::EdgeDirection::OUT, ms::View::NEW, {});
  std::unordered_set<uint64_t> distinct_gids;
  for (auto &e : resolved) distinct_gids.insert(e.Gid().AsUint());

  if (resolved.size() != degree || distinct_gids.size() != degree) {
    std::cerr << "[T4 FINDING] mode=" << mode << " (properties_on_edges=" << properties_on_edges
              << ", enable_edges_metadata=" << enable_edges_metadata << ") degree=" << degree << ": resolved "
              << resolved.size() << " edges, " << distinct_gids.size() << " distinct gids (expected " << degree
              << ") -- LIGHT-EDGE GID INSTABILITY under this config; see branch_engine.cpp:341-342 and "
                 "edge_accessor.cpp:580-584.\n";
    state.SkipWithError("T4: edge-gid correctness anomaly under this edge-mode -- see stderr FINDING above");
    return;
  }

  for (auto _ : state) {
    auto r = fx.branch_context_->ResolveEdges(hub_gid, ms::EdgeDirection::OUT, ms::View::NEW, {});
    benchmark::DoNotOptimize(r);
  }
  state.SetItemsProcessed(state.iterations());
  std::ostringstream label;
  label << "D=" << degree << " mode=" << mode;
  state.SetLabel(label.str());
}

BENCHMARK(BM_T4_ResolveEdges_EdgeMode)
    ->Args({4, 0})
    ->Args({64, 0})
    ->Args({1024, 0})
    ->Args({4, 1})
    ->Args({64, 1})
    ->Args({1024, 1})
    ->Args({4, 2})
    ->Args({64, 2})
    ->Args({1024, 2})
    ->Unit(benchmark::kNanosecond);

// ---------------------------------------------------------------------------------------------
// T5 -- vertex property baseline (control, not the focus): point-reads 4 properties of an
// UNTOUCHED fork vertex via ResolveVertex -> GetProperty, with `delta_churn` committed property
// updates applied to that SAME vertex on MAIN AFTER the fork pin is taken (RegisterForkPin), so
// the historical view pinned at fork_ts must walk back through every one of them
// (CreateHistoricalTransaction's start_timestamp=fork_ts, inmemory/storage.cpp:3090; the walk
// itself is detail::IsVisible/ApplyDeltasForRead, vertex_accessor.cpp:120-148) to reconstruct the
// fork-time property values. Two threshold arms isolate manyDeltasCache's effect:
//   - NoCache:      FLAGS_delta_chain_cache_threshold forced far above delta_churn, so the cache
//                   NEVER activates -- every iteration pays a full, uncached chain walk.
//   - DefaultCache: FLAGS_delta_chain_cache_threshold left at its real default (128) -- once
//                   delta_churn >= 128, the FIRST iteration's walk populates the cache and every
//                   subsequent iteration in the timed loop is an O(1) cache hit instead.
// Comparing the two at delta_churn=200 is the direct, quantified answer to the skeptic's
// manyDeltasCache-amortization flag.
// ---------------------------------------------------------------------------------------------
void RunT5(benchmark::State &state, uint64_t threshold_override) {
  const auto delta_churn = static_cast<int>(state.range(0));
  constexpr int kNumProps = 4;

  const uint64_t saved_threshold = FLAGS_delta_chain_cache_threshold;
  FLAGS_delta_chain_cache_threshold = threshold_override;
  auto restore_threshold = [&] { FLAGS_delta_chain_cache_threshold = saved_threshold; };

  BranchFixture fx;
  ms::Gid v_gid;
  std::vector<ms::PropertyId> props;
  {
    auto acc = fx.storage_->Access(ms::WRITE);
    auto v = acc->CreateVertex();
    v_gid = v.Gid();
    for (int i = 0; i < kNumProps; ++i) {
      auto p = acc->NameToProperty("p" + std::to_string(i));
      props.push_back(p);
      if (!v.SetProperty(p, ms::PropertyValue(static_cast<int64_t>(i))).has_value()) {
        state.SkipWithError("T5 setup: initial SetProperty failed");
        restore_threshold();
        return;
      }
    }
    if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) {
      state.SkipWithError("T5 setup: main commit failed");
      restore_threshold();
      return;
    }
  }

  fx.ForkNow();

  // Churn AFTER the fork pin: `delta_churn` SEPARATE committed transactions, each contributing at
  // least one surviving delta to v_gid's chain -- the historical view pinned at fork_ts must walk
  // back through every one of them.
  {
    auto churn_prop = fx.mem_storage_->NameToProperty("churn");
    for (int i = 0; i < delta_churn; ++i) {
      auto acc = fx.storage_->Access(ms::WRITE);
      auto v = acc->FindVertex(v_gid, ms::View::OLD);
      if (!v.has_value()) {
        state.SkipWithError("T5 setup: churn FindVertex failed");
        restore_threshold();
        return;
      }
      if (!v->SetProperty(churn_prop, ms::PropertyValue(static_cast<int64_t>(i + 1))).has_value()) {
        state.SkipWithError("T5 setup: churn SetProperty failed");
        restore_threshold();
        return;
      }
      if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) {
        state.SkipWithError("T5 setup: churn commit failed");
        restore_threshold();
        return;
      }
    }
  }

  fx.Checkout();

  // Setup-only sanity check: v_gid was never touched on the branch, so ResolveVertex must fall
  // through to historical_ and see the FORK-TIME properties, not the post-fork churn.
  auto sanity = fx.branch_context_->ResolveVertex(v_gid, ms::View::OLD);
  if (!sanity.has_value()) {
    state.SkipWithError("T5 setup sanity check failed: ResolveVertex returned nullopt");
    restore_threshold();
    return;
  }
  for (auto &p : props) {
    if (!sanity->GetProperty(p, ms::View::OLD).has_value()) {
      state.SkipWithError("T5 setup sanity check failed: GetProperty returned an error");
      restore_threshold();
      return;
    }
  }

  for (auto _ : state) {
    auto resolved = fx.branch_context_->ResolveVertex(v_gid, ms::View::OLD);
    for (auto &p : props) {
      auto val = resolved->GetProperty(p, ms::View::OLD);
      benchmark::DoNotOptimize(val);
    }
  }
  state.SetItemsProcessed(state.iterations() * kNumProps);
  state.SetLabel("delta_churn=" + std::to_string(delta_churn));

  restore_threshold();
}

void BM_T5_ResolveVertex_Property_NoCache(benchmark::State &state) {
  // Forced far above any delta_churn value used below, so manyDeltasCache NEVER activates -- every
  // iteration pays a full, uncached delta-chain walk (the "cold" baseline).
  RunT5(state, /*threshold_override=*/1'000'000);
}

BENCHMARK(BM_T5_ResolveVertex_Property_NoCache)->Arg(0)->Arg(200)->Unit(benchmark::kNanosecond);

void BM_T5_ResolveVertex_Property_DefaultCache(benchmark::State &state) { RunT5(state, /*threshold_override=*/128); }

BENCHMARK(BM_T5_ResolveVertex_Property_DefaultCache)->Arg(0)->Arg(200)->Unit(benchmark::kNanosecond);

}  // namespace

int main(int argc, char **argv) {
  std::error_code ec;
  std::filesystem::remove_all(BenchWalRoot(), ec);  // clear any stale dir from a prior crashed run

  std::cout << "=====================================================================\n"
               "Graph Versioning v1 -- BRANCH read/traversal resolve microbenchmark\n"
               "=====================================================================\n"
               "T1: ResolveEdges cost vs fork-degree D (historical-only union, the core cost)\n"
               "T2: k-hop expand crossing overlay<->historical (mixed COW'd/untouched vertices)\n"
               "T3: ResolveEdges cost with a non-trivial branch diff (N added, M tombstoned)\n"
               "T4: ResolveEdges cost under properties_on_edges true/false + edge-gid correctness probe\n"
               "T5: ResolveVertex property point-read -- manyDeltasCache threshold forced-off vs default\n"
               "Any correctness FINDING (esp. T4 light-edge gid) is printed to stderr, prefixed [T4 FINDING].\n"
               "---------------------------------------------------------------------\n";

  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();

  std::filesystem::remove_all(BenchWalRoot(), ec);
  return 0;
}

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

#include "versioning/branch_engine.hpp"

#include <fmt/format.h>

#include <unordered_map>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/logging.hpp"

namespace memgraph::versioning {

namespace {

// Recursive Enum detector -- see BranchContext::CowError's doc-comment for why an Enum anywhere
// (top-level or nested inside a List/Map) fails the whole COW rather than being silently dropped
// or silently (mis)copied.
bool ContainsEnum(const storage::PropertyValue &v) {
  if (v.IsEnum()) return true;
  if (v.IsList()) {
    for (const auto &elem : v.ValueList()) {
      if (ContainsEnum(elem)) return true;
    }
    return false;
  }
  if (v.IsMap()) {
    for (const auto &[key, elem] : v.ValueMap()) {
      if (ContainsEnum(elem)) return true;
    }
    return false;
  }
  return false;
}

// GID COLLISION FIX (see branch_engine.hpp's class comment): every branch-native `CREATE` runs
// through the diff engine's own auto-gid counter (InMemoryStorage's `vertex_id_`, starting at 0 on
// a fresh engine). Left alone, the FIRST branch-native create before any COW would collide with
// main's own fork-state gid 0 (near-universal in any non-empty graph) -- `ResolveVertex`/`Vertices`
// would then be unable to tell "branch-native vertex reusing gid 0" apart from "COW'd copy of
// historical gid 0", silently conflating two distinct vertices.
//
// There is no public API to set `vertex_id_` to a watermark without creating a vertex (see this
// slice's implementation notes) -- `CreateVertexEx`/`CreateEdgeEx` are the ONLY way to advance it
// (via `atomic_fetch_max` on the explicit gid, inmemory/storage.cpp), and both are protected except
// through `ReplicationAccessor`. So `BuildFromFork` creates ONE sacrificial vertex at this
// watermark gid (bumping the counter to watermark+1) and immediately deletes it again, leaving
// zero live vertices but the counter permanently past this reservation. `1ULL << 62` reserves the
// upper quarter of the 64-bit gid space exclusively for branch-native creates -- disjoint from any
// gid main could plausibly ever use (main allocates from 0 upward), at O(1) cost regardless of
// main's size (unlike the full-graph-scan alternative of computing an exact fork-state max-gid,
// which would reintroduce an O(N) step at CHECKOUT time -- exactly what this slice's lazy-diff
// redesign exists to eliminate).
//
// Slice E-2a FOLLOW-UP: edge gids need the symmetric reservation now that edges are handled --
// CreateEdgeEx bumps its own, separate `edge_id_` counter (inmemory/storage.cpp), so this same
// trick applies unchanged, just against edges instead of vertices (see ReserveBranchNativeGidRange
// below, which now reserves both in one transaction/commit).
constexpr uint64_t kBranchNativeGidWatermark = 1ULL << 62;

void ReserveBranchNativeGidRange(storage::InMemoryStorage &diff_engine, storage::CommitArgs commit_args) {
  std::unique_ptr<storage::ReplicationAccessor> reserve(
      static_cast<storage::ReplicationAccessor *>(diff_engine.Access(storage::StorageAccessType::WRITE).release()));

  // --- Vertex-gid watermark (E-1, unchanged) ---
  auto sacrificial = reserve->CreateVertexEx(storage::Gid::FromUint(kBranchNativeGidWatermark));
  MG_ASSERT(sacrificial.has_value(),
            "BranchContext::BuildFromFork: gid-watermark reservation collided on a freshly-created, empty diff "
            "engine -- should be impossible.");

  // --- Edge-gid watermark (E-2a) ---
  // CreateEdgeEx (unlike CreateVertexEx) needs two REAL, already-live, SAME-transaction vertex
  // endpoints -- it MG_ASSERTs `from->transaction_ == to->transaction_ == &transaction_`
  // (InMemoryAccessor::CreateEdgeEx, inmemory/storage.cpp). Plain auto-gid `CreateVertex()` is fine
  // for these two: they (and the sacrificial edge between them) are deleted a few lines down in
  // this SAME transaction/commit, so their auto-assigned gids are never observed by anything and
  // never even become visible outside this function.
  auto sac_from = reserve->CreateVertex();
  auto sac_to = reserve->CreateVertex();
  auto edge_type = reserve->NameToEdgeType("__branch_native_gid_watermark__");
  auto sacrificial_edge =
      reserve->CreateEdgeEx(&sac_from, &sac_to, edge_type, storage::Gid::FromUint(kBranchNativeGidWatermark));
  MG_ASSERT(sacrificial_edge.has_value(),
            "BranchContext::BuildFromFork: edge gid-watermark reservation collided on a freshly-created, empty diff "
            "engine -- should be impossible.");

  // Tear down in dependency order: the edge before its endpoints -- DeleteVertex (unlike
  // DetachDeleteVertex) does not detach incident edges for it, so the two sacrificial vertices
  // must already be edge-free by the time DeleteVertex reaches them.
  auto deleted_edge = reserve->DeleteEdge(&*sacrificial_edge);
  MG_ASSERT(deleted_edge.has_value() && deleted_edge->has_value(),
            "BranchContext::BuildFromFork: failed to remove the edge gid-watermark placeholder edge.");

  auto deleted_to = reserve->DeleteVertex(&sac_to);
  MG_ASSERT(deleted_to.has_value() && deleted_to->has_value(),
            "BranchContext::BuildFromFork: failed to remove an edge gid-watermark sacrificial vertex.");
  auto deleted_from = reserve->DeleteVertex(&sac_from);
  MG_ASSERT(deleted_from.has_value() && deleted_from->has_value(),
            "BranchContext::BuildFromFork: failed to remove an edge gid-watermark sacrificial vertex.");

  auto deleted = reserve->DeleteVertex(&*sacrificial);
  MG_ASSERT(deleted.has_value() && deleted->has_value(),
            "BranchContext::BuildFromFork: failed to remove the gid-watermark placeholder vertex.");

  auto commit_res = reserve->PrepareForCommitPhase(std::move(commit_args));
  MG_ASSERT(commit_res.has_value(), "BranchContext::BuildFromFork: gid-watermark reservation commit failed.");
}

}  // namespace

std::expected<std::unique_ptr<BranchContext>, BranchContext::BuildError> BranchContext::BuildFromFork(
    storage::InMemoryStorage &main, uint64_t fork_ts, storage::CommitArgs commit_args) {
  // -----------------------------------------------------------------------------------------
  // 1. Time-travel main back to fork_ts FIRST -- if this fails, we must not have constructed
  //    (and paid for) a diff engine at all.
  // -----------------------------------------------------------------------------------------
  auto hist_exp = main.HistoricalAccess(fork_ts);
  if (!hist_exp.has_value()) {
    return std::unexpected(
        BuildError{.message = fmt::format("Cannot check out a version: fork timestamp {} is not (or no longer) pinned.",
                                          fork_ts)});
  }
  std::unique_ptr<storage::Storage::Accessor> historical = std::move(*hist_exp);

  // -----------------------------------------------------------------------------------------
  // 2. Construct the lightweight, EMPTY private diff engine. Unlike the predecessor BranchEngine
  //    (which seeded a full copy of main's fork-state graph here, an O(main's size) step every
  //    checkout), this is O(1): nothing is copied. GC disabled, every durability mechanism off --
  //    this is a transient, in-RAM working copy, not a durable database.
  //
  //    SHARES main's own NameIdMapper (main.GetSharedNameIdMapper(), storage.hpp) rather than
  //    building a numerically-unrelated one of its own: the query engine decodes a vertex's
  //    label/property ids through ONE mapper per query (the diff-engine accessor's) -- a
  //    historical (not-yet-COW'd) vertex still carries main's own ids, so unless the diff engine
  //    shares main's exact id space, those ids are meaningless (or worse, silently wrong) when
  //    decoded through a different mapper. One shared id space also means CowVertex (below) copies
  //    ids DIRECTLY, no by-name translation needed. Safe to share concurrently: NameIdMapper's
  //    NameToId/IdToName are already used concurrently by main's own multiple transactions today
  //    (SkipListDb-backed, atomic counter_) -- a branch adding a brand-new name is the same
  //    already-supported access pattern, not a new concurrency hazard.
  // -----------------------------------------------------------------------------------------
  storage::Config config;
  config.gc.type = storage::Config::Gc::Type::NONE;
  config.durability.snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::DISABLED;
  config.durability.recover_on_startup = false;
  config.durability.snapshot_on_exit = false;
  // Slice E-2a CONFIG PARITY FIX: `storage::Config` defaults to `properties_on_edges = true`,
  // `enable_edges_metadata = false` -- but main may have been configured differently. These two
  // flags aren't just behavioral toggles: `storage::EdgeAccessor::Gid()` (edge_accessor.cpp) reads
  // a PHYSICALLY DIFFERENT field depending on `properties_on_edges` (a heavy `Edge*`-derived gid
  // vs. a light `EdgeRef`-embedded one, see `GidPropertiesOnEdges`/`GidNoPropertiesOnEdges` above
  // it), and edge property reads/writes reject outright when the flag is off (`Error::
  // PROPERTIES_DISABLED`). A diff engine built with the WRONG value for either flag would silently
  // decode/reject edges incompatibly with how `historical_`'s (main's) own edges actually behave --
  // exactly the same class of divergence the shared-NameIdMapper comment above already guards
  // against for labels/properties, just on the edge-layout axis instead. Copied directly (no
  // translation needed, unlike ids): both are plain bools on `main`'s own `Config`.
  config.salient.items.properties_on_edges = main.config_.salient.items.properties_on_edges;
  config.salient.items.enable_edges_metadata = main.config_.salient.items.enable_edges_metadata;
  auto diff_engine = std::make_unique<storage::InMemoryStorage>(config,
                                                                std::nullopt,
                                                                std::make_unique<storage::PlanInvalidatorDefault>(),
                                                                metrics::DatabaseMetricHandles{},
                                                                nullptr,
                                                                nullptr,
                                                                nullptr,
                                                                main.GetSharedNameIdMapper());

  // -----------------------------------------------------------------------------------------
  // 3. Reserve the branch-native gid range -- see kBranchNativeGidWatermark's doc-comment. The
  //    ONLY non-lazy, non-O(1)-in-spirit-but-actually-O(1)-in-practice step BuildFromFork performs.
  // -----------------------------------------------------------------------------------------
  ReserveBranchNativeGidRange(*diff_engine, std::move(commit_args));

  // Private ctor -- constructed via `new` rather than std::make_unique (which needs public ctor
  // access), from inside this static member function which does have that access.
  return std::unique_ptr<BranchContext>(new BranchContext(std::move(diff_engine), std::move(historical)));
}

std::expected<storage::VertexAccessor, BranchContext::CowError> BranchContext::CowVertex(storage::Gid gid) {
  // MED FIX (adversarial-review): this was DMG_ASSERT (a no-op in release builds) -- a null
  // current_diff_txn_ here means CurrentDB::SetupDatabaseTransaction never ran, a real invariant
  // break, not a state release builds should silently continue past into a null-deref a few lines
  // down. MG_ASSERT (always active) turns that into a controlled abort instead.
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::CowVertex: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call set_current_diff_txn() before any query "
            "runs against a checked-out branch.");
  storage::Storage::Accessor *diff_txn = current_diff_txn_;

  // Idempotent: a prior COW (or a branch-native create sharing this gid -- impossible per the
  // gid-watermark reservation, but FindVertex is the correct check regardless) already resident in
  // the diff engine wins outright. View::NEW so this transaction sees its own prior writes.
  if (auto existing = diff_txn->FindVertex(gid, storage::View::NEW)) {
    return *existing;
  }

  auto hist_vertex = historical_base_->FindVertex(gid, storage::View::OLD);
  if (!hist_vertex) {
    // Defensive: every caller resolves a vertex (ResolveVertex, or a prior Vertices() yield) before
    // ever calling CowVertex on its gid, so the gid is expected to exist in one of the two stores
    // -- and the diff-engine check above already ruled that side out.
    return std::unexpected(
        CowError{fmt::format("Cannot copy-on-write vertex {}: not found in the branch's fork-state base either -- "
                             "this should be unreachable (callers resolve a vertex before mutating it).",
                             gid.AsUint())});
  }

  auto labels_res = hist_vertex->Labels(storage::View::OLD);
  MG_ASSERT(labels_res.has_value(),
            "BranchContext::CowVertex: failed to read fork-state labels for vertex {}.",
            gid.AsUint());
  auto props_res = hist_vertex->Properties(storage::View::OLD);
  MG_ASSERT(props_res.has_value(),
            "BranchContext::CowVertex: failed to read fork-state properties for vertex {}.",
            gid.AsUint());

  // Enum check BEFORE touching the diff engine at all: if this rejects, the diff engine must be
  // left exactly as it was (no partially-COW'd vertex) -- a half-created vertex would be
  // indistinguishable from a genuine successful COW on the NEXT call for this same gid (the
  // idempotency check above would find it and silently return the incomplete copy).
  for (const auto &[pid, val] : *props_res) {
    if (ContainsEnum(val)) {
      return std::unexpected(CowError{
          fmt::format("Cannot copy-on-write vertex {}: has an enum property, which versioned branches do not yet "
                      "support.",
                      gid.AsUint())});
    }
  }

  // NO by-name translation: diff_engine_ SHARES main's own NameIdMapper instance (see
  // BuildFromFork's own doc-comment) -- one id space, so historical_'s label/property ids are
  // ALREADY valid, directly, in the diff engine. (The predecessor BranchEngine, and an earlier
  // version of this function, translated ids by name through two numerically-unrelated mappers;
  // that's gone now that there is only one mapper to begin with.)
  //
  // `diff_txn`'s actual object is guaranteed to be a ReplicationAccessor (or an InMemoryAccessor
  // whose layout ReplicationAccessor doesn't extend -- it adds no data members, only public
  // forwarding wrappers) -- mirrors the identical, already-reviewed idiom in the predecessor
  // BranchEngine and versioning::MergeBranch for reaching CreateVertexEx/CreateEdgeEx.
  auto *replication_accessor = static_cast<storage::ReplicationAccessor *>(diff_txn);
  auto nv = replication_accessor->CreateVertexEx(gid);
  MG_ASSERT(nv.has_value(),
            "BranchContext::CowVertex: gid {} collided while COW'ing into the diff engine -- should be impossible "
            "(the idempotency check above already ruled out a prior occupant, and the gid-watermark reservation "
            "keeps branch-native creates out of historical_'s gid range).",
            gid.AsUint());

  for (auto lbl : *labels_res) {
    auto add_res = nv->AddLabel(lbl);
    MG_ASSERT(
        add_res.has_value(), "BranchContext::CowVertex: failed to add a label while COW'ing vertex {}.", gid.AsUint());
  }
  for (const auto &[pid, val] : *props_res) {
    auto set_res = nv->SetProperty(pid, val);
    MG_ASSERT(set_res.has_value(),
              "BranchContext::CowVertex: failed to set a property while COW'ing vertex {}.",
              gid.AsUint());
  }

  return *nv;
}

std::optional<storage::VertexAccessor> BranchContext::ResolveVertex(storage::Gid gid, storage::View view) {
  // MED FIX (adversarial-review): MG_ASSERT, not DMG_ASSERT -- see CowVertex's own comment above.
  // ResolveVertex is now on the hot read path too (HIGH-2's self-correcting reads call it from
  // every VertexAccessor::Labels/Properties/GetProperty/HasLabel/GetPropertySize), so a release
  // build silently null-dereferencing here would be reachable far more often than before.
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::ResolveVertex: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call set_current_diff_txn() before any query "
            "runs against a checked-out branch.");
  if (auto diff_vertex = current_diff_txn_->FindVertex(gid, view)) {
    return diff_vertex;
  }
  return historical_base_->FindVertex(gid, storage::View::OLD);
}

// Graph Versioning v1, slice E-2a -- TODO(E-2d), NOT CURRENTLY CALLED: see the declaration's own
// doc-comment (branch_engine.hpp) for why `DbAccessor::FindEdge` does not use this today (the
// `historical_->FindEdge` half was adversarially found to not reliably resolve a historical edge by
// bare gid -- this shape mirrors ResolveVertex, but that mirroring was NOT verified sufficient for
// edges). Kept as an unverified starting point for E-2d, not deleted.
std::optional<storage::EdgeAccessor> BranchContext::ResolveEdge(storage::Gid edge_gid, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::ResolveEdge: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call set_current_diff_txn() before any query "
            "runs against a checked-out branch.");
  if (tombstoned_edges_.contains(edge_gid)) return std::nullopt;
  if (auto diff_edge = current_diff_txn_->FindEdge(edge_gid, view)) {
    return diff_edge;
  }
  return historical_base_->FindEdge(edge_gid, storage::View::OLD);
}

// Graph Versioning v1, slice E-2a -- see the declaration's own doc-comment (branch_engine.hpp) for
// the full historical-vs-diff union/tie-break rationale; this mirrors ResolveVertex's fixed-
// View::OLD-for-historical_ convention exactly, for the same reason (historical_ is a frozen,
// self-pinned snapshot -- it has no "NEW" relative to this or any other transaction).
std::vector<storage::EdgeAccessor> BranchContext::ResolveEdges(storage::Gid vertex_gid,
                                                               storage::EdgeDirection direction, storage::View view,
                                                               const std::vector<storage::EdgeTypeId> &edge_types) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::ResolveEdges: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call set_current_diff_txn() before any query "
            "runs against a checked-out branch.");

  std::vector<storage::EdgeAccessor> result;
  // gid -> index into `result`, so a later (diff-side) hit can overwrite an earlier
  // (historical-side) entry for the same gid -- the diff engine's copy is always authoritative on a
  // tie, mirroring UnionVerticesIterable::Iterator::SeekNext's own tie-break (branch_engine.cpp,
  // above).
  std::unordered_map<storage::Gid, size_t> seen;

  auto collect = [&](storage::VertexAccessor &v, storage::View collect_view, bool diff_wins) {
    auto maybe_result = (direction == storage::EdgeDirection::OUT) ? v.OutEdges(collect_view, edge_types, nullptr)
                                                                   : v.InEdges(collect_view, edge_types, nullptr);
    if (!maybe_result.has_value()) return;
    for (auto &edge : maybe_result->edges) {
      // Tombstone hook (inert this slice -- tombstoned_edges_ is never populated, see its own
      // doc-comment): once E-4 lands, a tombstoned gid must never enter `result`, from EITHER side.
      if (tombstoned_edges_.contains(edge.Gid())) continue;

      auto [it, inserted] = seen.try_emplace(edge.Gid(), result.size());
      if (inserted) {
        result.push_back(edge);
      } else if (diff_wins) {
        result[it->second] = edge;
      }
    }
  };

  if (auto hist_vertex = historical_base_->FindVertex(vertex_gid, storage::View::OLD)) {
    collect(*hist_vertex, storage::View::OLD, /*diff_wins=*/false);
  }
  if (auto diff_vertex = current_diff_txn_->FindVertex(vertex_gid, view)) {
    collect(*diff_vertex, view, /*diff_wins=*/true);
  }

  return result;
}

BranchContext::UnionVerticesIterable BranchContext::Vertices(storage::View view) {
  // MED FIX (adversarial-review): MG_ASSERT, not DMG_ASSERT -- see CowVertex's own comment above.
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::Vertices: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call set_current_diff_txn() before any query "
            "runs against a checked-out branch.");
  return UnionVerticesIterable(historical_base_->Vertices(storage::View::OLD), current_diff_txn_->Vertices(view));
}

BranchContext::UnionVerticesIterable::UnionVerticesIterable(storage::VerticesIterable hist_vertices,
                                                            storage::VerticesIterable diff_vertices)
    : hist_vertices_(std::move(hist_vertices)), diff_vertices_(std::move(diff_vertices)) {}

BranchContext::UnionVerticesIterable::Iterator BranchContext::UnionVerticesIterable::begin() {
  return Iterator(hist_vertices_.begin(), hist_vertices_.end(), diff_vertices_.begin(), diff_vertices_.end());
}

BranchContext::UnionVerticesIterable::Iterator::Iterator(storage::VerticesIterable::Iterator hist_it,
                                                         storage::VerticesIterable::Iterator hist_end,
                                                         storage::VerticesIterable::Iterator diff_it,
                                                         storage::VerticesIterable::Iterator diff_end)
    : hist_it_(std::move(hist_it)),
      hist_end_(std::move(hist_end)),
      diff_it_(std::move(diff_it)),
      diff_end_(std::move(diff_end)) {
  SeekNext();
}

BranchContext::UnionVerticesIterable::Iterator &BranchContext::UnionVerticesIterable::Iterator::operator++() {
  SeekNext();
  return *this;
}

// The streaming gid-ordered merge -- see the class comment in branch_engine.hpp for the full
// O(H + D) argument. No tombstone/skip case this slice (no delete op exists yet), so -- unlike
// BranchReconstruction::UnionVerticesIterable::Iterator::SeekNext, which needs a `continue` loop to
// skip tombstoned gids -- every call here returns after advancing exactly one (or, on a gid tie,
// both) of the two cursors.
void BranchContext::UnionVerticesIterable::Iterator::SeekNext() {
  const bool hist_has = hist_it_.has_value() && !(*hist_it_ == *hist_end_);
  const bool diff_has = diff_it_.has_value() && !(*diff_it_ == *diff_end_);

  if (!hist_has && !diff_has) {
    current_.reset();
    done_ = true;
    return;
  }

  if (hist_has && diff_has) {
    const auto hist_gid = (**hist_it_).Gid();
    const auto diff_gid = (**diff_it_).Gid();

    if (hist_gid < diff_gid) {
      current_ = **hist_it_;
      ++(*hist_it_);
    } else if (diff_gid < hist_gid) {
      current_ = **diff_it_;
      ++(*diff_it_);
    } else {
      // Tie: the diff engine's copy IS the branch's authoritative view of this gid (a COW'd,
      // possibly-modified copy) -- it wins outright; historical_'s fork-state copy is superseded
      // wholesale, never field-merged. Both cursors advance past this gid.
      current_ = **diff_it_;
      ++(*hist_it_);
      ++(*diff_it_);
    }
    done_ = false;
    return;
  }

  if (hist_has) {
    current_ = **hist_it_;
    ++(*hist_it_);
  } else {
    current_ = **diff_it_;
    ++(*diff_it_);
  }
  done_ = false;
}

}  // namespace memgraph::versioning

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

#include "storage/v2/durability/wal.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/logging.hpp"
#include "utils/uuid.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::versioning {

namespace {

namespace sd = storage::durability;

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

// Graph Versioning v1, slice D (replay-on-checkout): replays `changelog` -- the branch's ALREADY-
// captured, real change-log from every PRIOR, finalized checkout session (the caller gathers this
// via `CollectBranchChangelog`, interpreter.cpp -- the exact same enumeration MERGE BRANCH uses;
// NEVER includes the CURRENT session's own BranchLog, which BuildFromFork only opens AFTER this
// call returns -- see BuildFromFork's own call-site comment) into a freshly-built, otherwise-empty
// `diff_engine`. Two things this buys, from the SAME mechanism:
//   (a) the session sees the branch's own prior writes immediately upon checkout -- read-your-
//       branch's-own-writes across a checkout cycle, not just within one still-open session;
//   (b) every replayed explicit-gid create (CreateVertexEx/CreateEdgeEx) advances the diff
//       engine's OWN vertex_id_/edge_id_ counter past that gid -- an atomic_fetch_max baked into
//       both primitives themselves (inmemory/storage.cpp, see kBranchNativeGidWatermark's own
//       doc-comment above) -- so a SUBSEQUENT branch-native auto-gid create in THIS session can
//       never reuse a gid a PRIOR session already captured. THIS is what actually fixes the
//       cross-session crash: two independent checkout sessions used to each restart their own
//       auto-gid counter from the SAME watermark, so their first branch-native creates collided
//       the instant a MERGE concatenated both sessions' change-logs into one replay (a corrupt/
//       aliased vertex tripping the NULL_PTR delta MG_ASSERT in storage.cpp's append_deltas).
//
// FAITHFUL REPLAY, NOT A MERGE: unlike versioning::MergeBranch, this never remaps a gid and never
// conflict-checks anything against a live "main" -- `diff_engine` is freshly built and otherwise
// empty (modulo the reserved-and-freed watermark placeholder objects, see
// ReserveBranchNativeGidRange, ALWAYS run before this -- see BuildFromFork's own call ordering), so
// every explicit-gid create is EXPECTED to succeed outright; a collision here would mean the
// watermark reservation itself is broken, not a legitimate replay conflict -- surfaced as
// MG_ASSERT, mirroring ReserveBranchNativeGidRange's own style, not a recoverable error path.
//
// ORDERING WITHIN THE CHANGELOG (verified, not assumed): a WalEdgeCreate record's own endpoints
// must already be live, same-transaction VertexAccessors for CreateEdgeEx to accept them
// (InMemoryAccessor::CreateEdgeEx MG_ASSERTs this). This is always satisfied by a single, in-order
// forward pass over `changelog` because (i) CollectBranchChangelog concatenates PRIOR sessions'
// files in real CHRONOLOGICAL (wall-clock finalization) order, and (ii) WITHIN one captured commit,
// CaptureBranchCommit (branch_reconstruction.cpp) walks that commit's deltas in the order the
// storage engine itself produced them, which can never place an edge's ADD_OUT_EDGE delta before
// its endpoint vertices' own creation deltas -- Cypher cannot reference a pattern variable
// (`CREATE (a)-[:R]->(b)`) before the CREATE clause has bound it, so the endpoint VertexAccessors
// always exist (and therefore their own creation deltas were already recorded) before the edge
// between them can be created. No separate vertex-then-edge pass is needed.
//
// Scope: the operations CaptureBranchCommit ever actually produces (vertex/edge create, label
// add, property set on either, and -- as of slice E-4 -- vertex/edge DELETE) are handled;
// WalTransactionStart/WalTransactionEnd are pure delimiters. Everything else is a silent no-op,
// mirroring versioning::MergeBranch's own "everything else is out of scope" catch-all (merge.cpp).
//
// E-4 ADDITION: `tombstoned_vertices`/`tombstoned_edges` are OUT parameters -- this function runs
// INSIDE `BuildFromFork`, strictly BEFORE the `BranchContext` object it will end up feeding these
// into is constructed (its private ctor needs them up front, see BuildFromFork's own call site
// below), so there is no `BranchContext::TombstoneVertex/TombstoneEdge` to call yet. A prior
// session's own captured delete is replayed as a REAL delete against the fresh diff engine (plain
// `DeleteVertex`/`DeleteEdge`, never `DetachDelete` with a cascade -- the ORIGINAL delete already
// passed the "no incident edges" branch-aware guard when it first ran, query::DbAccessor::
// RemoveVertex/RemoveEdge/DetachDelete, db_accessor.hpp; replaying it in the SAME relative order
// re-creates that same edge-free state before the delete is replayed), then the gid is recorded in
// the OUT set exactly as `TombstoneVertex`/`TombstoneEdge` would.
void ReplayChangelogIntoDiffEngine(storage::InMemoryStorage &diff_engine,
                                   const std::vector<storage::durability::WalDeltaData> &changelog,
                                   storage::CommitArgs commit_args,
                                   std::unordered_set<storage::Gid> &tombstoned_vertices,
                                   std::unordered_set<storage::Gid> &tombstoned_edges) {
  if (changelog.empty()) return;  // fresh branch, first checkout -- nothing to replay

  std::unique_ptr<storage::ReplicationAccessor> replay(
      static_cast<storage::ReplicationAccessor *>(diff_engine.Access(storage::StorageAccessType::WRITE).release()));

  auto apply = utils::Overloaded{
      [&](sd::WalVertexCreate const &data) {
        auto v = replay->CreateVertexEx(data.gid);
        MG_ASSERT(v.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout found vertex gid {} already occupied in a "
                  "freshly-built, otherwise-empty diff engine -- should be impossible.",
                  data.gid.AsUint());
      },
      [&](sd::WalVertexAddLabel const &data) {
        auto v = replay->FindVertex(data.gid, storage::View::NEW);
        MG_ASSERT(v.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout could not find vertex {} to add a label to -- "
                  "the branch's own captured change-log should always create a vertex before mutating it.",
                  data.gid.AsUint());
        auto ret = v->AddLabel(replay->NameToLabel(data.label));
        MG_ASSERT(ret.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout failed to add a label to vertex {}.",
                  data.gid.AsUint());
      },
      [&](sd::WalVertexSetProperty const &data) {
        auto v = replay->FindVertex(data.gid, storage::View::NEW);
        MG_ASSERT(v.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout could not find vertex {} to set a property on.",
                  data.gid.AsUint());
        auto ret = v->SetProperty(replay->NameToProperty(data.property),
                                  storage::ToPropertyValue(data.value, replay->GetNameIdMapper()));
        MG_ASSERT(ret.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout failed to set a property on vertex {}.",
                  data.gid.AsUint());
      },
      [&](sd::WalEdgeCreate const &data) {
        // View::NEW: an endpoint may have been created earlier in THIS SAME replay pass (same
        // command_id throughout -- AdvanceCommand is never called here).
        auto from_v = replay->FindVertex(data.from_vertex, storage::View::NEW);
        auto to_v = replay->FindVertex(data.to_vertex, storage::View::NEW);
        MG_ASSERT(from_v.has_value() && to_v.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout could not find edge {}'s endpoints -- the "
                  "branch's own captured change-log should always create both endpoints before the edge (see "
                  "this function's own ORDERING doc-comment).",
                  data.gid.AsUint());
        auto edge_type = replay->NameToEdgeType(data.edge_type);
        auto e = replay->CreateEdgeEx(&*from_v, &*to_v, edge_type, data.gid);
        MG_ASSERT(e.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout found edge gid {} already occupied -- should "
                  "be impossible.",
                  data.gid.AsUint());
      },
      [&](sd::WalEdgeSetProperty const &data) {
        // Diff-engine-ONLY bare-gid lookup -- mirrors BranchContext::FindDiffEdge exactly (this IS
        // the diff engine; there is no historical_ fallback to consider during replay, so none of
        // HistoricalAccess::FindEdge's documented unreliability applies here).
        auto e = replay->FindEdge(data.gid, storage::View::NEW);
        MG_ASSERT(e.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout could not find edge {} to set a property on.",
                  data.gid.AsUint());
        auto ret = e->SetProperty(replay->NameToProperty(data.property),
                                  storage::ToPropertyValue(data.value, replay->GetNameIdMapper()));
        MG_ASSERT(ret.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout failed to set a property on edge {}.",
                  data.gid.AsUint());
      },
      // Graph Versioning v1, slice E-4: replay a prior session's own captured vertex DELETE. Plain
      // `DeleteVertex` (NEVER `DetachDelete` with a cascade) -- see this function's own top-of-file
      // doc-comment for why the vertex is guaranteed edge-free by the time this replays. View::NEW:
      // the vertex may have been (re-)created earlier in THIS SAME replay pass.
      [&](sd::WalVertexDelete const &data) {
        auto v = replay->FindVertex(data.gid, storage::View::NEW);
        MG_ASSERT(v.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout could not find vertex {} to delete -- the "
                  "branch's own captured change-log should always create a vertex before deleting it.",
                  data.gid.AsUint());
        auto ret = replay->DeleteVertex(&*v);
        MG_ASSERT(ret.has_value() && ret->has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout failed to delete vertex {}.",
                  data.gid.AsUint());
        tombstoned_vertices.insert(data.gid);
      },
      // Symmetric for edges -- bare diff-engine-only gid lookup, same reasoning as WalEdgeSetProperty
      // above (this IS the diff engine, so none of historical_->FindEdge's documented unreliability
      // for bare-gid edge lookups applies here).
      [&](sd::WalEdgeDelete const &data) {
        auto e = replay->FindEdge(data.gid, storage::View::NEW);
        MG_ASSERT(e.has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout could not find edge {} to delete.",
                  data.gid.AsUint());
        auto ret = replay->DeleteEdge(&*e);
        MG_ASSERT(ret.has_value() && ret->has_value(),
                  "BranchContext::BuildFromFork: replay-on-checkout failed to delete edge {}.",
                  data.gid.AsUint());
        tombstoned_edges.insert(data.gid);
      },
      [&](sd::WalTransactionStart const &) {},
      [&](sd::WalTransactionEnd const &) {},
      [&](auto const &) {}};

  for (const auto &delta : changelog) {
    std::visit(apply, delta.data_);
  }

  auto commit_res = replay->PrepareForCommitPhase(std::move(commit_args));
  MG_ASSERT(commit_res.has_value(), "BranchContext::BuildFromFork: replay-on-checkout commit failed.");
}

}  // namespace

std::expected<std::unique_ptr<BranchContext>, BranchContext::BuildError> BranchContext::BuildFromFork(
    storage::InMemoryStorage &main, uint64_t fork_ts, storage::CommitArgs commit_args,
    storage::CommitArgs replay_commit_args, std::filesystem::path branch_wal_root_directory,
    const std::vector<storage::durability::WalDeltaData> &changelog) {
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

  // -----------------------------------------------------------------------------------------
  // 3b. Slice D (replay-on-checkout): replay the branch's own already-captured history (every
  //     PRIOR, finalized checkout session's BranchLog, concatenated by the caller via
  //     CollectBranchChangelog -- see ReplayChangelogIntoDiffEngine's own doc-comment) into the
  //     diff engine BEFORE it is ever exposed to a query.
  //
  //     ORDERING vs. step 3 (VERIFIED, not just asserted): placed AFTER the watermark reservation,
  //     but this is not load-bearing for correctness -- CreateVertexEx/CreateEdgeEx's own
  //     atomic_fetch_max means the counter ends up in the same place regardless of call order
  //     (whichever of the two runs second just finds the counter already past its own target and
  //     no-ops the bump), and reservation's OWN explicit-gid target is the EXACT watermark value,
  //     which no replayed record (branch-native gids are always watermark+1 or higher; COW-mirrored
  //     gids sit in main's own low range) can ever equal, so there is no collision risk swapping
  //     the order either way. Kept AFTER purely for a simpler invariant to reason about: every
  //     MG_ASSERT in ReserveBranchNativeGidRange above talks about "a freshly-created, EMPTY diff
  //     engine" -- true in the literal sense only if nothing (including replayed history) has
  //     touched it yet. A no-op for an empty `changelog` (this branch's first-ever checkout).
  // -----------------------------------------------------------------------------------------
  // Slice E-4: OUT parameters -- populated by any WalVertexDelete/WalEdgeDelete records replayed
  // above, fed straight into the BranchContext private ctor below (see ReplayChangelogIntoDiffEngine's
  // own doc-comment for why this function cannot call TombstoneVertex/TombstoneEdge directly: the
  // BranchContext object they belong to doesn't exist yet at this point in BuildFromFork).
  std::unordered_set<storage::Gid> tombstoned_vertices;
  std::unordered_set<storage::Gid> tombstoned_edges;
  ReplayChangelogIntoDiffEngine(
      *diff_engine, changelog, std::move(replay_commit_args), tombstoned_vertices, tombstoned_edges);

  // -----------------------------------------------------------------------------------------
  // 4. Durable-capture slice, reshaped by the MULTI-COMMIT fix (2026-07-12): rather than opening
  //    ONE long-lived BranchLog here that would accumulate every commit this session makes (proven
  //    broken -- see `CreateCommitLog()`'s own doc-comment, branch_engine.hpp, for the
  //    `ReadWalInfo` multi-transaction round-trip bug this sidesteps), this just mints THIS
  //    session's own fresh, per-session-unique subdirectory under `branch_wal_root_directory` and
  //    stashes the ingredients (`config.salient.items`, main's own shared NameIdMapper)
  //    `CreateCommitLog()` needs to build a brand-new `BranchLog` for EACH individual commit, on
  //    demand, later. Every per-commit `BranchLog` still lands in this SAME session subdirectory --
  //    collision safety across DIFFERENT sessions is what matters (see the member doc-comment this
  //    used to sit on, now moved to `branch_log_session_directory_`), not across commits within one
  //    session, since each per-commit BranchLog gets its own fresh wall-clock-named file there.
  //    Shares `config.salient.items` (built above, main's own properties_on_edges/
  //    enable_edges_metadata parity) and main's own shared NameIdMapper: the branch log's own WAL
  //    encoding must match how `CaptureBranchCommit`'s `target_storage` argument (this same diff
  //    engine) actually lays out edges/ids, or MERGE's later `BranchLog::ReadAll` would silently
  //    misdecode it -- the identical reasoning the diff engine's own construction above already
  //    relies on.
  // -----------------------------------------------------------------------------------------
  auto branch_log_session_directory = branch_wal_root_directory / utils::GenerateUUID();

  // Private ctor -- constructed via `new` rather than std::make_unique (which needs public ctor
  // access), from inside this static member function which does have that access.
  // Chunk 10 (D5/R13): seed the cumulative changelog-length counter with the replayed changelog's
  // own size -- every record captured across all PRIOR sessions of this branch -- so the retention
  // cap (`FLAGS_versioning_max_changelog_length`) is enforced against the branch's whole life, not
  // reset back to 0 on every fresh checkout.
  return std::unique_ptr<BranchContext>(new BranchContext(std::move(diff_engine),
                                                          std::move(historical),
                                                          std::move(tombstoned_vertices),
                                                          std::move(tombstoned_edges),
                                                          std::move(branch_log_session_directory),
                                                          config.salient.items,
                                                          main.GetSharedNameIdMapper().get(),
                                                          changelog.size()));
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

// Graph Versioning v1, slice E-2c -- see the declaration's own doc-comment (branch_engine.hpp) for
// what `fork_edge` is expected to be and why it's passed in whole rather than re-found by gid.
std::expected<storage::EdgeAccessor, BranchContext::CowError> BranchContext::CowEdge(
    const storage::EdgeAccessor &fork_edge) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::CowEdge: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call set_current_diff_txn() before any query "
            "runs against a checked-out branch.");

  const auto edge_gid = fork_edge.Gid();

  // Idempotent: a prior COW (or a branch-native create sharing this gid) already resident in the
  // diff engine wins outright -- mirrors CowVertex's own idempotency check exactly. View::NEW so
  // this transaction sees its own prior writes. NOTE: `fork_edge`'s endpoints/properties are NOT
  // read below this point in this branch -- safe even when `fork_edge` itself already IS this same
  // diff-resident copy (e.g. a second SET in one statement).
  if (auto existing = FindDiffEdge(edge_gid, storage::View::NEW)) {
    return *existing;
  }

  // Not yet COW'd: `fork_edge` must still be historical_'s own copy. Its endpoints are therefore
  // historical_ vertices too (or already-COW'd ones, if some earlier statement touched them
  // directly) -- CowVertex resolves either case idempotently, mirroring
  // DbAccessor::InsertEdge's own COW-both-endpoints idiom (db_accessor.hpp).
  const auto from_gid = fork_edge.FromVertex().Gid();
  const auto to_gid = fork_edge.ToVertex().Gid();
  const auto edge_type = fork_edge.EdgeType();

  auto props_res = fork_edge.Properties(storage::View::OLD);
  MG_ASSERT(props_res.has_value(),
            "BranchContext::CowEdge: failed to read fork-state properties for edge {}.",
            edge_gid.AsUint());

  // Enum check BEFORE touching the diff engine AT ALL -- including before COW'ing the endpoints
  // below (E-2c review MED fix): a rejected COW must leave ZERO diff-engine side effects, else the
  // two endpoint vertex copies would be silently promoted into the diff engine for an edge COW that
  // never completed. (CowVertex's own enum check genuinely precedes its own CreateVertexEx; this
  // must match that guarantee for the whole edge COW, endpoints included.)
  for (const auto &[pid, val] : *props_res) {
    if (ContainsEnum(val)) {
      return std::unexpected(CowError{
          fmt::format("Cannot copy-on-write edge {}: has an enum property, which versioned branches do not yet "
                      "support.",
                      edge_gid.AsUint())});
    }
  }

  // Endpoints COW'd only after the edge itself passed the enum gate (see above). `fork_edge`'s
  // endpoints are historical_ vertices (or already-COW'd ones) -- CowVertex resolves either case
  // idempotently, mirroring DbAccessor::InsertEdge's own COW-both-endpoints idiom (db_accessor.hpp).
  auto from_diff = CowVertex(from_gid);
  if (!from_diff) return std::unexpected(from_diff.error());
  auto to_diff = CowVertex(to_gid);
  if (!to_diff) return std::unexpected(to_diff.error());

  // Same idiom as CowVertex: `diff_txn`'s actual object is guaranteed to be a ReplicationAccessor
  // (or an InMemoryAccessor whose layout ReplicationAccessor doesn't extend), reached through
  // `current_diff_txn_` rather than a parameter (see its own doc-comment).
  auto *replication_accessor = static_cast<storage::ReplicationAccessor *>(current_diff_txn_);
  // CreateEdgeEx PRESERVES `edge_gid` (unlike the auto-gid `CreateEdge` InsertEdge uses for a
  // brand-new branch-native edge) -- essential so ResolveEdges' historical-vs-diff de-dupe (which
  // keys purely on gid) still recognizes this as the SAME logical edge as historical_'s copy, not a
  // second, unrelated one.
  auto nv = replication_accessor->CreateEdgeEx(&*from_diff, &*to_diff, edge_type, edge_gid);
  MG_ASSERT(nv.has_value(),
            "BranchContext::CowEdge: gid {} collided while COW'ing into the diff engine -- should be impossible "
            "(the idempotency check above already ruled out a prior occupant, and the gid-watermark reservation "
            "keeps branch-native creates out of historical_'s gid range).",
            edge_gid.AsUint());

  for (const auto &[pid, val] : *props_res) {
    auto set_res = nv->SetProperty(pid, val);
    MG_ASSERT(set_res.has_value(),
              "BranchContext::CowEdge: failed to set a property while COW'ing edge {}.",
              edge_gid.AsUint());
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
  // Graph Versioning v1, slice E-4: checked FIRST, unconditionally -- see `tombstoned_vertices_`'s
  // own doc-comment (branch_engine.hpp) for why a bare diff-engine miss alone cannot tell "never
  // touched, fall through to historical_" apart from "explicitly deleted, hide" (both look like
  // nullopt to the diff engine for a not-yet-COW'd fork vertex).
  if (tombstoned_vertices_.contains(gid)) return std::nullopt;
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

// Graph Versioning v1, slice E-2c -- see the declaration's own doc-comment (branch_engine.hpp) for
// why this is deliberately narrower than ResolveEdge above (diff engine only, no historical_ half
// at all -- so none of ResolveEdge's own documented edge-lookup unreliability applies here).
std::optional<storage::EdgeAccessor> BranchContext::FindDiffEdge(storage::Gid edge_gid, storage::View view) {
  MG_ASSERT(current_diff_txn_ != nullptr,
            "BranchContext::FindDiffEdge: no current diff transaction set -- "
            "CurrentDB::SetupDatabaseTransaction must call set_current_diff_txn() before any query "
            "runs against a checked-out branch.");
  return current_diff_txn_->FindEdge(edge_gid, view);
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
      // Tombstone hook (ACTIVE as of slice E-4 -- tombstoned_edges_ is populated by
      // query::DbAccessor::RemoveEdge/DetachDelete and by ReplayChangelogIntoDiffEngine's own
      // WalEdgeDelete handler, see its own doc-comment, branch_engine.hpp): a tombstoned gid must
      // never enter `result`, from EITHER side.
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
  return UnionVerticesIterable(
      historical_base_->Vertices(storage::View::OLD), current_diff_txn_->Vertices(view), &tombstoned_vertices_);
}

BranchContext::UnionVerticesIterable::UnionVerticesIterable(storage::VerticesIterable hist_vertices,
                                                            storage::VerticesIterable diff_vertices,
                                                            const std::unordered_set<storage::Gid> *tombstoned_vertices)
    : hist_vertices_(std::move(hist_vertices)),
      diff_vertices_(std::move(diff_vertices)),
      tombstoned_vertices_(tombstoned_vertices) {}

BranchContext::UnionVerticesIterable::Iterator BranchContext::UnionVerticesIterable::begin() {
  return Iterator(
      hist_vertices_.begin(), hist_vertices_.end(), diff_vertices_.begin(), diff_vertices_.end(), tombstoned_vertices_);
}

BranchContext::UnionVerticesIterable::Iterator::Iterator(storage::VerticesIterable::Iterator hist_it,
                                                         storage::VerticesIterable::Iterator hist_end,
                                                         storage::VerticesIterable::Iterator diff_it,
                                                         storage::VerticesIterable::Iterator diff_end,
                                                         const std::unordered_set<storage::Gid> *tombstoned_vertices)
    : hist_it_(std::move(hist_it)),
      hist_end_(std::move(hist_end)),
      diff_it_(std::move(diff_it)),
      diff_end_(std::move(diff_end)),
      tombstoned_vertices_(tombstoned_vertices) {
  SeekNext();
}

BranchContext::UnionVerticesIterable::Iterator &BranchContext::UnionVerticesIterable::Iterator::operator++() {
  SeekNext();
  return *this;
}

// The streaming gid-ordered merge -- see the class comment in branch_engine.hpp for the full
// O(H + D) argument. Graph Versioning v1, slice E-4: a `while (true) { ... continue; }` skip-loop
// (mirrors BranchReconstruction::UnionVerticesIterable::Iterator::SeekNext exactly,
// branch_reconstruction.cpp) -- needed now that a gid CAN be tombstoned. Only the historical_-side
// yield paths ever need the check: a diff-engine-resident gid that has been deleted is ALREADY
// excluded by the diff engine's own native MVCC visibility (its `Vertices(view)` scan never yields a
// NEW-deleted object in the first place -- `tombstoned_vertices_` only ever matters for a gid that
// the diff engine itself has NO knowledge of having been touched at all, i.e. a not-yet-COW'd fork
// vertex the branch deleted directly). Every branch below still advances at least one cursor per
// iteration (including the tombstone-skip `continue`s), so the loop terminates.
void BranchContext::UnionVerticesIterable::Iterator::SeekNext() {
  while (true) {
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
        // historical_-only for this gid (the diff engine hasn't reached it yet) -- skip if
        // tombstoned, mirroring the hist_has-only case below exactly.
        const bool tombstoned = tombstoned_vertices_ != nullptr && tombstoned_vertices_->contains(hist_gid);
        auto vertex_copy = **hist_it_;
        ++(*hist_it_);
        if (tombstoned) continue;
        current_ = vertex_copy;
        done_ = false;
        return;
      }
      if (diff_gid < hist_gid) {
        current_ = **diff_it_;
        ++(*diff_it_);
        done_ = false;
        return;
      }
      // Tie: the diff engine's copy IS the branch's authoritative view of this gid (a COW'd,
      // possibly-modified copy) -- it wins outright; historical_'s fork-state copy is superseded
      // wholesale, never field-merged. Both cursors advance past this gid. No tombstone check
      // needed here: a diff-resident gid that was deleted is already excluded by the diff engine's
      // own scan (see this function's own top-of-file comment).
      current_ = **diff_it_;
      ++(*hist_it_);
      ++(*diff_it_);
      done_ = false;
      return;
    }

    if (hist_has) {
      const auto hist_gid = (**hist_it_).Gid();
      const bool tombstoned = tombstoned_vertices_ != nullptr && tombstoned_vertices_->contains(hist_gid);
      auto vertex_copy = **hist_it_;
      ++(*hist_it_);
      if (tombstoned) continue;
      current_ = vertex_copy;
      done_ = false;
      return;
    }

    // diff_has only.
    current_ = **diff_it_;
    ++(*diff_it_);
    done_ = false;
    return;
  }
}

}  // namespace memgraph::versioning

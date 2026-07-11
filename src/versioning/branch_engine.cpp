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

#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/logging.hpp"

namespace memgraph::versioning {

namespace {

// Recursive Enum detector: true iff `v` itself is an Enum, or is a List/Map that contains one at
// any depth. This is the load-bearing check for the kUnsupportedEnumProperty rejection below -- a
// top-level `v.IsEnum()` check alone would miss an Enum tucked inside a List or Map property (e.g.
// `[1, myEnum]` or `{k: myEnum}`), silently letting it through to be copied verbatim with main's
// foreign enum-type-id (see BranchEngineBuildError's doc-comment for why that is corruption, not
// merely loss).
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

}  // namespace

std::expected<std::unique_ptr<BranchEngine>, BranchEngineBuildError> BranchEngine::BuildFromFork(
    storage::InMemoryStorage &main, uint64_t fork_ts, storage::CommitArgs seed_commit_args) {
  // -----------------------------------------------------------------------------------------
  // 1. Construct the lightweight private engine. This is a transient, in-RAM working copy, not
  //    a durable database: GC is disabled (nothing needs to run in the background against a
  //    freshly-seeded, single-transaction store) and every durability mechanism is off (no
  //    snapshots, no WAL, no startup recovery, no snapshot-on-exit).
  // -----------------------------------------------------------------------------------------
  storage::Config config;
  config.gc.type = storage::Config::Gc::Type::NONE;
  config.durability.snapshot_wal_mode = storage::Config::Durability::SnapshotWalMode::DISABLED;
  config.durability.recover_on_startup = false;
  config.durability.snapshot_on_exit = false;
  auto engine = std::make_unique<storage::InMemoryStorage>(config);

  // -----------------------------------------------------------------------------------------
  // 2. Time-travel main back to fork_ts. Read-only, self-pinned for its own lifetime (R16/HIGH-1
  //    -- see InMemoryStorage::HistoricalAccess's own doc-comment). Wrapped into
  //    BranchEngineBuildError (kForkStateUnavailable) rather than propagated raw, since this
  //    function now has a second, structurally different failure mode (kUnsupportedEnumProperty)
  //    to report through the same return type.
  // -----------------------------------------------------------------------------------------
  auto hist_exp = main.HistoricalAccess(fork_ts);
  if (!hist_exp.has_value()) {
    return std::unexpected(BranchEngineBuildError{
        .kind = BranchEngineBuildError::Kind::kForkStateUnavailable,
        .message =
            fmt::format("Cannot check out a version: fork timestamp {} is not (or no longer) pinned.", fork_ts)});
  }
  std::unique_ptr<storage::Storage::Accessor> hist = std::move(*hist_exp);

  // -----------------------------------------------------------------------------------------
  // 3. Open the single seeding write transaction on the branch engine. `Access` returns a plain
  //    `Storage::Accessor`-typed handle, but the seeding loop below needs the explicit-gid create
  //    primitives (CreateVertexEx/CreateEdgeEx), which are protected on InMemoryAccessor and only
  //    exposed publicly via `ReplicationAccessor` -- mirrors versioning::MergeBranch's own,
  //    already-reviewed idiom (merge.cpp) for getting at those same two calls: ReplicationAccessor
  //    adds no data members of its own, only public forwarding wrappers, so this static_cast is
  //    safe for the same reason it is there. WRITE (not UniqueAccess) is sufficient here: `engine`
  //    is a brand-new, not-yet-published store with no other possible concurrent accessor, so none
  //    of MergeBranch's own reasons for preferring UniqueAccess over Access(WRITE) (a concurrent
  //    writer racing CreateVertexEx/CreateEdgeEx's counter-bump-then-insert sequence) apply here.
  // -----------------------------------------------------------------------------------------
  std::unique_ptr<storage::ReplicationAccessor> seed(
      static_cast<storage::ReplicationAccessor *>(engine->Access(storage::StorageAccessType::WRITE).release()));

  // -----------------------------------------------------------------------------------------
  // 4. Name-based label/property/edge-type translation: main's mapper -> branch engine's own,
  //    brand-new (and therefore numerically unrelated) mapper.
  // -----------------------------------------------------------------------------------------
  storage::NameIdMapper *main_mapper = hist->GetNameIdMapper();
  storage::NameIdMapper *branch_mapper = seed->GetNameIdMapper();

  auto translate_label = [&](storage::LabelId id) {
    return storage::LabelId::FromUint(branch_mapper->NameToId(main_mapper->IdToName(id.AsUint())));
  };
  auto translate_prop = [&](storage::PropertyId id) {
    return storage::PropertyId::FromUint(branch_mapper->NameToId(main_mapper->IdToName(id.AsUint())));
  };
  auto translate_etype = [&](storage::EdgeTypeId id) {
    return storage::EdgeTypeId::FromUint(branch_mapper->NameToId(main_mapper->IdToName(id.AsUint())));
  };

  // Copies every property from `props` (read via `Properties(View::OLD)`, main's fork-state) onto
  // `set_property`, translating each property id by name.
  //
  // FAILS LOUD, never silently skips: ANY property whose value contains an Enum anywhere --
  // top-level or nested inside a List/Map (ContainsEnum, above) -- aborts the whole build with
  // kUnsupportedEnumProperty. An Enum PropertyValue embeds an enum-TYPE id that is main's, not the
  // branch engine's; silently dropping it would be silent data loss, and silently copying it
  // verbatim would be silent corruption (the branch engine's own, unrelated enum-type numbering
  // could easily have a DIFFERENT enum type registered under that same numeric id, or none at
  // all) -- neither is acceptable, so this is a hard, reported rejection instead.
  //
  // TODO(versioning): translate enum properties by name (copy the enum type's own definitions
  // from main into the branch engine first) -- chunk 8's job.
  auto copy_properties = [&](const std::map<storage::PropertyId, storage::PropertyValue> &props,
                             auto &set_property,
                             storage::Gid owner_gid,
                             std::string_view owner_kind) -> std::expected<void, BranchEngineBuildError> {
    for (const auto &[pid, val] : props) {
      if (ContainsEnum(val)) {
        return std::unexpected(BranchEngineBuildError{
            .kind = BranchEngineBuildError::Kind::kUnsupportedEnumProperty,
            .message =
                fmt::format("Cannot check out a version: the fork-state {} {} has an enum property, which versioned "
                            "branches do not yet support.",
                            owner_kind,
                            owner_gid.AsUint())});
      }
      auto set_res = set_property(translate_prop(pid), val);
      MG_ASSERT(set_res.has_value(),
                "BranchEngine::BuildFromFork: failed to set a property while seeding {} {} -- the branch engine "
                "is freshly created and this write cannot legitimately fail.",
                owner_kind,
                owner_gid.AsUint());
    }
    return {};
  };

  // -----------------------------------------------------------------------------------------
  // 5. PASS 1 -- vertices: recreate every fork-state vertex at its original gid, with its
  //    original labels + properties.
  // -----------------------------------------------------------------------------------------
  for (auto v : hist->Vertices(storage::View::OLD)) {
    auto nv = seed->CreateVertexEx(v.Gid());
    MG_ASSERT(nv.has_value(),
              "BranchEngine::BuildFromFork: gid {} collided while seeding a freshly-created branch engine -- "
              "this should be impossible (the engine has no prior objects).",
              v.Gid().AsUint());

    auto labels_res = v.Labels(storage::View::OLD);
    MG_ASSERT(labels_res.has_value(),
              "BranchEngine::BuildFromFork: failed to read fork-state labels for vertex {}.",
              v.Gid().AsUint());
    for (auto lbl : *labels_res) {
      auto add_res = nv->AddLabel(translate_label(lbl));
      MG_ASSERT(add_res.has_value(),
                "BranchEngine::BuildFromFork: failed to add a label while seeding vertex {}.",
                v.Gid().AsUint());
    }

    auto props_res = v.Properties(storage::View::OLD);
    MG_ASSERT(props_res.has_value(),
              "BranchEngine::BuildFromFork: failed to read fork-state properties for vertex {}.",
              v.Gid().AsUint());
    auto set_vertex_property = [&](storage::PropertyId pid, const storage::PropertyValue &val) {
      return nv->SetProperty(pid, val);
    };
    if (auto check = copy_properties(*props_res, set_vertex_property, v.Gid(), "vertex"); !check.has_value()) {
      return std::unexpected(std::move(check.error()));
    }
  }

  // -----------------------------------------------------------------------------------------
  // 6. PASS 2 -- edges: walk OUT edges only (each edge is seen exactly once this way), resolve
  //    both endpoints in the branch engine by gid (both must already exist -- pass 1 created
  //    every fork-state vertex), and recreate the edge at its original gid with its original
  //    properties.
  // -----------------------------------------------------------------------------------------
  for (auto v : hist->Vertices(storage::View::OLD)) {
    auto oe_res = v.OutEdges(storage::View::OLD);
    MG_ASSERT(oe_res.has_value(),
              "BranchEngine::BuildFromFork: failed to read fork-state out-edges for vertex {}.",
              v.Gid().AsUint());

    for (const auto &e : oe_res->edges) {
      auto from = seed->FindVertex(e.FromVertex().Gid(), storage::View::NEW);
      auto to = seed->FindVertex(e.ToVertex().Gid(), storage::View::NEW);
      MG_ASSERT(from.has_value() && to.has_value(),
                "BranchEngine::BuildFromFork: edge {} references an endpoint that was not seeded -- pass 1 must "
                "have created every fork-state vertex before pass 2 runs.",
                e.Gid().AsUint());

      auto ne = seed->CreateEdgeEx(&*from, &*to, translate_etype(e.EdgeType()), e.Gid());
      MG_ASSERT(ne.has_value(),
                "BranchEngine::BuildFromFork: gid {} collided while seeding a freshly-created branch engine -- "
                "this should be impossible (the engine has no prior objects).",
                e.Gid().AsUint());

      auto eprops_res = e.Properties(storage::View::OLD);
      MG_ASSERT(eprops_res.has_value(),
                "BranchEngine::BuildFromFork: failed to read fork-state properties for edge {}.",
                e.Gid().AsUint());
      auto set_edge_property = [&](storage::PropertyId pid, const storage::PropertyValue &val) {
        return ne->SetProperty(pid, val);
      };
      if (auto check = copy_properties(*eprops_res, set_edge_property, e.Gid(), "edge"); !check.has_value()) {
        return std::unexpected(std::move(check.error()));
      }
    }
  }

  // -----------------------------------------------------------------------------------------
  // 7. Commit the single seeding transaction.
  // -----------------------------------------------------------------------------------------
  auto commit_res = seed->PrepareForCommitPhase(std::move(seed_commit_args));
  MG_ASSERT(commit_res.has_value(), "BranchEngine::BuildFromFork: branch engine seed commit failed.");

  // Private ctor -- constructed via `new` rather than std::make_unique (which needs public ctor
  // access), from inside this static member function which does have that access.
  return std::unique_ptr<BranchEngine>(new BranchEngine(std::move(engine)));
}

}  // namespace memgraph::versioning

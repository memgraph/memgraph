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

#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <string>

#include "storage/v2/commit_args.hpp"
#include "storage/v2/inmemory/storage.hpp"

namespace memgraph::versioning {

// BuildFromFork can fail for two structurally different reasons, so a single inherited
// HistoricalAccessError is no longer enough to report both:
//   - kForkStateUnavailable: main.HistoricalAccess(fork_ts) itself failed (fork_ts is not, or no
//     longer, pinned) -- the same failure BranchReconstruction::Open/MergeBranch already surface.
//   - kUnsupportedEnumProperty: the fork-state graph has an Enum property (top-level OR nested
//     inside a List/Map) on some vertex/edge. Enum PropertyValues embed an enum-TYPE id that is
//     MAIN's, meaningless under the branch engine's own (distinct) enum-type numbering -- copying
//     it verbatim would silently mislabel it as whatever type happens to share that numeric id in
//     the branch engine (silent corruption), and dropping it would be silent data loss. Neither is
//     acceptable, so this is a hard rejection until enum-by-name translation is implemented
//     (chunk 8).
struct BranchEngineBuildError {
  enum class Kind : uint8_t { kForkStateUnavailable, kUnsupportedEnumProperty };
  Kind kind;
  std::string message;
};

// Graph Versioning: the MATERIALIZE-PER-CHECKOUT substrate. A BranchEngine owns a private,
// lightweight `storage::InMemoryStorage` ("branch engine") that is a self-contained, physically
// independent COPY of main's state as of a fork point -- as opposed to BranchReconstruction/
// BranchOverlay (chunk 5a/5b), which reconcile a branch's view on the fly from a change-log
// layered over a `HistoricalAccess` reader without ever copying main's graph. Once built, ordinary
// Cypher can run against `storage()`'s own accessor with STOCK operators -- no union-cursor, no
// overlay lookups -- because every object physically exists in this engine already. Wiring that
// query-execution path up is a separate, later unit; this one only builds + seeds the engine and
// is independently testable.
//
// R35 (main is never written): `BuildFromFork` only ever READS `main` (via `HistoricalAccess`,
// itself a read-only, self-pinned time-travel accessor -- see inmemory/storage.hpp's own R16/R37
// doc-comments) and writes exclusively into the freshly-constructed, private `engine_`. Nothing
// here ever opens a write accessor on `main`.
//
// GIDS ARE PRESERVED, NAMES ARE NOT: every vertex/edge is recreated at its ORIGINAL gid via the
// engine's own explicit-gid create primitives (CreateVertexEx/CreateEdgeEx) -- a fresh engine has
// no prior objects, so (unlike versioning::MergeBranch replaying onto a LIVE main) there is no gid
// collision to remap here; a collision would be a hard invariant break. Labels/properties/edge-
// types, by contrast, are translated BY NAME: the branch engine owns its own `NameIdMapper`,
// entirely distinct from main's, so main's numeric ids are meaningless to it -- every id is
// round-tripped through `main_mapper->IdToName(id) -> branch_mapper->NameToId(name)` (mirrors
// BranchOverlay/BranchReconstruction/MergeBranch's own by-name translation contract).
class BranchEngine {
 public:
  // Builds a lightweight private InMemoryStorage (GC disabled, no snapshot/WAL durability -- this
  // is a transient, in-RAM working copy, not a durable database) and seeds it with a full copy of
  // main's state AS OF `fork_ts` (via `main.HistoricalAccess(fork_ts)`), as a SINGLE seeding
  // transaction committed with `seed_commit_args` (consumed; caller builds it, mirroring
  // versioning::MergeBranch's CommitArgs parameter -- keeps this file independent of dbms/).
  //
  // Fails with kForkStateUnavailable if `fork_ts` is not (or no longer) pinned (wraps the
  // underlying HistoricalAccessError), or with kUnsupportedEnumProperty if the fork-state graph
  // has an Enum property anywhere (top-level or nested in a List/Map) -- see BranchEngineBuildError
  // for why neither case can be silently approximated.
  static std::expected<std::unique_ptr<BranchEngine>, BranchEngineBuildError> BuildFromFork(
      storage::InMemoryStorage &main, uint64_t fork_ts, storage::CommitArgs seed_commit_args);

  BranchEngine(const BranchEngine &) = delete;
  BranchEngine &operator=(const BranchEngine &) = delete;
  BranchEngine(BranchEngine &&) = delete;
  BranchEngine &operator=(BranchEngine &&) = delete;

  // engine_ (a plain std::unique_ptr<InMemoryStorage>) tears itself down normally -- no branch-
  // specific cleanup needed (unlike HistoricalAccess/BranchReconstruction, this engine never took
  // a fork pin of its own; the seeding HistoricalAccess reader used to build it already released
  // its self-pin when BuildFromFork's local accessor went out of scope).
  ~BranchEngine() = default;

  storage::InMemoryStorage &storage() { return *engine_; }

 private:
  explicit BranchEngine(std::unique_ptr<storage::InMemoryStorage> engine) : engine_(std::move(engine)) {}

  std::unique_ptr<storage::InMemoryStorage> engine_;
};

}  // namespace memgraph::versioning

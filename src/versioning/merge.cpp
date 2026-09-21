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

// See merge.hpp for the correctness contract governing this MERGE implementation.
//
// Design (b): ordinary write transaction on main + explicit per-object conflict detection.
// Design (a) — reusing `is_historical_` for MarkFinished exemption — was rejected: that flag also
// hard-blocks writes via MG_ASSERT(!is_historical_) in Transaction::EnsureCommitInfoExists; a new
// writable-shared-start transaction mode would patch core MVCC for a single caller.
//
// Two passes required: `historical` holds a SHARED main_lock_ guard for its lifetime; a same-thread
// UniqueAccess() call while that guard is live blocks forever (ResourceLock has no per-thread
// re-entrancy). Pass 1 closes `historical` fully before pass 2 opens the merge transaction.
//
// UniqueAccess, not Access(WRITE): CreateVertexEx/CreateEdgeEx use atomic_fetch_max then a separate
// skip-list insert — a non-atomic compound that crashes via MG_ASSERT(inserted) if a concurrent
// CreateVertex/CreateEdge races the same gid. UniqueAccess excludes all other accessors.
//
// Commit-result handling: ReplicationError::transaction_committed==true means main committed but a
// replica ack failed — reporting kCommitFailed would cause a retry of an already-applied merge.
// Mirrors HandlePeriodicCommitError (query/plan/operator.cpp:270). Not unit-tested: MakeMainCommitArgs
// registers no replicas; verified by reading inmemory/storage.cpp:1379 and :1414.

#include "versioning/merge.hpp"

#include <map>
#include <set>
#include <unordered_set>

#include <fmt/format.h>

#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage_error.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/exceptions.hpp"
#include "utils/variant_helpers.hpp"
#include "versioning/edge_lookup.hpp"

namespace memgraph::versioning {

namespace {

namespace sd = storage::durability;

std::set<storage::LabelId> LabelSet(const storage::VertexAccessor &v) {
  auto labels = v.Labels(storage::View::OLD);
  if (!labels.has_value()) {
    throw utils::BasicException("Failed to read labels for vertex {} while checking for a merge conflict.",
                                v.Gid().AsUint());
  }
  return std::set<storage::LabelId>(labels->begin(), labels->end());
}

std::map<storage::PropertyId, storage::PropertyValue> PropertyMap(const storage::VertexAccessor &v) {
  auto props = v.Properties(storage::View::OLD);
  if (!props.has_value()) {
    throw utils::BasicException("Failed to read properties for vertex {} while checking for a merge conflict.",
                                v.Gid().AsUint());
  }
  return std::move(*props);
}

std::map<storage::PropertyId, storage::PropertyValue> PropertyMap(const storage::EdgeAccessor &e) {
  auto props = e.Properties(storage::View::OLD);
  if (!props.has_value()) {
    throw utils::BasicException("Failed to read properties for edge {} while checking for a merge conflict.",
                                e.Gid().AsUint());
  }
  return std::move(*props);
}

// D3, pass 2: has main changed fork-existing vertex `gid` (modified OR deleted) since the fork point?
// Deliberately View::OLD: AdvanceCommand is never called over the apply loop, so View::OLD undoes every
// delta this merge transaction already applied — presenting main's true pre-merge state for the comparison.
std::expected<void, MergeError> CheckVertexUnchangedSinceFork(const VertexForkSnapshot &fork_snapshot,
                                                              storage::ReplicationAccessor &merge, storage::Gid gid) {
  auto now_v = merge.FindVertex(gid, storage::View::OLD);
  if (!now_v.has_value()) {
    return std::unexpected(
        MergeError{.kind = MergeErrorKind::kModifyConflict,
                   .message = fmt::format("Merge conflict: main deleted vertex {} after the branch's fork point, "
                                          "but the branch also modified it.",
                                          gid.AsUint()),
                   .conflicting_gids = {gid}});
  }

  if (fork_snapshot.labels != LabelSet(*now_v) || fork_snapshot.properties != PropertyMap(*now_v)) {
    return std::unexpected(MergeError{
        .kind = MergeErrorKind::kModifyConflict,
        .message =
            fmt::format("Merge conflict: vertex {} was modified on main after the branch's fork point.", gid.AsUint()),
        .conflicting_gids = {gid}});
  }
  return {};
}

// Symmetric D3 check for edges (properties only -- endpoints/type never change post-creation).
std::expected<void, MergeError> CheckEdgeUnchangedSinceFork(const EdgeForkSnapshot &fork_snapshot,
                                                            storage::ReplicationAccessor &merge, storage::Gid gid) {
  // Not merge.FindEdge(gid) — unreliable for light edges; see FindHistoricalEdgeByEndpoint (edge_lookup.hpp).
  auto now_e = FindHistoricalEdgeByEndpoint(merge, fork_snapshot.from_vertex_gid, gid);
  if (!now_e.has_value()) {
    return std::unexpected(MergeError{
        .kind = MergeErrorKind::kModifyConflict,
        .message =
            fmt::format("Merge conflict: main deleted edge {} after the branch's fork point, but the branch also "
                        "modified it.",
                        gid.AsUint()),
        .conflicting_gids = {gid}});
  }

  if (fork_snapshot.properties != PropertyMap(*now_e)) {
    return std::unexpected(MergeError{
        .kind = MergeErrorKind::kModifyConflict,
        .message =
            fmt::format("Merge conflict: edge {} was modified on main after the branch's fork point.", gid.AsUint()),
        .conflicting_gids = {gid}});
  }
  return {};
}

}  // namespace

std::expected<MergeResult, MergeError> MergeBranch(storage::InMemoryStorage &main, uint64_t fork_ts,
                                                   const std::vector<sd::WalDeltaData> &changelog,
                                                   storage::NameIdMapper *name_id_mapper,
                                                   storage::CommitArgs commit_args) {
  // Branch-native objects: created by the branch (not at fork_ts), so D3's fork-vs-now check doesn't apply.
  std::unordered_set<storage::Gid> branch_local_vertices;
  std::unordered_set<storage::Gid> branch_local_edges;
  std::unordered_map<storage::Gid, VertexForkSnapshot> vertex_fork_snapshots;
  std::unordered_map<storage::Gid, EdgeForkSnapshot> edge_fork_snapshots;

  auto make_corrupt = [](std::string message, storage::Gid gid) {
    return std::unexpected(MergeError{
        .kind = MergeErrorKind::kCorruptChangelog, .message = std::move(message), .conflicting_gids = {gid}});
  };
  auto make_apply_failed = [](std::string message, storage::Gid gid) {
    return std::unexpected(
        MergeError{.kind = MergeErrorKind::kApplyFailed, .message = std::move(message), .conflicting_gids = {gid}});
  };

  // PASS 1: read-only against historical (fork_ts). Classifies branch-local creates; snapshots fork-state of changed
  // objects.
  {
    auto historical_or_err = main.HistoricalAccess(fork_ts);
    if (!historical_or_err.has_value()) {
      return std::unexpected(
          MergeError{.kind = MergeErrorKind::kForkPinLost,
                     .message = fmt::format("Cannot merge: fork timestamp {} is not (or no longer) pinned.", fork_ts),
                     .conflicting_gids = {}});
    }
    std::unique_ptr<storage::Storage::Accessor> historical = std::move(*historical_or_err);

    auto ensure_vertex_snapshot = [&](storage::Gid gid) -> std::expected<void, MergeError> {
      if (branch_local_vertices.contains(gid)) return {};
      auto [it, inserted] = vertex_fork_snapshots.try_emplace(gid);
      if (!inserted) return {};
      auto fork_v = historical->FindVertex(gid, storage::View::OLD);
      if (!fork_v.has_value()) {
        vertex_fork_snapshots.erase(it);
        return make_corrupt(
            fmt::format("Branch change-log modifies vertex {} which is not present in the fork-state base -- "
                        "corrupt change-log or wrong fork_ts.",
                        gid.AsUint()),
            gid);
      }
      it->second = VertexForkSnapshot{LabelSet(*fork_v), PropertyMap(*fork_v)};
      return {};
    };
    auto ensure_edge_snapshot = [&](storage::Gid gid, storage::Gid from_vertex_gid) -> std::expected<void, MergeError> {
      if (branch_local_edges.contains(gid)) return {};
      auto [it, inserted] = edge_fork_snapshots.try_emplace(gid);
      if (!inserted) return {};
      auto fork_e = FindHistoricalEdgeByEndpoint(*historical, from_vertex_gid, gid);
      if (!fork_e.has_value()) {
        edge_fork_snapshots.erase(it);
        return make_corrupt(
            fmt::format("Branch change-log modifies edge {} which is not present in the fork-state base -- "
                        "corrupt change-log or wrong fork_ts.",
                        gid.AsUint()),
            gid);
      }
      it->second = EdgeForkSnapshot{PropertyMap(*fork_e), fork_e->FromVertex().Gid()};
      return {};
    };
    // Only reachable from a pre-kEdgeSetDeltaWithVertexInfo record (no from-vertex hint).
    // Freshly-written logs always carry the hint; this bare scan is unreliable for deleted light edges (legacy format
    // only).
    auto ensure_edge_snapshot_legacy_no_hint = [&](storage::Gid gid) -> std::expected<void, MergeError> {
      if (branch_local_edges.contains(gid)) return {};
      auto [it, inserted] = edge_fork_snapshots.try_emplace(gid);
      if (!inserted) return {};
      auto fork_e = historical->FindEdge(gid, storage::View::OLD);
      if (!fork_e.has_value()) {
        edge_fork_snapshots.erase(it);
        return make_corrupt(
            fmt::format("Branch change-log modifies edge {} which is not present in the fork-state base -- "
                        "corrupt change-log or wrong fork_ts.",
                        gid.AsUint()),
            gid);
      }
      it->second = EdgeForkSnapshot{PropertyMap(*fork_e), fork_e->FromVertex().Gid()};
      return {};
    };

    // Snapshot endpoint vertices too: lets pass 2 distinguish "corrupt log" (endpoint never existed)
    // from "D3 conflict" (main deleted a fork-existing endpoint) — see missing_vertex_error below.
    auto classify =
        utils::Overloaded{[&](sd::WalVertexCreate const &data) -> std::expected<void, MergeError> {
                            // WalVertexCreate is also emitted for COW echoes of fork-existing vertices
                            // (BranchContext::CowVertex). Probe historical_: if the gid exists there, snapshot it as
                            // fork-existing (not branch-local).
                            if (auto fork_v = historical->FindVertex(data.gid, storage::View::OLD)) {
                              if (auto [it, inserted] = vertex_fork_snapshots.try_emplace(data.gid); inserted) {
                                it->second = VertexForkSnapshot{LabelSet(*fork_v), PropertyMap(*fork_v)};
                              }
                              return {};
                            }
                            branch_local_vertices.insert(data.gid);
                            return {};
                          },
                          [&](sd::WalVertexDelete const &data) { return ensure_vertex_snapshot(data.gid); },
                          [&](sd::WalVertexAddLabel const &data) { return ensure_vertex_snapshot(data.gid); },
                          [&](sd::WalVertexRemoveLabel const &data) { return ensure_vertex_snapshot(data.gid); },
                          [&](sd::WalVertexSetProperty const &data) { return ensure_vertex_snapshot(data.gid); },
                          [&](sd::WalEdgeCreate const &data) -> std::expected<void, MergeError> {
                            // WalEdgeCreate is also emitted for COW echoes of fork-existing edges
                            // (BranchContext::CowEdge). Reliable probe: walk FROM endpoint's OutEdges — not bare
                            // FindEdge (see FindHistoricalEdgeByEndpoint).
                            if (auto fork_edge = FindHistoricalEdgeByEndpoint(*historical, data.from_vertex, data.gid);
                                fork_edge.has_value()) {
                              if (auto check = ensure_vertex_snapshot(data.from_vertex); !check) return check;
                              if (auto check = ensure_vertex_snapshot(data.to_vertex); !check) return check;
                              if (auto [it, inserted] = edge_fork_snapshots.try_emplace(data.gid); inserted) {
                                it->second = EdgeForkSnapshot{PropertyMap(*fork_edge), fork_edge->FromVertex().Gid()};
                              }
                              return {};
                            }
                            branch_local_edges.insert(data.gid);
                            if (auto check = ensure_vertex_snapshot(data.from_vertex); !check) return check;
                            return ensure_vertex_snapshot(data.to_vertex);
                          },
                          [&](sd::WalEdgeDelete const &data) -> std::expected<void, MergeError> {
                            if (auto check = ensure_edge_snapshot(data.gid, data.from_vertex); !check) return check;
                            if (auto check = ensure_vertex_snapshot(data.from_vertex); !check) return check;
                            return ensure_vertex_snapshot(data.to_vertex);
                          },
                          [&](sd::WalEdgeSetProperty const &data) -> std::expected<void, MergeError> {
                            if (data.from_gid.has_value()) {
                              if (auto check = ensure_edge_snapshot(data.gid, *data.from_gid); !check) return check;
                            } else {
                              // No from-vertex hint (pre-kEdgeSetDeltaWithVertexInfo format): fall back to the legacy
                              // bare scan.
                              if (auto check = ensure_edge_snapshot_legacy_no_hint(data.gid); !check) return check;
                            }
                            if (data.from_gid.has_value()) {
                              if (auto check = ensure_vertex_snapshot(*data.from_gid); !check) return check;
                            }
                            if (data.to_gid.has_value() && *data.to_gid != storage::kInvalidGid) {
                              if (auto check = ensure_vertex_snapshot(*data.to_gid); !check) return check;
                            }
                            return {};
                          },
                          [&](sd::WalTransactionStart const &) -> std::expected<void, MergeError> { return {}; },
                          [&](sd::WalTransactionEnd const &) -> std::expected<void, MergeError> { return {}; },
                          [&](auto const &) -> std::expected<void, MergeError> {
                            return std::unexpected(MergeError{
                                .kind = MergeErrorKind::kCorruptChangelog,
                                .message = "Branch change-log contains a record type that branch capture never "
                                           "writes (schema, index, TTL, or unknown format extension) — corrupt log.",
                                .conflicting_gids = {}});
                          }};

    for (const auto &delta : changelog) {
      auto result = std::visit(classify, delta.data_);
      if (!result.has_value()) {
        // Pass 1 is read-only: main is untouched; propagate and let `historical` unwind on scope exit.
        return std::unexpected(std::move(result.error()));
      }
    }
    // historical (SHARED main_lock_ guard) released here — before pass 2 opens UniqueAccess (see top-of-file).
  }

  // PASS 2: real committing transaction under UniqueAccess (exclusive). See top-of-file for why UniqueAccess, not
  // Access(WRITE).
  std::unique_ptr<storage::ReplicationAccessor> merge(
      static_cast<storage::ReplicationAccessor *>(main.UniqueAccess().release()));

  // Original-branch-gid -> actual main-gid, populated only on collision remap. Keyed by original gid so all subsequent
  // references resolve consistently.
  std::unordered_map<storage::Gid, storage::Gid> vertex_remap;
  std::unordered_map<storage::Gid, storage::Gid> edge_remap;

  auto resolve_vertex = [&](storage::Gid gid) {
    auto it = vertex_remap.find(gid);
    return it == vertex_remap.end() ? gid : it->second;
  };
  auto resolve_edge = [&](storage::Gid gid) {
    auto it = edge_remap.find(gid);
    return it == edge_remap.end() ? gid : it->second;
  };

  // Missing endpoint vertex: if pass 1 has a snapshot for it, main deleted it post-fork (D3 conflict
  // on the vertex, not a corrupt log). If neither snapshotted nor branch-local, pass 1 already failed.
  auto missing_vertex_error = [&](storage::Gid gid, std::string_view context) -> MergeError {
    if (vertex_fork_snapshots.contains(gid)) {
      return MergeError{
          .kind = MergeErrorKind::kModifyConflict,
          .message = fmt::format("Merge conflict: main deleted vertex {} after the branch's fork point, but the "
                                 "branch's change-log still references it {}.",
                                 gid.AsUint(),
                                 context),
          .conflicting_gids = {gid}};
    }
    return MergeError{
        .kind = MergeErrorKind::kCorruptChangelog,
        .message = fmt::format("Branch change-log references vertex {} {} that cannot be found on main and was "
                               "never present in the fork-state base.",
                               gid.AsUint(),
                               context),
        .conflicting_gids = {gid}};
  };

  MergeResult stats;

  auto apply = utils::Overloaded{
      [&](sd::WalVertexCreate const &data) -> std::expected<void, MergeError> {
        if (!branch_local_vertices.contains(data.gid)) {
          // Fork-existing vertex (COW echo): no-op the create; the real vertex is already on main.
          // D3 check required: a bare echo with no following modify record is the only chance to catch a main-side
          // change.
          if (auto check = CheckVertexUnchangedSinceFork(vertex_fork_snapshots.at(data.gid), *merge, data.gid);
              !check) {
            return std::unexpected(check.error());
          }
          return {};
        }
        auto v = merge->CreateVertexEx(data.gid);
        if (!v.has_value()) {
          // Gid collision: remap to a fresh gid rather than losing this vertex.
          auto remapped = merge->CreateVertex();
          vertex_remap[data.gid] = remapped.Gid();
        }
        ++stats.vertices_created;
        return {};
      },
      [&](sd::WalVertexDelete const &data) -> std::expected<void, MergeError> {
        auto resolved = resolve_vertex(data.gid);
        if (!branch_local_vertices.contains(data.gid)) {
          if (auto check = CheckVertexUnchangedSinceFork(vertex_fork_snapshots.at(data.gid), *merge, data.gid);
              !check) {
            return std::unexpected(check.error());
          }
        }
        // View::NEW: this vertex may have been created earlier in this same pass (AdvanceCommand is
        // never called), and View::OLD would undo that create's DELETE_OBJECT delta, hiding it.
        auto v = merge->FindVertex(resolved, storage::View::NEW);
        if (!v.has_value()) {
          return make_corrupt(
              fmt::format("Branch change-log deletes vertex {} which cannot be found on main.", resolved.AsUint()),
              data.gid);
        }
        auto del = merge->DeleteVertex(&*v);
        if (!del.has_value()) {
          if (del.error() == storage::Error::VERTEX_HAS_EDGES) {
            // VERTEX_HAS_EDGES: the branch only records a delete when the vertex had no edges at that time,
            // so edges present now means main attached them post-fork — a D3 conflict, not a corrupt log.
            return std::unexpected(MergeError{
                .kind = MergeErrorKind::kModifyConflict,
                .message = fmt::format("Merge conflict: main attached an edge to vertex {} after the branch's "
                                       "fork point, so the branch's delete can no longer be applied.",
                                       resolved.AsUint()),
                .conflicting_gids = {data.gid}});
          }
          return make_apply_failed(
              fmt::format("Failed to delete vertex {} while applying the merge.", resolved.AsUint()), data.gid);
        }
        if (!del->has_value()) {
          return make_apply_failed(
              fmt::format("Failed to delete vertex {} while applying the merge.", resolved.AsUint()), data.gid);
        }
        // Deleted objects not captured for after-commit triggers: retaining `merge` (a UNIQUE
        // main_lock_ holder) past this function's return deadlocks — see MergeResult's doc-comment.
        ++stats.objects_deleted;
        return {};
      },
      [&](sd::WalVertexAddLabel const &data) -> std::expected<void, MergeError> {
        auto resolved = resolve_vertex(data.gid);
        if (!branch_local_vertices.contains(data.gid)) {
          if (auto check = CheckVertexUnchangedSinceFork(vertex_fork_snapshots.at(data.gid), *merge, data.gid);
              !check) {
            return std::unexpected(check.error());
          }
        }
        // View::NEW -- see the WalVertexDelete case above for why.
        auto v = merge->FindVertex(resolved, storage::View::NEW);
        if (!v.has_value()) {
          return make_corrupt(fmt::format("Branch change-log adds a label to vertex {} which cannot be found on main.",
                                          resolved.AsUint()),
                              data.gid);
        }
        auto ret = v->AddLabel(merge->NameToLabel(data.label));
        if (!ret.has_value()) {
          return make_apply_failed(
              fmt::format("Failed to add label to vertex {} while applying the merge.", resolved.AsUint()), data.gid);
        }
        ++stats.objects_modified;
        return {};
      },
      [&](sd::WalVertexRemoveLabel const &data) -> std::expected<void, MergeError> {
        auto resolved = resolve_vertex(data.gid);
        if (!branch_local_vertices.contains(data.gid)) {
          if (auto check = CheckVertexUnchangedSinceFork(vertex_fork_snapshots.at(data.gid), *merge, data.gid);
              !check) {
            return std::unexpected(check.error());
          }
        }
        // View::NEW -- see the WalVertexDelete case above for why.
        auto v = merge->FindVertex(resolved, storage::View::NEW);
        if (!v.has_value()) {
          return make_corrupt(
              fmt::format("Branch change-log removes a label from vertex {} which cannot be found on main.",
                          resolved.AsUint()),
              data.gid);
        }
        auto ret = v->RemoveLabel(merge->NameToLabel(data.label));
        if (!ret.has_value()) {
          return make_apply_failed(
              fmt::format("Failed to remove label from vertex {} while applying the merge.", resolved.AsUint()),
              data.gid);
        }
        ++stats.objects_modified;
        return {};
      },
      [&](sd::WalVertexSetProperty const &data) -> std::expected<void, MergeError> {
        auto resolved = resolve_vertex(data.gid);
        if (!branch_local_vertices.contains(data.gid)) {
          if (auto check = CheckVertexUnchangedSinceFork(vertex_fork_snapshots.at(data.gid), *merge, data.gid);
              !check) {
            return std::unexpected(check.error());
          }
        }
        // View::NEW -- see the WalVertexDelete case above for why.
        auto v = merge->FindVertex(resolved, storage::View::NEW);
        if (!v.has_value()) {
          return make_corrupt(
              fmt::format("Branch change-log sets a property on vertex {} which cannot be found on main.",
                          resolved.AsUint()),
              data.gid);
        }
        auto ret =
            v->SetProperty(merge->NameToProperty(data.property), storage::ToPropertyValue(data.value, name_id_mapper));
        if (!ret.has_value()) {
          return make_apply_failed(
              fmt::format("Failed to set a property on vertex {} while applying the merge.", resolved.AsUint()),
              data.gid);
        }
        ++stats.objects_modified;
        return {};
      },
      [&](sd::WalEdgeCreate const &data) -> std::expected<void, MergeError> {
        if (!branch_local_edges.contains(data.gid)) {
          // Fork-existing edge (COW echo): no-op the create; the real edge is already on main.
          // D3 check required: a bare echo with no following modify record is the only chance to
          // catch a main-side change to this edge.
          if (auto check = CheckEdgeUnchangedSinceFork(edge_fork_snapshots.at(data.gid), *merge, data.gid); !check) {
            return std::unexpected(check.error());
          }
          return {};
        }
        auto resolved_from = resolve_vertex(data.from_vertex);
        auto resolved_to = resolve_vertex(data.to_vertex);
        // View::NEW: endpoint may have been created in this same pass (see WalVertexDelete above).
        auto from_v = merge->FindVertex(resolved_from, storage::View::NEW);
        auto to_v = merge->FindVertex(resolved_to, storage::View::NEW);
        if (!from_v.has_value()) {
          return std::unexpected(missing_vertex_error(
              data.from_vertex, fmt::format("as the source endpoint of edge {}", data.gid.AsUint())));
        }
        if (!to_v.has_value()) {
          return std::unexpected(missing_vertex_error(
              data.to_vertex, fmt::format("as the target endpoint of edge {}", data.gid.AsUint())));
        }
        auto edge_type = merge->NameToEdgeType(data.edge_type);
        if (merge->EdgeGidExists(data.gid)) {
          // Gid collision: remap via auto-gid rather than calling CreateEdgeEx on a colliding gid (MG_ASSERT-crash).
          auto edge_result = merge->CreateEdge(&*from_v, &*to_v, edge_type);
          if (!edge_result.has_value()) {
            return make_apply_failed(
                fmt::format("Failed to create a remapped edge for branch edge {}.", data.gid.AsUint()), data.gid);
          }
          edge_remap[data.gid] = edge_result->Gid();
        } else {
          auto edge_result = merge->CreateEdgeEx(&*from_v, &*to_v, edge_type, data.gid);
          if (!edge_result.has_value()) {
            return make_apply_failed(
                fmt::format("Failed to create edge {} while applying the merge.", data.gid.AsUint()), data.gid);
          }
        }
        ++stats.edges_created;
        return {};
      },
      [&](sd::WalEdgeDelete const &data) -> std::expected<void, MergeError> {
        const bool is_branch_local = branch_local_edges.contains(data.gid);
        if (!is_branch_local) {
          if (auto check = CheckEdgeUnchangedSinceFork(edge_fork_snapshots.at(data.gid), *merge, data.gid); !check) {
            return std::unexpected(check.error());
          }
        }
        auto resolved = is_branch_local ? resolve_edge(data.gid) : data.gid;
        auto resolved_from = resolve_vertex(data.from_vertex);
        auto resolved_to = resolve_vertex(data.to_vertex);
        // View::NEW throughout this branch -- see the WalVertexDelete case above for why.
        auto from_v = merge->FindVertex(resolved_from, storage::View::NEW);
        auto to_v = merge->FindVertex(resolved_to, storage::View::NEW);
        if (!from_v.has_value()) {
          return std::unexpected(missing_vertex_error(
              data.from_vertex, fmt::format("as the source endpoint of edge {}", resolved.AsUint())));
        }
        if (!to_v.has_value()) {
          return std::unexpected(missing_vertex_error(
              data.to_vertex, fmt::format("as the target endpoint of edge {}", resolved.AsUint())));
        }
        auto edge_type = merge->NameToEdgeType(data.edge_type);
        auto e = merge->FindEdge(resolved, storage::View::NEW, edge_type, &*from_v, &*to_v);
        if (!e.has_value()) {
          return make_corrupt(
              fmt::format("Branch change-log deletes edge {} which cannot be found on main.", resolved.AsUint()),
              data.gid);
        }
        auto ret = merge->DeleteEdge(&*e);
        if (!ret.has_value()) {
          return make_apply_failed(fmt::format("Failed to delete edge {} while applying the merge.", resolved.AsUint()),
                                   data.gid);
        }
        // Deleted edges not captured for after-commit triggers — see WalVertexDelete above.
        ++stats.objects_deleted;
        return {};
      },
      [&](sd::WalEdgeSetProperty const &data) -> std::expected<void, MergeError> {
        const bool is_branch_local = branch_local_edges.contains(data.gid);
        if (!is_branch_local) {
          if (auto check = CheckEdgeUnchangedSinceFork(edge_fork_snapshots.at(data.gid), *merge, data.gid); !check) {
            return std::unexpected(check.error());
          }
        }
        auto resolved = is_branch_local ? resolve_edge(data.gid) : data.gid;

        // Resolve endpoints through vertex_remap (branch-local endpoints may have been remapped).
        // View::NEW throughout: endpoint or edge may have been created in this same pass (see WalVertexDelete).
        std::optional<storage::EdgeAccessor> e;
        if (data.from_gid.has_value() && data.to_gid.has_value() && data.edge_type.has_value() &&
            *data.to_gid != storage::kInvalidGid && !data.edge_type->empty()) {
          // Defense-in-depth, structurally unreachable via a well-formed log (NOT unit-tested):
          // from_gid/to_gid always mirror an edge's true endpoints at record time, and VERTEX_HAS_EDGES
          // prevents endpoint deletion while the edge still exists — a findable edge always has findable endpoints.
          auto from_v = merge->FindVertex(resolve_vertex(*data.from_gid), storage::View::NEW);
          if (!from_v.has_value()) {
            return std::unexpected(missing_vertex_error(
                *data.from_gid, fmt::format("as the source endpoint of edge {}", resolved.AsUint())));
          }
          auto to_v = merge->FindVertex(resolve_vertex(*data.to_gid), storage::View::NEW);
          if (!to_v.has_value()) {
            return std::unexpected(missing_vertex_error(
                *data.to_gid, fmt::format("as the target endpoint of edge {}", resolved.AsUint())));
          }
          e = merge->FindEdge(resolved, storage::View::NEW, merge->NameToEdgeType(*data.edge_type), &*from_v, &*to_v);
        } else if (data.from_gid.has_value()) {
          auto from_v = merge->FindVertex(resolve_vertex(*data.from_gid), storage::View::NEW);
          if (!from_v.has_value()) {
            return std::unexpected(
                missing_vertex_error(*data.from_gid, fmt::format("as an endpoint of edge {}", resolved.AsUint())));
          }
          e = merge->FindEdge(resolved, resolve_vertex(*data.from_gid), storage::View::NEW);
        } else {
          e = merge->FindEdge(resolved, storage::View::NEW);
        }

        if (!e.has_value()) {
          return make_corrupt(fmt::format("Branch change-log sets a property on edge {} which cannot be found on main.",
                                          resolved.AsUint()),
                              data.gid);
        }
        auto ret =
            e->SetProperty(merge->NameToProperty(data.property), storage::ToPropertyValue(data.value, name_id_mapper));
        if (!ret.has_value()) {
          return make_apply_failed(
              fmt::format("Failed to set a property on edge {} while applying the merge.", resolved.AsUint()),
              data.gid);
        }
        ++stats.objects_modified;
        return {};
      },
      [&](sd::WalTransactionStart const &) -> std::expected<void, MergeError> { return {}; },
      [&](sd::WalTransactionEnd const &) -> std::expected<void, MergeError> { return {}; },
      [&](auto const &) -> std::expected<void, MergeError> {
        return std::unexpected(MergeError{
            .kind = MergeErrorKind::kCorruptChangelog,
            .message = "Branch change-log contains a record type that branch capture never writes (schema, index, "
                       "TTL, or unknown format extension) — corrupt log.",
            .conflicting_gids = {}});
      }};

  for (const auto &delta : changelog) {
    auto result = std::visit(apply, delta.data_);
    if (!result.has_value()) {
      // Conflict or apply failure: abort our own transaction — main is untouched, branch's fork pin is unaffected.
      merge->Abort();
      return std::unexpected(std::move(result.error()));
    }
  }

  auto commit_result = merge->PrepareForCommitPhase(std::move(commit_args));
  if (!commit_result.has_value()) {
    // Not all failures mean main is untouched — see top-of-file commit-result handling.
    auto committed_with_warning =
        std::visit(utils::Overloaded{[](storage::ReplicationError const &err) -> std::optional<std::string> {
                                       if (err.transaction_committed) return storage::FormatReplicationError(err);
                                       return std::nullopt;
                                     },
                                     [](auto const &) -> std::optional<std::string> { return std::nullopt; }},
                   commit_result.error());

    if (!committed_with_warning.has_value()) {
      // transaction_committed==false or non-replication error: main not committed (AbortAndResetCommitTs ran for
      // STRICT_SYNC).
      return std::unexpected(MergeError{.kind = MergeErrorKind::kCommitFailed,
                                        .message = "Merge failed to commit onto main.",
                                        .conflicting_gids = {}});
    }
    // Main DID commit the merge; only a replica's ack failed to come back -- this is a SUCCESS,
    // just one worth surfacing to the caller (not unit-tested, see the top-of-file comment).
    stats.replication_warning = std::move(committed_with_warning);
  }

  stats.vertex_gid_remap = std::move(vertex_remap);
  stats.edge_gid_remap = std::move(edge_remap);
  // Expose pass-1's fork-state snapshots for after-commit trigger "old" property values (see MergeResult).
  stats.vertex_fork_snapshots = std::move(vertex_fork_snapshots);
  stats.edge_fork_snapshots = std::move(edge_fork_snapshots);
  // Expose branch-local classification: COW echoes must not be reported as CREATED trigger rows (see MergeResult).
  stats.branch_local_vertices = std::move(branch_local_vertices);
  stats.branch_local_edges = std::move(branch_local_edges);
  // `merge` falls out of scope here: keeping a UNIQUE accessor (exclusive main_lock_) alive past this
  // return while the interpreter dispatches async after-commit triggers would deadlock.
  return stats;
}

}  // namespace memgraph::versioning

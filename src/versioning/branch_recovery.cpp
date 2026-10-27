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

#include "versioning/branch_recovery.hpp"

#include <unistd.h>

#include <algorithm>
#include <exception>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "memory/db_arena.hpp"
#include "storage/v2/commit_args.hpp"
#include "storage/v2/durability/exceptions.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/atomic_utils.hpp"
#include "utils/crc_accumulator.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/variant_helpers.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/version_store.hpp"

namespace memgraph::versioning {

namespace {

namespace dur = storage::durability;

struct WalFileEntry {
  std::filesystem::path path;
  dur::WalInfo info;
};

bool HasRegularFiles(const std::filesystem::path &dir) {
  std::error_code ec;
  if (!std::filesystem::is_directory(dir, ec)) return false;
  for (const auto &entry : std::filesystem::directory_iterator(dir, ec)) {
    if (entry.is_regular_file(ec)) return true;
  }
  return false;
}

constexpr std::string_view kTornSuffix = ".torn";

// The branch's WAL files, oldest first (the clock is moved past replayed timestamps before the next run writes, so the
// first timestamp orders files across runs). An unreadable file is a crash tail only if no readable file with data is
// newer (names start with a zero-padded timestamp); it is renamed aside so later runs do not append behind it.
// @throw utils::BasicException a file cannot be read and is not the crash tail
std::vector<WalFileEntry> ListWalFiles(const std::filesystem::path &dir) {
  std::vector<WalFileEntry> files;
  std::vector<std::filesystem::path> unreadable;
  for (const auto &entry : std::filesystem::directory_iterator(dir)) {
    if (!entry.is_regular_file()) continue;
    if (entry.path().extension() == kTornSuffix) continue;
    std::error_code ec;
    if (entry.file_size(ec) == 0 && !ec) continue;
    try {
      auto info = dur::ReadWalContents(entry.path());
      if (info.num_deltas == 0) continue;
      files.push_back({.path = entry.path(), .info = std::move(info)});
    } catch (const dur::RecoveryFailure &e) {
      spdlog::warn("Branch WAL '{}' is unreadable: {}", entry.path().string(), e.what());
      unreadable.push_back(entry.path());
    }
  }
  if (!unreadable.empty()) {
    const auto newest_unreadable =
        *std::ranges::max_element(unreadable, [](const auto &a, const auto &b) { return a.filename() < b.filename(); });
    const bool mid_stream = unreadable.size() > 1 || std::ranges::any_of(files, [&](const WalFileEntry &f) {
                              return f.path.filename() > newest_unreadable.filename();
                            });
    if (mid_stream) {
      throw utils::BasicException(
          "Branch WAL corruption in the middle of the stream: unreadable file(s) in '{}' are followed by newer data "
          "(newest unreadable: '{}').",
          dir.string(),
          newest_unreadable.filename().string());
    }
    auto torn = newest_unreadable;
    torn += kTornSuffix;
    std::error_code ec;
    std::filesystem::rename(newest_unreadable, torn, ec);
    if (ec) {
      throw utils::BasicException(
          "Cannot move the torn branch WAL '{}' aside: {}.", newest_unreadable.string(), ec.message());
    }
    if (auto synced = utils::SyncDirectory(dir); !synced) throw utils::BasicException("{}", synced.error());
    spdlog::warn("Branch WAL '{}' is a torn crash tail with no complete transaction; skipped and renamed to '{}'.",
                 newest_unreadable.string(),
                 torn.string());
  }
  std::ranges::sort(files, [](const WalFileEntry &a, const WalFileEntry &b) {
    return std::tie(a.info.from_timestamp, a.info.seq_num) < std::tie(b.info.from_timestamp, b.info.seq_num);
  });
  return files;
}

struct TxnState {
  std::unique_ptr<storage::Storage::Accessor> holder;
  storage::ReplicationAccessor *acc{nullptr};
  uint64_t timestamp{0};
  bool skip{false};
  std::vector<dur::WalOverlayPut> puts;
  std::unordered_map<uint64_t, storage::EdgeAccessor> created_edges;
};

// Replays a branch's diff-engine WAL into its freshly built core: native objects through the diff engine's accessor,
// overlay records into the overlay store; fork-vertex edge endpoints resolve to anchors as BranchContext::CowVertex
// does.
class BranchWalReplayer {
 public:
  BranchWalReplayer(BranchCore &core, storage::Storage::Accessor &historical)
      : core_(core), diff_(core.diff_engine()), historical_(historical), mapper_(diff_.GetSharedNameIdMapper().get()) {}

  // @throw std::exception the WAL is corrupt or does not fit the fork base
  void Run(const std::vector<WalFileEntry> &files) {
    const storage::InMemoryStorage::SuppressWalScope no_wal{diff_};
    // Build's mirror commits left a durable timestamp of their own; the WAL's timestamps must win.
    auto info = diff_.repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire);
    info.ldt_ = 0;
    diff_.repl_storage_state_.commit_ts_info_.store(info, std::memory_order_release);

    for (const auto &file : files) ReplayFile(file);
    DeriveState();
    diff_.BumpClockPastDurable();
  }

 private:
  void ReplayFile(const WalFileEntry &file) {
    dur::Decoder decoder;
    const auto version = decoder.Initialize(file.path, dur::kWalMagic);
    if (!version) throw utils::BasicException("Cannot read the WAL magic and version of '{}'.", file.path.string());
    if (!dur::IsVersionSupported(*version)) {
      throw utils::BasicException("WAL '{}' has an unsupported version {}.", file.path.string(), *version);
    }
    if (!decoder.SetPosition(file.info.offset_deltas)) {
      throw utils::BasicException("Cannot seek to the deltas of WAL '{}'.", file.path.string());
    }

    uint64_t consumed = 0;
    while (consumed < file.info.num_deltas) consumed += ReplayTransaction(decoder, *version);
    // A transaction runs to its end marker, so overshooting means a marker rotted into another delta.
    if (consumed != file.info.num_deltas) {
      throw utils::BasicException("WAL '{}' states {} deltas but replaying it consumed {}.",
                                  file.path.string(),
                                  file.info.num_deltas,
                                  consumed);
    }
  }

  uint64_t ReplayTransaction(dur::Decoder &decoder, uint64_t version) {
    decoder.ResetCrcAcc();
    TxnState txn;
    uint64_t consumed = 0;
    bool complete = false;
    while (!complete) {
      const auto timestamp = dur::ReadWalDeltaHeader(&decoder);
      const auto delta = dur::ReadWalDeltaData(&decoder, version);
      ++consumed;
      complete = dur::IsWalDeltaDataTransactionEnd(delta, version);
      Apply(txn, decoder, timestamp, delta);
    }
    return consumed;
  }

  storage::ReplicationAccessor &Acc(TxnState &txn, uint64_t timestamp) {
    if (txn.acc == nullptr) {
      txn.holder = diff_.Access(storage::WRITE);
      txn.acc = static_cast<storage::ReplicationAccessor *>(txn.holder.get());
      txn.timestamp = timestamp;
    } else if (txn.timestamp != timestamp) {
      throw utils::BasicException("A WAL transaction mixes timestamps {} and {}.", txn.timestamp, timestamp);
    }
    return *txn.acc;
  }

  void Apply(TxnState &txn, dur::Decoder &decoder, uint64_t ts, const dur::WalDeltaData &delta) {
    std::visit(utils::Overloaded{
                   [&](const dur::WalTransactionStart &start) {
                     txn.skip = !start.commit.value_or(true);
                     if (!txn.skip) Acc(txn, ts);
                   },
                   [&](const dur::WalTransactionEnd &end) {
                     if (txn.skip) return;
                     if (txn.acc == nullptr || txn.timestamp != ts) throw utils::BasicException("Invalid commit data.");
                     if (end.txn_crc.has_value() && !utils::CrcAccumulator::Verify(decoder.CrcAccValue())) {
                       throw utils::BasicException(
                           "WAL CRC mismatch (stored {}, residue {}).", *end.txn_crc, decoder.CrcAccValue());
                     }
                     decoder.ResetCrcAcc();
                     Commit(txn);
                   },
                   [&](const dur::WalVertexCreate &d) {
                     if (txn.skip) return;
                     if (!Acc(txn, ts).CreateVertexEx(d.gid)) {
                       throw utils::BasicException("Vertex {} already exists in the branch.", d.gid.AsUint());
                     }
                   },
                   [&](const dur::WalVertexDelete &d) {
                     if (txn.skip) return;
                     auto &acc = Acc(txn, ts);
                     auto vertex = acc.FindVertex(d.gid, storage::View::NEW);
                     if (!vertex)
                       throw utils::BasicException("Vertex {} to delete is not in the branch.", d.gid.AsUint());
                     auto deleted = acc.DeleteVertex(&*vertex);
                     if (!deleted || !deleted.value()) {
                       throw utils::BasicException("Deleting vertex {} failed.", d.gid.AsUint());
                     }
                   },
                   [&](const dur::WalVertexAddLabel &d) {
                     if (txn.skip) return;
                     auto &acc = Acc(txn, ts);
                     auto vertex = acc.FindVertex(d.gid, storage::View::NEW);
                     if (!vertex)
                       throw utils::BasicException("Vertex {} to label is not in the branch.", d.gid.AsUint());
                     auto added = vertex->AddLabel(acc.NameToLabel(d.label));
                     if (!added || !added.value()) {
                       throw utils::BasicException("Adding a label to vertex {} failed.", d.gid.AsUint());
                     }
                   },
                   [&](const dur::WalVertexRemoveLabel &d) {
                     if (txn.skip) return;
                     auto &acc = Acc(txn, ts);
                     auto vertex = acc.FindVertex(d.gid, storage::View::NEW);
                     if (!vertex)
                       throw utils::BasicException("Vertex {} to unlabel is not in the branch.", d.gid.AsUint());
                     auto removed = vertex->RemoveLabel(acc.NameToLabel(d.label));
                     if (!removed || !removed.value()) {
                       throw utils::BasicException("Removing a label from vertex {} failed.", d.gid.AsUint());
                     }
                   },
                   [&](const dur::WalVertexSetProperty &d) {
                     if (txn.skip) return;
                     auto &acc = Acc(txn, ts);
                     auto vertex = acc.FindVertex(d.gid, storage::View::NEW);
                     if (!vertex)
                       throw utils::BasicException("Vertex {} to modify is not in the branch.", d.gid.AsUint());
                     auto set = vertex->SetProperty(acc.NameToProperty(d.property),
                                                    storage::ToPropertyValue(d.value, mapper_));
                     if (!set) throw utils::BasicException("Setting a property of vertex {} failed.", d.gid.AsUint());
                   },
                   [&](const dur::WalEdgeCreate &d) {
                     if (txn.skip) return;
                     auto &acc = Acc(txn, ts);
                     auto from = Endpoint(acc, d.from_vertex);
                     auto to = Endpoint(acc, d.to_vertex);
                     if (!from || !to) {
                       throw utils::BasicException("An endpoint of edge {} ({} -> {}) is not available.",
                                                   d.gid.AsUint(),
                                                   d.from_vertex.AsUint(),
                                                   d.to_vertex.AsUint());
                     }
                     auto edge = acc.CreateEdgeEx(&*from, &*to, acc.NameToEdgeType(d.edge_type), d.gid);
                     if (!edge) throw utils::BasicException("Creating edge {} failed.", d.gid.AsUint());
                     txn.created_edges.emplace(d.gid.AsUint(), *edge);
                   },
                   [&](const dur::WalEdgeDelete &d) {
                     if (txn.skip) return;
                     auto &acc = Acc(txn, ts);
                     auto edge = FindNativeEdge(acc, d.gid, d.from_vertex, d.to_vertex, d.edge_type);
                     if (!edge) throw utils::BasicException("Edge {} to delete is not in the branch.", d.gid.AsUint());
                     auto deleted = acc.DeleteEdge(&*edge);
                     if (!deleted) throw utils::BasicException("Deleting edge {} failed.", d.gid.AsUint());
                   },
                   [&](const dur::WalEdgeSetProperty &d) {
                     if (txn.skip) return;
                     auto &acc = Acc(txn, ts);
                     auto cached = txn.created_edges.find(d.gid.AsUint());
                     if (cached == txn.created_edges.end()) {
                       // Edges created in this transaction carry no endpoints in the delta; every other one does.
                       if (!d.from_gid || !d.to_gid || !d.edge_type || *d.to_gid == storage::kInvalidGid ||
                           d.edge_type->empty()) {
                         throw utils::BasicException("Edge {} property delta names no endpoints.", d.gid.AsUint());
                       }
                       auto edge = FindNativeEdge(acc, d.gid, *d.from_gid, *d.to_gid, *d.edge_type);
                       if (!edge)
                         throw utils::BasicException("Edge {} to modify is not in the branch.", d.gid.AsUint());
                       cached = txn.created_edges.emplace(d.gid.AsUint(), *edge).first;
                     }
                     auto set = cached->second.SetProperty(acc.NameToProperty(d.property),
                                                           storage::ToPropertyValue(d.value, mapper_));
                     if (!set) throw utils::BasicException("Setting a property of edge {} failed.", d.gid.AsUint());
                   },
                   [&](const dur::WalOverlayPut &d) {
                     if (txn.skip) return;
                     Acc(txn, ts);
                     txn.puts.push_back(d);
                   },
                   [&](const auto & /*unsupported*/) {
                     throw utils::BasicException("A branch WAL holds a delta that a branch diff engine never writes.");
                   }},
               delta.data_);
  }

  // The diff-engine vertex for `gid`: a native vertex, or the anchor of a fork vertex (created on first use, and the
  // fork vertex marked branched so main-side readers consult the branch).
  std::optional<storage::VertexAccessor> Endpoint(storage::ReplicationAccessor &acc, storage::Gid gid) {
    if (gid.AsUint() >= core_.vertex_gid_floor()) return acc.FindVertex(gid, storage::View::NEW);
    auto fork = historical_.FindVertex(gid, storage::View::OLD);
    if (!fork) return std::nullopt;
    MarkBranched(fork->vertex_);
    core_.RecordEdgeChange(gid);
    auto *stub = core_.stub_index().GetOrCreateStub(gid.AsUint(), fork->vertex_);
    return storage::VertexAccessor{stub, &diff_, &acc.GetTransaction()};
  }

  std::optional<storage::EdgeAccessor> FindNativeEdge(storage::ReplicationAccessor &acc, storage::Gid edge_gid,
                                                      storage::Gid from_gid, storage::Gid to_gid,
                                                      const std::string &edge_type) {
    auto from = Endpoint(acc, from_gid);
    auto to = Endpoint(acc, to_gid);
    if (!from || !to) return std::nullopt;
    return acc.FindEdge(edge_gid, storage::View::NEW, acc.NameToEdgeType(edge_type), &*from, &*to);
  }

  // Overlay-only transactions have no diff commit, so their local stamp is the accessor's start timestamp: past every
  // earlier replayed commit and below the clock any later transaction starts from.
  void Commit(TxnState &txn) {
    auto &acc = *txn.acc;
    const uint64_t start_ts = acc.GetTransaction().start_timestamp;
    auto committed = acc.PrepareForCommitPhase(
        storage::CommitArgs::make_replica_write(txn.timestamp, /*two_phase_commit=*/false, [] {}));
    if (!committed) throw utils::BasicException("Committing the replayed transaction {} failed.", txn.timestamp);
    const uint64_t local_ts = acc.GetCommitTimestamp().value_or(start_ts);

    for (const auto &put : txn.puts) InstallOverlay(put, local_ts);

    auto info = diff_.repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire);
    if (info.ldt_ < txn.timestamp) {
      info.ldt_ = txn.timestamp;
      diff_.repl_storage_state_.commit_ts_info_.store(info, std::memory_order_release);
    }
    txn.acc = nullptr;
    txn.holder.reset();
  }

  void InstallOverlay(const dur::WalOverlayPut &put, uint64_t local_ts) {
    const bool edge = IsEdgeKey(put.key);
    const uint64_t gid = put.key & ~kEdgeKeyBit;
    if (gid >= (edge ? core_.edge_gid_floor() : core_.vertex_gid_floor())) {
      throw utils::BasicException(
          "Overlay record for {} {} names an object created on the branch.", edge ? "edge" : "vertex", gid);
    }

    OverlayState state;
    if (put.deleted) {
      state.MarkDeleted();
    } else {
      for (const auto &name : put.added_labels) state.AddLabel(diff_.NameToLabel(name));
      for (const auto &name : put.removed_labels) state.RemoveLabel(diff_.NameToLabel(name));
      for (const auto &[name, value] : put.properties) {
        state.SetProperty(diff_.NameToProperty(name), storage::ToPropertyValue(value, mapper_));
      }
      for (const auto &name : put.removed_properties) state.RemoveProperty(diff_.NameToProperty(name));
    }
    core_.overlay_store().InstallCommittedState(put.key, std::move(state), local_ts);
    if (edge) edge_ends_[put.key] = {put.from_gid, put.to_gid};
  }

  // Rebuilds from the installed heads what BranchContext's write paths record next to every overlay write: the change
  // filters, the touched sets and the `branched()` hints. None of it is persisted.
  void DeriveState() {
    std::exception_ptr failure;
    core_.overlay_store().ForEachHead([&](uint64_t key, const OverlayVersion &version) {
      if (failure) return;
      try {
        if (IsEdgeKey(key)) {
          DeriveEdge(key, version);
        } else {
          DeriveVertex(key, version);
        }
      } catch (...) {
        failure = std::current_exception();
      }
    });
    if (failure) std::rethrow_exception(failure);
  }

  void DeriveVertex(uint64_t key, const OverlayVersion &version) {
    const auto gid = storage::Gid::FromUint(key);
    MarkBranched(ForkVertex(gid));
    if (version.deleted()) {
      core_.RecordVertexDeleted(gid);
      core_.RecordVertexChange(gid, BranchChangeKind::kLabel);
      core_.RecordVertexChange(gid, BranchChangeKind::kProperty);
      core_.RecordEdgeChange(gid);
      return;
    }

    bool labels = false;
    for (const auto label : version.added_labels()) {
      core_.RecordLabelTouched(label, gid);
      labels = true;
    }
    for (const auto label : version.removed_labels()) {
      core_.RecordLabelTouched(label, gid);
      labels = true;
    }
    if (labels) core_.RecordVertexChange(gid, BranchChangeKind::kLabel);

    bool properties = false;
    const auto note_property = [&](storage::PropertyId pid) {
      core_.RecordPropertyFieldChange(gid, pid);
      core_.RecordPropertyTouched(pid, gid);
      properties = true;
    };
    for (const auto &[pid, value] : version.properties().Properties()) note_property(pid);
    for (const auto pid : version.removed_pids()) note_property(pid);
    if (properties) core_.RecordVertexChange(gid, BranchChangeKind::kProperty);
  }

  void DeriveEdge(uint64_t key, const OverlayVersion &version) {
    const auto edge_gid = storage::Gid::FromUint(key & ~kEdgeKeyBit);
    const auto ends = edge_ends_.find(key);
    if (ends == edge_ends_.end()) {
      throw utils::BasicException("Overlay record of edge {} carries no endpoints.", edge_gid.AsUint());
    }
    const auto from = storage::Gid::FromUint(ends->second.first);
    const auto to = storage::Gid::FromUint(ends->second.second);

    if (version.deleted()) {
      core_.RecordForkEdgeDeleted(edge_gid);
      core_.RecordEdgeChange(from);
      core_.RecordEdgeChange(to);
      MarkBranched(ForkVertex(from));
      MarkBranched(ForkVertex(to));
    } else {
      const auto note_property = [&](storage::PropertyId pid) {
        core_.RecordForkEdgeProperty(edge_gid, pid);
        core_.RecordEdgePropertyTouched(pid, edge_gid, from);
      };
      for (const auto &[pid, value] : version.properties().Properties()) note_property(pid);
      for (const auto pid : version.removed_pids()) note_property(pid);
    }
    MarkForkEdgeBranched(edge_gid, from);
  }

  storage::Vertex *ForkVertex(storage::Gid gid) {
    auto fork = historical_.FindVertex(gid, storage::View::OLD);
    if (!fork) {
      throw utils::BasicException("Fork vertex {} named by the branch is not in main's history at the fork.",
                                  gid.AsUint());
    }
    return fork->vertex_;
  }

  void MarkForkEdgeBranched(storage::Gid edge_gid, storage::Gid from_gid) {
    if (!diff_.config_.salient.items.properties_on_edges) return;  // reference edges: no Edge object to mark
    auto from = historical_.FindVertex(from_gid, storage::View::OLD);
    if (!from) {
      throw utils::BasicException("Fork vertex {} named by the branch is not in main's history at the fork.",
                                  from_gid.AsUint());
    }
    auto out = from->OutEdges(storage::View::OLD);
    if (!out) throw utils::BasicException("Cannot list the fork edges of vertex {}.", from_gid.AsUint());
    for (const auto &edge : out->edges) {
      if (edge.Gid() != edge_gid) continue;
      storage::Edge *const main_edge = edge.edge_.ptr;
      auto guard = std::unique_lock{main_edge->lock};
      main_edge->SetBranched(true);
      return;
    }
    throw utils::BasicException("Fork edge {} named by the branch is not in main's history at the fork.",
                                edge_gid.AsUint());
  }

  static void MarkBranched(storage::Vertex *main_vertex) {
    auto guard = std::unique_lock{main_vertex->lock};
    main_vertex->SetBranched(true);
  }

  BranchCore &core_;
  storage::InMemoryStorage &diff_;
  storage::Storage::Accessor &historical_;
  storage::NameIdMapper *mapper_;
  // Edge overlay key -> (from gid, to gid), from the records; versions do not hold endpoints.
  std::unordered_map<uint64_t, std::pair<uint64_t, uint64_t>> edge_ends_;
};

std::expected<void, std::string> ReplayBranchWal(BranchCore &core, const std::filesystem::path &wal_dir) {
  const memory::DbArenaScope arena_scope{core.main().DbArenaPool()};
  try {
    const auto files = ListWalFiles(wal_dir);
    if (files.empty()) return {};
    auto historical = core.main().HistoricalAccess(core.fork_ts());
    if (!historical) return std::unexpected(fmt::format("fork timestamp {} is not pinned", core.fork_ts()));
    BranchWalReplayer replayer{core, **historical};
    replayer.Run(files);
  } catch (const std::exception &e) {
    return std::unexpected(std::string{e.what()});
  }
  return {};
}

// A branch with a WAL was built before, so the ledger holds its floors; without them the native/fork boundary is lost.
std::expected<std::shared_ptr<BranchCore>, std::string> BuildAndReplay(storage::InMemoryStorage &main,
                                                                       const BranchInfo &info,
                                                                       const std::filesystem::path &wal_dir) {
  if (!info.vertex_gid_floor || !info.edge_gid_floor) {
    return std::unexpected("the ledger holds no gid floors for it although its WAL exists");
  }
  auto built = BranchCore::Build(
      main,
      info.fork_ts,
      info.number,
      BranchCore::RecordedFloors{.vertex_gid_floor = *info.vertex_gid_floor, .edge_gid_floor = *info.edge_gid_floor});
  if (!built) return std::unexpected(std::move(built.error().message));
  if (auto replayed = ReplayBranchWal(**built, wal_dir); !replayed) {
    return std::unexpected(std::move(replayed.error()));
  }
  return std::move(*built);
}

// New main objects must not take a gid a branch uses: gids below a floor are fork objects, so a native object taking
// one would be misread as a fork object, and a native gid that main hands out again would alias two objects.
void RaiseGidCounters(VersionStore &store, storage::InMemoryStorage &main,
                      const std::vector<std::shared_ptr<BranchCore>> &recovered) {
  for (const auto &entry : store.List()) {
    const BranchInfo &info = entry.second;
    if (info.vertex_gid_floor) {
      atomic_fetch_max_explicit(&main.vertex_id_, *info.vertex_gid_floor, std::memory_order_acq_rel);
    }
    if (info.edge_gid_floor) atomic_fetch_max_explicit(&main.edge_id_, *info.edge_gid_floor, std::memory_order_acq_rel);
  }
  // The diff engine's counters sit one past the highest native gid it replayed.
  for (const auto &core : recovered) {
    atomic_fetch_max_explicit(
        &main.vertex_id_, core->diff_engine().vertex_id_.load(std::memory_order_acquire), std::memory_order_acq_rel);
    atomic_fetch_max_explicit(
        &main.edge_id_, core->diff_engine().edge_id_.load(std::memory_order_acquire), std::memory_order_acq_rel);
  }
}

}  // namespace

void RecoverBranchContents(VersionStore &store, storage::InMemoryStorage &main) {
  const memory::DbArenaScope arena_scope{main.DbArenaPool()};
  const auto branches_dir = main.config_.durability.storage_directory / "branches";
  std::vector<std::shared_ptr<BranchCore>> recovered;

  try {
    for (const auto &entry : store.List()) {
      const std::string &name = entry.first;
      const BranchInfo &info = entry.second;
      const auto wal_dir = branches_dir / std::to_string(info.number) / "wal";
      // No WAL means the branch never committed anything; its core is built empty on first checkout.
      if (!HasRegularFiles(wal_dir)) continue;

      auto attached = store.TryAcquireCheckout(name);
      if (!attached) continue;
      std::expected<std::shared_ptr<BranchCore>, std::string> core;
      try {
        core = store.GetOrBuildCore(info.number, [&] { return BuildAndReplay(main, info, wal_dir); });
      } catch (const std::exception &e) {
        core = std::unexpected(std::string{e.what()});
      }
      store.ReleaseCheckout(name);

      if (!core) {
        // Keeping the branch with its contents lost would show main@fork as the branch's state, and its new commits
        // would sit next to a WAL that cannot be replayed. Dropping it makes the loss visible; the WAL stays on disk.
        spdlog::warn(
            "Graph Versioning: the contents of branch '{}' cannot be recovered ({}); the branch is dropped. Its "
            "files remain in '{}'.",
            name,
            core.error(),
            (wal_dir.parent_path()).string());
        if (auto dropped = store.DropBranch(name); !dropped) {
          spdlog::warn("Graph Versioning: branch '{}' could not be dropped: {}", name, dropped.error());
        } else if (!*dropped) {
          spdlog::warn("Graph Versioning: branch '{}' could not be dropped.", name);
        }
        continue;
      }
      recovered.push_back(std::move(*core));
      spdlog::info("Graph Versioning: recovered the contents of branch '{}'.", name);
    }
    RaiseGidCounters(store, main, recovered);
  } catch (const std::exception &e) {
    spdlog::error("Graph Versioning: recovering branch contents failed unexpectedly: {}", e.what());
  }
}

}  // namespace memgraph::versioning

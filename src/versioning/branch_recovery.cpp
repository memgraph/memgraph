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

#include <algorithm>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "memory/db_arena.hpp"
#include "storage/v2/commit_args.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/exceptions.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/crc_accumulator.hpp"
#include "utils/exceptions.hpp"
#include "utils/variant_helpers.hpp"
#include "versioning/branch_engine.hpp"

namespace memgraph::versioning {

namespace {

namespace dur = storage::durability;

struct WalFileEntry {
  std::filesystem::path path;
  dur::WalInfo info;
};

// The branch's WAL files ordered by (first timestamp, seq), selected as main's GetWalFiles does: unreadable files and
// files with no complete transaction are warned about and skipped, never renamed or deleted.
// @throw utils::BasicException the directory cannot be listed
std::vector<WalFileEntry> ListWalFiles(const std::filesystem::path &dir) {
  std::vector<WalFileEntry> files;
  std::error_code ec;
  for (const auto &entry : std::filesystem::directory_iterator(dir, ec)) {
    if (!dur::ValidateDurabilityFile(entry)) continue;
    try {
      auto header = dur::ReadWalHeader(entry.path());
      auto info = dur::ReadWalContents(entry.path(), std::move(header));
      files.push_back({.path = entry.path(), .info = std::move(info)});
    } catch (const dur::RecoveryFailure &e) {
      spdlog::warn("Failed to read branch WAL file '{}'. Error: {}", entry.path().string(), e.what());
    }
  }
  if (ec) throw utils::BasicException("Cannot list '{}': {}", dir.string(), ec.message());
  std::ranges::sort(files, [](const WalFileEntry &a, const WalFileEntry &b) {
    return std::tie(a.info.from_timestamp, a.info.seq_num) < std::tie(b.info.from_timestamp, b.info.seq_num);
  });
  return files;
}

// Main's chain rules (RecoverData) with no snapshot: the first file has seq 0 and each next one follows its
// predecessor. BranchWalReplayer::Run continues the numbering across runs, so a missing file anywhere shows as a gap; a
// skipped newest file leaves none, as its number is reused.
// @throw utils::BasicException a file is missing
void CheckWalChain(const std::vector<WalFileEntry> &files) {
  if (files.empty()) return;
  if (files.front().info.seq_num != 0) {
    throw utils::BasicException("There are missing prefix WAL files: '{}' has sequence number {}, not 0.",
                                files.front().path.string(),
                                files.front().info.seq_num);
  }
  for (size_t i = 1; i < files.size(); ++i) {
    const uint64_t previous = files[i - 1].info.seq_num;
    if (files[i].info.seq_num - previous > 1) {
      throw utils::BasicException(
          "You are missing a WAL file with the sequence number {} before '{}'.", previous + 1, files[i].path.string());
    }
  }
}

uint64_t OpenAtDeltas(dur::Decoder &decoder, const WalFileEntry &file) {
  const auto version = decoder.Initialize(file.path, dur::kWalMagic);
  if (!version) throw utils::BasicException("Cannot read the WAL magic and version of '{}'.", file.path.string());
  if (!dur::IsVersionSupported(*version)) {
    throw utils::BasicException("WAL '{}' has an unsupported version {}.", file.path.string(), *version);
  }
  if (!decoder.SetPosition(file.info.offset_deltas)) {
    throw utils::BasicException("Cannot seek to the deltas of WAL '{}'.", file.path.string());
  }
  return *version;
}

struct TxnState {
  std::unique_ptr<storage::Storage::Accessor> holder;
  storage::ReplicationAccessor *acc{nullptr};
  uint64_t timestamp{0};
  bool skip{false};
  std::unordered_map<uint64_t, storage::EdgeAccessor> created_edges;
};

// Replays a branch's diff-engine WAL into its freshly built core: native objects through the diff engine's accessor;
// fork-vertex edge endpoints resolve to anchors as BranchContext::CowVertex does.
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
    diff_.BumpClockPastDurable();
    // The next run's files continue the numbering, so a file lost from either run leaves a gap CheckWalChain sees.
    if (!files.empty()) diff_.SetNextWalSeqNum(files.back().info.seq_num + 1);
  }

 private:
  void ReplayFile(const WalFileEntry &file) {
    dur::Decoder decoder;
    const auto version = OpenAtDeltas(decoder, file);

    uint64_t consumed = 0;
    while (consumed < file.info.num_deltas) consumed += ReplayTransaction(decoder, version);
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

  void Commit(TxnState &txn) {
    auto &acc = *txn.acc;
    auto committed = acc.PrepareForCommitPhase(
        storage::CommitArgs::make_replica_write(txn.timestamp, /*two_phase_commit=*/false, [] {}));
    if (!committed) throw utils::BasicException("Committing the replayed transaction {} failed.", txn.timestamp);

    auto info = diff_.repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire);
    if (info.ldt_ < txn.timestamp) {
      info.ldt_ = txn.timestamp;
      diff_.repl_storage_state_.commit_ts_info_.store(info, std::memory_order_release);
    }
    txn.acc = nullptr;
    txn.holder.reset();
  }

  static void MarkBranched(storage::Vertex *main_vertex) {
    auto guard = std::unique_lock{main_vertex->lock};
    main_vertex->SetBranched(true);
  }

  BranchCore &core_;
  storage::InMemoryStorage &diff_;
  storage::Storage::Accessor &historical_;
  storage::NameIdMapper *mapper_;
};

}  // namespace

std::expected<void, std::string> ReplayBranchWal(BranchCore &core, const std::filesystem::path &wal_dir) {
  const memory::DbArenaScope arena_scope{core.main().DbArenaPool()};
  try {
    const auto files = ListWalFiles(wal_dir);
    CheckWalChain(files);
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

}  // namespace memgraph::versioning

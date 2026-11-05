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
  // Fork vertices the transaction deletes, applied at its end (see ReplayTransaction).
  std::vector<storage::Gid> fork_vertex_deletes;
  // Edges created or found in this transaction: later records name them by gid only.
  std::unordered_map<uint64_t, storage::EdgeAccessor> edges;
};

// Replays a branch's diff-engine WAL into its freshly built core. The log holds user operations (see
// BranchContext's writers and the encoder in wal.cpp): a record on an object below the gid floors is a write to a fork
// object and goes through the BranchContext write resolver, as the live write did, which rebuilds the stub, the change
// filters and the fork-delete counts. A record on a native object is the diff engine's stock operation.
class BranchWalReplayer {
 public:
  explicit BranchWalReplayer(BranchContext &ctx)
      : ctx_(ctx), core_(*ctx.core()), diff_(ctx.diff_engine()), mapper_(diff_.GetSharedNameIdMapper().get()) {}

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

  // Opens the transaction on first use: the historical accessor (main's READ guard) before the diff accessor, as a
  // session does.
  storage::ReplicationAccessor &Acc(TxnState &txn, uint64_t timestamp) {
    if (txn.acc == nullptr) {
      if (auto opened = ctx_.OpenHistorical(); !opened) throw utils::BasicException("{}", opened.error().message);
      txn.holder = diff_.Access(storage::WRITE);
      txn.acc = static_cast<storage::ReplicationAccessor *>(txn.holder.get());
      ctx_.SetCurrentDiffTxn(txn.acc);
      txn.timestamp = timestamp;
    } else if (txn.timestamp != timestamp) {
      throw utils::BasicException("A WAL transaction mixes timestamps {} and {}.", txn.timestamp, timestamp);
    }
    return *txn.acc;
  }

  template <typename T>
  static T Require(storage::Result<T> result, std::string_view what, storage::Gid gid) {
    if (!result) {
      throw utils::BasicException("Replaying the branch WAL failed: {} {} (storage error {}).",
                                  what,
                                  gid.AsUint(),
                                  static_cast<int>(result.error()));
    }
    return std::move(*result);
  }

  // The vertex a record names: a fork vertex of main at the fork, or the diff engine's native one.
  storage::VertexAccessor Vertex(TxnState &txn, storage::Gid gid, std::string_view what) {
    auto vertex =
        ctx_.IsForkCandidateVertex(gid) ? ctx_.FindForkVertex(gid) : txn.acc->FindVertex(gid, storage::View::NEW);
    if (!vertex) throw utils::BasicException("Vertex {} to {} is not in the branch.", gid.AsUint(), what);
    return std::move(*vertex);
  }

  // The edge a record names without the transaction having created it: the record carries the FROM gid.
  storage::EdgeAccessor FindEdge(TxnState &txn, storage::Gid gid, std::optional<storage::Gid> from) {
    if (auto const known = txn.edges.find(gid.AsUint()); known != txn.edges.end()) return known->second;
    if (!from || *from == storage::kInvalidGid) {
      throw utils::BasicException("Edge {} names no endpoints.", gid.AsUint());
    }
    auto edge = ctx_.IsForkCandidateEdge(gid) ? ctx_.FindForkEdge(gid, *from)
                                              : ctx_.FindDiffEdge(gid, *from, storage::View::NEW);
    if (!edge) throw utils::BasicException("Edge {} is not in the branch.", gid.AsUint());
    return txn.edges.emplace(gid.AsUint(), *edge).first->second;
  }

  void ToggleLabel(TxnState &txn, uint64_t ts, storage::Gid gid, const std::string &name, bool add) {
    Acc(txn, ts);
    auto vertex = Vertex(txn, gid, add ? "label" : "unlabel");
    const auto label = core_.NameToLabel(name);
    const bool changed = Require(add ? ctx_.AddLabel(vertex, label) : ctx_.RemoveLabel(vertex, label),
                                 add ? "adding a label to vertex" : "removing a label from vertex",
                                 gid);
    // A fork vertex's record can restate what main already implies; a native vertex's cannot.
    if (!changed && !ctx_.IsForkCandidateVertex(gid)) {
      throw utils::BasicException("Label '{}' of vertex {} is already as the WAL states it.", name, gid.AsUint());
    }
  }

  void Apply(TxnState &txn, dur::Decoder &decoder, uint64_t ts, const dur::WalDeltaData &delta) {
    std::visit(
        utils::Overloaded{
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
              Acc(txn, ts);
              if (ctx_.IsForkCandidateVertex(d.gid)) {
                throw utils::BasicException("The branch WAL creates vertex {}, a gid of the fork state.",
                                            d.gid.AsUint());
              }
              if (!ctx_.CreateNativeVertexAt(d.gid)) {
                throw utils::BasicException("Vertex {} already exists in the branch.", d.gid.AsUint());
              }
            },
            [&](const dur::WalVertexDelete &d) {
              if (txn.skip) return;
              auto &acc = Acc(txn, ts);
              if (ctx_.IsForkCandidateVertex(d.gid)) {
                // The record precedes those of the native edges on the vertex and of fork edges deleted before
                // it in the transaction, which need the vertex alive.
                txn.fork_vertex_deletes.push_back(d.gid);
                return;
              }
              auto vertex = Vertex(txn, d.gid, "delete");
              auto deleted = acc.DeleteVertex(&vertex);
              if (!deleted || !deleted.value()) {
                throw utils::BasicException("Deleting vertex {} failed.", d.gid.AsUint());
              }
            },
            [&](const dur::WalVertexAddLabel &d) {
              if (!txn.skip) ToggleLabel(txn, ts, d.gid, d.label, /*add=*/true);
            },
            [&](const dur::WalVertexRemoveLabel &d) {
              if (!txn.skip) ToggleLabel(txn, ts, d.gid, d.label, /*add=*/false);
            },
            [&](const dur::WalVertexSetProperty &d) {
              if (txn.skip) return;
              Acc(txn, ts);
              auto vertex = Vertex(txn, d.gid, "modify");
              Require(ctx_.SetProperty(
                          vertex, core_.NameToProperty(d.property), storage::ToPropertyValue(d.value, mapper_)),
                      "setting a property of vertex",
                      d.gid);
            },
            [&](const dur::WalEdgeCreate &d) {
              if (txn.skip) return;
              Acc(txn, ts);
              if (ctx_.IsForkCandidateEdge(d.gid)) {
                throw utils::BasicException("The branch WAL creates edge {}, a gid of the fork state.", d.gid.AsUint());
              }
              auto from = ctx_.EnsureDiffVertex(d.from_vertex);
              if (!from) throw utils::BasicException("{}", from.error().message);
              auto to = ctx_.EnsureDiffVertex(d.to_vertex);
              if (!to) throw utils::BasicException("{}", to.error().message);
              auto edge = Require(ctx_.CreateNativeEdgeAt(d.gid, *from, *to, core_.NameToEdgeType(d.edge_type)),
                                  "creating edge",
                                  d.gid);
              txn.edges.emplace(d.gid.AsUint(), std::move(edge));
            },
            [&](const dur::WalEdgeDelete &d) {
              if (txn.skip) return;
              auto &acc = Acc(txn, ts);
              auto edge = FindEdge(txn, d.gid, d.from_vertex);
              if (ctx_.IsForkCandidateEdge(d.gid)) {
                if (!Require(ctx_.DeleteForkEdge(edge), "deleting fork edge", d.gid)) {
                  throw utils::BasicException("Fork edge {} is already deleted in the branch.", d.gid.AsUint());
                }
                return;
              }
              if (!acc.DeleteEdge(&edge)) throw utils::BasicException("Deleting edge {} failed.", d.gid.AsUint());
              txn.edges.erase(d.gid.AsUint());
            },
            [&](const dur::WalEdgeSetProperty &d) {
              if (txn.skip) return;
              Acc(txn, ts);
              auto edge = FindEdge(txn, d.gid, d.from_gid);
              Require(
                  ctx_.SetProperty(edge, core_.NameToProperty(d.property), storage::ToPropertyValue(d.value, mapper_)),
                  "setting a property of edge",
                  d.gid);
            },
            [&](const auto & /*unsupported*/) {
              throw utils::BasicException("A branch WAL holds a delta that a branch diff engine never writes.");
            }},
        delta.data_);
  }

  void Commit(TxnState &txn) {
    for (const auto gid : txn.fork_vertex_deletes) {
      if (!Require(ctx_.DeleteForkVertex(gid), "deleting fork vertex", gid)) {
        throw utils::BasicException("Fork vertex {} is already deleted in the branch.", gid.AsUint());
      }
    }
    auto committed = ctx_.CommitTransaction(
        storage::CommitArgs::make_replica_write(txn.timestamp, /*two_phase_commit=*/false, [] {}));
    if (!committed) throw utils::BasicException("Committing the replayed transaction {} failed.", txn.timestamp);

    auto info = diff_.repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire);
    if (info.ldt_ < txn.timestamp) {
      info.ldt_ = txn.timestamp;
      diff_.repl_storage_state_.commit_ts_info_.store(info, std::memory_order_release);
    }
    txn.edges.clear();
    txn.acc = nullptr;
    txn.holder.reset();
    ctx_.EndTransaction();
  }

  BranchContext &ctx_;
  BranchCore &core_;
  storage::InMemoryStorage &diff_;
  storage::NameIdMapper *mapper_;
};

}  // namespace

std::expected<void, std::string> ReplayBranchWal(std::shared_ptr<BranchCore> core,
                                                 const std::filesystem::path &wal_dir) {
  const memory::DbArenaScope arena_scope{core->main().DbArenaPool()};
  try {
    const auto files = ListWalFiles(wal_dir);
    CheckWalChain(files);
    if (files.empty()) return {};
    BranchContext ctx{std::move(core)};
    BranchWalReplayer{ctx}.Run(files);
  } catch (const std::exception &e) {
    return std::unexpected(std::string{e.what()});
  }
  return {};
}

}  // namespace memgraph::versioning

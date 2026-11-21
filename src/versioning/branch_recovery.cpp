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
#include "utils/atomic_utils.hpp"
#include "utils/crc_accumulator.hpp"
#include "utils/exceptions.hpp"
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

// Selects files as main's GetWalFiles does: unreadable ones and ones with no complete transaction are warned about and
// skipped, never renamed or deleted. Except a finalized (`_from_`) file: FinalizeWal renames a file (RemakeWalName)
// only after syncing its summary, so it held a transaction and a read failure is damage, and an error.
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
      if (entry.path().filename().string().contains("_from_")) {
        throw utils::BasicException(
            "The finalized branch WAL file '{}' is damaged: {}", entry.path().string(), e.what());
      }
      spdlog::warn("Failed to read branch WAL file '{}'. Error: {}", entry.path().string(), e.what());
    }
  }
  if (ec) throw utils::BasicException("Cannot list '{}': {}", dir.string(), ec.message());
  std::ranges::sort(files, [](const WalFileEntry &a, const WalFileEntry &b) {
    return std::tie(a.info.from_timestamp, a.info.seq_num) < std::tie(b.info.from_timestamp, b.info.seq_num);
  });
  return files;
}

// Main's chain rules (RecoverData) with no snapshot. Run continues the numbering across runs, so a missing file
// anywhere shows as a gap; a skipped newest file leaves none, as its number is reused.
void CheckWalChain(const std::vector<WalFileEntry> &files) {
  if (files.empty()) return;
  if (files.front().info.seq_num != 0) {
    throw utils::BasicException("There are missing prefix WAL files: '{}' has sequence number {}, not 0.",
                                files.front().path.string(),
                                files.front().info.seq_num);
  }
  for (size_t i = 1; i < files.size(); ++i) {
    const uint64_t previous = files[i - 1].info.seq_num;
    if (files[i].info.seq_num <= previous) {
      throw utils::BasicException(
          "The WAL files '{}' and '{}' have sequence numbers {} and {}, repeated or out of order.",
          files[i - 1].path.string(),
          files[i].path.string(),
          previous,
          files[i].info.seq_num);
    }
    if (files[i].info.seq_num != previous + 1) {
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
  std::vector<storage::Gid> base_vertex_deletes;
  // Edges created or found in this transaction: later records name them by gid only.
  std::unordered_map<uint64_t, storage::EdgeAccessor> edges;
};

// Replays a branch's WAL into its freshly built core. A record on an object below the gid floors is a base-object write
// and goes through the BranchContext write resolver, as the live write did: the first one materializes the overlay with
// the base's labels, so a label record lands on the full set, a null SET_PROPERTY is an explicit null, a vertex DELETE
// a delete mark and an edge DELETE the base-edge delete. A native object gets the stock operation at its recorded gid.
class BranchWalReplayer {
 public:
  explicit BranchWalReplayer(BranchContext &ctx)
      : ctx_(ctx), core_(*ctx.core()), diff_(ctx.diff_engine()), mapper_(diff_.GetSharedNameIdMapper().get()) {}

  // @throw std::exception the WAL is corrupt or does not fit the base state
  NativeGidEnds Run(const std::vector<WalFileEntry> &files) {
    // PublishCommit asserts ldt_ never decreases, and Build's mirror commits already published their own.
    auto info = diff_.repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire);
    info.ldt_ = 0;
    diff_.repl_storage_state_.commit_ts_info_.store(info, std::memory_order_release);

    for (const auto &file : files) ReplayFile(file);
    diff_.BumpClockPastDurable();
    if (!files.empty()) diff_.SetNextWalSeqNum(files.back().info.seq_num + 1);
    return native_ends_;
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
      auto historical = ctx_.OpenHistorical();
      if (!historical) throw utils::BasicException("{}", historical.error().message);
      txn.holder = diff_.Access(storage::WRITE);
      txn.acc = static_cast<storage::ReplicationAccessor *>(txn.holder.get());
      ctx_.BeginTransaction(std::move(*historical), *txn.acc);
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

  // The vertex a record names: a base vertex of main at the fork, or the diff engine's native one.
  storage::VertexAccessor Vertex(TxnState &txn, storage::Gid gid, std::string_view what) {
    if (!ctx_.IsBaseCandidateVertex(gid)) {
      auto native = txn.acc->FindVertex(gid, storage::View::NEW);
      if (!native) throw utils::BasicException("Vertex {} to {} is not in the branch.", gid.AsUint(), what);
      return *native;
    }
    // Cached per replay: reading a base vertex as of the fork walks main's history of it (see FindBaseVertexPtr).
    auto [cached, first] = base_vertices_.try_emplace(gid.AsUint(), nullptr);
    if (first) cached->second = ctx_.FindBaseVertexPtr(gid);
    if (cached->second == nullptr) {
      throw utils::BasicException("Vertex {} to {} is not in the branch.", gid.AsUint(), what);
    }
    return ctx_.BaseVertexAccessor(cached->second);
  }

  // The edge a record names without the transaction having created it: the record carries the FROM gid.
  storage::EdgeAccessor FindEdge(TxnState &txn, storage::Gid gid, std::optional<storage::Gid> from) {
    if (auto const known = txn.edges.find(gid.AsUint()); known != txn.edges.end()) return known->second;
    if (!from || *from == storage::kInvalidGid) {
      throw utils::BasicException("Edge {} names no endpoints.", gid.AsUint());
    }
    auto edge = ctx_.IsBaseCandidateEdge(gid) ? ctx_.FindBaseEdge(gid, *from)
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
    // The overlay starts from the base's labels, so a logged change always changes the set.
    if (!changed) {
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
              // A skipped transaction's too, as main does: a damaged commit flag must not drop a committed one.
              if (end.txn_crc.has_value() && !utils::CrcAccumulator::Verify(decoder.CrcAccValue())) {
                throw utils::BasicException(
                    "WAL CRC mismatch (stored {}, residue {}).", *end.txn_crc, decoder.CrcAccValue());
              }
              decoder.ResetCrcAcc();
              if (txn.skip) return;
              if (txn.acc == nullptr || txn.timestamp != ts) throw utils::BasicException("Invalid commit data.");
              Commit(txn);
            },
            [&](const dur::WalVertexCreate &d) {
              if (txn.skip) return;
              auto &acc = Acc(txn, ts);
              if (ctx_.IsBaseCandidateVertex(d.gid)) {
                throw utils::BasicException("The branch WAL creates vertex {}, a gid of the base state.",
                                            d.gid.AsUint());
              }
              CheckVertexGidFreeInMain(d.gid);
              if (!acc.CreateVertexEx(d.gid)) {
                throw utils::BasicException("Vertex {} already exists in the branch.", d.gid.AsUint());
              }
              native_ends_.vertex = std::max(native_ends_.vertex, d.gid.AsUint() + 1);
            },
            [&](const dur::WalVertexDelete &d) {
              if (txn.skip) return;
              auto &acc = Acc(txn, ts);
              if (ctx_.IsBaseCandidateVertex(d.gid)) {
                // Deferred to commit: the record precedes the deletes of the vertex's native and base edges, which need
                // it alive.
                txn.base_vertex_deletes.push_back(d.gid);
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
              if (ctx_.IsBaseCandidateEdge(d.gid)) {
                throw utils::BasicException("The branch WAL creates edge {}, a gid of the base state.", d.gid.AsUint());
              }
              if (ctx_.FindDiffEdge(d.gid, d.from_vertex, storage::View::NEW)) {
                throw utils::BasicException("Edge {} already exists in the branch.", d.gid.AsUint());
              }
              CheckEdgeGidFreeInMain(d.gid);
              auto from = ctx_.EnsureDiffVertex(d.from_vertex);
              if (!from) throw utils::BasicException("{}", from.error().message);
              auto to = ctx_.EnsureDiffVertex(d.to_vertex);
              if (!to) throw utils::BasicException("{}", to.error().message);
              auto edge = Require(ctx_.CreateNativeEdgeAt(d.gid, *from, *to, core_.NameToEdgeType(d.edge_type)),
                                  "creating edge",
                                  d.gid);
              txn.edges.emplace(d.gid.AsUint(), edge);
              native_ends_.edge = std::max(native_ends_.edge, d.gid.AsUint() + 1);
            },
            [&](const dur::WalEdgeDelete &d) {
              if (txn.skip) return;
              auto &acc = Acc(txn, ts);
              auto edge = FindEdge(txn, d.gid, d.from_vertex);
              if (ctx_.IsBaseCandidateEdge(d.gid)) {
                if (!Require(ctx_.DeleteBaseEdge(edge), "deleting base edge", d.gid)) {
                  throw utils::BasicException("Base edge {} is already deleted in the branch.", d.gid.AsUint());
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

  // Main recovers independently of branches, so it may already hold a gid the branch created natively; both objects
  // would then answer to it. The branch is refused, never remapped.
  storage::Storage::Accessor &MainAccessor() {
    if (!main_acc_) main_acc_ = core_.main().Access(storage::READ);
    return *main_acc_;
  }

  void CheckVertexGidFreeInMain(storage::Gid gid) {
    if (MainAccessor().FindVertex(gid, storage::View::NEW)) {
      throw utils::BasicException("gid conflict with main: vertex {} exists in main", gid.AsUint());
    }
  }

  // Found by gid alone: the colliding main edge has other endpoints than the branch's. Main finds an edge this way
  // only with properties_on_edges, a metadata index or light edges, so otherwise no edge clash is seen.
  void CheckEdgeGidFreeInMain(storage::Gid gid) {
    if (MainAccessor().FindEdge(gid, storage::View::NEW)) {
      throw utils::BasicException("gid conflict with main: edge {} exists in main", gid.AsUint());
    }
  }

  void Commit(TxnState &txn) {
    // Not a detach, as stock replay (InMemoryStorage wal_apply.cpp, WalVertexDelete -> Accessor::DeleteVertex): the WAL
    // records every edge delete of the vertex, applied above, so an edge left over is an inconsistent WAL and fails the
    // replay (VERTEX_HAS_EDGES) instead of deleting edges the WAL never recorded.
    for (const auto gid : txn.base_vertex_deletes) {
      if (!Require(ctx_.DeleteBaseVertex(gid, /*detach=*/false), "deleting base vertex", gid)) {
        throw utils::BasicException("Base vertex {} is already deleted in the branch.", gid.AsUint());
      }
    }
    auto committed = ctx_.CommitTransaction(
        storage::CommitArgs::make_replica_write(txn.timestamp, /*two_phase_commit=*/false, [] {}).without_wal());
    if (!committed) throw utils::BasicException("Committing the replayed transaction {} failed.", txn.timestamp);
    txn.edges.clear();
    txn.acc = nullptr;
    txn.holder.reset();
    ctx_.EndTransaction();
  }

  BranchContext &ctx_;
  BranchCore &core_;
  storage::InMemoryStorage &diff_;
  storage::NameIdMapper *mapper_;
  std::unordered_map<uint64_t, storage::Vertex *> base_vertices_;
  NativeGidEnds native_ends_;
  std::unique_ptr<storage::Storage::Accessor> main_acc_;
};

}  // namespace

std::expected<NativeGidEnds, std::string> ReplayBranchWal(std::shared_ptr<BranchCore> core,
                                                          const std::filesystem::path &wal_dir) {
  const memory::DbArenaScope arena_scope{core->main().DbArenaPool()};
  try {
    const auto files = ListWalFiles(wal_dir);
    CheckWalChain(files);
    if (files.empty()) return NativeGidEnds{};
    BranchContext ctx{std::move(core), {}, 0};
    return BranchWalReplayer{ctx}.Run(files);
  } catch (const std::exception &e) {
    return std::unexpected(std::string{e.what()});
  }
}

namespace {

// False when the branch never committed (no WAL file). Throws when the directory cannot be inspected.
bool HasWalFiles(const std::filesystem::path &dir) {
  std::error_code ec;
  if (!std::filesystem::exists(dir, ec)) {
    if (ec) throw utils::BasicException("Cannot inspect '{}': {}", dir.string(), ec.message());
    return false;
  }
  for (std::filesystem::directory_iterator it{dir, ec}, end; !ec && it != end; it.increment(ec)) {
    if (it->is_regular_file(ec)) return true;
  }
  if (ec) throw utils::BasicException("Cannot list '{}': {}", dir.string(), ec.message());
  return false;
}

// Builds `name`'s core in the store with its WAL replayed and returns the native gids it holds, or the reason it
// cannot.
std::expected<NativeGidEnds, std::string> RecoverBranch(VersionStore &store, storage::InMemoryStorage &main,
                                                        const std::string &name, const std::filesystem::path &wal_dir) {
  try {
    if (!HasWalFiles(wal_dir)) return NativeGidEnds{};
    auto branch = store.Checkout(name);
    if (!branch) return std::unexpected(std::move(branch.error()));
    const auto &info = (*branch)->info();
    NativeGidEnds ends;
    const auto core = (*branch)->GetOrBuildCore([&]() -> CoreResult {
      auto built = BranchCore::Build(main, info.fork, (*branch)->fork_ts(), info.id);
      if (!built) return std::unexpected(std::move(built.error().message));
      auto replayed = ReplayBranchWal(*built, wal_dir);
      if (!replayed) return std::unexpected(std::move(replayed.error()));
      ends = *replayed;
      return std::move(*built);
    });
    if (!core) return std::unexpected(core.error());
    return ends;
  } catch (const std::exception &e) {
    return std::unexpected(std::string{e.what()});
  }
}

}  // namespace

void RecoverBranches(VersionStore &store, storage::InMemoryStorage &main) {
  const memory::DbArenaScope arena_scope{main.DbArenaPool()};
  const auto branches_dir = main.config_.durability.storage_directory / "branches";
  for (const auto &listing : store.List()) {
    if (listing.unavailable_reason) continue;
    const auto &info = listing.info;
    const auto ends = RecoverBranch(store, main, listing.name, branches_dir / std::to_string(info.id) / "wal");
    if (!ends) {
      store.MarkUnavailable(info.id, ends.error());
      spdlog::warn("Graph Versioning: branch '{}' of database '{}' cannot be recovered and is unavailable: {}.",
                   listing.name,
                   main.config_.salient.name.str(),
                   ends.error());
      continue;
    }
    atomic_fetch_max_explicit(&main.vertex_id_, ends->vertex, std::memory_order_acq_rel);
    atomic_fetch_max_explicit(&main.edge_id_, ends->edge, std::memory_order_acq_rel);
  }
}

}  // namespace memgraph::versioning

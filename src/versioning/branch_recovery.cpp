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

struct WalListing {
  std::vector<WalFileEntry> files;
  // Regular files in the directory, readable as a WAL or not.
  size_t regular_files{0};
};

// Selects files as main's GetWalFiles does: unreadable ones and ones with no complete transaction are warned about and
// skipped, never renamed or deleted. FinalizeWal renames a file (RemakeWalName) only after syncing its summary, so a
// summary that fails in a file so named is damage, not a crash, and an error.
WalListing ListWalFiles(const std::filesystem::path &dir) {
  WalListing listing;
  auto &files = listing.files;
  std::error_code ec;
  for (const auto &entry : std::filesystem::directory_iterator(dir, ec)) {
    if (entry.is_regular_file()) ++listing.regular_files;
    if (!dur::ValidateDurabilityFile(entry)) continue;
    try {
      auto header = dur::ReadWalHeader(entry.path());
      auto info = dur::ReadWalContents(entry.path(), std::move(header));
      files.push_back({.path = entry.path(), .info = std::move(info)});
    } catch (const dur::RecoveryFailure &e) {
      if (dynamic_cast<const dur::WalSummaryMismatch *>(&e) != nullptr &&
          entry.path().filename().string().contains("_from_")) {
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
  return listing;
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
  std::vector<storage::Gid> fork_vertex_deletes;
  // Edges created or found in this transaction: later records name them by gid only.
  std::unordered_map<uint64_t, storage::EdgeAccessor> edges;
};

// Replays a branch's WAL into its freshly built core. A record on an object below the gid floors is a fork-object write
// and goes through the BranchContext write resolver, as the live write did; a native object gets the stock operation.
class BranchWalReplayer {
 public:
  explicit BranchWalReplayer(BranchContext &ctx)
      : ctx_(ctx), core_(*ctx.core()), diff_(ctx.diff_engine()), mapper_(diff_.GetSharedNameIdMapper().get()) {}

  // @throw std::exception the WAL is corrupt or does not fit the fork base
  void Run(const std::vector<WalFileEntry> &files) {
    const storage::InMemoryStorage::SuppressWalScope no_wal{diff_};
    // PublishCommit asserts ldt_ never decreases, and Build's mirror commits already published their own.
    auto info = diff_.repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire);
    info.ldt_ = 0;
    diff_.repl_storage_state_.commit_ts_info_.store(info, std::memory_order_release);

    for (const auto &file : files) ReplayFile(file);
    diff_.BumpClockPastDurable();
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
    if (!ctx_.IsForkCandidateVertex(gid)) {
      auto native = txn.acc->FindVertex(gid, storage::View::NEW);
      if (!native) throw utils::BasicException("Vertex {} to {} is not in the branch.", gid.AsUint(), what);
      return std::move(*native);
    }
    // Cached per replay: reading a fork vertex as of the fork walks main's history of it (see FindForkVertexPtr).
    auto [cached, first] = fork_vertices_.try_emplace(gid.AsUint(), nullptr);
    if (first) cached->second = ctx_.FindForkVertexPtr(gid);
    if (cached->second == nullptr) {
      throw utils::BasicException("Vertex {} to {} is not in the branch.", gid.AsUint(), what);
    }
    return ctx_.ForkVertexAccessor(cached->second);
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
              if (ctx_.IsForkCandidateVertex(d.gid)) {
                throw utils::BasicException("The branch WAL creates vertex {}, a gid of the fork state.",
                                            d.gid.AsUint());
              }
              if (!acc.CreateVertexEx(d.gid)) {
                throw utils::BasicException("Vertex {} already exists in the branch.", d.gid.AsUint());
              }
            },
            [&](const dur::WalVertexDelete &d) {
              if (txn.skip) return;
              auto &acc = Acc(txn, ts);
              if (ctx_.IsForkCandidateVertex(d.gid)) {
                // Deferred to commit: the record precedes the deletes of the vertex's native and fork edges, which need
                // it alive.
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
              if (ctx_.FindDiffEdge(d.gid, d.from_vertex, storage::View::NEW)) {
                throw utils::BasicException("Edge {} already exists in the branch.", d.gid.AsUint());
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
    txn.edges.clear();
    txn.acc = nullptr;
    txn.holder.reset();
    ctx_.EndTransaction();
  }

  BranchContext &ctx_;
  BranchCore &core_;
  storage::InMemoryStorage &diff_;
  storage::NameIdMapper *mapper_;
  std::unordered_map<uint64_t, storage::Vertex *> fork_vertices_;
};

// False for a branch that never committed (no WAL file). Throws if the directory cannot be inspected.
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

// Why the branch's files cannot be used, or nullopt. Floors in the ledger mean the diff engine was built and created
// both directories, so a missing one is lost writes; a wrong-kind or uninspectable path is damage.
std::optional<std::string> FindDamagedBranchFiles(const BranchInfo &info, const std::filesystem::path &branch_dir,
                                                  const std::filesystem::path &wal_dir) {
  const bool was_used = info.vertex_gid_floor && info.edge_gid_floor;
  for (const auto *path : {&branch_dir, &wal_dir}) {
    const char *what = path == &branch_dir ? "branch directory" : "write-ahead log directory";
    std::error_code ec;
    const auto status = std::filesystem::symlink_status(*path, ec);
    if (status.type() == std::filesystem::file_type::not_found) {
      if (was_used) return fmt::format("the {} '{}' is missing", what, path->string());
      return std::nullopt;
    }
    if (ec) return fmt::format("the {} '{}' cannot be inspected: {}", what, path->string(), ec.message());
    if (status.type() == std::filesystem::file_type::symlink) {
      const auto target = std::filesystem::status(*path, ec);
      if (target.type() == std::filesystem::file_type::not_found) {
        return fmt::format("the {} '{}' is a link to a path that does not exist", what, path->string());
      }
      if (ec) return fmt::format("the {} '{}' cannot be inspected: {}", what, path->string(), ec.message());
      if (target.type() != std::filesystem::file_type::directory) {
        return fmt::format("the {} '{}' is not a directory", what, path->string());
      }
    } else if (status.type() != std::filesystem::file_type::directory) {
      return fmt::format("the {} '{}' is not a directory", what, path->string());
    }
  }
  return std::nullopt;
}

// Without the ledger's floors the native/fork gid boundary is lost, so the replay cannot be trusted.
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
  if (auto replayed = ReplayBranchWal(*built, wal_dir); !replayed) return std::unexpected(std::move(replayed.error()));
  return std::move(*built);
}

// A new main object below a branch's floor would be misread as a fork object, and a reused native gid would alias two
// objects; the ledger's ceilings cover both, no branch file is read.
void RaiseMainCountersToLedger(storage::InMemoryStorage &main, const BranchInfo &info) {
  atomic_fetch_max_explicit(&main.vertex_id_, GidsHeldBelow(info, GidKind::kVertex), std::memory_order_acq_rel);
  atomic_fetch_max_explicit(&main.edge_id_, GidsHeldBelow(info, GidKind::kEdge), std::memory_order_acq_rel);
}

// Recovers one branch; the reason it cannot be, or nullopt.
std::optional<std::string> RecoverBranch(VersionStore &store, storage::InMemoryStorage &main, const std::string &name,
                                         const BranchInfo &info, const std::filesystem::path &branch_dir) {
  const auto wal_dir = branch_dir / "wal";
  try {
    if (auto damaged = FindDamagedBranchFiles(info, branch_dir, wal_dir)) return damaged;
    if (!HasWalFiles(wal_dir)) return std::nullopt;
    const auto core = store.GetOrBuildCore(info.number, [&] {
      auto built = BuildAndReplay(main, info, wal_dir);
      if (built) (*built)->SetGidReserver(store.GidReserverOf(info.number));
      return built;
    });
    if (!core) return core.error();
    spdlog::info("Graph Versioning: recovered the contents of branch '{}'.", name);
    return std::nullopt;
  } catch (const std::exception &e) {
    return std::string{e.what()};
  }
}

std::string DescribeUnrecoveredBranch(std::string_view database, std::string_view name, uint64_t number,
                                      std::string_view reason, const std::filesystem::path &storage_directory) {
  while (reason.ends_with('.')) reason.remove_suffix(1);
  const auto branches_dir = storage_directory / "branches";
  const auto branch_dir = branches_dir / std::to_string(number);
  return fmt::format(
      "branch '{0}' of database '{1}' cannot be recovered and is set aside as unavailable: {2}. Main and the other "
      "branches start normally; nothing was changed on disk and no data was deleted.\n"
      "  branch directory: {3}\n"
      "  write-ahead log:  {4}\n"
      "  branch ledger:    {5}\n"
      "The branch keeps main's history from its fork point, so main's disk use grows meanwhile. SHOW BRANCHES lists it "
      "with status 'unavailable'; using it fails. To bring it back: restore its files from a backup and restart "
      "Memgraph. To remove it and release that history: DROP BRANCH '{0}' (deletes the branch directory). Do not edit "
      "the branch ledger: it is checksummed.",
      name,
      database,
      reason,
      branch_dir.string(),
      (branch_dir / "wal").string(),
      (branches_dir / "ledger").string());
}

}  // namespace

std::expected<void, std::string> ReplayBranchWal(std::shared_ptr<BranchCore> core,
                                                 const std::filesystem::path &wal_dir) {
  const memory::DbArenaScope arena_scope{core->main().DbArenaPool()};
  try {
    const auto listing = ListWalFiles(wal_dir);
    const auto &files = listing.files;
    CheckWalChain(files);
    if (files.empty()) {
      // A crash while the first transaction is written tears the only file, and main skips such a file too; a log of
      // several files with not one readable is not a crash's work.
      if (listing.regular_files > 1) {
        throw utils::BasicException(
            "none of the {} files in '{}' is a readable branch WAL", listing.regular_files, wal_dir.string());
      }
      spdlog::warn(
          "Graph Versioning: none of the files in '{}' is a readable branch WAL, so the writes made on the branch are "
          "lost and it starts as it was forked.",
          wal_dir.string());
      return {};
    }
    BranchContext ctx{std::move(core)};
    BranchWalReplayer{ctx}.Run(files);
  } catch (const std::exception &e) {
    return std::unexpected(std::string{e.what()});
  }
  return {};
}

std::unordered_map<uint64_t, std::string> FindHistoryShortfalls(const LedgerContents &ledger,
                                                                std::span<const uint64_t> durable_at_fork) {
  std::unordered_map<uint64_t, std::string> shortfalls;
  for (size_t i = 0; i < ledger.branches.size() && i < durable_at_fork.size(); ++i) {
    const auto &info = ledger.branches[i].info;
    if (!info.fork_durable_ts || durable_at_fork[i] >= *info.fork_durable_ts) continue;
    shortfalls.emplace(info.number,
                       fmt::format("main's recovered history ends at timestamp {} before the fork at {}, but main had "
                                   "committed up to {} by then",
                                   durable_at_fork[i],
                                   info.recorded_fork_ts,
                                   *info.fork_durable_ts));
  }
  return shortfalls;
}

void RecoverBranches(VersionStore &store, storage::InMemoryStorage &main,
                     const std::unordered_map<uint64_t, std::string> &unrecoverable) {
  const memory::DbArenaScope arena_scope{main.DbArenaPool()};
  const auto branches_dir = main.config_.durability.storage_directory / "branches";
  for (const auto &[name, info] : store.List()) {
    const auto branch_dir = branches_dir / std::to_string(info.number);
    const auto known = unrecoverable.find(info.number);
    auto failure = known != unrecoverable.end() ? std::optional{known->second}
                                                : RecoverBranch(store, main, name, info, branch_dir);
    // Also for a set-aside branch, so a repaired one later does not collide with main's new objects.
    RaiseMainCountersToLedger(main, info);
    if (!failure) continue;
    store.MarkUnavailable(info.number, *failure);
    spdlog::warn(
        "Graph Versioning: {}",
        DescribeUnrecoveredBranch(
            main.config_.salient.name.str(), name, info.number, *failure, main.config_.durability.storage_directory));
  }
}

void KeepMainClearOfHiddenBranches(const LedgerContents &ledger, storage::InMemoryStorage &main) {
  if (ledger.branches.empty()) return;
  uint64_t newest_fork = 0;
  for (const auto &[name, info] : ledger.branches) {
    newest_fork = std::max(newest_fork, info.recorded_fork_ts);
    // No branch file is read: the ledger's ceilings, durable before the branch used a gid, alone keep gids disjoint.
    RaiseMainCountersToLedger(main, info);
  }
  main.BumpClockPastDurable(newest_fork);
}

void RaiseGidCountersForBranches(const VersionStore &store, storage::InMemoryStorage &main) {
  for (const auto &[name, info] : store.List()) RaiseMainCountersToLedger(main, info);
}

}  // namespace memgraph::versioning

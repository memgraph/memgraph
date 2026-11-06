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

// What the branch-recovery tests share: a durable database with a base graph and one or more branches, a view of what a
// session reads on a branch (and what its change filters, touched gates and fork-delete counts say), and the round trip
// "run a workload, restart, compare". Each test executable keeps its files under a directory named after itself.
#pragma once

#include <errno.h>  // program_invocation_short_name

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/scheduler.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/version_store.hpp"

import memgraph.storage.property_value;

namespace memgraph::tests::versioning_recovery {

namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;
namespace dur = memgraph::storage::durability;
namespace fs = std::filesystem;

inline const std::filesystem::path kStorageDirectory{std::filesystem::temp_directory_path() /
                                                     program_invocation_short_name};
inline const std::filesystem::path kOtherStorageDirectory{std::filesystem::temp_directory_path() /
                                                          (std::string{program_invocation_short_name} + "_other")};

struct Options {
  bool properties_on_edges{true};
  bool auto_index{false};
  bool versioning{true};
};

inline ms::Config MakeConfig(bool recover_on_startup, Options options = {},
                             const std::filesystem::path &directory = kStorageDirectory) {
  ms::Config config{
      .durability = {.storage_directory = directory,
                     .recover_on_startup = recover_on_startup,
                     .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                     // Effectively disable periodic snapshotting so recovery is WAL-only.
                     .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)}},
      .experimental_versioning = options.versioning,
  };
  config.salient.items.properties_on_edges = options.properties_on_edges;
  config.salient.items.enable_label_index_auto_creation = options.auto_index;
  config.salient.items.enable_edge_type_index_auto_creation = options.auto_index;
  return config;
}

inline auto *Mem(memgraph::dbms::Database &db) { return static_cast<ms::InMemoryStorage *>(db.storage()); }

inline void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

// A vertex created on main by a plain write.
inline std::optional<ms::Gid> NewMainVertex(memgraph::dbms::Database &db) {
  auto acc = db.Access(ms::WRITE);
  const auto gid = acc->CreateVertex().Gid();
  if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) return std::nullopt;
  return gid;
}

// Every regular file under `directory` by relative path, with its bytes.
inline std::map<std::string, std::string> FileContents(const std::filesystem::path &directory) {
  std::map<std::string, std::string> files;
  for (const auto &entry : std::filesystem::recursive_directory_iterator(directory)) {
    if (!entry.is_regular_file()) continue;
    std::ifstream in{entry.path(), std::ios::binary};
    files.emplace(std::filesystem::relative(entry.path(), directory).string(),
                  std::string{std::istreambuf_iterator<char>{in}, {}});
  }
  return files;
}

// What startup refuses with over the files in kStorageDirectory; empty when the database started.
inline std::string StartupRefusal() {
  try {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  } catch (const memgraph::utils::BasicException &e) {
    return e.what();
  }
  return {};
}

// Everything a refused startup must leave alone: main's snapshots and WAL and every branch file. The trigger and
// stream stores are opened by every start.
inline std::map<std::string, std::string> DurableFiles() {
  auto files = FileContents(kStorageDirectory);
  std::erase_if(files, [](const auto &file) {
    return file.first.starts_with("triggers/") || file.first.starts_with("streams/");
  });
  return files;
}

// Runs the `mv '<from>' '<to>'` lines of `message` the way an administrator would.
inline void FollowMoveSteps(const std::string &message) {
  size_t moved = 0;
  for (auto at = message.find("mv '"); at != std::string::npos; at = message.find("mv '", at + 1)) {
    const auto from_begin = at + 4;
    const auto from_end = message.find('\'', from_begin);
    const auto to_begin = message.find('\'', from_end + 1) + 1;
    const auto to_end = message.find('\'', to_begin);
    ASSERT_NE(to_end, std::string::npos) << message;
    std::filesystem::rename(message.substr(from_begin, from_end - from_begin),
                            message.substr(to_begin, to_end - to_begin));
    ++moved;
  }
  ASSERT_GT(moved, 0U) << "the message names no step to take: " << message;
}

inline std::filesystem::path BranchWalDir(uint64_t number, const std::filesystem::path &directory = kStorageDirectory) {
  return directory / "branches" / std::to_string(number) / "wal";
}

inline std::vector<std::filesystem::path> WalFiles(uint64_t number) {
  std::vector<std::filesystem::path> files;
  if (!std::filesystem::exists(BranchWalDir(number))) return files;
  for (const auto &entry : std::filesystem::directory_iterator(BranchWalDir(number))) {
    if (entry.is_regular_file()) files.push_back(entry.path());
  }
  std::ranges::sort(files);
  return files;
}

// v1(:L:M {p:1, q:5}) -e1[:T {w:1}]-> v2(:M);  v3(:L) -e2[:T {w:2}]-> v1;  v4.
struct Base {
  ms::Gid v1, v2, v3, v4, e1, e2;
};

inline Base CreateBase(memgraph::dbms::Database &db, bool properties_on_edges = true) {
  auto *st = db.storage();
  auto acc = db.Access(ms::WRITE);
  auto v1 = acc->CreateVertex();
  auto v2 = acc->CreateVertex();
  auto v3 = acc->CreateVertex();
  auto v4 = acc->CreateVertex();
  EXPECT_TRUE(v1.AddLabel(st->NameToLabel("L")).has_value());
  EXPECT_TRUE(v1.AddLabel(st->NameToLabel("M")).has_value());
  EXPECT_TRUE(v1.SetProperty(st->NameToProperty("p"), ms::PropertyValue(1)).has_value());
  EXPECT_TRUE(v1.SetProperty(st->NameToProperty("q"), ms::PropertyValue(5)).has_value());
  EXPECT_TRUE(v2.AddLabel(st->NameToLabel("M")).has_value());
  EXPECT_TRUE(v3.AddLabel(st->NameToLabel("L")).has_value());
  auto e1 = acc->CreateEdge(&v1, &v2, st->NameToEdgeType("T"));
  auto e2 = acc->CreateEdge(&v3, &v1, st->NameToEdgeType("T"));
  EXPECT_TRUE(e1.has_value() && e2.has_value());
  if (properties_on_edges) {
    EXPECT_TRUE(e1->SetProperty(st->NameToProperty("w"), ms::PropertyValue(1)).has_value());
    EXPECT_TRUE(e2->SetProperty(st->NameToProperty("w"), ms::PropertyValue(2)).has_value());
  }
  Base base{v1.Gid(), v2.Gid(), v3.Gid(), v4.Gid(), e1->Gid(), e2->Gid()};
  Commit(*acc);
  return base;
}

// What a session reads of the branch, as plain data by names, plus what the branch's bookkeeping says about it.
struct EdgeDump {
  uint64_t gid;
  std::string type;
  uint64_t to;
  std::map<std::string, std::string> props;
};

struct VertexDump {
  uint64_t gid;
  std::set<std::string> labels;
  std::map<std::string, std::string> props;
  std::vector<EdgeDump> out;
};

struct Captured {
  std::vector<VertexDump> vertices;
  std::string bookkeeping;  // fork-delete counts, touched gates, change filters

  const VertexDump *Find(ms::Gid gid) const {
    const auto it = std::ranges::find(vertices, gid.AsUint(), &VertexDump::gid);
    return it == vertices.end() ? nullptr : &*it;
  }

  // The vertex a test expects to exist; a missing one fails the test and yields an empty dump.
  const VertexDump &At(ms::Gid gid) const {
    static const VertexDump kMissing{};
    const auto *found = Find(gid);
    if (found == nullptr) ADD_FAILURE() << "vertex " << gid.AsUint() << " is not in the branch";
    return found != nullptr ? *found : kMissing;
  }

  std::string Render() const {
    std::ostringstream out;
    for (const auto &v : vertices) {
      out << "v" << v.gid << " [";
      for (const auto &label : v.labels) out << label << ",";
      out << "] {";
      for (const auto &[k, value] : v.props) out << k << "=" << value << ";";
      out << "}\n";
      for (const auto &e : v.out) {
        out << "  e" << e.gid << " " << e.type << " -> v" << e.to << " {";
        for (const auto &[k, value] : e.props) out << k << "=" << value << ";";
        out << "}\n";
      }
    }
    out << bookkeeping;
    return out.str();
  }
};

inline std::string Show(const ms::PropertyValue &value) {
  if (value.IsNull()) return "null";
  if (value.IsBool()) return value.ValueBool() ? "true" : "false";
  if (value.IsInt()) return std::to_string(value.ValueInt());
  return "?";
}

inline std::map<std::string, std::string> Named(ms::Storage &st,
                                                const std::map<ms::PropertyId, ms::PropertyValue> &props) {
  std::map<std::string, std::string> named;
  for (const auto &[pid, value] : props) named.emplace(st.PropertyToName(pid), Show(value));
  return named;
}

// The branch's counts, fork-delete counts, the touched gates of every label and property named in the workloads, and
// the change filters of every base object. Every write of a workload changes something, so replay (which records only
// logged changes) must leave the same traces as the live run.
inline std::string Bookkeeping(memgraph::dbms::Database &db, mv::BranchCore &core, const Base &base) {
  auto &st = *db.storage();
  std::ostringstream out;
  const auto deletes = core.ForkDeletes();
  out << "deletes v=" << deletes.vertices << " e=" << deletes.edges << "\n";
  const auto counts = core.ApproximateCounts();
  out << "counts v=" << counts.vertices << " e=" << counts.edges << "\n";
  for (const auto *name : {"L", "M", "X", "NewL", "N"}) {
    out << "label " << name << ":" << core.MayHaveLabelTouched(st.NameToLabel(name)) << "\n";
  }
  for (const auto *name : {"p", "q", "w", "a"}) {
    out << "prop " << name << ":" << core.MayHavePropertyTouched(st.NameToProperty(name)) << "\n";
  }
  for (const auto gid : {base.v1, base.v2, base.v3, base.v4}) {
    out << "filter v" << gid.AsUint() << " label=" << core.MayHaveLabelChange(gid)
        << " prop=" << core.MayHavePropertyChange(gid) << " edge=" << core.MayHaveEdgeChange(gid)
        << " deleted=" << core.MayHaveVertexDeleted(gid) << "\n";
  }
  for (const auto gid : {base.e1, base.e2}) {
    out << "filter e" << gid.AsUint() << " deleted=" << core.MayHaveForkEdgeDeleted(gid)
        << " prop=" << core.MayHaveForkEdgePropertyChange(gid) << "\n";
  }
  return out.str();
}

// Every vertex the branch holds as the open transaction of `ctx` reads it, by gid.
inline std::vector<VertexDump> ReadVertices(memgraph::dbms::Database &db, mv::BranchContext &ctx) {
  auto &st = *db.storage();
  std::vector<VertexDump> vertices;
  for (auto v : ctx.Vertices(ms::View::NEW)) {
    VertexDump d;
    d.gid = v.Gid().AsUint();
    for (const auto label : *ctx.Labels(v, ms::View::NEW)) d.labels.insert(st.LabelToName(label));
    d.props = Named(st, *ctx.Properties(v, ms::View::NEW));
    auto edges = ctx.OutEdges(v, ms::View::NEW);
    EXPECT_TRUE(edges.has_value());
    for (const auto &e : *edges) {
      d.out.push_back({.gid = e.Gid().AsUint(),
                       .type = st.EdgeTypeToName(e.EdgeType()),
                       .to = e.ToVertex().Gid().AsUint(),
                       .props = Named(st, *ctx.Properties(e, ms::View::NEW))});
    }
    std::ranges::sort(d.out, {}, &EdgeDump::gid);
    vertices.push_back(std::move(d));
  }
  std::ranges::sort(vertices, {}, &VertexDump::gid);
  return vertices;
}

inline Captured Capture(memgraph::dbms::Database &db, mv::BranchContext &ctx, const Base &base) {
  Captured captured;
  auto diff_acc = ctx.diff_engine().Access(ms::READ);
  EXPECT_TRUE(ctx.BeginTransaction(diff_acc.get()).has_value());
  captured.vertices = ReadVertices(db, ctx);
  ctx.EndTransaction();
  captured.bookkeeping = Bookkeeping(db, *ctx.core(), base);
  return captured;
}

// Main as a reader sees it: a branch's writes must never reach it.
inline std::string DumpMain(memgraph::dbms::Database &db) {
  auto &st = *db.storage();
  std::ostringstream out;
  auto acc = db.Access(ms::READ);
  for (auto v : acc->Vertices(ms::View::OLD)) {
    out << "v" << v.Gid().AsUint() << " [";
    for (const auto label : *v.Labels(ms::View::OLD)) out << st.LabelToName(label) << ",";
    out << "] {";
    for (const auto &[k, value] : Named(st, *v.Properties(ms::View::OLD))) out << k << "=" << value << ";";
    out << "}\n";
    for (const auto &e : v.OutEdges(ms::View::OLD)->edges) {
      out << "  e" << e.Gid().AsUint() << " -> v" << e.ToVertex().Gid().AsUint() << "\n";
    }
  }
  return out.str();
}

// Attaches to `name` the way USE BRANCH does: the registry's core, built on first use, with its floors recorded.
inline std::unique_ptr<mv::BranchContext> Checkout(memgraph::dbms::Database &db, std::string_view name,
                                                   bool *built = nullptr) {
  auto *store = db.version_store();
  auto info = store->Get(name);
  EXPECT_TRUE(info.has_value());
  if (!info) return nullptr;
  auto core = store->GetOrBuildCore(info->number, [&]() -> std::expected<std::shared_ptr<mv::BranchCore>, std::string> {
    if (built != nullptr) *built = true;
    auto fresh = mv::BranchCore::Build(*Mem(db), info->fork_ts, info->number, std::nullopt, info->fork_counts);
    if (!fresh) return std::unexpected(fresh.error().message);
    return std::move(*fresh);
  });
  EXPECT_TRUE(core.has_value());
  if (!core) return nullptr;
  EXPECT_TRUE(store->RecordGidFloors(info->number, (*core)->vertex_gid_floor(), (*core)->edge_gid_floor()).has_value());
  return std::make_unique<mv::BranchContext>(*core);
}

// A running branch: the pieces a workload writes with.
struct Env {
  memgraph::dbms::Database &db;
  mv::BranchContext &ctx;
  Base base;

  ms::LabelId Label(std::string_view name) { return db.storage()->NameToLabel(name); }

  ms::PropertyId Prop(std::string_view name) { return db.storage()->NameToProperty(name); }

  ms::EdgeTypeId EdgeType(std::string_view name) { return db.storage()->NameToEdgeType(name); }

  ms::VertexAccessor Fork(ms::Gid gid) {
    auto fork = ctx.FindForkVertex(gid);
    EXPECT_TRUE(fork.has_value());
    return std::move(*fork);
  }

  ms::EdgeAccessor ForkEdge(ms::Gid edge, ms::Gid from) {
    auto fork = ctx.FindForkEdge(edge, from);
    EXPECT_TRUE(fork.has_value());
    return std::move(*fork);
  }

  ms::VertexAccessor Stub(ms::Gid gid) {
    auto stub = ctx.EnsureDiffVertex(gid);
    EXPECT_TRUE(stub.has_value());
    return std::move(*stub);
  }

  // Runs `body` as one branch transaction that commits.
  template <typename Body>
  void Txn(Body &&body) {
    auto diff_acc = ctx.diff_engine().Access(ms::WRITE);
    ASSERT_TRUE(ctx.BeginTransaction(diff_acc.get()).has_value());
    body(ctx, *diff_acc);
    ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
    ctx.EndTransaction();
  }
};

// Removes the storage directories around each test.
class RecoveryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all(kStorageDirectory);
    std::filesystem::remove_all(kOtherStorageDirectory);
  }

  void TearDown() override {
    std::filesystem::remove_all(kStorageDirectory);
    std::filesystem::remove_all(kOtherStorageDirectory);
  }
};

struct RoundTrip {
  Captured live;
  Captured replayed;
  Base base;
};

// Runs `workload` on a durable branch, restarts the database, replays the branch WAL, and requires everything the
// session reads and everything the branch's bookkeeping says to be the same as before, and main untouched.
inline RoundTrip RunRoundTrip(const std::function<void(Env &)> &workload, Options options = {}) {
  RoundTrip out;
  std::string main_before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false, options)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    out.base = CreateBase(db, options.properties_on_edges);
    EXPECT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto ctx = Checkout(db, "b");
    EXPECT_NE(ctx, nullptr);
    if (ctx == nullptr) return out;
    Env env{db, *ctx, out.base};
    workload(env);
    if (::testing::Test::HasFatalFailure()) return out;
    out.live = Capture(db, *ctx, out.base);
    main_before = DumpMain(db);
  }
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true, options)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  bool built = false;
  auto ctx = Checkout(db, "b", &built);
  EXPECT_NE(ctx, nullptr);
  EXPECT_FALSE(built) << "startup recovery registered the core";
  if (ctx == nullptr) return out;
  out.replayed = Capture(db, *ctx, out.base);
  EXPECT_EQ(out.replayed.Render(), out.live.Render());
  EXPECT_EQ(DumpMain(db), main_before) << "branch writes must not reach main";
  return out;
}

inline std::string Joined(const std::set<std::string> &names) {
  std::string joined;
  for (const auto &name : names) joined += name + ",";
  return joined;
}

// ---- WAL files as bytes: reading, cutting into transactions, and leaving a file as a crash would

inline std::string ReadFile(const fs::path &path) {
  std::ifstream in(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

inline void WriteFile(const fs::path &path, std::string_view data) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out.write(data.data(), static_cast<std::streamsize>(data.size()));
}

struct DeltaSpan {
  uint64_t begin{0};
  uint64_t end{0};
  bool on_fork{false};  // a record that names an object of the fork state
};

struct TxnSpan {
  uint64_t begin{0};
  uint64_t end{0};
  std::vector<DeltaSpan> deltas;
};

// The byte range of every transaction of a cleanly finalized WAL, or of an unfinalized one with whole transactions;
// `on_fork` classifies each record.
inline std::vector<TxnSpan> ReadWalSpans(
    const fs::path &wal,
    const std::function<bool(const dur::WalDeltaData &)> &on_fork = [](const auto &) { return false; }) {
  std::vector<TxnSpan> txns;
  const auto header = dur::ReadWalHeader(wal);
  dur::Decoder decoder;
  const auto version = decoder.Initialize(wal, dur::kWalMagic);
  if (!version || !decoder.SetPosition(header.offset_deltas)) {
    ADD_FAILURE() << "cannot open " << wal;
    return txns;
  }
  const uint64_t size = decoder.GetSize();
  bool in_txn = false;
  while (decoder.GetPosition() < size) {
    const uint64_t begin = decoder.GetPosition();
    static_cast<void>(dur::ReadWalDeltaHeader(&decoder));
    const auto delta = dur::ReadWalDeltaData(&decoder, *version);
    const uint64_t end = decoder.GetPosition();
    if (!in_txn) {
      txns.push_back({.begin = begin});
      in_txn = true;
    }
    txns.back().deltas.push_back({.begin = begin, .end = end, .on_fork = on_fork(delta)});
    if (dur::IsWalDeltaDataTransactionEnd(delta, *version)) {
      txns.back().end = end;
      in_txn = false;
    }
  }
  EXPECT_FALSE(in_txn) << "the file ends inside a transaction";
  return txns;
}

// The summary region of a never-finalized file: three zero values and their CRC trailer, as WalFile's constructor
// writes them. Produced by the same encoder so the CRC is right by construction.
inline std::string SummaryPlaceholder() {
  const auto scratch = fs::temp_directory_path() / "versioning_crash_recovery_summary";
  {
    dur::Encoder<memgraph::utils::OutputFile> encoder;
    EXPECT_TRUE(encoder.Initialize(scratch));
    encoder.ResetCrcAcc();
    encoder.WriteUint(0);
    encoder.WriteUint(0);
    encoder.WriteUint(0);
    encoder.WriteCrc();
    encoder.Close();
  }
  auto bytes = ReadFile(scratch);
  fs::remove(scratch);
  return bytes;
}

// Leaves `wal` as a crash would have: only its first `keep` bytes, and without the summary FinalizeWal wrote.
// `wal` must be a cleanly finalized file.
inline void CrashAt(const fs::path &wal, uint64_t keep) {
  const auto header = dur::ReadWalHeader(wal);
  constexpr uint64_t kSummaryBytes = 4 * (sizeof(dur::Marker) + sizeof(uint64_t));
  const auto placeholder = SummaryPlaceholder();
  ASSERT_EQ(placeholder.size(), kSummaryBytes);
  ASSERT_GE(header.offset_deltas, kSummaryBytes);
  auto bytes = ReadFile(wal);
  bytes.replace(header.offset_deltas - kSummaryBytes, kSummaryBytes, placeholder);
  bytes.resize(std::min<uint64_t>(keep, bytes.size()));
  WriteFile(wal, bytes);
  if (keep >= header.offset_deltas) {
    ASSERT_FALSE(dur::ReadWalHeader(wal).summary.has_value()) << "the crash image must read as unfinalized";
  }
}

inline void FlipByte(const fs::path &file, uint64_t offset) {
  auto bytes = ReadFile(file);
  ASSERT_LT(offset, bytes.size());
  bytes[offset] = static_cast<char>(bytes[offset] ^ 0xFF);
  WriteFile(file, bytes);
}

}  // namespace memgraph::tests::versioning_recovery

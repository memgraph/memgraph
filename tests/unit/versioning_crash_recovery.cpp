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

// Crash consistency of branch durability: a crash image (a cleanly closed database's files, truncated inside the last
// transaction, summary reset) must recover the last complete transaction. Other damage sets the branch aside as
// unavailable.
#include <unistd.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <set>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/file.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/version_store.hpp"
#include "versioning_recovery_support.hpp"

using namespace memgraph::tests::versioning_recovery;  // NOLINT(google-build-using-namespace)

using ::testing::HasSubstr;

namespace {
const fs::path kImageDirectory{fs::temp_directory_path() / "versioning_crash_recovery_image"};

// Byte-wise copy: std::filesystem::copy_file is avoided on purpose, it fails on some virtiofs mounts.
void CopyTree(const fs::path &from, const fs::path &to) {
  fs::create_directories(to);
  for (const auto &entry : fs::recursive_directory_iterator(from)) {
    const auto target = to / fs::relative(entry.path(), from);
    if (entry.is_directory()) {
      fs::create_directories(target);
    } else if (entry.is_regular_file()) {
      fs::create_directories(target.parent_path());
      WriteFile(target, ReadFile(entry.path()));
    }
  }
}

void SaveImage() {
  fs::remove_all(kImageDirectory);
  CopyTree(kStorageDirectory, kImageDirectory);
}

void RestoreImage() {
  fs::remove_all(kStorageDirectory);
  CopyTree(kImageDirectory, kStorageDirectory);
}

fs::path MainWalDir() { return kStorageDirectory / std::string{dur::kWalDirectory}; }

fs::path BranchDir(uint64_t number) { return kStorageDirectory / "branches" / std::to_string(number); }

void FlipByte(const fs::path &file, uint64_t offset) {
  auto bytes = ReadFile(file);
  ASSERT_LT(offset, bytes.size());
  bytes[offset] = static_cast<char>(bytes[offset] ^ 0xFF);
  WriteFile(file, bytes);
}

fs::path NewestWalFile(const fs::path &dir) {
  const auto files = WalFilesIn(dir);
  const auto it =
      std::ranges::find_if(files.rbegin(), files.rend(), [](const fs::path &f) { return fs::file_size(f) != 0; });
  return it == files.rend() ? fs::path{} : *it;
}

// Gids below these floors name fork-state objects (main's counters at the fork).
struct ForkFloors {
  uint64_t vertex{0};
  uint64_t edge{0};
};

ForkFloors FloorsOf(const Base &b) {
  return {.vertex = std::max({b.v1.AsUint(), b.v2.AsUint(), b.v3.AsUint(), b.v4.AsUint()}) + 1,
          .edge = std::max(b.e1.AsUint(), b.e2.AsUint()) + 1};
}

bool NamesForkObject(const dur::WalDeltaData &delta, const ForkFloors &floors) {
  return std::visit(
      [&](const auto &d) -> bool {
        using T = std::decay_t<decltype(d)>;
        if constexpr (std::is_same_v<T, dur::WalVertexDelete> || std::is_same_v<T, dur::WalVertexAddLabel> ||
                      std::is_same_v<T, dur::WalVertexRemoveLabel> || std::is_same_v<T, dur::WalVertexSetProperty>) {
          return d.gid.AsUint() < floors.vertex;
        } else if constexpr (std::is_same_v<T, dur::WalEdgeSetProperty> || std::is_same_v<T, dur::WalEdgeDelete>) {
          return d.gid.AsUint() < floors.edge;
        } else {
          return false;
        }
      },
      delta.data_);
}

std::vector<TxnSpan> ParseTransactions(const fs::path &wal, const ForkFloors &floors = {}) {
  return ReadWalSpans(wal, [&](const dur::WalDeltaData &delta) { return NamesForkObject(delta, floors); });
}

// Offsets below txn.end at which a crash can tear `txn`: always its first bytes, the edges and middle of every fork-
// object record, and the end marker's edges; other delta boundaries are thinned to keep the total near `budget`.
std::vector<uint64_t> CutPoints(const TxnSpan &txn, size_t budget = 24) {
  std::set<uint64_t> must{txn.begin, txn.begin + 1};
  std::set<uint64_t> optional;
  for (size_t i = 0; i < txn.deltas.size(); ++i) {
    const auto &d = txn.deltas[i];
    const uint64_t middle = d.begin + (d.end - d.begin) / 2;
    if (i + 1 == txn.deltas.size()) {
      must.insert({d.begin, d.begin + 1, d.end - 1});
    } else if (d.on_fork) {
      must.insert({d.begin, middle, d.end - 1, d.end});
    } else {
      optional.insert({d.begin, middle, d.end - 1});
    }
  }
  std::vector<uint64_t> spare;
  for (const auto cut : optional) {
    if (!must.contains(cut)) spare.push_back(cut);
  }
  const size_t room = budget > must.size() ? budget - must.size() : 0;
  if (room != 0 && !spare.empty()) {
    const size_t stride = (spare.size() + room - 1) / room;
    for (size_t i = 0; i < spare.size(); i += stride) must.insert(spare[i]);
  }
  std::vector<uint64_t> cuts;
  for (const auto cut : must) {
    if (cut < txn.end) cuts.push_back(cut);
  }
  return cuts;
}

struct State {
  std::string branch;
  std::string main;

  bool operator==(const State &) const = default;
};

State StateOf(memgraph::dbms::Database &db, mv::BranchContext &ctx, const Base &base) {
  return {.branch = Capture(db, ctx, base).Render(), .main = DumpMain(db)};
}

struct Scenario {
  uint64_t number{0};
  Base base{};
  State before_last;  // after the last complete transaction
  State full;         // after the transaction that gets torn
};

enum class LastTxn {
  kMixed,        // a fork vertex property and a native vertex
  kForkRecords,  // a label and properties of two fork vertices
  kForkEdge,     // a fork edge property
  kDeletes,      // a fork edge delete and a fork vertex delete
};

void RunPrior(Env &e) {
  e.Txn([&](mv::BranchContext &c, auto &) {
    auto v1 = e.Fork(e.base.v1);
    ASSERT_TRUE(c.SetProperty(v1, e.Prop("p"), ms::PropertyValue(2)).has_value());
    auto n1 = c.CreateNativeVertex();
    ASSERT_TRUE(*c.AddLabel(n1, e.Label("N")));
    ASSERT_TRUE(c.SetProperty(n1, e.Prop("a"), ms::PropertyValue(1)).has_value());
    auto s4 = e.Stub(e.base.v4);
    ASSERT_TRUE(c.CreateNativeEdge(s4, n1, e.EdgeType("T")).has_value());
  });
}

void RunLast(LastTxn kind, Env &e) {
  e.Txn([&](mv::BranchContext &c, auto &) {
    switch (kind) {
      case LastTxn::kMixed: {
        auto v2 = e.Fork(e.base.v2);
        ASSERT_TRUE(c.SetProperty(v2, e.Prop("p"), ms::PropertyValue(7)).has_value());
        auto nv = c.CreateNativeVertex();
        ASSERT_TRUE(*c.AddLabel(nv, e.Label("N")));
        ASSERT_TRUE(c.SetProperty(nv, e.Prop("a"), ms::PropertyValue(42)).has_value());
        break;
      }
      case LastTxn::kForkRecords: {
        auto v3 = e.Fork(e.base.v3);
        ASSERT_TRUE(c.SetProperty(v3, e.Prop("p"), ms::PropertyValue(8)).has_value());
        ASSERT_TRUE(*c.AddLabel(v3, e.Label("X")));
        auto v2 = e.Fork(e.base.v2);
        ASSERT_TRUE(c.SetProperty(v2, e.Prop("q"), ms::PropertyValue(3)).has_value());
        break;
      }
      case LastTxn::kForkEdge: {
        auto e1 = e.ForkEdge(e.base.e1, e.base.v1);
        ASSERT_TRUE(c.SetProperty(e1, e.Prop("w"), ms::PropertyValue(9)).has_value());
        break;
      }
      case LastTxn::kDeletes: {
        auto e2 = e.ForkEdge(e.base.e2, e.base.v3);
        ASSERT_TRUE(*c.DeleteForkEdge(e2));
        ASSERT_TRUE(*c.DeleteForkVertex(e.base.v2));
        break;
      }
    }
  });
}

// A branch "b" with RunPrior committed and then `kind` committed, both in one WAL file; the database is closed cleanly.
void BuildScenario(LastTxn kind, Scenario &out) {
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  out.base = CreateBase(db);
  auto info = db.version_store()->CreateBranch("b");
  ASSERT_TRUE(info.has_value());
  out.number = info->number;
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  Env env{db, *ctx, out.base};
  RunPrior(env);
  out.before_last = StateOf(db, *ctx, out.base);
  RunLast(kind, env);
  out.full = StateOf(db, *ctx, out.base);
  ASSERT_NE(out.before_last.branch, out.full.branch) << "the last transaction must change the branch";
}

void ExpectRecovers(const State &want, const Base &base, std::string_view name = "b") {
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_TRUE(db.version_store()->Exists(name)) << "the branch must survive the restart";
  EXPECT_FALSE(db.version_store()->Get(name)->unavailable_reason.has_value())
      << "the branch must come back available: " << db.version_store()->Get(name)->unavailable_reason.value_or("");
  bool built = false;
  auto ctx = Checkout(db, name, &built);
  ASSERT_NE(ctx, nullptr);
  EXPECT_FALSE(built) << "startup recovered the branch";
  EXPECT_EQ(StateOf(db, *ctx, base), want);
}

void ExpectUnavailable(std::string_view name, std::string_view reason_part) {
  const auto files = DurableFiles();
  const auto unavailable = UnavailableAfterAStart();
  const auto reason = unavailable.find(std::string{name});
  ASSERT_NE(reason, unavailable.end()) << "branch '" << name << "' cannot be replayed, so it must be set aside";
  EXPECT_THAT(reason->second, HasSubstr(reason_part));
  EXPECT_EQ(unavailable.size(), 1U) << "only the branch that fails is set aside";
  EXPECT_EQ(DurableFiles(), files) << "setting a branch aside changes no file";
}

// As in main, a torn file recovery could not parse is skipped and left as is, not renamed or deleted.
void ExpectTornFileLeftInPlace(const fs::path &wal) {
  auto torn = wal;
  torn += ".torn";
  EXPECT_TRUE(fs::exists(wal)) << "the torn file must stay in place";
  EXPECT_FALSE(fs::exists(torn)) << "recovery must not rename the torn file";
}

std::string CutTrace(uint64_t cut, const TxnSpan &txn) {
  return "cut " + std::to_string(cut) + " (txn bytes [" + std::to_string(txn.begin) + ", " + std::to_string(txn.end) +
         "))";
}

std::string TxnLabel(const TxnSpan &txn) {
  return "txn bytes [" + std::to_string(txn.begin) + ", " + std::to_string(txn.end) + ")";
}

void ExpectTornNewestFileIsSkipped(const fs::path &wal, const std::vector<uint64_t> &cuts, const State &want,
                                   const Base &base, const std::string &what) {
  for (const auto cut : cuts) {
    SCOPED_TRACE(what + ", cut " + std::to_string(cut));
    ASSERT_NO_FATAL_FAILURE(RestoreImage());
    ASSERT_NO_FATAL_FAILURE(CrashAt(wal, cut));
    ASSERT_NO_FATAL_FAILURE(ExpectRecovers(want, base));
    ASSERT_NO_FATAL_FAILURE(ExpectTornFileLeftInPlace(wal));
    ASSERT_NO_FATAL_FAILURE(ExpectRecovers(want, base));
  }
}

void ExpectTornLastTransactionIsDropped(LastTxn kind) {
  Scenario s;
  ASSERT_NO_FATAL_FAILURE(BuildScenario(kind, s));
  ASSERT_NO_FATAL_FAILURE(SaveImage());

  const auto wal = NewestWalFile(BranchWalDir(s.number));
  ASSERT_FALSE(wal.empty());
  const auto txns = ParseTransactions(wal, FloorsOf(s.base));
  ASSERT_EQ(txns.size(), 2u) << "every committed branch transaction is one WAL transaction";
  const auto &last = txns.back();
  ASSERT_TRUE(std::ranges::any_of(last.deltas, &DeltaSpan::on_fork))
      << "the last transaction carries a record on a fork object";
  const auto cuts = CutPoints(last);
  ASSERT_GE(cuts.size(), 6u);

  for (const auto cut : cuts) {
    SCOPED_TRACE(CutTrace(cut, last));
    ASSERT_NO_FATAL_FAILURE(RestoreImage());
    ASSERT_NO_FATAL_FAILURE(CrashAt(wal, cut));
    ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.before_last, s.base));
  }

  // Control: the same crash image with every byte kept recovers the last transaction. It proves the zeroed summary
  // alone does not lose anything, so the cases above lose the transaction because of the tear.
  SCOPED_TRACE("control: nothing torn");
  ASSERT_NO_FATAL_FAILURE(RestoreImage());
  ASSERT_NO_FATAL_FAILURE(CrashAt(wal, last.end));
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.full, s.base));
}

// A crashed run of the unreadable-directory test leaves directories nobody can enter or remove.
void MakeWritable(const fs::path &dir) {
  std::error_code ec;
  if (!fs::is_directory(dir, ec)) return;
  fs::permissions(dir, fs::perms::owner_all, fs::perm_options::add, ec);
  for (const auto &entry : fs::directory_iterator(dir, ec)) MakeWritable(entry.path());
}
}  // namespace

class VersioningCrashRecoveryTest : public RecoveryTest {
 protected:
  void SetUp() override {
    MakeWritable(kStorageDirectory);
    RecoveryTest::SetUp();
    fs::remove_all(kImageDirectory);
  }

  void TearDown() override {
    RecoveryTest::TearDown();
    fs::remove_all(kImageDirectory);
  }
};

// A crash image has no summary, so recovery scans the deltas (ScanWalDeltas, wal.cpp), which counts a transaction's
// deltas only once its end marker passes the CRC check: a transaction replays whole or not at all.

class TornLastTransactionTest : public VersioningCrashRecoveryTest, public ::testing::WithParamInterface<LastTxn> {};

TEST_P(TornLastTransactionTest, BranchWalTornInTheLastTransactionRecoversThePreviousOne) {
  ExpectTornLastTransactionIsDropped(GetParam());
}

INSTANTIATE_TEST_SUITE_P(Kinds, TornLastTransactionTest,
                         ::testing::Values(LastTxn::kMixed, LastTxn::kForkRecords, LastTxn::kForkEdge,
                                           LastTxn::kDeletes),
                         [](const ::testing::TestParamInfo<LastTxn> &info) {
                           switch (info.param) {
                             case LastTxn::kMixed:
                               return "Mixed";
                             case LastTxn::kForkRecords:
                               return "ForkRecords";
                             case LastTxn::kForkEdge:
                               return "ForkEdge";
                             case LastTxn::kDeletes:
                               return "Deletes";
                           }
                           return "Unknown";
                         });

// The next run writes a new file, so the torn one becomes a middle file whose complete prefix replays first; numbering
// continues past it, so there is no gap.
TEST_F(VersioningCrashRecoveryTest, RecoveryAfterATornTransactionIsRepeatableAndLaterWritesSurvive) {
  Scenario s;
  ASSERT_NO_FATAL_FAILURE(BuildScenario(LastTxn::kMixed, s));
  ASSERT_NO_FATAL_FAILURE(SaveImage());
  const auto wal = NewestWalFile(BranchWalDir(s.number));
  const auto txns = ParseTransactions(wal, FloorsOf(s.base));
  ASSERT_EQ(txns.size(), 2u);
  const auto cuts = CutPoints(txns.back());
  ASSERT_FALSE(cuts.empty());
  ASSERT_NO_FATAL_FAILURE(RestoreImage());
  ASSERT_NO_FATAL_FAILURE(CrashAt(wal, cuts[cuts.size() / 2]));

  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.before_last, s.base));
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.before_last, s.base));
  EXPECT_EQ(WalFilesIn(BranchWalDir(s.number)).size(), 1u) << "restarts without writes must not add branch WAL files";

  State after_write;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, s.base};
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto v3 = env.Fork(s.base.v3);
      ASSERT_TRUE(c.SetProperty(v3, env.Prop("p"), ms::PropertyValue(100)).has_value());
      auto nv = c.CreateNativeVertex();
      ASSERT_TRUE(*c.AddLabel(nv, env.Label("N")));
    });
    after_write = StateOf(db, *ctx, s.base);
    ASSERT_NE(after_write.branch, s.before_last.branch);
  }
  EXPECT_EQ(WalFilesIn(BranchWalDir(s.number)).size(), 2u) << "the new run writes a new file beside the torn one";
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(after_write, s.base));
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(after_write, s.base));
}

struct MiddleScenario {
  uint64_t b_number{0};
  Base base{};
  State after_t1;
  State after_t3;
  State c_state;
  std::string main_dump;
};

void BuildMiddleScenario(MiddleScenario &out) {
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  out.base = CreateBase(db);
  auto b_info = db.version_store()->CreateBranch("b");
  ASSERT_TRUE(b_info.has_value());
  out.b_number = b_info->number;
  ASSERT_TRUE(db.version_store()->CreateBranch("c").has_value());
  auto b = Checkout(db, "b");
  auto c = Checkout(db, "c");
  ASSERT_NE(b, nullptr);
  ASSERT_NE(c, nullptr);
  Env be{db, *b, out.base};
  Env ce{db, *c, out.base};

  be.Txn([&](mv::BranchContext &ctx, auto &) {
    auto v1 = be.Fork(out.base.v1);
    ASSERT_TRUE(ctx.SetProperty(v1, be.Prop("p"), ms::PropertyValue(2)).has_value());
    auto nv = ctx.CreateNativeVertex();
    ASSERT_TRUE(*ctx.AddLabel(nv, be.Label("N")));
  });
  out.after_t1 = StateOf(db, *b, out.base);
  be.Txn([&](mv::BranchContext &ctx, auto &) {
    auto v2 = be.Fork(out.base.v2);
    ASSERT_TRUE(ctx.SetProperty(v2, be.Prop("p"), ms::PropertyValue(3)).has_value());
  });
  be.Txn([&](mv::BranchContext &ctx, auto &) {
    auto v3 = be.Fork(out.base.v3);
    ASSERT_TRUE(ctx.SetProperty(v3, be.Prop("p"), ms::PropertyValue(4)).has_value());
  });
  out.after_t3 = StateOf(db, *b, out.base);
  ASSERT_NE(out.after_t1.branch, out.after_t3.branch);

  ce.Txn([&](mv::BranchContext &ctx, auto &) {
    auto v4 = ce.Fork(out.base.v4);
    ASSERT_TRUE(ctx.SetProperty(v4, ce.Prop("p"), ms::PropertyValue(5)).has_value());
  });
  out.c_state = StateOf(db, *c, out.base);
  out.main_dump = DumpMain(db);
}

// t2's CRC trailer is damaged with t3 after it. A finalized file's summary is trusted, so replay hits the bad CRC and
// the branch is set aside; an unfinalized file is scanned and cut at the first bad CRC, leaving t1. Branch "c" and main
// are untouched.
TEST_F(VersioningCrashRecoveryTest,
       MiddleTransactionDamagedSetsAFinalizedBranchAsideAndCutsAnUnfinalizedOneToThePrefix) {
  for (const bool finalized : {true, false}) {
    SCOPED_TRACE(finalized ? "finalized" : "unfinalized");
    fs::remove_all(kStorageDirectory);
    MiddleScenario s;
    ASSERT_NO_FATAL_FAILURE(BuildMiddleScenario(s));
    const auto wal = NewestWalFile(BranchWalDir(s.b_number));
    const auto txns = ParseTransactions(wal, FloorsOf(s.base));
    ASSERT_EQ(txns.size(), 3u);
    if (!finalized) ASSERT_NO_FATAL_FAILURE(CrashAt(wal, fs::file_size(wal)));
    ASSERT_NO_FATAL_FAILURE(FlipByte(wal, txns[1].end - 1));
    if (finalized) ASSERT_NO_FATAL_FAILURE(ExpectUnavailable("b", "CRC"));

    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    EXPECT_EQ(DumpMain(db), s.main_dump);
    if (!finalized) {
      EXPECT_FALSE(db.version_store()->Get("b")->unavailable_reason.has_value());
      auto b = Checkout(db, "b");
      ASSERT_NE(b, nullptr);
      EXPECT_EQ(StateOf(db, *b, s.base), s.after_t1);
    }
    auto c = Checkout(db, "c");
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(StateOf(db, *c, s.base), s.c_state);
  }
}

// Main's last file has no summary, so recovery counts only complete transactions; the branch WAL replays on the rebuilt
// fork base, which lacks the torn transaction because it was committed after the fork.
TEST_F(VersioningCrashRecoveryTest, MainWalTornInItsLastTransactionKeepsMainAtThePreviousOneAndTheBranchIntact) {
  Base base{};
  State before_last;  // main after M1, branch as written
  std::string main_full;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    RunPrior(env);

    {  // M1
      auto acc = db.Access(ms::WRITE);
      auto v1 = acc->FindVertex(base.v1, ms::View::OLD);
      ASSERT_TRUE(v1.has_value());
      ASSERT_TRUE(v1->SetProperty(env.Prop("p"), ms::PropertyValue(10)).has_value());
      Commit(*acc);
    }
    before_last = StateOf(db, *ctx, base);
    {  // M2: a property write and a new vertex
      auto acc = db.Access(ms::WRITE);
      auto v1 = acc->FindVertex(base.v1, ms::View::OLD);
      ASSERT_TRUE(v1.has_value());
      ASSERT_TRUE(v1->SetProperty(env.Prop("p"), ms::PropertyValue(20)).has_value());
      auto created = acc->CreateVertex();
      ASSERT_TRUE(created.AddLabel(env.Label("M")).has_value());
      Commit(*acc);
    }
    main_full = DumpMain(db);
    ASSERT_NE(main_full, before_last.main);
    ASSERT_EQ(StateOf(db, *ctx, base).branch, before_last.branch)
        << "main writes after the fork do not reach the branch";
  }
  ASSERT_NO_FATAL_FAILURE(SaveImage());

  const auto wal = NewestWalFile(MainWalDir());
  ASSERT_FALSE(wal.empty());
  const auto txns = ParseTransactions(wal);
  ASSERT_GE(txns.size(), 3u);
  const auto &last = txns.back();
  const auto cuts = CutPoints(last);
  ASSERT_GE(cuts.size(), 6u);

  for (const auto cut : cuts) {
    SCOPED_TRACE(CutTrace(cut, last));
    ASSERT_NO_FATAL_FAILURE(RestoreImage());
    ASSERT_NO_FATAL_FAILURE(CrashAt(wal, cut));
    ASSERT_NO_FATAL_FAILURE(ExpectRecovers(before_last, base));
  }

  SCOPED_TRACE("control: nothing torn");
  ASSERT_NO_FATAL_FAILURE(RestoreImage());
  ASSERT_NO_FATAL_FAILURE(CrashAt(wal, last.end));
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(State{.branch = before_last.branch, .main = main_full}, base));
}

// The branch forks after main's last transaction, which is then torn: main's history ends before the fork, so the
// branch is set aside rather than replayed onto a different base.
TEST_F(VersioningCrashRecoveryTest, MainTornAtTheTransactionABranchForkedOnSetsTheBranchAside) {
  Base base{};
  ms::Gid late;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    {
      auto acc = db.Access(ms::WRITE);
      auto created = acc->CreateVertex();
      late = created.Gid();
      ASSERT_TRUE(created.AddLabel(db.storage()->NameToLabel("L")).has_value());
      ASSERT_TRUE(created.SetProperty(db.storage()->NameToProperty("p"), ms::PropertyValue(5)).has_value());
      Commit(*acc);
    }
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto fork = env.Fork(late);
      ASSERT_TRUE(c.SetProperty(fork, env.Prop("p"), ms::PropertyValue(9)).has_value());
    });
  }
  ASSERT_NO_FATAL_FAILURE(SaveImage());

  const auto wal = NewestWalFile(MainWalDir());
  const auto txns = ParseTransactions(wal);
  ASSERT_GE(txns.size(), 2u);
  const auto &last = txns.back();

  {
    SCOPED_TRACE("control: nothing torn");
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    EXPECT_THAT(Capture(db, *ctx, base).Render(), HasSubstr("p=9"));
  }

  for (const auto cut : CutPoints(last, 8)) {
    SCOPED_TRACE(CutTrace(cut, last));
    ASSERT_NO_FATAL_FAILURE(RestoreImage());
    ASSERT_NO_FATAL_FAILURE(CrashAt(wal, cut));
    ASSERT_NO_FATAL_FAILURE(ExpectUnavailable("b", "before the fork"));
  }
}

// A file with no complete transaction is skipped unrenamed and the branch recovers from the others (empty at the fork
// if none). Here the only file has its only transaction torn, or its header torn.
TEST_F(VersioningCrashRecoveryTest, TornOnlyBranchWalFileRecoversEmptyAtTheFork) {
  uint64_t number = 0;
  Base base{};
  State before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b");
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    before = StateOf(db, *ctx, base);
    RunLast(LastTxn::kForkRecords, env);
    ASSERT_NE(StateOf(db, *ctx, base).branch, before.branch);
  }
  ASSERT_NO_FATAL_FAILURE(SaveImage());
  const auto wal = NewestWalFile(BranchWalDir(number));
  const auto txns = ParseTransactions(wal, FloorsOf(base));
  ASSERT_EQ(txns.size(), 1u);
  const auto header = dur::ReadWalHeader(wal);  // before the cuts below tear the file
  ASSERT_NO_FATAL_FAILURE(
      ExpectTornNewestFileIsSkipped(wal, CutPoints(txns.back(), 8), before, base, TxnLabel(txns.back())));

  ASSERT_NO_FATAL_FAILURE(ExpectTornNewestFileIsSkipped(
      wal, {uint64_t{10}, header.offset_metadata + 3, header.offset_deltas - 1}, before, base, "header"));
}

// Main fails startup on a WAL chain gap; a branch with one is set aside instead. Each transaction below carries a label
// over 1 KiB, which fills a 1 KiB WAL file, so one run writes files 0, 1 and 2.
struct ChainScenario {
  uint64_t number{0};
  Base base{};
  std::vector<fs::path> wals;  // oldest first
  State after[3];
};

ms::Config SmallWalConfig(bool recover_on_startup) {
  auto config = MakeConfig(recover_on_startup);
  config.durability.wal_file_size_kibibytes = 1;
  return config;
}

std::string BigLabel(char c) { return std::string(1100, c); }

void BuildChainScenario(ChainScenario &out) {
  memgraph::dbms::Database db{SmallWalConfig(/*recover_on_startup=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  out.base = CreateBase(db);
  auto info = db.version_store()->CreateBranch("b");
  ASSERT_TRUE(info.has_value());
  out.number = info->number;
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  Env env{db, *ctx, out.base};
  const ms::Gid targets[3] = {out.base.v1, out.base.v2, out.base.v3};
  for (int i = 0; i < 3; ++i) {
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto v = env.Fork(targets[i]);
      ASSERT_TRUE(*c.AddLabel(v, env.Label(BigLabel(static_cast<char>('a' + i)))));
    });
    out.after[i] = StateOf(db, *ctx, out.base);
  }
}

// The files of the chain, oldest first, after checking that the scenario produced the sequence 0, 1, ... count - 1.
void ListChain(ChainScenario &s, size_t count = 3) {
  s.wals = WalFilesIn(BranchWalDir(s.number));
  std::ranges::sort(s.wals, {}, [](const fs::path &f) { return dur::ReadWalHeader(f).seq_num; });
  ASSERT_EQ(s.wals.size(), count) << "the transactions must land in one file each";
  for (uint64_t seq = 0; seq < count; ++seq) ASSERT_EQ(dur::ReadWalHeader(s.wals[seq]).seq_num, seq);
}

// Run 1 writes files 0, 1, 2; the restart recovers them and run 2 writes two more transactions. Main numbers its WAL
// files across restarts, so run 2's files must be 3 and 4.
void BuildTwoRunChainScenario(ChainScenario &out, State &final_state) {
  ASSERT_NO_FATAL_FAILURE(BuildChainScenario(out));
  memgraph::dbms::Database db{SmallWalConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(StateOf(db, *ctx, out.base), out.after[2]);
  Env env{db, *ctx, out.base};
  const ms::Gid targets[2] = {out.base.v4, out.base.v1};
  for (int i = 0; i < 2; ++i) {
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto v = env.Fork(targets[i]);
      ASSERT_TRUE(*c.AddLabel(v, env.Label(BigLabel(static_cast<char>('d' + i)))));
    });
  }
  final_state = StateOf(db, *ctx, out.base);
}

// A damaged file in the middle of the chain is a gap and sets the branch aside, keeping its files; a damaged newest
// file is skipped as in main and the branch recovers from the files before it.
TEST_F(VersioningCrashRecoveryTest, ADamagedWalFileInTheChainIsAGapUnlessItIsTheNewest) {
  struct Row {
    const char *name;
    size_t file;
    bool remove;  // else cut to 10 bytes
    bool gap;     // else recovers the state after the file before it
  };

  const Row rows[] = {
      {"unreadable middle file", 1, false, true},
      {"missing middle file", 1, true, true},
      {"unreadable newest file", 2, false, false},
  };
  for (const auto &row : rows) {
    SCOPED_TRACE(row.name);
    fs::remove_all(kStorageDirectory);
    ChainScenario s;
    ASSERT_NO_FATAL_FAILURE(BuildChainScenario(s));
    ASSERT_NO_FATAL_FAILURE(ListChain(s));
    if (row.remove) {
      fs::remove(s.wals[row.file]);
    } else {
      fs::resize_file(s.wals[row.file], 10);
    }
    if (row.gap) {
      ASSERT_NO_FATAL_FAILURE(ExpectUnavailable("b", "missing a WAL file"));
      if (!row.remove) {
        for (const auto &wal : s.wals) EXPECT_TRUE(fs::exists(wal)) << "a branch set aside keeps its files";
      }
    } else {
      ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.after[1], s.base));
      EXPECT_TRUE(fs::exists(s.wals[row.file])) << "the skipped file stays in place";
    }
  }
}

TEST_F(VersioningCrashRecoveryTest, SecondRunContinuesTheWalSequenceAndRecovers) {
  ChainScenario s;
  State final_state;
  ASSERT_NO_FATAL_FAILURE(BuildTwoRunChainScenario(s, final_state));
  ASSERT_NO_FATAL_FAILURE(ListChain(s, 5));
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(final_state, s.base));
}

// A branch directory that cannot be read sets that branch aside, naming it only, instead of dropping it. With the
// permissions repaired the instance starts and both branches are as they were.
TEST_F(VersioningCrashRecoveryTest, AnUnreadableBranchDirectorySetsTheBranchAsideUntilItIsRepaired) {
  if (::geteuid() == 0) GTEST_SKIP() << "root bypasses directory permissions";

  uint64_t a_number = 0;
  Base base{};
  State b_before;
  std::string main_before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto a_info = db.version_store()->CreateBranch("a");
    ASSERT_TRUE(a_info.has_value());
    a_number = a_info->number;
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto a = Checkout(db, "a");
    auto b = Checkout(db, "b");
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    Env ae{db, *a, base};
    Env be{db, *b, base};
    ae.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = ae.Fork(base.v1);
      ASSERT_TRUE(c.SetProperty(v1, ae.Prop("p"), ms::PropertyValue(7)).has_value());
    });
    be.Txn([&](mv::BranchContext &c, auto &) {
      auto v2 = be.Fork(base.v2);
      ASSERT_TRUE(c.SetProperty(v2, be.Prop("p"), ms::PropertyValue(8)).has_value());
      auto nv = c.CreateNativeVertex();
      ASSERT_TRUE(*c.AddLabel(nv, be.Label("N")));
    });
    b_before = StateOf(db, *b, base);
    main_before = DumpMain(db);
  }

  const auto a_dir = BranchDir(a_number);
  ASSERT_TRUE(fs::is_directory(a_dir));

  struct RestorePermissions {
    fs::path dir;

    ~RestorePermissions() {
      std::error_code ec;
      fs::permissions(dir, fs::perms::owner_all, ec);
    }
  } restore{a_dir};

  const auto files = DurableFiles();
  fs::permissions(a_dir, fs::perms::none);
  {
    const auto unavailable = UnavailableAfterAStart();
    ASSERT_TRUE(unavailable.contains("a")) << "a branch directory that cannot be read sets the branch aside";
    EXPECT_THAT(unavailable.at("a"), HasSubstr("cannot be inspected"));
    EXPECT_EQ(unavailable.size(), 1U) << "only the branch that fails is set aside";
  }

  fs::permissions(a_dir, fs::perms::owner_all);
  EXPECT_EQ(DurableFiles(), files) << "setting a branch aside changes no file";
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  EXPECT_EQ(DumpMain(db), main_before);
  EXPECT_TRUE(db.version_store()->Exists("a"));
  auto b = Checkout(db, "b");
  ASSERT_NE(b, nullptr);
  EXPECT_EQ(StateOf(db, *b, base), b_before);
}

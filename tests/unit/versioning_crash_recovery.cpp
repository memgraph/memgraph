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

// Crash consistency of branch durability. A crash leaves the active WAL file unfinalized (no summary) and possibly
// ending in the middle of a transaction. The tests build that state from a cleanly closed database: a copy of the files
// is truncated inside the last transaction and its summary is reset to the placeholder an unfinalized file carries.
// Recovery must land on the last complete transaction, never on a half-applied one. Damage it cannot account for (a
// gap in the WAL chain, a damaged finalized file, a main that lost a fork object) makes the instance refuse to start
// with a message that names the branch; no file is touched.
#include <unistd.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
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
#include "versioning/branch_ledger.hpp"
#include "versioning/version_store.hpp"
#include "versioning_recovery_support.hpp"

using namespace memgraph::tests::versioning_recovery;  // NOLINT(google-build-using-namespace)

using ::testing::HasSubstr;

namespace {
const fs::path kImageDirectory{fs::temp_directory_path() / "versioning_crash_recovery_image"};

// ---------------------------------------------------------------------------------------------------------------
// Files: copying, parsing a WAL into transactions, and tearing it the way a crash does.
// ---------------------------------------------------------------------------------------------------------------

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

std::vector<fs::path> WalFilesIn(const fs::path &dir) {
  std::vector<fs::path> files;
  if (!fs::exists(dir)) return files;
  for (const auto &entry : fs::directory_iterator(dir)) {
    if (entry.is_regular_file()) files.push_back(entry.path());
  }
  std::ranges::sort(files);
  return files;
}

// The newest non-empty WAL file of `dir`.
fs::path NewestWalFile(const fs::path &dir) {
  const auto files = WalFilesIn(dir);
  const auto it =
      std::ranges::find_if(files.rbegin(), files.rend(), [](const fs::path &f) { return fs::file_size(f) != 0; });
  return it == files.rend() ? fs::path{} : *it;
}

// Whether a gid names an object of the fork state: below the floors main's counters had when the branch was forked.
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

// The byte range of every transaction of `wal`; records that name a fork object are marked.
std::vector<TxnSpan> ParseTransactions(const fs::path &wal, const ForkFloors &floors = {}) {
  return ReadWalSpans(wal, [&](const dur::WalDeltaData &delta) { return NamesForkObject(delta, floors); });
}

uint64_t DeltaCount(const std::vector<TxnSpan> &txns, size_t whole) {
  uint64_t count = 0;
  for (size_t i = 0; i < whole && i < txns.size(); ++i) count += txns[i].deltas.size();
  return count;
}

// Offsets inside `txn` at which a crash can tear it. All of them are below txn.end, so none keeps the whole
// transaction. Always included: the transaction's first bytes, the inside of every record on a fork object (start,
// middle, last byte and the boundary after it), and the transaction end marker (its start, one byte in, and its last
// byte short). Other delta boundaries are thinned out to keep the total near `budget`.
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

// ---------------------------------------------------------------------------------------------------------------
// Scenarios.
// ---------------------------------------------------------------------------------------------------------------

// What a session reads of the branch plus what main holds.
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

// The complete transaction that precedes the torn one: a fork vertex property, a native vertex with a label and a
// property, and a native edge from the stub of a fork vertex to it.
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

// Restarts the way a crash recovery does and checks that branch `name` is available and reads as `want`.
void ExpectRecovers(const State &want, const Base &base, std::string_view name = "b") {
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_TRUE(db.version_store()->Exists(name)) << "the branch must survive the restart";
  bool built = false;
  auto ctx = Checkout(db, name, &built);
  ASSERT_NE(ctx, nullptr);
  EXPECT_FALSE(built) << "startup recovered the branch";
  EXPECT_EQ(StateOf(db, *ctx, base), want);
}

// Restarts and expects the startup to refuse over branch `name` with `reason_part` in the message. No file on disk
// changes, main's included.
void ExpectRefusesToStart(std::string_view name, std::string_view reason_part) {
  const auto files = DurableFiles();
  const auto message = StartupRefusal();
  ASSERT_FALSE(message.empty()) << "branch '" << name << "' cannot be replayed, so the startup must refuse";
  EXPECT_THAT(message, HasSubstr("Branch '" + std::string{name} + "' cannot be replayed"));
  EXPECT_THAT(message, HasSubstr(reason_part));
  EXPECT_EQ(DurableFiles(), files) << "a refused startup changes no file";
}

// As in main, a crash-torn file recovery could not parse is skipped and left where it is: not renamed, not deleted.
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

// Crashes `wal` at every cut in `cuts`: each restart must recover `want`, leave the file in place, and recover `want`
// again.
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

// ---- 1. Branch WAL torn inside its last transaction -------------------------------------------------------------
//
// A crash image has no summary, so recovery parses the deltas (ScanWalDeltas in wal.cpp), which adds a transaction's
// deltas to num_deltas only once its end marker passed the CRC check, and replays exactly num_deltas deltas: a
// transaction is applied whole or not at all. The previous transaction is in the same file, so the file has a complete
// transaction and is not the case of test 6.

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

// The file header of an unfinalized file states no deltas, so the count recovery replays is the scan's: the deltas of
// the transactions that are complete, and nothing of a torn tail. It is also what the finalized file states.
TEST_F(VersioningCrashRecoveryTest, TornTailAddsNoDeltasToTheCountRecoveryReplays) {
  Scenario s;
  ASSERT_NO_FATAL_FAILURE(BuildScenario(LastTxn::kMixed, s));
  ASSERT_NO_FATAL_FAILURE(SaveImage());
  const auto wal = NewestWalFile(BranchWalDir(s.number));
  const auto txns = ParseTransactions(wal, FloorsOf(s.base));
  ASSERT_EQ(txns.size(), 2u);
  const uint64_t first = DeltaCount(txns, 1);
  const uint64_t both = DeltaCount(txns, 2);
  ASSERT_GT(txns.back().deltas.size(), 2u);
  ASSERT_TRUE(dur::ReadWalHeader(wal).summary.has_value());
  EXPECT_EQ(dur::ReadWalHeader(wal).summary->num_deltas, both) << "a finalized file states every record";

  for (const auto cut : CutPoints(txns.back())) {
    SCOPED_TRACE(CutTrace(cut, txns.back()));
    ASSERT_NO_FATAL_FAILURE(RestoreImage());
    ASSERT_NO_FATAL_FAILURE(CrashAt(wal, cut));
    EXPECT_EQ(dur::ReadWalContents(wal).num_deltas, first);
  }
  ASSERT_NO_FATAL_FAILURE(RestoreImage());
  ASSERT_NO_FATAL_FAILURE(CrashAt(wal, txns.back().end));
  EXPECT_EQ(dur::ReadWalContents(wal).num_deltas, both);
}

// Recovery must be repeatable, and the torn file must not get in the way of later runs: the next run writes a new file,
// so the torn one becomes a middle file whose complete prefix is replayed before the new file. The new run continues
// the sequence numbers past the torn file, so it leaves no gap, and its clock starts past the replayed timestamps.
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
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.before_last, s.base));  // no writes in between
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

// ---- 2. Branch WAL damaged inside a middle transaction ----------------------------------------------------------
//
// Three transactions t1, t2, t3 in one file; t2's CRC trailer is damaged (its last byte flipped) while t3 follows.
// Branch "c" has a WAL of its own and must be untouched in both variants.

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

// Finalized file (the summary is intact) with the middle transaction's CRC damaged. A finalized file's summary is
// trusted (no scan), so replay runs into t2's end marker and the CRC check fails: the startup refuses and touches no
// file. Following the message's steps starts the instance with the sibling and main as they were.
TEST_F(VersioningCrashRecoveryTest, MiddleTransactionDamagedInAFinalizedBranchWalRefusesStartup) {
  MiddleScenario s;
  ASSERT_NO_FATAL_FAILURE(BuildMiddleScenario(s));
  const auto wal = NewestWalFile(BranchWalDir(s.b_number));
  const auto txns = ParseTransactions(wal, FloorsOf(s.base));
  ASSERT_EQ(txns.size(), 3u);
  ASSERT_NO_FATAL_FAILURE(FlipByte(wal, txns[1].end - 1));

  const auto files = DurableFiles();
  const auto message = StartupRefusal();
  ASSERT_FALSE(message.empty()) << "a branch whose log does not replay must stop the startup";
  EXPECT_THAT(message, HasSubstr("Branch 'b' cannot be replayed"));
  EXPECT_THAT(message, HasSubstr("CRC"));
  EXPECT_EQ(DurableFiles(), files);

  ASSERT_NO_FATAL_FAILURE(FollowMoveSteps(message));
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  EXPECT_EQ(DumpMain(db), s.main_dump);
  auto c = Checkout(db, "c");
  ASSERT_NE(c, nullptr);
  EXPECT_EQ(StateOf(db, *c, s.base), s.c_state);

  ASSERT_TRUE(db.version_store()->DropBranch("b").has_value());
  EXPECT_FALSE(db.version_store()->Exists("b"));
  EXPECT_FALSE(fs::exists(BranchDir(s.b_number)));
}

// The same damage in a file that was never finalized (a crash image): the scan stops at the first transaction whose CRC
// fails, so the file reports only t1 and the replay lands on the state after t1, losing t2 and t3 without refusing the
// startup. This is a consistent prefix (never a mix) and is how main's WAL recovery treats the same damage.
TEST_F(VersioningCrashRecoveryTest, MiddleTransactionDamagedInAnUnfinalizedBranchWalRecoversThePrefixBeforeIt) {
  MiddleScenario s;
  ASSERT_NO_FATAL_FAILURE(BuildMiddleScenario(s));
  const auto wal = NewestWalFile(BranchWalDir(s.b_number));
  const auto txns = ParseTransactions(wal, FloorsOf(s.base));
  ASSERT_EQ(txns.size(), 3u);
  ASSERT_NO_FATAL_FAILURE(CrashAt(wal, fs::file_size(wal)));
  ASSERT_NO_FATAL_FAILURE(FlipByte(wal, txns[1].end - 1));

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  EXPECT_EQ(DumpMain(db), s.main_dump);
  auto b = Checkout(db, "b");
  auto c = Checkout(db, "c");
  ASSERT_NE(b, nullptr);
  ASSERT_NE(c, nullptr);
  EXPECT_EQ(StateOf(db, *b, s.base), s.after_t1);
  EXPECT_EQ(StateOf(db, *c, s.base), s.c_state);
}

// ---- 3. Main WAL torn in its last transaction while a branch exists ----------------------------------------------
//
// Main's last file has no summary, so its recovery derives num_deltas from the complete transactions only. The branch's
// WAL is a separate file set replayed on the rebuilt fork base, which does not contain the torn transaction because it
// was committed after the fork.

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

// The branch is forked after main's last transaction, and that transaction is torn: main's recovery loses a commit the
// ledger's fork point already includes. The branch has a record for a vertex that only the lost transaction created, so
// replaying it finds no fork object: the startup refuses, the branch is never replayed onto a different base.
TEST_F(VersioningCrashRecoveryTest, MainTornAtTheTransactionABranchForkedOnRefusesStartup) {
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
    ASSERT_NO_FATAL_FAILURE(ExpectRefusesToStart("b", ""));
  }
}

// ---- 4. Ledger written, branch WAL missing or empty
// ------------------------------------------------------------------------

// No WAL files means the core is not built at startup and is built empty on first use, reading main at the fork.
TEST_F(VersioningCrashRecoveryTest, LedgerWithoutABranchWalDirectoryRecoversEmptyAtTheFork) {
  uint64_t number = 0;
  Base base{};
  State expected;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b");
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    expected = StateOf(db, *ctx, base);
  }
  fs::remove_all(BranchDir(number));
  ASSERT_FALSE(fs::exists(BranchWalDir(number)));
  ASSERT_TRUE(fs::exists(kStorageDirectory / "branches" / "ledger"));

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_TRUE(db.version_store()->Exists("b"));
  bool built = false;
  auto ctx = Checkout(db, "b", &built);
  ASSERT_NE(ctx, nullptr);
  EXPECT_TRUE(built);
  EXPECT_EQ(StateOf(db, *ctx, base), expected);
}

// The crash left a zero-length WAL file (created, nothing written). Its header cannot be read, so it is skipped as
// main's recovery skips such a file, and the branch is the fork state.
TEST_F(VersioningCrashRecoveryTest, EmptyBranchWalFileRecoversEmptyAtTheFork) {
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
  const auto wal = NewestWalFile(BranchWalDir(number));
  ASSERT_FALSE(wal.empty());
  fs::resize_file(wal, 0);
  ASSERT_EQ(WalFilesIn(BranchWalDir(number)).size(), 1u);
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(before, base));
  ASSERT_NO_FATAL_FAILURE(ExpectTornFileLeftInPlace(wal));
}

// ---- 5. Restarting twice with no writes in between ---------------------------------------------------------------

// Replay writes no branch WAL, and the ledger keeps the original fork point.
TEST_F(VersioningCrashRecoveryTest, RestartingTwiceWithoutWritesIsIdempotent) {
  Scenario s;
  ASSERT_NO_FATAL_FAILURE(BuildScenario(LastTxn::kMixed, s));
  const auto branch_files = WalFilesIn(BranchWalDir(s.number)).size();
  ASSERT_GE(branch_files, 1u);

  const auto fork_ts_in_ledger = [] {
    auto ledger = mv::BranchLedger{kStorageDirectory / "branches" / "ledger"}.Read();
    EXPECT_TRUE(ledger.has_value() && ledger->has_value());
    if (!ledger || !*ledger || (*ledger)->branches.size() != 1) return uint64_t{0};
    return (*ledger)->branches[0].info.fork_ts;
  };
  const auto fork_ts = fork_ts_in_ledger();
  ASSERT_NE(fork_ts, 0u);

  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.full, s.base));
  const auto main_files_after_first = WalFilesIn(MainWalDir()).size();
  EXPECT_EQ(WalFilesIn(BranchWalDir(s.number)).size(), branch_files);
  EXPECT_EQ(fork_ts_in_ledger(), fork_ts);

  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.full, s.base));
  EXPECT_EQ(WalFilesIn(MainWalDir()).size(), main_files_after_first);
  EXPECT_EQ(WalFilesIn(BranchWalDir(s.number)).size(), branch_files);
  EXPECT_EQ(fork_ts_in_ledger(), fork_ts);
}

// ---- 6. A WAL file without a complete transaction ----------------------------------------------------------------

// Nothing was ever committed to the torn file, so the branch recovers from the other files (or empty at the fork). Such
// a file is skipped without being renamed. One the sequence numbers show to sit in the middle of the stream is a
// missing file, which refuses the startup (test 7).

// The branch's only WAL file has its only transaction torn, or its header torn (a crash while the header is written).
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

// A later run's new file is torn in its first transaction while the earlier run's file is intact.
TEST_F(VersioningCrashRecoveryTest, TornFirstTransactionOfALaterRunKeepsTheEarlierRunsData) {
  uint64_t number = 0;
  Base base{};
  State before_last;
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
    RunPrior(env);
  }
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    before_last = StateOf(db, *ctx, base);
    RunLast(LastTxn::kForkRecords, env);
    ASSERT_NE(StateOf(db, *ctx, base).branch, before_last.branch);
  }
  ASSERT_EQ(WalFilesIn(BranchWalDir(number)).size(), 2u) << "one file per run";
  ASSERT_NO_FATAL_FAILURE(SaveImage());

  const auto newest = NewestWalFile(BranchWalDir(number));
  const auto txns = ParseTransactions(newest, FloorsOf(base));
  ASSERT_EQ(txns.size(), 1u);
  ASSERT_NO_FATAL_FAILURE(
      ExpectTornNewestFileIsSkipped(newest, CutPoints(txns.back(), 8), before_last, base, TxnLabel(txns.back())));
}

// ---- 7. A file missing from the sequence-number chain -----------------------------------------------------------
//
// Main fails startup when its WAL chain has a gap or lacks its first file, and ignores a damaged newest file. A branch
// with such a chain refuses the startup instead, and a damaged newest file is ignored as in main. Each transaction
// below carries a label name of over 1 KiB, which fills a 1 KiB WAL file, so one run writes files 0, 1 and 2.

struct ChainScenario {
  uint64_t number{0};
  Base base{};
  std::vector<fs::path> wals;  // oldest first
  State after[3];
  std::string main_dump;
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
  out.main_dump = DumpMain(db);
}

// The files of the chain, oldest first, after checking that the scenario produced the sequence 0, 1, ... count - 1.
void ListChain(ChainScenario &s, size_t count = 3) {
  s.wals = WalFilesIn(BranchWalDir(s.number));
  std::ranges::sort(s.wals, {}, [](const fs::path &f) { return dur::ReadWalHeader(f).seq_num; });
  ASSERT_EQ(s.wals.size(), count) << "the transactions must land in one file each";
  for (uint64_t seq = 0; seq < count; ++seq) ASSERT_EQ(dur::ReadWalHeader(s.wals[seq]).seq_num, seq);
}

// Run 1 writes files 0, 1, 2; the restart recovers them and run 2 writes two more transactions. Main numbers its WAL
// files across restarts, so run 2's files must be 3 and 4, and a file lost from either run must leave a visible gap.
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
  out.main_dump = DumpMain(db);
}

void ExpectRefusesForTheGap(std::string_view reason_part) {
  ASSERT_NO_FATAL_FAILURE(ExpectRefusesToStart("b", reason_part));
}

TEST_F(VersioningCrashRecoveryTest, UnreadableMiddleFileIsAGapAndRefusesStartup) {
  ChainScenario s;
  ASSERT_NO_FATAL_FAILURE(BuildChainScenario(s));
  ASSERT_NO_FATAL_FAILURE(ListChain(s));
  fs::resize_file(s.wals[1], 10);
  ASSERT_NO_FATAL_FAILURE(ExpectRefusesForTheGap("missing a WAL file"));
  for (const auto &wal : s.wals) EXPECT_TRUE(fs::exists(wal)) << "a refused startup keeps the files";
}

TEST_F(VersioningCrashRecoveryTest, MissingMiddleFileIsAGapAndRefusesStartup) {
  ChainScenario s;
  ASSERT_NO_FATAL_FAILURE(BuildChainScenario(s));
  ASSERT_NO_FATAL_FAILURE(ListChain(s));
  fs::remove(s.wals[1]);
  ASSERT_NO_FATAL_FAILURE(ExpectRefusesForTheGap("missing a WAL file"));
}

TEST_F(VersioningCrashRecoveryTest, MissingFirstFileRefusesStartup) {
  ChainScenario s;
  ASSERT_NO_FATAL_FAILURE(BuildChainScenario(s));
  ASSERT_NO_FATAL_FAILURE(ListChain(s));
  fs::remove(s.wals[0]);
  ASSERT_NO_FATAL_FAILURE(ExpectRefusesForTheGap("missing prefix WAL files"));
}

TEST_F(VersioningCrashRecoveryTest, UnreadableNewestFileIsSkippedWithoutAGap) {
  ChainScenario s;
  ASSERT_NO_FATAL_FAILURE(BuildChainScenario(s));
  ASSERT_NO_FATAL_FAILURE(ListChain(s));
  fs::resize_file(s.wals[2], 10);
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.after[1], s.base));
  EXPECT_TRUE(fs::exists(s.wals[2])) << "the skipped file stays in place";
}

// The second run's files continue the first run's numbering, as main's do.
TEST_F(VersioningCrashRecoveryTest, SecondRunContinuesTheWalSequenceAndRecovers) {
  ChainScenario s;
  State final_state;
  ASSERT_NO_FATAL_FAILURE(BuildTwoRunChainScenario(s, final_state));
  ASSERT_NO_FATAL_FAILURE(ListChain(s, 5));
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(final_state, s.base));
}

TEST_F(VersioningCrashRecoveryTest, MissingLastFileOfTheFirstRunIsAGapAcrossRestarts) {
  ChainScenario s;
  State final_state;
  ASSERT_NO_FATAL_FAILURE(BuildTwoRunChainScenario(s, final_state));
  ASSERT_NO_FATAL_FAILURE(ListChain(s, 5));
  fs::remove(s.wals[2]);
  ASSERT_NO_FATAL_FAILURE(ExpectRefusesForTheGap("missing a WAL file"));
}

TEST_F(VersioningCrashRecoveryTest, MissingFirstFileOfTheSecondRunIsAGapAcrossRestarts) {
  ChainScenario s;
  State final_state;
  ASSERT_NO_FATAL_FAILURE(BuildTwoRunChainScenario(s, final_state));
  ASSERT_NO_FATAL_FAILURE(ListChain(s, 5));
  fs::remove(s.wals[3]);
  ASSERT_NO_FATAL_FAILURE(ExpectRefusesForTheGap("missing a WAL file"));
}

TEST_F(VersioningCrashRecoveryTest, MissingMiddleFileOfTheFirstRunIsAGapAcrossRestarts) {
  ChainScenario s;
  State final_state;
  ASSERT_NO_FATAL_FAILURE(BuildTwoRunChainScenario(s, final_state));
  ASSERT_NO_FATAL_FAILURE(ListChain(s, 5));
  fs::remove(s.wals[1]);
  ASSERT_NO_FATAL_FAILURE(ExpectRefusesForTheGap("missing a WAL file"));
}

// A branch directory that cannot be read refuses the startup, naming that branch only, instead of dropping it. With
// the permissions repaired the instance starts and both branches are as they were.
TEST_F(VersioningCrashRecoveryTest, AnUnreadableBranchDirectoryRefusesStartupUntilItIsRepaired) {
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
  const auto message = StartupRefusal();
  ASSERT_FALSE(message.empty()) << "a branch directory that cannot be read must stop the startup";
  EXPECT_THAT(message, HasSubstr("Branch 'a' cannot be replayed"));
  EXPECT_THAT(message, ::testing::Not(HasSubstr("'b'"))) << "only the branch that fails is named";

  fs::permissions(a_dir, fs::perms::owner_all);
  EXPECT_EQ(DurableFiles(), files) << "a refused startup changes no file";
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  EXPECT_EQ(DumpMain(db), main_before);
  EXPECT_TRUE(db.version_store()->Exists("a"));
  auto b = Checkout(db, "b");
  ASSERT_NE(b, nullptr);
  EXPECT_EQ(StateOf(db, *b, base), b_before);
}

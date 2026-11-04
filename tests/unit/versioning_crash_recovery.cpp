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
// Recovery must land on the last complete transaction, never on a half-applied one; damage it cannot account for
// refuses startup and leaves every file as it is.
#include <unistd.h>

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
#include "tests/versioning_startup_refusal.hpp"
#include "utils/file.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/branch_ledger.hpp"
#include "versioning/version_store.hpp"
#include "versioning_recovery_support.hpp"

using namespace memgraph::tests::versioning_recovery;  // NOLINT(google-build-using-namespace)

namespace dur = memgraph::storage::durability;
namespace fs = std::filesystem;

namespace {
const fs::path kStorageDirectory{fs::temp_directory_path() / "versioning_crash_recovery"};
const fs::path kImageDirectory{fs::temp_directory_path() / "versioning_crash_recovery_image"};

ms::Config MakeConfig(bool recover_on_startup) {
  return memgraph::tests::versioning_recovery::MakeConfig(kStorageDirectory, recover_on_startup);
}

// ---------------------------------------------------------------------------------------------------------------
// Files: copying, parsing a WAL into transactions, and tearing it the way a crash does.
// ---------------------------------------------------------------------------------------------------------------

std::string ReadFile(const fs::path &path) {
  std::ifstream in(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void WriteFile(const fs::path &path, std::string_view data) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out.write(data.data(), static_cast<std::streamsize>(data.size()));
}

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

fs::path BranchWalDir(uint64_t number) {
  return memgraph::tests::versioning_recovery::BranchWalDir(kStorageDirectory, number);
}

fs::path MainWalDir() { return kStorageDirectory / std::string{dur::kWalDirectory}; }

size_t CountFiles(const fs::path &dir) { return WalFiles(dir).size(); }

// The newest non-empty WAL file of `dir`.
fs::path NewestWalFile(const fs::path &dir) {
  const auto files = WalFiles(dir);
  const auto it =
      std::ranges::find_if(files.rbegin(), files.rend(), [](const fs::path &f) { return fs::file_size(f) != 0; });
  return it == files.rend() ? fs::path{} : *it;
}

struct DeltaSpan {
  uint64_t begin{0};
  uint64_t end{0};
  bool overlay_put{false};
};

struct TxnSpan {
  uint64_t begin{0};
  uint64_t end{0};
  std::vector<DeltaSpan> deltas;
};

// The byte range of every transaction of a cleanly finalized or unfinalized WAL with complete transactions.
std::vector<TxnSpan> ParseTransactions(const fs::path &wal) {
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
    txns.back().deltas.push_back(
        {.begin = begin, .end = end, .overlay_put = std::holds_alternative<dur::WalOverlayPut>(delta.data_)});
    if (dur::IsWalDeltaDataTransactionEnd(delta, *version)) {
      txns.back().end = end;
      in_txn = false;
    }
  }
  EXPECT_FALSE(in_txn) << "the file ends inside a transaction";
  return txns;
}

// Offsets inside `txn` at which a crash can tear it. All of them are below txn.end, so none keeps the whole
// transaction. Always included: the transaction's first bytes, the inside of every OVERLAY_PUT (start, middle, last
// byte and the boundary after it), and the transaction end marker (its start, one byte in, and its last byte short).
// Other delta boundaries are thinned out to keep the total near `budget`.
std::vector<uint64_t> CutPoints(const TxnSpan &txn, size_t budget = 24) {
  std::set<uint64_t> must{txn.begin, txn.begin + 1};
  std::set<uint64_t> optional;
  for (size_t i = 0; i < txn.deltas.size(); ++i) {
    const auto &d = txn.deltas[i];
    const uint64_t middle = d.begin + (d.end - d.begin) / 2;
    if (i + 1 == txn.deltas.size()) {
      must.insert({d.begin, d.begin + 1, d.end - 1});
    } else if (d.overlay_put) {
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

// The summary region of a never-finalized file: three zero values and their CRC trailer, as WalFile's constructor
// writes them. Produced by the same encoder so the CRC is right by construction.
std::string SummaryPlaceholder() {
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
void CrashAt(const fs::path &wal, uint64_t keep) {
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

void FlipByte(const fs::path &file, uint64_t offset) {
  auto bytes = ReadFile(file);
  ASSERT_LT(offset, bytes.size());
  bytes[offset] = static_cast<char>(bytes[offset] ^ 0xFF);
  WriteFile(file, bytes);
}

// ---------------------------------------------------------------------------------------------------------------
// Scenarios.
// ---------------------------------------------------------------------------------------------------------------

struct Expected {
  std::vector<VertexDump> branch;
  std::vector<VertexDump> main;
};

struct Scenario {
  uint64_t number{0};
  Base base{};
  Expected before_last;  // after the last complete transaction
  Expected full;         // after the transaction that gets torn
};

enum class LastTxn {
  kMixed,        // native vertex create + fork-vertex SET
  kOverlayOnly,  // fork-vertex SET (two keys)
  kForkEdge,     // fork-edge property SET
};

// The complete transaction that precedes the torn one: overlay write of a fork vertex, a native vertex with a label and
// a property, and a native edge from the anchor of a fork vertex to it.
void RunPrior(mv::BranchContext &ctx, const Base &b, const Names &n) {
  BranchTxn(ctx, [&](mv::BranchContext &c, ms::Storage::Accessor &) {
    ASSERT_TRUE(c.PrepareForkWrite(b.v1).has_value());
    ASSERT_TRUE(c.OverlaySetProperty(b.v1, n.p, ms::PropertyValue(2)).has_value());
    auto n1 = c.CreateNativeVertex();
    ASSERT_TRUE(n1.AddLabel(n.n).has_value());
    ASSERT_TRUE(n1.SetProperty(n.a, ms::PropertyValue(1)).has_value());
    auto a4 = c.CowVertex(b.v4);
    ASSERT_TRUE(a4.has_value());
    ASSERT_TRUE(c.CreateNativeEdge(&*a4, &n1, n.t).has_value());
  });
}

void RunLast(LastTxn kind, mv::BranchContext &ctx, const Base &b, const Names &n) {
  BranchTxn(ctx, [&](mv::BranchContext &c, ms::Storage::Accessor &) {
    switch (kind) {
      case LastTxn::kMixed: {
        ASSERT_TRUE(c.PrepareForkWrite(b.v2).has_value());
        ASSERT_TRUE(c.OverlaySetProperty(b.v2, n.p, ms::PropertyValue(7)).has_value());
        auto nv = c.CreateNativeVertex();
        ASSERT_TRUE(nv.AddLabel(n.n).has_value());
        ASSERT_TRUE(nv.SetProperty(n.a, ms::PropertyValue(42)).has_value());
        break;
      }
      case LastTxn::kOverlayOnly: {
        ASSERT_TRUE(c.PrepareForkWrite(b.v3).has_value());
        ASSERT_TRUE(c.OverlaySetProperty(b.v3, n.p, ms::PropertyValue(8)).has_value());
        ASSERT_TRUE(c.OverlayAddLabel(b.v3, n.x).has_value());
        ASSERT_TRUE(c.PrepareForkWrite(b.v2).has_value());
        ASSERT_TRUE(c.OverlaySetProperty(b.v2, n.q, ms::PropertyValue(3)).has_value());
        break;
      }
      case LastTxn::kForkEdge: {
        auto e1 = c.FindForkEdge(b.e1, b.v1);
        ASSERT_TRUE(e1.has_value());
        ASSERT_TRUE(c.PrepareForkEdgeWrite(*e1).has_value());
        ASSERT_TRUE(c.EdgeOverlaySetProperty(*e1, n.w, ms::PropertyValue(9)).has_value());
        break;
      }
    }
  });
}

Expected Capture(memgraph::dbms::Database &db, mv::BranchContext &ctx) {
  return {.branch = DumpBranch(db, ctx), .main = DumpMain(db)};
}

// A branch "b" with RunPrior committed and then `kind` committed, both in one WAL file; the database is closed cleanly.
void BuildScenario(LastTxn kind, Scenario &out) {
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto n = ResolveNames(db);
  out.base = CreateBase(db);
  auto info = db.version_store()->CreateBranch("b", "main");
  ASSERT_TRUE(info.has_value());
  out.number = info->number;
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  RunPrior(*ctx, out.base, n);
  out.before_last = Capture(db, *ctx);
  RunLast(kind, *ctx, out.base, n);
  out.full = Capture(db, *ctx);
  ASSERT_NE(out.before_last.branch, out.full.branch) << "the last transaction must change the branch";
}

// Restarts the way a crash recovery does and checks branch `name` against `want`.
void ExpectRecovers(const Expected &want, std::string_view name = "b") {
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_TRUE(db.version_store()->Exists(name)) << "the branch must survive the restart";
  auto ctx = Checkout(db, name);
  ASSERT_NE(ctx, nullptr);
  EXPECT_EQ(DumpBranch(db, *ctx), want.branch);
  EXPECT_EQ(DumpMain(db), want.main);
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
void ExpectTornNewestFileIsSkipped(const fs::path &wal, const std::vector<uint64_t> &cuts, const Expected &want,
                                   const std::string &what) {
  for (const auto cut : cuts) {
    SCOPED_TRACE(what + ", cut " + std::to_string(cut));
    ASSERT_NO_FATAL_FAILURE(RestoreImage());
    ASSERT_NO_FATAL_FAILURE(CrashAt(wal, cut));
    ASSERT_NO_FATAL_FAILURE(ExpectRecovers(want));
    ASSERT_NO_FATAL_FAILURE(ExpectTornFileLeftInPlace(wal));
    ASSERT_NO_FATAL_FAILURE(ExpectRecovers(want));
  }
}

void ExpectTornLastTransactionIsDropped(LastTxn kind) {
  Scenario s;
  ASSERT_NO_FATAL_FAILURE(BuildScenario(kind, s));
  ASSERT_NO_FATAL_FAILURE(SaveImage());

  const auto wal = NewestWalFile(BranchWalDir(s.number));
  ASSERT_FALSE(wal.empty());
  const auto txns = ParseTransactions(wal);
  ASSERT_EQ(txns.size(), 2u) << "every committed branch transaction is one WAL transaction";
  const auto &last = txns.back();
  ASSERT_TRUE(std::ranges::any_of(last.deltas, &DeltaSpan::overlay_put))
      << "the last transaction carries an overlay put";
  const auto cuts = CutPoints(last);
  ASSERT_GE(cuts.size(), 8u);

  for (const auto cut : cuts) {
    SCOPED_TRACE(CutTrace(cut, last));
    ASSERT_NO_FATAL_FAILURE(RestoreImage());
    ASSERT_NO_FATAL_FAILURE(CrashAt(wal, cut));
    ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.before_last));
  }

  // Control: the same crash image with every byte kept recovers the last transaction. It proves the zeroed summary
  // alone does not lose anything, so the cases above lose the transaction because of the tear.
  SCOPED_TRACE("control: nothing torn");
  ASSERT_NO_FATAL_FAILURE(RestoreImage());
  ASSERT_NO_FATAL_FAILURE(CrashAt(wal, last.end));
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.full));
}
}  // namespace

class VersioningCrashRecoveryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    fs::remove_all(kStorageDirectory);
    fs::remove_all(kImageDirectory);
  }

  void TearDown() override {
    fs::remove_all(kStorageDirectory);
    fs::remove_all(kImageDirectory);
  }
};

// ---- 1. Branch WAL torn inside its last transaction -------------------------------------------------------------
//
// A crash image has no summary, so ListWalFiles (branch_recovery.cpp) calls ReadWalContents, which
// parses the deltas (ScanWalDeltas in wal.cpp). The scan adds a transaction's deltas to num_deltas only when its end
// marker passed the CRC check (wal.cpp), so a torn tail contributes nothing. ReplayFile then consumes exactly
// num_deltas deltas (branch_recovery.cpp), and the overlay puts of a transaction are held back until its commit
// (branch_recovery.cpp), so a transaction is applied whole or not at all. The previous transaction is
// in the same file, so the file has a complete transaction and is not the case of test 6.

class TornLastTransactionTest : public VersioningCrashRecoveryTest, public ::testing::WithParamInterface<LastTxn> {};

TEST_P(TornLastTransactionTest, BranchWalTornInTheLastTransactionRecoversThePreviousOne) {
  ExpectTornLastTransactionIsDropped(GetParam());
}

INSTANTIATE_TEST_SUITE_P(Kinds, TornLastTransactionTest,
                         ::testing::Values(LastTxn::kMixed, LastTxn::kOverlayOnly, LastTxn::kForkEdge),
                         [](const ::testing::TestParamInfo<LastTxn> &info) {
                           switch (info.param) {
                             case LastTxn::kMixed:
                               return "Mixed";
                             case LastTxn::kOverlayOnly:
                               return "OverlayOnly";
                             case LastTxn::kForkEdge:
                               return "ForkEdge";
                           }
                           return "Unknown";
                         });

// Recovery must be repeatable, and the torn file must not get in the way of later runs: the next run writes a new file,
// so the torn one becomes a middle file whose complete prefix is replayed before the new file. The new run continues
// the sequence numbers past the torn file, so it leaves no gap, and its clock starts past the replayed timestamps.
TEST_F(VersioningCrashRecoveryTest, RecoveryAfterATornTransactionIsRepeatableAndLaterWritesSurvive) {
  Scenario s;
  ASSERT_NO_FATAL_FAILURE(BuildScenario(LastTxn::kMixed, s));
  ASSERT_NO_FATAL_FAILURE(SaveImage());
  const auto wal = NewestWalFile(BranchWalDir(s.number));
  const auto txns = ParseTransactions(wal);
  ASSERT_EQ(txns.size(), 2u);
  const auto cuts = CutPoints(txns.back());
  ASSERT_FALSE(cuts.empty());
  ASSERT_NO_FATAL_FAILURE(RestoreImage());
  ASSERT_NO_FATAL_FAILURE(CrashAt(wal, cuts[cuts.size() / 2]));

  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.before_last));
  // A second restart with no writes in between.
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.before_last));
  EXPECT_EQ(CountFiles(BranchWalDir(s.number)), 1u) << "restarts without writes must not add branch WAL files";

  Expected after_write;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto n = ResolveNames(db);
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    BranchTxn(*ctx, [&](mv::BranchContext &c, ms::Storage::Accessor &) {
      ASSERT_TRUE(c.PrepareForkWrite(s.base.v3).has_value());
      ASSERT_TRUE(c.OverlaySetProperty(s.base.v3, n.p, ms::PropertyValue(100)).has_value());
      auto nv = c.CreateNativeVertex();
      ASSERT_TRUE(nv.AddLabel(n.n).has_value());
    });
    after_write = Capture(db, *ctx);
    ASSERT_NE(after_write.branch, s.before_last.branch);
  }
  EXPECT_EQ(CountFiles(BranchWalDir(s.number)), 2u) << "the new run writes a new file beside the torn one";
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(after_write));
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(after_write));
}

// ---- 2. Branch WAL damaged inside a middle transaction ----------------------------------------------------------
//
// Three transactions t1, t2, t3 in one file; t2's CRC trailer is damaged (its last byte flipped) while t3 follows.
// Branch "c" has a WAL of its own and must be untouched in both variants.

struct MiddleScenario {
  uint64_t b_number{0};
  Base base{};
  Expected after_t1;
  Expected after_t3;
  std::vector<VertexDump> c_dump;
  std::vector<VertexDump> main_dump;
};

void BuildMiddleScenario(MiddleScenario &out) {
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto n = ResolveNames(db);
  out.base = CreateBase(db);
  auto b_info = db.version_store()->CreateBranch("b", "main");
  ASSERT_TRUE(b_info.has_value());
  out.b_number = b_info->number;
  ASSERT_TRUE(db.version_store()->CreateBranch("c", "main").has_value());
  auto b = Checkout(db, "b");
  auto c = Checkout(db, "c");
  ASSERT_NE(b, nullptr);
  ASSERT_NE(c, nullptr);
  const auto &base = out.base;

  BranchTxn(*b, [&](mv::BranchContext &ctx, ms::Storage::Accessor &) {
    ASSERT_TRUE(ctx.PrepareForkWrite(base.v1).has_value());
    ASSERT_TRUE(ctx.OverlaySetProperty(base.v1, n.p, ms::PropertyValue(2)).has_value());
    auto nv = ctx.CreateNativeVertex();
    ASSERT_TRUE(nv.AddLabel(n.n).has_value());
  });
  out.after_t1 = Capture(db, *b);
  BranchTxn(*b, [&](mv::BranchContext &ctx, ms::Storage::Accessor &) {
    ASSERT_TRUE(ctx.PrepareForkWrite(base.v2).has_value());
    ASSERT_TRUE(ctx.OverlaySetProperty(base.v2, n.p, ms::PropertyValue(3)).has_value());
  });
  BranchTxn(*b, [&](mv::BranchContext &ctx, ms::Storage::Accessor &) {
    ASSERT_TRUE(ctx.PrepareForkWrite(base.v3).has_value());
    ASSERT_TRUE(ctx.OverlaySetProperty(base.v3, n.p, ms::PropertyValue(4)).has_value());
  });
  out.after_t3 = Capture(db, *b);
  ASSERT_NE(out.after_t1.branch, out.after_t3.branch);

  BranchTxn(*c, [&](mv::BranchContext &ctx, ms::Storage::Accessor &) {
    ASSERT_TRUE(ctx.PrepareForkWrite(base.v4).has_value());
    ASSERT_TRUE(ctx.OverlaySetProperty(base.v4, n.p, ms::PropertyValue(5)).has_value());
  });
  out.c_dump = DumpBranch(db, *c);
  out.main_dump = DumpMain(db);
}

// Finalized file (the summary is intact) with the middle transaction's CRC damaged.
// ListWalFiles trusts the summary (wal.cpp, no scan), so replay runs into t2's end marker
// and the CRC check throws (branch_recovery.cpp). ReplayBranchWal turns it into an error and RecoverBranchContents
// reports it, so startup is refused and every file stays as it is.
TEST_F(VersioningCrashRecoveryTest, MiddleTransactionDamagedInAFinalizedBranchWalRefusesStartup) {
  MiddleScenario s;
  ASSERT_NO_FATAL_FAILURE(BuildMiddleScenario(s));
  const auto wal = NewestWalFile(BranchWalDir(s.b_number));
  const auto txns = ParseTransactions(wal);
  ASSERT_EQ(txns.size(), 3u);
  ASSERT_NO_FATAL_FAILURE(FlipByte(wal, txns[1].end - 1));

  memgraph::tests::ExpectStartupRefused(
      kStorageDirectory, MakeConfig(/*recover_on_startup=*/true), {"Database '", "[b]", "branch 'b'", "DROP BRANCH"});
  EXPECT_TRUE(fs::exists(wal));
}

// The same damage in a file that was never finalized (a crash image): the scan stops at the first transaction whose CRC
// fails (wal.cpp, then the loop breaks), so the file reports only t1 and the replay lands on the state after
// t1, losing t2 and t3 without dropping the branch. This is a consistent prefix (never a mix) and is how main's WAL
// recovery treats the same damage; the loss of t3, which is intact, is only visible in the log.
TEST_F(VersioningCrashRecoveryTest, MiddleTransactionDamagedInAnUnfinalizedBranchWalRecoversThePrefixBeforeIt) {
  MiddleScenario s;
  ASSERT_NO_FATAL_FAILURE(BuildMiddleScenario(s));
  const auto wal = NewestWalFile(BranchWalDir(s.b_number));
  const auto txns = ParseTransactions(wal);
  ASSERT_EQ(txns.size(), 3u);
  ASSERT_NO_FATAL_FAILURE(CrashAt(wal, fs::file_size(wal)));
  ASSERT_NO_FATAL_FAILURE(FlipByte(wal, txns[1].end - 1));

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_TRUE(db.version_store()->Exists("b"));
  ASSERT_TRUE(db.version_store()->Exists("c"));
  EXPECT_EQ(DumpMain(db), s.main_dump);
  auto b = Checkout(db, "b");
  auto c = Checkout(db, "c");
  ASSERT_NE(b, nullptr);
  ASSERT_NE(c, nullptr);
  EXPECT_EQ(DumpBranch(db, *b), s.after_t1.branch);
  EXPECT_EQ(DumpBranch(db, *c), s.c_dump);
}

// ---- 3. Main WAL torn in its last transaction while a branch exists ----------------------------------------------
//
// Main's last file has no summary, so GetWalFiles (durability.cpp) derives num_deltas from the
// complete transactions only (wal.cpp), and the bounded recovery and the tail replay both consume exactly that
// many deltas (durability.cpp, replication_handlers.cpp). The branch's WAL is a separate file set and is
// replayed on the rebuilt fork base (branch_recovery.cpp), which does not contain the torn transaction because
// it was committed after the fork.

TEST_F(VersioningCrashRecoveryTest, MainWalTornInItsLastTransactionKeepsMainAtThePreviousOneAndTheBranchIntact) {
  Expected before_last;  // main after M1, branch as written
  std::vector<VertexDump> main_full;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto n = ResolveNames(db);
    const auto base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b", "main").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    RunPrior(*ctx, base, n);

    {  // M1
      auto acc = db.Access(ms::WRITE);
      auto v1 = acc->FindVertex(base.v1, ms::View::OLD);
      ASSERT_TRUE(v1.has_value());
      ASSERT_TRUE(v1->SetProperty(n.p, ms::PropertyValue(10)).has_value());
      Commit(*acc);
    }
    before_last = Capture(db, *ctx);
    {  // M2: a property write and a new vertex
      auto acc = db.Access(ms::WRITE);
      auto v1 = acc->FindVertex(base.v1, ms::View::OLD);
      ASSERT_TRUE(v1.has_value());
      ASSERT_TRUE(v1->SetProperty(n.p, ms::PropertyValue(20)).has_value());
      auto created = acc->CreateVertex();
      ASSERT_TRUE(created.AddLabel(n.m).has_value());
      Commit(*acc);
    }
    main_full = DumpMain(db);
    ASSERT_NE(main_full, before_last.main);
    ASSERT_EQ(DumpBranch(db, *ctx), before_last.branch) << "main writes after the fork do not reach the branch";
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
    ASSERT_NO_FATAL_FAILURE(ExpectRecovers(before_last));
  }

  SCOPED_TRACE("control: nothing torn");
  ASSERT_NO_FATAL_FAILURE(RestoreImage());
  ASSERT_NO_FATAL_FAILURE(CrashAt(wal, last.end));
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(Expected{.branch = before_last.branch, .main = main_full}));
}

// The branch is forked after main's last transaction, and that transaction is torn: main's recovery loses a commit the
// ledger's fork point already includes (nothing compares the ledger's fork point with what main recovered). The branch
// has an overlay record for a vertex that only the lost transaction created.
// Expected: startup is refused: replaying the overlay record for the missing fork vertex throws (DeriveVertex), and
// the files stay as they are.
TEST_F(VersioningCrashRecoveryTest, MainTornAtTheTransactionABranchForkedOnRefusesStartup) {
  std::vector<VertexDump> main_prev;
  ms::Gid late;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto n = ResolveNames(db);
    static_cast<void>(CreateBase(db));
    main_prev = DumpMain(db);
    {
      auto acc = db.Access(ms::WRITE);
      auto created = acc->CreateVertex();
      late = created.Gid();
      ASSERT_TRUE(created.AddLabel(n.l).has_value());
      ASSERT_TRUE(created.SetProperty(n.p, ms::PropertyValue(5)).has_value());
      Commit(*acc);
    }
    ASSERT_TRUE(db.version_store()->CreateBranch("b", "main").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    BranchTxn(*ctx, [&](mv::BranchContext &c, ms::Storage::Accessor &) {
      ASSERT_TRUE(c.PrepareForkWrite(late).has_value());
      ASSERT_TRUE(c.OverlaySetProperty(late, n.p, ms::PropertyValue(9)).has_value());
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
    ASSERT_TRUE(db.version_store()->Exists("b"));
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    const auto *v = FindVertex(DumpBranch(db, *ctx), late);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(v->props.at("p"), ms::PropertyValue(9));
  }

  for (const auto cut : CutPoints(last, 8)) {
    SCOPED_TRACE(CutTrace(cut, last));
    ASSERT_NO_FATAL_FAILURE(RestoreImage());
    ASSERT_NO_FATAL_FAILURE(CrashAt(wal, cut));

    memgraph::tests::ExpectStartupRefused(
        kStorageDirectory, MakeConfig(/*recover_on_startup=*/true), {"Database '", "[b]", "DROP BRANCH"});
  }
}

// ---- 4. Ledger written, branch WAL missing or empty
// ------------------------------------------------------------------------

// No WAL files means the core is not built at startup and is built empty on first use,
// reading main at the fork.
TEST_F(VersioningCrashRecoveryTest, LedgerWithoutABranchWalDirectoryRecoversEmptyAtTheFork) {
  uint64_t number = 0;
  Expected expected;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    static_cast<void>(CreateBase(db));
    auto info = db.version_store()->CreateBranch("b", "main");
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    expected = Capture(db, *ctx);
  }
  fs::remove_all(kStorageDirectory / "branches" / std::to_string(number));
  ASSERT_FALSE(fs::exists(BranchWalDir(number)));
  ASSERT_TRUE(fs::exists(kStorageDirectory / "branches" / "ledger"));

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  ASSERT_TRUE(db.version_store()->Exists("b"));
  bool built = false;
  auto ctx = Checkout(db, "b", &built);
  ASSERT_NE(ctx, nullptr);
  EXPECT_TRUE(built);
  EXPECT_EQ(DumpBranch(db, *ctx), expected.branch);
  EXPECT_EQ(DumpMain(db), expected.main);
}

// The crash left a zero-length WAL file (created, nothing written). Its header cannot be read, so
// ListWalFiles skips it as main's GetWalFiles does (branch_recovery.cpp) and ReplayBranchWal returns with the freshly
// built core (branch_recovery.cpp).
TEST_F(VersioningCrashRecoveryTest, EmptyBranchWalFileRecoversEmptyAtTheFork) {
  uint64_t number = 0;
  Expected before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto n = ResolveNames(db);
    const auto base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b", "main");
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    before = Capture(db, *ctx);
    RunLast(LastTxn::kOverlayOnly, *ctx, base, n);
    ASSERT_NE(DumpBranch(db, *ctx), before.branch);
  }
  const auto wal = NewestWalFile(BranchWalDir(number));
  ASSERT_FALSE(wal.empty());
  fs::resize_file(wal, 0);
  ASSERT_EQ(CountFiles(BranchWalDir(number)), 1u);
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(before));
  auto torn = wal;
  torn += ".torn";
  EXPECT_TRUE(fs::exists(wal)) << "a zero-length file is skipped and left in place";
  EXPECT_FALSE(fs::exists(torn));
}

// ---- 5. Restarting twice with no writes in between ---------------------------------------------------------------

// Replay runs under SuppressWalScope (branch_recovery.cpp), so a restart writes no branch WAL,
// and the ledger keeps the original fork point (BranchInfo::recorded_fork_ts is what every ledger write persists).
TEST_F(VersioningCrashRecoveryTest, RestartingTwiceWithoutWritesIsIdempotent) {
  Scenario s;
  ASSERT_NO_FATAL_FAILURE(BuildScenario(LastTxn::kMixed, s));
  const auto branch_files = CountFiles(BranchWalDir(s.number));
  ASSERT_GE(branch_files, 1u);

  const auto fork_ts_in_ledger = [] {
    auto ledger = mv::BranchLedger{kStorageDirectory / "branches" / "ledger"}.Read();
    EXPECT_TRUE(ledger.has_value() && ledger->has_value());
    if (!ledger || !*ledger || (*ledger)->branches.size() != 1) return uint64_t{0};
    return (*ledger)->branches[0].info.fork_ts;
  };
  const auto fork_ts = fork_ts_in_ledger();
  ASSERT_NE(fork_ts, 0u);

  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.full));
  const auto main_files_after_first = CountFiles(MainWalDir());
  EXPECT_EQ(CountFiles(BranchWalDir(s.number)), branch_files);
  EXPECT_EQ(fork_ts_in_ledger(), fork_ts);

  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.full));
  EXPECT_EQ(CountFiles(MainWalDir()), main_files_after_first);
  EXPECT_EQ(CountFiles(BranchWalDir(s.number)), branch_files);
  EXPECT_EQ(fork_ts_in_ledger(), fork_ts);
}

// ---- 6. A WAL file without a complete transaction ----------------------------------------------------------------

// Nothing was ever committed to the torn file, so the branch must recover from the other files (or empty at the fork).
// ScanWalDeltas throws RecoveryFailure for such a file; ListWalFiles catches it per file and skips it, as main's
// GetWalFiles does, without renaming it. A skipped file the sequence numbers show to sit in the middle of the stream is
// a missing file, which refuses startup as main does (test 7).

// The branch's only WAL file has its only transaction torn, or its header torn (a crash while the header is written).
TEST_F(VersioningCrashRecoveryTest, TornOnlyBranchWalFileRecoversEmptyAtTheFork) {
  uint64_t number = 0;
  Expected before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto n = ResolveNames(db);
    const auto base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b", "main");
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    before = Capture(db, *ctx);
    RunLast(LastTxn::kOverlayOnly, *ctx, base, n);
    ASSERT_NE(DumpBranch(db, *ctx), before.branch);
  }
  ASSERT_NO_FATAL_FAILURE(SaveImage());
  const auto wal = NewestWalFile(BranchWalDir(number));
  const auto txns = ParseTransactions(wal);
  ASSERT_EQ(txns.size(), 1u);
  const auto header = dur::ReadWalHeader(wal);  // before the cuts below tear and rename the file
  ASSERT_NO_FATAL_FAILURE(ExpectTornNewestFileIsSkipped(wal, CutPoints(txns.back(), 8), before, TxnLabel(txns.back())));

  ASSERT_NO_FATAL_FAILURE(ExpectTornNewestFileIsSkipped(
      wal, {uint64_t{10}, header.offset_metadata + 3, header.offset_deltas - 1}, before, "header"));
}

// A later run's new file is torn in its first transaction while the earlier run's file is intact.
TEST_F(VersioningCrashRecoveryTest, TornFirstTransactionOfALaterRunKeepsTheEarlierRunsData) {
  uint64_t number = 0;
  Base base{};
  Expected before_last;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto n = ResolveNames(db);
    base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b", "main");
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    RunPrior(*ctx, base, n);
  }
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto n = ResolveNames(db);
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    before_last = Capture(db, *ctx);
    RunLast(LastTxn::kOverlayOnly, *ctx, base, n);
    ASSERT_NE(DumpBranch(db, *ctx), before_last.branch);
  }
  ASSERT_EQ(CountFiles(BranchWalDir(number)), 2u) << "one file per run";
  ASSERT_NO_FATAL_FAILURE(SaveImage());

  const auto newest = NewestWalFile(BranchWalDir(number));
  const auto txns = ParseTransactions(newest);
  ASSERT_EQ(txns.size(), 1u);
  ASSERT_NO_FATAL_FAILURE(
      ExpectTornNewestFileIsSkipped(newest, CutPoints(txns.back(), 8), before_last, TxnLabel(txns.back())));
}

// ---- 7. A file missing from the sequence-number chain -----------------------------------------------------------
//
// Main fails startup when its WAL chain has a gap or lacks its first file, and ignores a damaged newest file. The
// branch equivalent refuses startup as well. Each transaction below fills a 1 KiB WAL file, so one run writes files 0,
// 1 and 2.

struct ChainScenario {
  uint64_t number{0};
  std::vector<fs::path> wals;  // oldest first
  Expected after[3];
  std::vector<VertexDump> main_dump;
};

void BuildChainScenario(ChainScenario &out) {
  auto config = MakeConfig(/*recover_on_startup=*/false);
  config.durability.wal_file_size_kibibytes = 1;
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto n = ResolveNames(db);
  const auto base = CreateBase(db);
  auto info = db.version_store()->CreateBranch("b", "main");
  ASSERT_TRUE(info.has_value());
  out.number = info->number;
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  const ms::Gid targets[3] = {base.v1, base.v2, base.v3};
  for (int i = 0; i < 3; ++i) {
    BranchTxn(*ctx, [&](mv::BranchContext &c, ms::Storage::Accessor &) {
      ASSERT_TRUE(c.PrepareForkWrite(targets[i]).has_value());
      ASSERT_TRUE(
          c.OverlaySetProperty(targets[i], n.p, ms::PropertyValue(std::string(2048, static_cast<char>('a' + i))))
              .has_value());
    });
    out.after[i] = Capture(db, *ctx);
  }
  out.main_dump = DumpMain(db);
}

// The files of the chain, oldest first, after checking that the scenario produced the sequence 0, 1, ... count - 1.
void ListChain(ChainScenario &s, size_t count = 3) {
  s.wals = WalFiles(BranchWalDir(s.number));
  ASSERT_EQ(s.wals.size(), count) << "the transactions must land in one file each";
  for (uint64_t seq = 0; seq < count; ++seq) ASSERT_EQ(dur::ReadWalHeader(s.wals[seq]).seq_num, seq);
}

// Run 1 writes files 0, 1, 2; the restart recovers them and run 2 writes two more transactions. Main numbers its WAL
// files across restarts, so run 2's files must be 3 and 4, and a file lost from either run must leave a visible gap.
void BuildTwoRunChainScenario(ChainScenario &out, Expected &final_state) {
  ASSERT_NO_FATAL_FAILURE(BuildChainScenario(out));
  auto config = MakeConfig(/*recover_on_startup=*/true);
  config.durability.wal_file_size_kibibytes = 1;
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  const auto n = ResolveNames(db);
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(DumpBranch(db, *ctx), out.after[2].branch);
  // The vertex gids of the base are recovered with main; take them from the branch dump of run 1.
  std::vector<ms::Gid> targets;
  for (const auto &v : out.after[2].branch) targets.push_back(ms::Gid::FromUint(v.gid));
  ASSERT_GE(targets.size(), 2u);
  for (int i = 0; i < 2; ++i) {
    BranchTxn(*ctx, [&](mv::BranchContext &c, ms::Storage::Accessor &) {
      ASSERT_TRUE(c.PrepareForkWrite(targets[i]).has_value());
      ASSERT_TRUE(
          c.OverlaySetProperty(targets[i], n.p, ms::PropertyValue(std::string(2048, static_cast<char>('d' + i))))
              .has_value());
    });
  }
  final_state = Capture(db, *ctx);
  out.main_dump = DumpMain(db);
}

void ExpectStartupRefusedOverBranchB() {
  memgraph::tests::ExpectStartupRefused(
      kStorageDirectory, MakeConfig(/*recover_on_startup=*/true), {"Database '", "[b]", "branch 'b'", "DROP BRANCH"});
}

TEST_F(VersioningCrashRecoveryTest, ControlTheUndamagedChainRecoversAllThreeFiles) {
  ChainScenario s;
  ASSERT_NO_FATAL_FAILURE(BuildChainScenario(s));
  ASSERT_NO_FATAL_FAILURE(ListChain(s));
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.after[2]));
}

TEST_F(VersioningCrashRecoveryTest, UnreadableMiddleFileIsAGapAndRefusesStartup) {
  ChainScenario s;
  ASSERT_NO_FATAL_FAILURE(BuildChainScenario(s));
  ASSERT_NO_FATAL_FAILURE(ListChain(s));
  fs::resize_file(s.wals[1], 10);
  ASSERT_NO_FATAL_FAILURE(ExpectStartupRefusedOverBranchB());
  for (const auto &wal : s.wals) EXPECT_TRUE(fs::exists(wal)) << "a refused startup keeps the files";
}

TEST_F(VersioningCrashRecoveryTest, MissingMiddleFileIsAGapAndRefusesStartup) {
  ChainScenario s;
  ASSERT_NO_FATAL_FAILURE(BuildChainScenario(s));
  ASSERT_NO_FATAL_FAILURE(ListChain(s));
  fs::remove(s.wals[1]);
  ASSERT_NO_FATAL_FAILURE(ExpectStartupRefusedOverBranchB());
}

TEST_F(VersioningCrashRecoveryTest, MissingFirstFileRefusesStartup) {
  ChainScenario s;
  ASSERT_NO_FATAL_FAILURE(BuildChainScenario(s));
  ASSERT_NO_FATAL_FAILURE(ListChain(s));
  fs::remove(s.wals[0]);
  ASSERT_NO_FATAL_FAILURE(ExpectStartupRefusedOverBranchB());
}

TEST_F(VersioningCrashRecoveryTest, UnreadableNewestFileIsSkippedWithoutAGap) {
  ChainScenario s;
  ASSERT_NO_FATAL_FAILURE(BuildChainScenario(s));
  ASSERT_NO_FATAL_FAILURE(ListChain(s));
  fs::resize_file(s.wals[2], 10);
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(s.after[1]));
  EXPECT_TRUE(fs::exists(s.wals[2])) << "the skipped file stays in place";
}

// The second run's files continue the first run's numbering, as main's do.
TEST_F(VersioningCrashRecoveryTest, SecondRunContinuesTheWalSequenceAndRecovers) {
  ChainScenario s;
  Expected final_state;
  ASSERT_NO_FATAL_FAILURE(BuildTwoRunChainScenario(s, final_state));
  ASSERT_NO_FATAL_FAILURE(ListChain(s, 5));
  ASSERT_NO_FATAL_FAILURE(ExpectRecovers(final_state));
}

TEST_F(VersioningCrashRecoveryTest, MissingLastFileOfTheFirstRunIsAGapAcrossRestarts) {
  ChainScenario s;
  Expected final_state;
  ASSERT_NO_FATAL_FAILURE(BuildTwoRunChainScenario(s, final_state));
  ASSERT_NO_FATAL_FAILURE(ListChain(s, 5));
  fs::remove(s.wals[2]);
  ASSERT_NO_FATAL_FAILURE(ExpectStartupRefusedOverBranchB());
}

TEST_F(VersioningCrashRecoveryTest, MissingFirstFileOfTheSecondRunIsAGapAcrossRestarts) {
  ChainScenario s;
  Expected final_state;
  ASSERT_NO_FATAL_FAILURE(BuildTwoRunChainScenario(s, final_state));
  ASSERT_NO_FATAL_FAILURE(ListChain(s, 5));
  fs::remove(s.wals[3]);
  ASSERT_NO_FATAL_FAILURE(ExpectStartupRefusedOverBranchB());
}

TEST_F(VersioningCrashRecoveryTest, MissingMiddleFileOfTheFirstRunIsAGapAcrossRestarts) {
  ChainScenario s;
  Expected final_state;
  ASSERT_NO_FATAL_FAILURE(BuildTwoRunChainScenario(s, final_state));
  ASSERT_NO_FATAL_FAILURE(ListChain(s, 5));
  fs::remove(s.wals[1]);
  ASSERT_NO_FATAL_FAILURE(ExpectStartupRefusedOverBranchB());
}

// A branch directory that cannot be read refuses startup instead of dropping the branch.
TEST_F(VersioningCrashRecoveryTest, AnUnreadableBranchDirectoryRefusesStartup) {
  if (::geteuid() == 0) GTEST_SKIP() << "root bypasses directory permissions";

  uint64_t a_number = 0;
  uint64_t b_native_gid = 0;
  std::vector<VertexDump> b_before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    const auto n = ResolveNames(db);
    const auto base = CreateBase(db);
    auto a_info = db.version_store()->CreateBranch("a", "main");
    ASSERT_TRUE(a_info.has_value());
    a_number = a_info->number;
    auto b_info = db.version_store()->CreateBranch("b", "main");
    ASSERT_TRUE(b_info.has_value());
    ASSERT_LT(a_info->number, b_info->number);
    auto a = Checkout(db, "a");
    auto b = Checkout(db, "b");
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    BranchTxn(*a, [&](mv::BranchContext &ctx, ms::Storage::Accessor &) {
      ASSERT_TRUE(ctx.PrepareForkWrite(base.v1).has_value());
      ASSERT_TRUE(ctx.OverlaySetProperty(base.v1, n.p, ms::PropertyValue(7)).has_value());
    });
    BranchTxn(*b, [&](mv::BranchContext &ctx, ms::Storage::Accessor &) {
      ASSERT_TRUE(ctx.PrepareForkWrite(base.v2).has_value());
      ASSERT_TRUE(ctx.OverlaySetProperty(base.v2, n.p, ms::PropertyValue(8)).has_value());
      auto nv = ctx.CreateNativeVertex();
      ASSERT_TRUE(nv.AddLabel(n.n).has_value());
      b_native_gid = nv.Gid().AsUint();
    });
    b_before = DumpBranch(db, *b);
  }
  ASSERT_NE(b_native_gid, 0u);

  const auto a_dir = kStorageDirectory / "branches" / std::to_string(a_number);
  ASSERT_TRUE(fs::is_directory(a_dir));

  struct RestorePermissions {
    fs::path dir;

    ~RestorePermissions() {
      std::error_code ec;
      fs::permissions(dir, fs::perms::owner_all, ec);
    }
  } restore{a_dir};

  fs::permissions(a_dir, fs::perms::none);

  memgraph::tests::ExpectStartupRefused(
      kStorageDirectory, MakeConfig(/*recover_on_startup=*/true), {"Database '", "[a]", "branch 'a'", "DROP BRANCH"});
}

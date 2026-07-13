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

// Graph Versioning v1 branch durability -- slice S3d RESTART integration test (design doc
// opencode-work/versioning-v1/2026-07-13--durability-S2S3-design-v4.html). Builds a real
// dbms::Database, creates a branch (persisting fork_ts via versioning::VersionStore, exactly the
// way `CREATE BRANCH` does through the interpreter), writes past the fork point on main, destroys
// the Database (simulating a process restart), then reconstructs a NEW Database on the SAME
// on-disk directory with recover_on_startup=true and asserts:
//
//   (1) Database's ctor pre-read (§2) correctly threaded the persisted branch's fork_ts into
//       Config::Durability::recover_oldest_fork_ts / recover_fork_timestamps.
//   (2) storage::InMemoryStorage::HistoricalAccess(fork_ts) succeeds post-restart (no
//       ForkTimestampNotPinned) -- the exact primitive CHECKOUT uses, and the exact "pass 1"
//       precondition versioning::MergeBranch itself depends on (see merge.hpp's top-of-file
//       comment: "a read-only classification/snapshot pass against HistoricalAccess(fork_ts)").
//   (3) that historical accessor's reads reflect the FORK-TIME state, not main's current
//       (post-window) state.
//   (4) main's OWN post-restart state reflects the FULL history, including everything committed
//       strictly after the fork point (the windowed WAL replay, §4).
//   (5) a real (small, non-trivial) versioning::MergeBranch call succeeds post-restart, proving
//       MERGE's own precondition end-to-end, not just the HistoricalAccess primitive it depends on.
//
// A second test proves the byte-identical fast path: with no branch ever created, the ctor
// pre-read leaves both new Config fields at their default and none of this file's machinery runs.
//
// SCOPE NOTE (deliberately NOT covered here -- see the S3d implementation report):
//   - F < S (a branch forked before the oldest retained snapshot) is a documented v1 limitation
//     (durability.cpp's RecoverData has an explicit, loud LOG_FATAL guard for it). This test uses
//     an effectively snapshot-free configuration (a 24h snapshot interval, no snapshot_on_exit), so
//     recovery here is WAL-only -- F >= S trivially (no snapshot exists at all) -- the common case
//     the design doc calls out as the one to cover first.
//
// BUG-1 fix (schema-complete windowed replay): a schema/index/constraint/enum/TTL/description-store
// op committed by main strictly inside a branch's replay window used to be warn-and-skipped by
// wal_window_replay.cpp, silently regressing main's own schema on restart whenever a branch existed.
// SchemaOpAfterForkSurvivesRestart (below) is the regression test: it creates a label index on main
// strictly AFTER a branch's fork point and asserts it still exists post-restart.
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <map>
#include <optional>
#include <set>
#include <vector>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/scheduler.hpp"
#include "versioning/merge.hpp"
#include "versioning/version_store.hpp"

namespace {
constexpr auto kTestSuite = "versioning_durability_restart";
const std::filesystem::path kStorageDirectory{std::filesystem::temp_directory_path() / kTestSuite};

// Decodes every persisted WAL file directly (mirrors storage_v2_durability_inmemory.cpp's
// LoadWalStopAtTimestampCeiling test) and returns, for every WalVertexCreate delta, a mapping from
// the created vertex's Gid to that delta's own commit timestamp (all deltas of a single
// transaction share one commit timestamp -- see wal.cpp's LoadWal / this file's design doc). Used
// purely to INDEPENDENTLY verify -- outside any MVCC/recovery machinery -- that this test's
// engineered mechanism actually landed the "exact" vertex's commit_ts precisely on fork_ts, before
// trusting the HistoricalAccess assertions that are the actual point of the test.
std::map<memgraph::storage::Gid, uint64_t> DecodeVertexCreateCommitTimestamps(
    std::filesystem::path const &storage_directory) {
  using namespace memgraph::storage::durability;
  std::map<memgraph::storage::Gid, uint64_t> result;
  auto const wal_dir = storage_directory / kWalDirectory;
  if (!std::filesystem::exists(wal_dir)) return result;
  for (auto const &entry : std::filesystem::directory_iterator(wal_dir)) {
    if (!entry.is_regular_file()) continue;
    auto const &path = entry.path();
    auto info = ReadWalInfo(path);
    Decoder wal;
    auto version = wal.Initialize(path, kWalMagic);
    if (!version) continue;
    wal.SetPosition(info.offset_deltas);
    for (uint64_t i = 0; i < info.num_deltas; ++i) {
      auto const delta_ts = ReadWalDeltaHeader(&wal);
      auto delta = ReadWalDeltaData(&wal, *version);
      if (auto const *create = std::get_if<WalVertexCreate>(&delta.data_)) {
        result[create->gid] = delta_ts;
      }
    }
  }
  return result;
}

memgraph::storage::Config MakeConfig(bool recover_on_startup) {
  return memgraph::storage::Config{
      .durability = {.storage_directory = kStorageDirectory,
                     .recover_on_startup = recover_on_startup,
                     .snapshot_wal_mode =
                         memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                     // Effectively disable periodic snapshotting -- this test's WAL-only recovery
                     // is the F >= S (no snapshot at all) case; see this file's header comment.
                     .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)}},
  };
}

}  // namespace

class VersioningDurabilityRestartTest : public ::testing::Test {
 protected:
  void SetUp() override { std::filesystem::remove_all(kStorageDirectory); }

  void TearDown() override { std::filesystem::remove_all(kStorageDirectory); }
};

TEST_F(VersioningDurabilityRestartTest, BranchSurvivesRestart_HistoricalAccessAndMerge) {
  uint64_t fork_ts = 0;
  memgraph::storage::Gid pre_fork_vertex;
  memgraph::storage::Gid post_fork_vertex;

  {
    auto config = MakeConfig(/*recover_on_startup=*/false);
    memgraph::dbms::Database db{config};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

    {
      auto acc = db.Access(memgraph::storage::WRITE);
      auto v = acc->CreateVertex();
      pre_fork_vertex = v.Gid();
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    ASSERT_NE(db.version_store(), nullptr) << "versioning only runs against IN_MEMORY_TRANSACTIONAL storage";
    auto branch = db.version_store()->CreateBranch("b1", "main", std::nullopt);
    ASSERT_TRUE(branch.has_value()) << (branch.has_value() ? std::string{} : branch.error());
    fork_ts = branch->fork_ts;

    {
      // Advances timestamp_ strictly past fork_ts -- this is the (F, now] window that must be
      // reconstructed by S3d's windowed WAL replay after the restart below.
      auto acc = db.Access(memgraph::storage::WRITE);
      auto v = acc->CreateVertex();
      post_fork_vertex = v.Gid();
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    // `db` destructs here -- finalizes the WAL file -- simulating a process restart.
  }

  // Reconstruct on the SAME on-disk directory, with recovery enabled.
  auto config = MakeConfig(/*recover_on_startup=*/true);
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

  // (1) Database's ctor pre-read (§2) correctly threaded the persisted branch's fork_ts into Config.
  ASSERT_TRUE(db.config().durability.recover_oldest_fork_ts.has_value());
  EXPECT_EQ(*db.config().durability.recover_oldest_fork_ts, fork_ts);
  EXPECT_EQ(db.config().durability.recover_fork_timestamps, std::set<uint64_t>{fork_ts});

  auto *storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

  // (2) CHECKOUT's own primitive -- HistoricalAccess(fork_ts) -- must succeed post-restart (no
  // ForkTimestampNotPinned): the ctor's pin-seeding + windowed replay rebuilt the version chain and
  // re-registered the pin.
  auto historical = storage->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());

  // (3) The historical accessor sees ONLY the pre-fork vertex -- fork-state, not main's current
  // (post-window) state.
  {
    auto found_pre = (*historical)->FindVertex(pre_fork_vertex, memgraph::storage::View::OLD);
    EXPECT_TRUE(found_pre.has_value());
    auto found_post = (*historical)->FindVertex(post_fork_vertex, memgraph::storage::View::OLD);
    EXPECT_FALSE(found_post.has_value())
        << "the historical accessor must not see a vertex created strictly AFTER the fork point";
  }
  historical->reset();  // InMemoryAccessor::Abort() via the destructor (mirrors versioning_historical_access.cpp).

  // (4) Main's OWN post-restart state must reflect the FULL history: both the pre-fork vertex AND
  // the one committed strictly after the fork point (the windowed WAL replay's own job, §4).
  {
    auto acc = db.Access(memgraph::storage::READ);
    EXPECT_TRUE(acc->FindVertex(pre_fork_vertex, memgraph::storage::View::OLD).has_value());
    EXPECT_TRUE(acc->FindVertex(post_fork_vertex, memgraph::storage::View::OLD).has_value())
        << "windowed WAL replay must have rebuilt main's history past the fork point too";
  }

  // (5) MERGE's own precondition, exercised end-to-end (not just the HistoricalAccess primitive
  // above): a small, non-trivial changelog creating one new vertex, replayed onto post-restart main.
  // The raw NameIdMapper* outlives this transient accessor -- it points into InMemoryStorage's own
  // long-lived name_id_mapper_ (mirrors tests/unit/versioning_merge.cpp's identical `Mapper()` helper).
  auto *name_id_mapper = db.Access(memgraph::storage::READ)->GetNameIdMapper();
  auto const merge_vertex_gid = memgraph::storage::Gid::FromUint(9'000'000);
  std::vector<memgraph::storage::durability::WalDeltaData> const changelog{
      memgraph::storage::durability::WalDeltaData{memgraph::storage::durability::WalVertexCreate{merge_vertex_gid}},
  };
  auto merge_result = memgraph::versioning::MergeBranch(
      *storage, fork_ts, changelog, name_id_mapper, memgraph::tests::MakeMainCommitArgs());
  ASSERT_TRUE(merge_result.has_value()) << (merge_result.has_value() ? std::string{} : merge_result.error().message);
  EXPECT_EQ(merge_result->vertices_created, 1u);

  {
    auto acc = db.Access(memgraph::storage::READ);
    EXPECT_TRUE(acc->FindVertex(merge_vertex_gid, memgraph::storage::View::OLD).has_value());
  }
}

// BUG-2 regression test (design doc / logic-verifier pass, 2026-07-13): base-to-F recovery used to
// apply WAL deltas with commit_ts <= F FLAT into the SkipList (no MVCC delta), while
// CreateHistoricalTransaction(F) uses strict `ts < start_timestamp` visibility (mvcc.hpp) -- so a
// transaction committing at EXACTLY ts == F was wrongly VISIBLE to the fork after a restart, even
// though it is correctly HIDDEN pre-restart (live). The fix makes the fork boundary EXCLUSIVE: base
// materializes only ts < F, and the windowed WAL replay applies ts >= F (including the exact-F txn)
// as a real MVCC delta, which the fork's strict `<` then correctly hides.
//
// Mechanism for engineering commit_ts == fork_ts exactly: InMemoryStorage::CreateTransaction consumes
// a `timestamp_++` tick at Access()-time (the accessor's start_timestamp), while RegisterForkPin (via
// CreateBranch) only PEEKS the current `timestamp_` without incrementing it, and GetCommitTimestamp
// consumes another `timestamp_++` tick at commit time. So: open a write accessor (consumes tick T,
// timestamp_ becomes T+1) but do NOT commit it yet; create the branch (fork_ts = timestamp_ = T+1,
// unconsumed); THEN commit that same still-open accessor (commit_ts = timestamp_++ = T+1). No other
// timestamp-consuming operation runs between opening and committing that accessor, so its commit_ts
// lands exactly on fork_ts.
TEST_F(VersioningDurabilityRestartTest, ExactForkTimestampBoundaryIsExclusive) {
  uint64_t fork_ts = 0;
  memgraph::storage::Gid before_vertex;  // committed strictly BEFORE fork_ts -- must stay visible.
  memgraph::storage::Gid exact_vertex;   // committed at EXACTLY fork_ts -- must be HIDDEN (BUG-2).
  memgraph::storage::Gid after_vertex;   // committed strictly AFTER fork_ts -- must stay hidden.

  {
    auto config = MakeConfig(/*recover_on_startup=*/false);
    memgraph::dbms::Database db{config};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

    // (a) A vertex committed strictly before the fork point.
    {
      auto acc = db.Access(memgraph::storage::WRITE);
      auto v = acc->CreateVertex();
      before_vertex = v.Gid();
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    ASSERT_NE(db.version_store(), nullptr) << "versioning only runs against IN_MEMORY_TRANSACTIONAL storage";

    // (b) Open the accessor that will land on the exact fork boundary -- do NOT commit yet. This
    // consumes a start_timestamp tick (T); timestamp_ is now T+1.
    auto exact_acc = db.Access(memgraph::storage::WRITE);
    auto exact_v = exact_acc->CreateVertex();
    exact_vertex = exact_v.Gid();

    // (c) Create the branch NOW, while exact_acc is still open and uncommitted. RegisterForkPin
    // peeks timestamp_ (== T+1) WITHOUT incrementing it -- that becomes fork_ts.
    auto branch = db.version_store()->CreateBranch("b1", "main", std::nullopt);
    ASSERT_TRUE(branch.has_value()) << (branch.has_value() ? std::string{} : branch.error());
    fork_ts = branch->fork_ts;

    // (d) Commit exact_acc: GetCommitTimestamp() consumes timestamp_++ == T+1 == fork_ts. No other
    // timestamp-consuming operation ran between (b) and here, so this lands exactly on the boundary.
    ASSERT_TRUE(exact_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

    // Pre-restart (live) sanity: the exact-F transaction must ALREADY be hidden from the fork by
    // ordinary live MVCC (strict `ts < start_timestamp`) -- this is the ground truth the post-restart
    // behavior below must match.
    {
      auto *live_storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());
      auto live_historical = live_storage->HistoricalAccess(fork_ts);
      ASSERT_TRUE(live_historical.has_value());
      EXPECT_TRUE((*live_historical)->FindVertex(before_vertex, memgraph::storage::View::OLD).has_value());
      EXPECT_FALSE((*live_historical)->FindVertex(exact_vertex, memgraph::storage::View::OLD).has_value())
          << "live (pre-restart) MVCC must already hide the exact-F transaction from the fork";
      live_historical->reset();
    }

    // (e) A vertex committed strictly AFTER the fork point.
    {
      auto acc = db.Access(memgraph::storage::WRITE);
      auto v = acc->CreateVertex();
      after_vertex = v.Gid();
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    // `db` destructs here -- finalizes the WAL file -- simulating a process restart.
  }

  // INDEPENDENT verification of the engineered mechanism, decoding the persisted WAL directly
  // (outside any MVCC/recovery machinery) -- prove commit_ts(exact_vertex) == fork_ts EXACTLY, and
  // the other two vertices strictly straddle it, before trusting the HistoricalAccess assertions
  // below. If this ever fails, the mechanism described in this test's header comment stopped
  // landing exactly on the boundary (e.g. an unrelated storage-internal timestamp consumer was
  // added on this path) and the test needs revisiting -- it must not silently pass on a
  // near-boundary case that isn't actually BUG-2.
  {
    auto const commit_ts_by_gid = DecodeVertexCreateCommitTimestamps(kStorageDirectory);
    auto const before_it = commit_ts_by_gid.find(before_vertex);
    auto const exact_it = commit_ts_by_gid.find(exact_vertex);
    auto const after_it = commit_ts_by_gid.find(after_vertex);
    ASSERT_NE(before_it, commit_ts_by_gid.end());
    ASSERT_NE(exact_it, commit_ts_by_gid.end());
    ASSERT_NE(after_it, commit_ts_by_gid.end());
    ASSERT_EQ(exact_it->second, fork_ts) << "test mechanism failed to land commit_ts exactly on fork_ts";
    ASSERT_LT(before_it->second, fork_ts);
    ASSERT_GT(after_it->second, fork_ts);
  }

  // Reconstruct on the SAME on-disk directory, with recovery enabled.
  auto config = MakeConfig(/*recover_on_startup=*/true);
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

  ASSERT_TRUE(db.config().durability.recover_oldest_fork_ts.has_value());
  EXPECT_EQ(*db.config().durability.recover_oldest_fork_ts, fork_ts);

  auto *storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());
  auto historical = storage->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());

  // THE BUG-2 ASSERTIONS: post-restart HistoricalAccess(fork_ts) must match the pre-restart (live)
  // behavior exactly.
  EXPECT_TRUE((*historical)->FindVertex(before_vertex, memgraph::storage::View::OLD).has_value())
      << "a vertex committed strictly BEFORE fork_ts must remain visible to the fork";
  EXPECT_FALSE((*historical)->FindVertex(exact_vertex, memgraph::storage::View::OLD).has_value())
      << "BUG-2: a vertex committed at EXACTLY fork_ts must be HIDDEN from the fork post-restart, "
         "matching live/pre-restart MVCC visibility (strict ts < start_timestamp) -- if this fails, "
         "the exact-F transaction was baked flat into base instead of being left for the windowed "
         "MVCC replay";
  EXPECT_FALSE((*historical)->FindVertex(after_vertex, memgraph::storage::View::OLD).has_value())
      << "a vertex committed strictly AFTER fork_ts must remain hidden from the fork";
  historical->reset();

  // Main's own post-restart state must still reflect the FULL history (all three vertices), proving
  // the exact-F transaction was not lost -- only made invisible to the fork -- by the windowed replay.
  {
    auto acc = db.Access(memgraph::storage::READ);
    EXPECT_TRUE(acc->FindVertex(before_vertex, memgraph::storage::View::OLD).has_value());
    EXPECT_TRUE(acc->FindVertex(exact_vertex, memgraph::storage::View::OLD).has_value())
        << "the exact-F transaction must still be applied to main's own history (via the windowed "
           "replay), just not visible to the fork";
    EXPECT_TRUE(acc->FindVertex(after_vertex, memgraph::storage::View::OLD).has_value());
  }
}

TEST_F(VersioningDurabilityRestartTest, NoBranchRestartIsByteIdenticalFastPath) {
  memgraph::storage::Gid vertex_gid;
  {
    auto config = MakeConfig(/*recover_on_startup=*/false);
    memgraph::dbms::Database db{config};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto acc = db.Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    vertex_gid = v.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto config = MakeConfig(/*recover_on_startup=*/true);
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

  // No branch was ever created -- the ctor pre-read must leave both new Config fields at their
  // default, so none of the S3d window-replay machinery runs (byte-identical to today).
  EXPECT_FALSE(db.config().durability.recover_oldest_fork_ts.has_value());
  EXPECT_TRUE(db.config().durability.recover_fork_timestamps.empty());

  auto acc = db.Access(memgraph::storage::READ);
  EXPECT_TRUE(acc->FindVertex(vertex_gid, memgraph::storage::View::OLD).has_value());
}

// BUG-1 regression test (schema-complete windowed replay): a schema op (here, a label index)
// committed on main STRICTLY AFTER a branch's fork point must survive a restart, exactly like a
// data-plane op does in BranchSurvivesRestart_HistoricalAccessAndMerge above. Before the fix,
// wal_window_replay.cpp's ReplayOneWindowTransaction warn-and-skipped every non-data-plane WAL
// delta, so the index would silently vanish from main's own post-restart state whenever a branch
// existed -- even though the branch itself never asked for it. This test creates the index via a
// real UNIQUE-access accessor (db.UniqueAccess()) so it commits through the ordinary WAL-emitting
// path (WalLabelIndexCreate), exactly like `CREATE INDEX` would.
TEST_F(VersioningDurabilityRestartTest, SchemaOpAfterForkSurvivesRestart) {
  uint64_t fork_ts = 0;
  memgraph::storage::Gid pre_fork_vertex;
  memgraph::storage::Gid post_fork_vertex;
  memgraph::storage::LabelId indexed_label{};

  {
    auto config = MakeConfig(/*recover_on_startup=*/false);
    memgraph::dbms::Database db{config};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

    // Pre-fork vertex.
    {
      auto acc = db.Access(memgraph::storage::WRITE);
      auto v = acc->CreateVertex();
      pre_fork_vertex = v.Gid();
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    ASSERT_NE(db.version_store(), nullptr) << "versioning only runs against IN_MEMORY_TRANSACTIONAL storage";
    auto branch = db.version_store()->CreateBranch("b1", "main", std::nullopt);
    ASSERT_TRUE(branch.has_value()) << (branch.has_value() ? std::string{} : branch.error());
    fork_ts = branch->fork_ts;

    // BUG-1: a schema op (label index create) on main, strictly AFTER the fork point -- this is
    // exactly the WAL delta kind the old window-replay code warn-and-skipped.
    {
      auto acc = db.UniqueAccess();
      indexed_label = acc->NameToLabel("PostForkLabel");
      ASSERT_TRUE(acc->CreateIndex(indexed_label).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    // A data-plane op after the schema op, still strictly after the fork point -- proves the
    // schema-then-data ordering within the replay window keeps working too.
    {
      auto acc = db.Access(memgraph::storage::WRITE);
      auto v = acc->CreateVertex();
      post_fork_vertex = v.Gid();
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    // Pre-restart (live) sanity: the index actually exists before we even test recovery.
    {
      auto acc = db.Access(memgraph::storage::READ);
      EXPECT_THAT(acc->ListAllIndices().label, ::testing::Contains(indexed_label))
          << "test setup sanity: the index must exist live, before restart, for this test to mean anything";
    }
    // `db` destructs here -- finalizes the WAL file -- simulating a process restart.
  }

  // Reconstruct on the SAME on-disk directory, with recovery enabled.
  auto config = MakeConfig(/*recover_on_startup=*/true);
  memgraph::dbms::Database db{config};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};

  ASSERT_TRUE(db.config().durability.recover_oldest_fork_ts.has_value());
  EXPECT_EQ(*db.config().durability.recover_oldest_fork_ts, fork_ts);

  auto *storage = static_cast<memgraph::storage::InMemoryStorage *>(db.storage());

  // (a) THE BUG-1 ASSERTION: the label index created strictly after the fork point must have
  // survived the restart -- windowed WAL replay must have applied the WalLabelIndexCreate delta via
  // storage::ApplyWalSchemaDelta (through a UNIQUE-typed recovery-replay accessor), not
  // warn-and-skipped it.
  {
    auto acc = db.Access(memgraph::storage::READ);
    EXPECT_THAT(acc->ListAllIndices().label, ::testing::Contains(indexed_label))
        << "BUG-1: a label index created on main strictly after a branch's fork point must survive "
           "restart -- windowed WAL replay must apply schema-plane deltas via ApplyWalSchemaDelta, "
           "not silently skip them";
  }

  // (b) Both vertices (pre-fork and the one committed after the schema op) must still be present --
  // proves the schema delta didn't corrupt or truncate the surrounding data-plane replay.
  {
    auto acc = db.Access(memgraph::storage::READ);
    EXPECT_TRUE(acc->FindVertex(pre_fork_vertex, memgraph::storage::View::OLD).has_value());
    EXPECT_TRUE(acc->FindVertex(post_fork_vertex, memgraph::storage::View::OLD).has_value())
        << "windowed WAL replay must still rebuild main's data-plane history around a schema op";
  }

  // (c) HistoricalAccess(fork_ts) must still work post-restart, unaffected by the schema replay
  // that happened strictly after the fork point (the branch's own historical view predates it).
  auto historical = storage->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  EXPECT_TRUE((*historical)->FindVertex(pre_fork_vertex, memgraph::storage::View::OLD).has_value());
  EXPECT_FALSE((*historical)->FindVertex(post_fork_vertex, memgraph::storage::View::OLD).has_value())
      << "the historical accessor must not see a vertex created strictly AFTER the fork point";
  historical->reset();
}

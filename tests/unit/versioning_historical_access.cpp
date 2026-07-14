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

// Graph Versioning CHUNK 4: historical-timestamp read accessor (R16) + its R37 no-release
// invariant. InMemoryStorage::HistoricalAccess(fork_ts) opens a READ-ONLY accessor whose
// transaction's start_timestamp is an explicit PAST fork_ts (captured and retained by a live
// RegisterForkPin, chunk 2A) instead of a freshly issued tick, so MVCC time-travel
// (ApplyDeltasForRead, mvcc.hpp) reconstructs main "as of" that fork point with no change to the
// read path itself. Its finalization must never call commit_log_->MarkFinished(fork_ts) -- that
// bit belongs solely to whoever the tick is genuinely dispensed to next (see
// Transaction::is_historical_'s doc-comment in transaction.hpp for the full rationale).

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "flags/general.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "storage/v2/indices/index_order.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage_test_utils.hpp"
#include "tests/test_commit_args_helper.hpp"

namespace ms = memgraph::storage;

namespace {

// Lightweight fixture for tests that don't need delta-retention metrics (reconstruction +
// below-horizon rejection): a plain InMemoryStorage with GC disabled so nothing runs in the
// background while the test is asserting on state.
class VersioningHistoricalAccessTest : public testing::Test {
 protected:
  void SetUp() override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    storage = std::make_unique<ms::InMemoryStorage>(config);
  }

  void TearDown() override { storage.reset(); }

  std::unique_ptr<ms::Storage> storage;
};

// R16: a historical accessor opened at fork_ts must see main exactly as it was at the fork point
// -- neither later property edits nor later-created vertices -- while a normal, present-day
// accessor keeps seeing the live, current state.
TEST_F(VersioningHistoricalAccessTest, ReconstructsMainAsOfFork) {
  auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());
  const auto prop_id = storage->NameToProperty("p");

  ms::Gid v_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto v = acc->CreateVertex();
    v_gid = v.Gid();
    ASSERT_TRUE(v.SetProperty(prop_id, ms::PropertyValue(1)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const uint64_t fork_ts = mem_storage->RegisterForkPin();

  // Post-fork: bump the property AND add an unrelated new vertex. Neither must be visible
  // through the historical accessor below.
  {
    auto acc = storage->Access(ms::WRITE);
    auto v = acc->FindVertex(v_gid, ms::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->SetProperty(prop_id, ms::PropertyValue(2)).has_value());
    (void)acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto historical = mem_storage->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  {
    auto &hist_acc = **historical;
    auto v = hist_acc.FindVertex(v_gid, ms::View::OLD);
    ASSERT_TRUE(v.has_value());
    auto prop = v->GetProperty(prop_id, ms::View::OLD);
    ASSERT_TRUE(prop.has_value());
    EXPECT_EQ(*prop, ms::PropertyValue(1)) << "historical read must see main as of fork_ts, not the post-fork edit";
    EXPECT_EQ(CountVertices(hist_acc, ms::View::OLD), 1U)
        << "the post-fork vertex must not be visible through the historical accessor";
  }
  historical->reset();

  // A normal, present-day accessor must be unaffected and keep seeing the current state.
  {
    auto acc = storage->Access(ms::WRITE);
    auto v = acc->FindVertex(v_gid, ms::View::OLD);
    ASSERT_TRUE(v.has_value());
    auto prop = v->GetProperty(prop_id, ms::View::OLD);
    ASSERT_TRUE(prop.has_value());
    EXPECT_EQ(*prop, ms::PropertyValue(2));
    EXPECT_EQ(CountVertices(*acc, ms::View::OLD), 2U);
  }

  mem_storage->ReleaseForkPin(fork_ts);
}

// Defensive misuse guard: a fork_ts that was never registered via RegisterForkPin() (so nothing
// guarantees main's history back to it is still reconstructable) must be rejected cleanly, not
// crash or silently return a corrupt/incomplete view.
TEST_F(VersioningHistoricalAccessTest, RejectsUnpinnedForkTimestamp) {
  auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());

  {
    auto acc = storage->Access(ms::WRITE);
    (void)acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Never pinned via RegisterForkPin -- must be refused rather than approximated.
  constexpr uint64_t kNeverPinnedForkTs = 1;
  auto result = mem_storage->HistoricalAccess(kNeverPinnedForkTs);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), ms::InMemoryStorage::HistoricalAccessError::ForkTimestampNotPinned);

  // A pin that WAS registered and then released must also be rejected -- same class of misuse
  // (DROP BRANCH already happened; nothing protects this fork_ts anymore).
  const uint64_t fork_ts = mem_storage->RegisterForkPin();
  mem_storage->ReleaseForkPin(fork_ts);
  auto result_after_release = mem_storage->HistoricalAccess(fork_ts);
  ASSERT_FALSE(result_after_release.has_value());
  EXPECT_EQ(result_after_release.error(), ms::InMemoryStorage::HistoricalAccessError::ForkTimestampNotPinned);
}

// HIGH-2: HistoricalAccess must force SNAPSHOT_ISOLATION unconditionally, regardless of the
// database's configured ambient isolation level. Under READ_COMMITTED (and READ_UNCOMMITTED),
// ApplyDeltasForRead's visibility rule is "any committed change, regardless of timestamp" (or
// "any change at all"), NOT "changes committed before my start_timestamp" -- so a historical
// transaction that inherited the ambient level would silently see the post-fork commit instead of
// the fork_ts snapshot. This test fails if CreateHistoricalTransaction ever reads
// `isolation_level_` (or accepts/honors an override) instead of hardcoding SNAPSHOT_ISOLATION.
TEST(VersioningHistoricalAccessIsolation, ForcesSnapshotIsolationRegardlessOfDbLevel) {
  for (auto ambient_level : {ms::IsolationLevel::READ_COMMITTED, ms::IsolationLevel::READ_UNCOMMITTED}) {
    SCOPED_TRACE(testing::Message() << "ambient isolation level = " << static_cast<int>(ambient_level));

    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    config.transaction.isolation_level = ambient_level;
    auto storage = std::make_unique<ms::InMemoryStorage>(config);
    auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());
    const auto prop_id = storage->NameToProperty("p");

    ms::Gid v_gid;
    {
      auto acc = storage->Access(ms::WRITE);
      auto v = acc->CreateVertex();
      v_gid = v.Gid();
      ASSERT_TRUE(v.SetProperty(prop_id, ms::PropertyValue(1)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    const uint64_t fork_ts = mem_storage->RegisterForkPin();

    // Post-fork commit: under the ambient READ_COMMITTED/READ_UNCOMMITTED level this would be
    // visible immediately to any OTHER accessor using that ambient level, but must never leak
    // through the historical accessor below.
    {
      auto acc = storage->Access(ms::WRITE);
      auto v = acc->FindVertex(v_gid, ms::View::OLD);
      ASSERT_TRUE(v.has_value());
      ASSERT_TRUE(v->SetProperty(prop_id, ms::PropertyValue(2)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    auto historical = mem_storage->HistoricalAccess(fork_ts);
    ASSERT_TRUE(historical.has_value());
    auto v = (*historical)->FindVertex(v_gid, ms::View::OLD);
    ASSERT_TRUE(v.has_value());
    auto prop = v->GetProperty(prop_id, ms::View::OLD);
    ASSERT_TRUE(prop.has_value());
    EXPECT_EQ(*prop, ms::PropertyValue(1))
        << "HistoricalAccess must see only the fork_ts snapshot even though the database's ambient "
           "isolation level would otherwise make the post-fork commit immediately visible";

    historical->reset();
    mem_storage->ReleaseForkPin(fork_ts);
  }
}

// Metrics-backed fixture (mirrors StorageV2GcMetricsTest in storage_v2_gc.cpp): needed for the
// R37 regression test below, which distinguishes "correctly retained" from "wrongly reclaimed"
// via the unreleased_delta_objects gauge -- the same ground truth chunk 2A's own fork-pin tests
// use, since GC only actually unlinks/frees deltas via this exact bookkeeping.
class VersioningHistoricalAccessGcTest : public testing::Test {
 protected:
  void SetUp() override {
    FLAGS_metrics_format = "OpenMetrics";
    db_name_ = testing::UnitTest::GetInstance()->current_test_info()->name();
    memgraph::storage::Config config;
    config.salient.name = db_name_;
    // Long interval: periodic GC never fires on its own during the test: retention is asserted
    // only around our own explicit storage->FreeMemory() calls, deterministically.
    config.gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)};
    uuid_ = memgraph::utils::UUID{};
    handles_ = memgraph::metrics::Metrics().AddDatabase(uuid_, db_name_);
    storage = std::make_unique<memgraph::storage::InMemoryStorage>(
        config, std::nullopt, std::make_unique<memgraph::storage::PlanInvalidatorDefault>(), handles_);
  }

  void TearDown() override {
    storage.reset();
    memgraph::metrics::Metrics().RemoveDatabase(uuid_);
    handles_ = {};
    uuid_ = {};
  }

  std::unique_ptr<memgraph::storage::Storage> storage;
  memgraph::metrics::DatabaseMetricHandles handles_{};
  memgraph::utils::UUID uuid_{};

 private:
  std::string db_name_;
};

// R37 regression: repeatedly opening (and letting destruct) transient HistoricalAccess readers
// at a fixed fork_ts must NOT release/finalize that shared fork_ts in the commit log -- only
// ReleaseForkPin (DROP BRANCH) may do that.
//
// To make this observable through the public API (commit_log_ itself is private, with no test
// hook), we exploit the same deterministic tick coincidence chunk 2A's own
// ForkPin_BlocksFastDiscardAtEqualTimestampBoundary test relies on: RegisterForkPin() peeks
// `timestamp_` WITHOUT incrementing it, so the very next real transaction created afterwards is
// deterministically assigned start_timestamp == fork_ts. We keep that real transaction
// (`real_reader`) open across the whole test: it is a completely ordinary, unrelated transaction
// that merely happens to share fork_ts's tick, and its own MVCC snapshot correctness must be
// preserved on its own merits, independent of the fork pin.
//
// If a historical accessor's Abort() ever called commit_log_->MarkFinished(fork_ts) (the R37
// bug), it would falsely mark real_reader's own still-live start_timestamp as "finished" in the
// commit log. Once the fork pin is released (simulating DROP BRANCH), that corruption is no
// longer masked by the pin's independent floor, and GC would incorrectly conclude nothing needs
// history back to fork_ts anymore -- reclaiming deltas real_reader still needs -- even though
// real_reader is demonstrably still alive and unrelated to the branch's lifecycle.
TEST_F(VersioningHistoricalAccessGcTest, TransientReadsDoNotReleaseSharedForkTimestamp) {
  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(storage.get());

  memgraph::storage::Gid v_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    v_gid = v.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const uint64_t fork_ts = mem_storage->RegisterForkPin();

  // Deterministic (single-threaded, nothing else touches `timestamp_` in between): real_reader's
  // start_timestamp == fork_ts. Uses READ (an ordinary concurrent reader) -- HistoricalAccess
  // itself also uses a plain SHARED/READ-type main_lock_ guard (not READ_ONLY, see
  // Accessor(HistoricalAccess, ...) in storage.cpp), so this coexists freely either way; READ is
  // simply the natural choice to model "some unrelated ordinary reader happens to share this
  // tick".
  auto real_reader = storage->Access(memgraph::storage::READ);

  // Repeatedly open transient historical accessors at fork_ts and let them destruct -- this is
  // exactly the R37 guard's load-bearing path (InMemoryAccessor::Abort()).
  for (int i = 0; i < 5; ++i) {
    auto historical = mem_storage->HistoricalAccess(fork_ts);
    ASSERT_TRUE(historical.has_value());
    auto v = (*historical)->FindVertex(v_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
    // `historical` destructs at the end of this scope -> InMemoryAccessor::Abort().
  }

  // Build up post-fork delta history that real_reader (start_timestamp == fork_ts) must be able
  // to undo through to reconstruct its own snapshot. real_reader being alive and older than these
  // commits means they cannot take the fast-discard shortcut, so this exercises CollectGarbage's
  // normal committed_transactions_ unlinking path.
  for (int i = 0; i < 5; ++i) {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v = acc->FindVertex(v_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->AddLabel(acc->NameToLabel("L" + std::to_string(i))).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ASSERT_GT(handles_.unreleased_delta_objects.Value(), 0);

  // Release the fork pin (simulating DROP BRANCH). From here on, only commit_log_'s own state --
  // corrupted or not -- gates retention; the pin can no longer mask an R37 regression.
  mem_storage->ReleaseForkPin(fork_ts);

  // Force GC repeatedly. Use the no-arg convenience overload (a deferred, non-owning lock): it
  // takes main_lock_ SHARED internally, which is compatible with real_reader's own shared hold,
  // so this does not deadlock against the still-open accessor (unlike the exclusive-lock variant
  // used elsewhere in this suite once no other accessor remains).
  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }

  // The historical reads must NOT have finalized fork_ts: real_reader is a completely ordinary,
  // still-open transaction with that exact start_timestamp, so its needed history must still be
  // retained. If the R37 guard were missing, this would read 0 (wrongly reclaimed).
  EXPECT_GT(handles_.unreleased_delta_objects.Value(), 0)
      << "real_reader (start_timestamp == fork_ts, still open) needs this history regardless of "
         "the fork pin; the transient historical reads above must not have finalized it";

  // Directly exercise the concrete, "does a subsequent time-travel read still work" signal too:
  // real_reader's snapshot is exactly at fork_ts, i.e. before any of the 5 label-adding commits
  // above, so it must still see the vertex with NONE of those labels. If GC had wrongly unlinked
  // the undo-deltas (the R37 regression), this would silently observe some/all of "L0".."L4"
  // instead -- wrong results rather than a crash.
  {
    auto v = real_reader->FindVertex(v_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
    for (int i = 0; i < 5; ++i) {
      EXPECT_FALSE(
          v->HasLabel(real_reader->NameToLabel("L" + std::to_string(i)), memgraph::storage::View::OLD).value_or(true))
          << "real_reader's fork_ts snapshot must not see post-fork label L" << i;
    }
  }

  // Closing the loop: once real_reader itself finishes, its own (correct, un-tampered-with)
  // finalization releases the timestamp, and GC is now free to reclaim everything.
  real_reader.reset();
  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }
  EXPECT_EQ(0, handles_.unreleased_delta_objects.Value());
}

// HIGH-1 (UAF) self-pin bookkeeping, tested directly against AddForkPinAt/ReleaseForkPin --
// independent of HistoricalAccess's own accessor-level locking. A lower-level, deterministic
// complement to DropDuringOpenReadDoesNotReclaim below (which exercises the same invariant
// end-to-end through the real API, concurrently). AddForkPinAt is the exact primitive
// HistoricalAccess uses to self-pin; this test simulates "a reader has self-pinned fork_ts"
// directly and verifies retention survives the BRANCH's own pin being released, and stops
// surviving once the reader's own pin is ALSO released. Fails without the extra self-pin:
// retention would already read 0 right after the first ReleaseForkPin below.
TEST_F(VersioningHistoricalAccessGcTest, SelfPinKeepsHistoryAfterBranchPinReleased) {
  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(storage.get());

  memgraph::storage::Gid v_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    v_gid = v.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const uint64_t fork_ts = mem_storage->RegisterForkPin();  // the branch's own pin

  // A reader "opens" at fork_ts and self-pins -- exactly what HistoricalAccess does internally
  // (under version_fork_pin_lock_, atomically with its pinned-check; here we just exercise the
  // insert directly since there's no live accessor in this test).
  mem_storage->AddForkPinAt(fork_ts);

  for (int i = 0; i < 5; ++i) {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v = acc->FindVertex(v_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->AddLabel(acc->NameToLabel("L" + std::to_string(i))).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ASSERT_GT(handles_.unreleased_delta_objects.Value(), 0);

  // Simulate DROP BRANCH: releases the branch's OWN pin. version_fork_pins_ still holds ONE entry
  // for fork_ts (the reader's self-pin), so GC's floor must not advance past it.
  mem_storage->ReleaseForkPin(fork_ts);
  for (int i = 0; i < 4; ++i) {
    auto main_guard = std::unique_lock{storage->main_lock_};
    storage->FreeMemory(std::move(main_guard), false);
  }
  EXPECT_GT(handles_.unreleased_delta_objects.Value(), 0)
      << "the reader's self-pin (AddForkPinAt) must keep this history alive even after the "
         "branch's own pin is released (HIGH-1)";

  // Reader "closes": releases its own self-pin (mirrors ReleaseForkPin(transaction_.start_timestamp)
  // in InMemoryAccessor's is_historical_ finalize paths). GC is now free to reclaim.
  mem_storage->ReleaseForkPin(fork_ts);
  for (int i = 0; i < 4; ++i) {
    auto main_guard = std::unique_lock{storage->main_lock_};
    storage->FreeMemory(std::move(main_guard), false);
  }
  EXPECT_EQ(0, handles_.unreleased_delta_objects.Value());
}

// HIGH-1 (UAF) integration regression through the REAL HistoricalAccess API, with GC running
// GENUINELY CONCURRENTLY: open a reader at fork_ts, simulate DROP BRANCH (ReleaseForkPin) WHILE
// it is still open, hammer GC from a background thread while repeatedly reading through the
// still-open reader, then close it and confirm eventual full reclaim.
//
// This now exercises the real race HIGH-1 defends against. HistoricalAccess uses a plain
// SHARED/READ-type main_lock_ guard (not READ_ONLY -- see Accessor(HistoricalAccess, ...) in
// storage.cpp), which coexists with GC's own lock acquisition (CollectGarbage's
// `main_lock_.lock_shared()` defaults to the WRITE LockReq, whose condition only checks
// `ro_count`/`ro_pending_count`, never `r_count` -- so it is NOT gated by our READ-type hold).
// GC can therefore genuinely run while `historical` stays open, so the self-pin -- not any
// incidental lock exclusivity -- is what has to hold the floor at fork_ts here.
//
// Why this fails without HIGH-1 (reasoned, not just asserted): once the 5 post-fork commits
// below run, the FIRST of them is deterministically assigned start_timestamp == fork_ts (the same
// RegisterForkPin-peeks-without-incrementing coincidence used elsewhere in this file), and that
// ordinary transaction legitimately finishes and calls commit_log_->MarkFinished(fork_ts) on its
// own account shortly after. So by the time we release the branch's pin below,
// commit_log_->OldestActive() has ALREADY raced past fork_ts on its own, for reasons entirely
// unrelated to HistoricalAccess -- confirming that the explicit version_fork_pins_ multiset (not
// commit_log_) is the ONLY thing that can still protect this history. Without the self-pin, that
// multiset would go empty the moment DROP BRANCH releases the branch's own pin, and the
// concurrent GC below would be free to unlink the label-adding deltas `historical` still needs to
// undo through -- surfacing as wrong (or crashing) reads from the assertions in the loop.
TEST_F(VersioningHistoricalAccessGcTest, DropDuringOpenReadDoesNotReclaim) {
  auto *mem_storage = static_cast<memgraph::storage::InMemoryStorage *>(storage.get());

  memgraph::storage::Gid v_gid;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    v_gid = v.Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const uint64_t fork_ts = mem_storage->RegisterForkPin();

  for (int i = 0; i < 5; ++i) {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v = acc->FindVertex(v_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->AddLabel(acc->NameToLabel("L" + std::to_string(i))).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ASSERT_GT(handles_.unreleased_delta_objects.Value(), 0);

  // Open the reader: SHARED/READ-type guard + HIGH-1 self-pin taken internally.
  auto historical = mem_storage->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  {
    auto v = (*historical)->FindVertex(v_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
  }

  // Simulate DROP BRANCH while the reader is still open: releases the branch's OWN pin. From
  // here on, ONLY the reader's self-pin (HIGH-1) can be protecting this history (see the
  // doc-comment above for why commit_log_ itself can no longer be relied on for this).
  mem_storage->ReleaseForkPin(fork_ts);

  // Hammer GC from a background thread while the reader stays open -- genuinely concurrent now.
  std::atomic<bool> keep_running{true};
  std::thread gc_thread([&] {
    while (keep_running.load(std::memory_order_acquire)) {
      storage->FreeMemory();
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    storage->FreeMemory();
  });

  // While GC is concurrently hammering and the branch pin is already gone, the reader must
  // consistently see the correct fork-state -- proving the self-pin (not the branch's pin, which
  // is already released, nor commit_log_, which has already raced past fork_ts) is what's
  // protecting it.
  for (int iter = 0; iter < 100; ++iter) {
    auto v = (*historical)->FindVertex(v_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
    for (int i = 0; i < 5; ++i) {
      ASSERT_FALSE(
          v->HasLabel((*historical)->NameToLabel("L" + std::to_string(i)), memgraph::storage::View::OLD).value_or(true))
          << "iteration " << iter << ": reader's fork_ts snapshot must not see post-fork label L" << i;
    }
    std::this_thread::yield();
  }

  keep_running.store(false, std::memory_order_release);
  gc_thread.join();

  // The reader is STILL open at this point and STILL read correctly throughout -- the delta
  // history must therefore still be present, not reclaimed, despite the concurrent GC hammering
  // above and the branch's own pin already being gone.
  EXPECT_GT(handles_.unreleased_delta_objects.Value(), 0)
      << "the reader's self-pin must have kept this history alive through concurrent GC, even "
         "though the branch's own pin was already released";

  // Close the reader: releases the self-pin exactly once (no crash -- see the
  // is_transaction_active_ double-release fix in InMemoryAccessor::PrepareForCommitPhase / Abort).
  // GC can now fully reclaim.
  historical->reset();
  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }
  EXPECT_EQ(0, handles_.unreleased_delta_objects.Value());
}

// R1: a HistoricalAccess(fork_ts) accessor must be able to drive a label-property INDEX SCAN
// (Storage::Accessor::Vertices(label, properties, property_ranges, view, order) -- the same entry
// point ScanAllByLabelProperties uses) and get back fork-state, property-ordered results. This is a
// NEW, previously-untested code path: no existing caller drives an index scan through
// HistoricalAccess. Two failure modes are distinguished by the assertions below:
//   (a) fork-state leak -- a post-fork write/create/delete becomes visible through the scan, or
//   (b) ordering break -- the index returns vertices out of property order (e.g. falls back to
//       gid/creation order), which would silently defeat any ORDER BY-elision plan that relies on
//       index order.
//
// API shapes below are copied, not guessed, from tests/unit/storage_v2_indices.cpp:
//   - CreateIndex(label, PropertiesPaths, IndexOrder) is called through storage->ReadOnlyAccess()
//     (see IndexTest::CreateIndexAccessor, storage_v2_indices.cpp:121-128 -- a metadata-only DDL
//     access mode for InMemoryStorage) and committed exactly like a normal write.
//   - The index-scan overload of Vertices() and its std::array<PropertyPath>/PropertyValueRange
//     argument shapes are copied from IndexTest.LabelPropertyIndexBasic
//     (storage_v2_indices.cpp:911-946) and IndexTest.LabelPropertyDescIndexRangeBoundsVariants
//     (storage_v2_indices.cpp:2321-2347), which is also where the DESC-index requirement below
//     (see TEST 2) comes from.
TEST_F(VersioningHistoricalAccessTest, HistoricalLabelPropertyIndexScanReturnsForkStateOrderedAsc) {
  auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());
  const auto label = storage->NameToLabel("Person");
  const auto age_prop = storage->NameToProperty("age");

  // Label-property index on :Person(age), ASC (the default order).
  {
    auto index_acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(index_acc->CreateIndex(label, {ms::PropertyPath{age_prop}}, ms::IndexOrder::ASC).has_value());
    ASSERT_TRUE(index_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Pre-fork: 5 Person vertices, created in an order where gid-ascending != age-ascending -- so a
  // correct-looking result that actually came back in creation/gid order would still be caught.
  ms::Gid v20_gid;
  ms::Gid v40_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    for (int64_t age : {50, 20, 40, 10, 30}) {
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(label).has_value());
      ASSERT_TRUE(v.SetProperty(age_prop, ms::PropertyValue(age)).has_value());
      if (age == 20) v20_gid = v.Gid();
      if (age == 40) v40_gid = v.Gid();
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const uint64_t fork_ts = mem_storage->RegisterForkPin();

  // Post-fork committed mutations on MAIN: an in-place property edit, a new vertex, and a delete of
  // an existing (pre-fork) vertex. None of this may leak into the fork_ts index scan below; the
  // deleted vertex specifically MUST still appear, since it existed at fork_ts.
  {
    auto acc = storage->Access(ms::WRITE);

    auto v20 = acc->FindVertex(v20_gid, ms::View::OLD);
    ASSERT_TRUE(v20.has_value());
    ASSERT_TRUE(v20->SetProperty(age_prop, ms::PropertyValue(999)).has_value());

    auto v5 = acc->CreateVertex();
    ASSERT_TRUE(v5.AddLabel(label).has_value());
    ASSERT_TRUE(v5.SetProperty(age_prop, ms::PropertyValue(5)).has_value());

    auto v40 = acc->FindVertex(v40_gid, ms::View::OLD);
    ASSERT_TRUE(v40.has_value());
    ASSERT_TRUE(acc->DetachDeleteVertex(&*v40).has_value());

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto historical = mem_storage->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  {
    auto &hist_acc = **historical;

    std::vector<int64_t> ages;
    for (auto v : hist_acc.Vertices(label,
                                    std::array{ms::PropertyPath{age_prop}},
                                    std::array{ms::PropertyValueRange::IsNotNull()},
                                    ms::View::OLD,
                                    ms::IndexOrder::ASC)) {
      auto prop = v.GetProperty(age_prop, ms::View::OLD);
      ASSERT_TRUE(prop.has_value());
      ages.push_back(prop->ValueInt());
    }

    EXPECT_THAT(ages, ::testing::ElementsAre(10, 20, 30, 40, 50))
        << "expected exactly the pre-fork ages {10,20,30,40,50} in ascending order; a mismatched "
           "SET (age=20 must read 20, not the post-fork 999), an extra 5 (the post-fork-created "
           "vertex must not appear), a missing 40 (the post-fork-deleted vertex must still appear, "
           "since it existed at fork_ts), or any out-of-order sequence (the scan must be "
           "property-ordered by the index, not gid/creation order) all indicate a real failure of "
           "index-time-travel through HistoricalAccess";
  }
  historical->reset();
  mem_storage->ReleaseForkPin(fork_ts);
}

// Same claim (R1) as above, but exercising IndexOrder::DESC. Per
// IndexTest.LabelPropertyDescIndexRangeBoundsVariants (storage_v2_indices.cpp:2316-2411,
// specifically the `GTEST_SKIP` for DiskStorage and the fact that every DESC scan there is preceded
// by a CreateIndex(..., IndexOrder::DESC) call), DESC-ordered scanning is NOT a runtime option on
// top of an ASC index -- it requires a separately-created DESC label-property index. This test
// therefore creates its own DESC index (fresh storage instance via the fixture's per-test SetUp,
// so there is no conflict with the ASC index created in the test above).
TEST_F(VersioningHistoricalAccessTest, HistoricalLabelPropertyIndexScanRespectsDescOrder) {
  auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());
  const auto label = storage->NameToLabel("Person");
  const auto age_prop = storage->NameToProperty("age");

  // Label-property index on :Person(age), DESC -- a distinct index from the ASC variant above.
  {
    auto index_acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(index_acc->CreateIndex(label, {ms::PropertyPath{age_prop}}, ms::IndexOrder::DESC).has_value());
    ASSERT_TRUE(index_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  ms::Gid v20_gid;
  ms::Gid v40_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    for (int64_t age : {50, 20, 40, 10, 30}) {
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(label).has_value());
      ASSERT_TRUE(v.SetProperty(age_prop, ms::PropertyValue(age)).has_value());
      if (age == 20) v20_gid = v.Gid();
      if (age == 40) v40_gid = v.Gid();
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const uint64_t fork_ts = mem_storage->RegisterForkPin();

  {
    auto acc = storage->Access(ms::WRITE);

    auto v20 = acc->FindVertex(v20_gid, ms::View::OLD);
    ASSERT_TRUE(v20.has_value());
    ASSERT_TRUE(v20->SetProperty(age_prop, ms::PropertyValue(999)).has_value());

    auto v5 = acc->CreateVertex();
    ASSERT_TRUE(v5.AddLabel(label).has_value());
    ASSERT_TRUE(v5.SetProperty(age_prop, ms::PropertyValue(5)).has_value());

    auto v40 = acc->FindVertex(v40_gid, ms::View::OLD);
    ASSERT_TRUE(v40.has_value());
    ASSERT_TRUE(acc->DetachDeleteVertex(&*v40).has_value());

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto historical = mem_storage->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  {
    auto &hist_acc = **historical;

    std::vector<int64_t> ages;
    for (auto v : hist_acc.Vertices(label,
                                    std::array{ms::PropertyPath{age_prop}},
                                    std::array{ms::PropertyValueRange::IsNotNull()},
                                    ms::View::OLD,
                                    ms::IndexOrder::DESC)) {
      auto prop = v.GetProperty(age_prop, ms::View::OLD);
      ASSERT_TRUE(prop.has_value());
      ages.push_back(prop->ValueInt());
    }

    EXPECT_THAT(ages, ::testing::ElementsAre(50, 40, 30, 20, 10))
        << "expected exactly the pre-fork ages {50,40,30,20,10} in descending order through a "
           "DESC-created index; see the ASC test above for what a fork-state leak vs. an "
           "ordering break each look like here";
  }
  historical->reset();
  mem_storage->ReleaseForkPin(fork_ts);
}

}  // namespace

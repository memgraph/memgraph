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

// InMemoryStorage::HistoricalAccess(fork_ts) opens a read-only accessor pinned at a past, retained
// fork_ts so MVCC time-travel reconstructs main as of that point. These tests cover reconstruction,
// pin lifetime/GC retention, and index scans through such an accessor.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "flags/general.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "storage/v2/indices/index_order.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage_test_utils.hpp"
#include "storage_v2_gc_metrics_fixture.hpp"
#include "tests/test_commit_args_helper.hpp"

namespace ms = memgraph::storage;

namespace {

// GC disabled so nothing runs in the background while the test asserts on state.
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

// A historical accessor sees main exactly as of fork_ts (no later edits, no later vertices) while a
// present-day accessor keeps seeing live state. HistoricalAccess must force SNAPSHOT_ISOLATION:
// under READ_COMMITTED/READ_UNCOMMITTED an inherited ambient level would expose the post-fork commit.
TEST(VersioningHistoricalAccess, ReconstructsMainAsOfForkUnderAnyAmbientIsolation) {
  for (auto ambient_level : {ms::IsolationLevel::SNAPSHOT_ISOLATION,
                             ms::IsolationLevel::READ_COMMITTED,
                             ms::IsolationLevel::READ_UNCOMMITTED}) {
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

    // Post-fork: bump the property and add an unrelated vertex; neither may be visible historically.
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
}

// A fork_ts that was never pinned, or whose pin was already released, must be rejected cleanly.
TEST_F(VersioningHistoricalAccessTest, RejectsUnpinnedForkTimestamp) {
  auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());

  {
    auto acc = storage->Access(ms::WRITE);
    (void)acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // A live pin on an unrelated timestamp must not make other timestamps look pinned.
  const uint64_t unrelated_pin = mem_storage->RegisterForkPin();

  constexpr uint64_t kNeverPinnedForkTs = 1;
  ASSERT_NE(unrelated_pin, kNeverPinnedForkTs);
  auto result = mem_storage->HistoricalAccess(kNeverPinnedForkTs);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), ms::InMemoryStorage::HistoricalAccessError::ForkTimestampNotPinned);

  // Tick the clock so the next pin lands on a different timestamp than unrelated_pin.
  {
    auto acc = storage->Access(ms::WRITE);
    (void)acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Registered and then released (DROP BRANCH) must be rejected too, even with another pin alive.
  const uint64_t fork_ts = mem_storage->RegisterForkPin();
  ASSERT_NE(fork_ts, unrelated_pin);
  mem_storage->ReleaseForkPin(fork_ts);
  auto result_after_release = mem_storage->HistoricalAccess(fork_ts);
  ASSERT_FALSE(result_after_release.has_value());
  EXPECT_EQ(result_after_release.error(), ms::InMemoryStorage::HistoricalAccessError::ForkTimestampNotPinned);

  mem_storage->ReleaseForkPin(unrelated_pin);
}

// Metrics-backed fixture: unreleased_delta_objects distinguishes "retained" from "wrongly reclaimed".
class VersioningHistoricalAccessGcTest : public testing::Test {
 protected:
  void SetUp() override {
    FLAGS_metrics_format = "OpenMetrics";
    db_name_ = testing::UnitTest::GetInstance()->current_test_info()->name();
    memgraph::storage::Config config;
    config.salient.name = db_name_;
    // Long interval: GC only runs via our explicit storage->FreeMemory() calls.
    config.gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(3600)};
    uuid_ = memgraph::utils::UUID{};
    registration_ = memgraph::metrics::Metrics().AddDatabase(uuid_, db_name_);
    storage = std::make_unique<memgraph::storage::InMemoryStorage>(
        config, std::nullopt, std::make_unique<memgraph::storage::PlanInvalidatorDefault>(), registration_.handles());
  }

  void TearDown() override {
    storage.reset();
    registration_ = {};
    uuid_ = {};
  }

  std::unique_ptr<memgraph::storage::Storage> storage;
  memgraph::metrics::PrometheusMetrics::Registration registration_{};
  memgraph::utils::UUID uuid_{};

 private:
  std::string db_name_;
};

// Transient HistoricalAccess readers must not finalize the shared fork_ts in the commit log; only
// ReleaseForkPin may. RegisterForkPin peeks `timestamp_` without incrementing it, so the next real
// transaction (`real_reader`) deterministically gets start_timestamp == fork_ts. If a historical
// Abort() wrongly marked fork_ts finished, GC would reclaim deltas real_reader still needs once the
// branch pin is released.
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

  auto real_reader = storage->Access(memgraph::storage::READ);
  ASSERT_EQ(real_reader->GetStartTimestamp(), std::optional<uint64_t>{fork_ts})
      << "test precondition: the plain reader must start exactly at fork_ts";

  for (int i = 0; i < 5; ++i) {
    auto historical = mem_storage->HistoricalAccess(fork_ts);
    ASSERT_TRUE(historical.has_value());
    auto v = (*historical)->FindVertex(v_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
  }

  // Post-fork commits real_reader must undo through; it is older, so no fast-discard shortcut.
  for (int i = 0; i < 5; ++i) {
    auto acc = storage->Access(memgraph::storage::WRITE);
    auto v = acc->FindVertex(v_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->AddLabel(acc->NameToLabel("L" + std::to_string(i))).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ASSERT_GT(registration_.handles().unreleased_delta_objects.Value(), 0);

  // From here only commit_log_ state gates retention; the pin can no longer mask the regression.
  mem_storage->ReleaseForkPin(fork_ts);

  // No-arg FreeMemory takes main_lock_ shared, compatible with real_reader's hold.
  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }

  EXPECT_GT(registration_.handles().unreleased_delta_objects.Value(), 0)
      << "real_reader (start_timestamp == fork_ts, still open) needs this history regardless of "
         "the fork pin; the transient historical reads above must not have finalized it";

  // Wrongly unlinked deltas would show up as wrong results (visible L0..L4), not a crash.
  {
    auto v = real_reader->FindVertex(v_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
    for (int i = 0; i < 5; ++i) {
      EXPECT_FALSE(
          v->HasLabel(real_reader->NameToLabel("L" + std::to_string(i)), memgraph::storage::View::OLD).value_or(true))
          << "real_reader's fork_ts snapshot must not see post-fork label L" << i;
    }
  }

  real_reader.reset();
  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }
  EXPECT_EQ(0, registration_.handles().unreleased_delta_objects.Value());
}

// Dropping the branch (ReleaseForkPin) while a historical reader is open must not let GC reclaim
// the history it needs: the reader's self-pin holds the floor. GC runs on the test thread; it takes
// main_lock_ shared, which coexists with the reader's READ-type hold.
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
  ASSERT_GT(registration_.handles().unreleased_delta_objects.Value(), 0);

  auto historical = mem_storage->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  {
    auto v = (*historical)->FindVertex(v_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
  }

  // Simulate DROP BRANCH: only the reader's self-pin can now protect this history (commit_log_
  // has already advanced past fork_ts because the first post-fork commit shared that tick).
  mem_storage->ReleaseForkPin(fork_ts);
  EXPECT_EQ(mem_storage->OldestForkPin(), std::optional<uint64_t>{fork_ts}) << "reader must self-pin fork_ts";

  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }

  EXPECT_GT(registration_.handles().unreleased_delta_objects.Value(), 0)
      << "the reader's self-pin must keep the history alive after the branch pin is released";
  {
    auto v = (*historical)->FindVertex(v_gid, memgraph::storage::View::OLD);
    ASSERT_TRUE(v.has_value());
    for (int i = 0; i < 5; ++i) {
      ASSERT_FALSE(
          v->HasLabel((*historical)->NameToLabel("L" + std::to_string(i)), memgraph::storage::View::OLD).value_or(true))
          << "reader's fork_ts snapshot must not see post-fork label L" << i;
    }
  }
  EXPECT_EQ(mem_storage->OldestForkPin(), std::optional<uint64_t>{fork_ts});

  // Closing releases the self-pin exactly once; GC can then reclaim everything.
  historical->reset();
  EXPECT_EQ(mem_storage->OldestForkPin(), std::nullopt);
  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }
  EXPECT_EQ(0, registration_.handles().unreleased_delta_objects.Value());

  // Defensive path: Commit() on a historical reader takes the empty-delta branch, releases the
  // self-pin and terminates the transaction, so the later destruction must not release it twice.
  const uint64_t fork_ts2 = mem_storage->RegisterForkPin();
  {
    auto reader = mem_storage->HistoricalAccess(fork_ts2);
    ASSERT_TRUE(reader.has_value());
    ASSERT_TRUE((*reader)->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    reader->reset();
  }
  EXPECT_EQ(mem_storage->OldestForkPin(), std::optional<uint64_t>{fork_ts2}) << "only the branch pin may remain";
  mem_storage->ReleaseForkPin(fork_ts2);
  EXPECT_EQ(mem_storage->OldestForkPin(), std::nullopt);
}

// A writer opened BEFORE the fork pin but committed AFTER it has commit_ts >= fork_ts, so the fork
// snapshot must exclude its writes, and GC/fast-discard must keep the pre-image needed to hide them.
TEST_F(VersioningHistoricalAccessGcTest, HistoricalReadExcludesWriterThatStraddlesTheFork) {
  namespace st = memgraph::storage;
  auto *mem_storage = static_cast<st::InMemoryStorage *>(storage.get());
  const auto prop_id = storage->NameToProperty("p");

  st::Gid v_gid;
  {
    auto acc = storage->Access(st::WRITE);
    auto v = acc->CreateVertex();
    v_gid = v.Gid();
    ASSERT_TRUE(v.SetProperty(prop_id, st::PropertyValue(1)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto writer = storage->Access(st::WRITE);
  {
    auto v = writer->FindVertex(v_gid, st::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->SetProperty(prop_id, st::PropertyValue(2)).has_value());
    (void)writer->CreateVertex();
  }

  const uint64_t fork_ts = mem_storage->RegisterForkPin();

  ASSERT_TRUE(writer->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  writer.reset();

  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }

  auto historical = mem_storage->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  {
    auto &hist_acc = **historical;
    auto v = hist_acc.FindVertex(v_gid, st::View::OLD);
    ASSERT_TRUE(v.has_value());
    auto prop = v->GetProperty(prop_id, st::View::OLD);
    ASSERT_TRUE(prop.has_value());
    EXPECT_EQ(*prop, st::PropertyValue(1)) << "a writer committing at/after fork_ts must be invisible";
    EXPECT_EQ(CountVertices(hist_acc, st::View::OLD), 1U) << "the straddling writer's vertex must be invisible";
  }
  historical->reset();
  mem_storage->ReleaseForkPin(fork_ts);
}

// Two pins at different timestamps: the GC floor must be the OLDEST pin (a newest-pin floor would unlink A's history). After releasing the older
// pin, the newer pin's history stays reconstructible and the older timestamp is rejected.
TEST_F(VersioningHistoricalAccessGcTest, GcFloorIsOldestPinAndAdvancesWhenOlderPinReleased) {
  namespace st = memgraph::storage;
  auto *mem_storage = static_cast<st::InMemoryStorage *>(storage.get());
  const auto prop_id = storage->NameToProperty("p");

  auto set_prop = [&](st::Gid gid, int64_t value) {
    auto acc = storage->Access(st::WRITE);
    auto v = acc->FindVertex(gid, st::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->SetProperty(prop_id, st::PropertyValue(value)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };
  auto read_prop_at = [&](uint64_t fork_ts, st::Gid gid) -> std::optional<int64_t> {
    auto historical = mem_storage->HistoricalAccess(fork_ts);
    if (!historical.has_value()) return std::nullopt;
    auto v = (*historical)->FindVertex(gid, st::View::OLD);
    if (!v.has_value()) return std::nullopt;
    auto prop = v->GetProperty(prop_id, st::View::OLD);
    if (!prop.has_value()) return std::nullopt;
    return prop->ValueInt();
  };

  st::Gid v_gid;
  {
    auto acc = storage->Access(st::WRITE);
    auto v = acc->CreateVertex();
    v_gid = v.Gid();
    ASSERT_TRUE(v.SetProperty(prop_id, st::PropertyValue(1)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const uint64_t pin_a = mem_storage->RegisterForkPin();
  set_prop(v_gid, 2);
  const uint64_t pin_b = mem_storage->RegisterForkPin();
  ASSERT_LT(pin_a, pin_b);
  set_prop(v_gid, 3);

  EXPECT_EQ(mem_storage->OldestForkPin(), std::optional<uint64_t>{pin_a});
  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }
  EXPECT_EQ(read_prop_at(pin_a, v_gid), std::optional<int64_t>{1});
  EXPECT_EQ(read_prop_at(pin_b, v_gid), std::optional<int64_t>{2});

  mem_storage->ReleaseForkPin(pin_a);
  EXPECT_EQ(mem_storage->OldestForkPin(), std::optional<uint64_t>{pin_b});
  EXPECT_EQ(mem_storage->HistoricalAccess(pin_a).error_or(ms::InMemoryStorage::HistoricalAccessError{}),
            ms::InMemoryStorage::HistoricalAccessError::ForkTimestampNotPinned);
  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }

  EXPECT_EQ(read_prop_at(pin_b, v_gid), std::optional<int64_t>{2}) << "history needed for pin B must be retained";
  EXPECT_GT(registration_.handles().unreleased_delta_objects.Value(), 0);

  mem_storage->ReleaseForkPin(pin_b);
  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }
  EXPECT_EQ(0, registration_.handles().unreleased_delta_objects.Value());
}

// Label-property index scan through a historical accessor returns fork-state vertices in index
// order. Pre-fork ages are created out of order (gid order != age order); post-fork edit (20->999),
// create (5) and delete (40) must not leak, and the deleted vertex must still appear. DESC scanning
// needs a separately created DESC index, hence a fresh storage per order.
TEST(VersioningHistoricalAccess, HistoricalLabelPropertyIndexScanReturnsForkStateInIndexOrder) {
  struct Case {
    ms::IndexOrder order;
    std::vector<int64_t> expected;
  };
  const Case cases[] = {{ms::IndexOrder::ASC, {10, 20, 30, 40, 50}}, {ms::IndexOrder::DESC, {50, 40, 30, 20, 10}}};

  for (const auto &[order, expected] : cases) {
    SCOPED_TRACE(order == ms::IndexOrder::ASC ? "ASC" : "DESC");

    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    auto storage = std::make_unique<ms::InMemoryStorage>(config);
    auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());
    const auto label = storage->NameToLabel("Person");
    const auto age_prop = storage->NameToProperty("age");

    {
      auto index_acc = storage->ReadOnlyAccess();
      ASSERT_TRUE(index_acc->CreateIndex(label, {ms::PropertyPath{age_prop}}, order).has_value());
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
                                      order)) {
        auto prop = v.GetProperty(age_prop, ms::View::OLD);
        ASSERT_TRUE(prop.has_value());
        ages.push_back(prop->ValueInt());
      }

      EXPECT_THAT(ages, ::testing::ElementsAreArray(expected))
          << "expected exactly the pre-fork ages in index order: age=20 must read 20 (not 999), no 5 "
             "(post-fork create), 40 present (post-fork delete), property order rather than gid order";
    }
    historical->reset();
    mem_storage->ReleaseForkPin(fork_ts);
  }
}

// Edge-type index scan through a historical accessor returns fork-state edges only.
TEST_F(VersioningHistoricalAccessTest, HistoricalEdgeTypeIndexScanReturnsForkState) {
  auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());
  const auto knows_type = storage->NameToEdgeType("KNOWS");
  const auto likes_type = storage->NameToEdgeType("LIKES");

  // Edge-type index on :KNOWS.
  {
    auto index_acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(index_acc->CreateIndex(knows_type).has_value());
    ASSERT_TRUE(index_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Pre-fork: A, B, C vertices; A-[:KNOWS]->B, B-[:KNOWS]->C (the 2 fork-time KNOWS edges), and
  // A-[:LIKES]->C (a distractor of a different edge type, must never appear in a KNOWS scan).
  ms::Gid a_gid;
  ms::Gid b_gid;
  ms::Gid c_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    auto b = acc->CreateVertex();
    auto c = acc->CreateVertex();
    a_gid = a.Gid();
    b_gid = b.Gid();
    c_gid = c.Gid();

    auto ab = acc->CreateEdge(&a, &b, knows_type);
    ASSERT_TRUE(ab.has_value());
    auto bc = acc->CreateEdge(&b, &c, knows_type);
    ASSERT_TRUE(bc.has_value());
    auto ac_likes = acc->CreateEdge(&a, &c, likes_type);
    ASSERT_TRUE(ac_likes.has_value());

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const uint64_t fork_ts = mem_storage->RegisterForkPin();

  // Post-fork committed mutations on MAIN: a NEW KNOWS edge (A->C), and a DELETE of the pre-fork
  // A-[:KNOWS]->B edge. Neither may leak into the fork_ts scan below; the deleted edge specifically
  // MUST still appear, since it existed at fork_ts.
  {
    auto acc = storage->Access(ms::WRITE);

    auto a = acc->FindVertex(a_gid, ms::View::OLD);
    ASSERT_TRUE(a.has_value());
    auto c = acc->FindVertex(c_gid, ms::View::OLD);
    ASSERT_TRUE(c.has_value());
    auto ac_new = acc->CreateEdge(&*a, &*c, knows_type);
    ASSERT_TRUE(ac_new.has_value());

    auto out_edges = a->OutEdges(ms::View::OLD, {knows_type});
    ASSERT_TRUE(out_edges.has_value());
    bool deleted_ab = false;
    for (auto &edge : out_edges->edges) {
      if (edge.ToVertex().Gid() == b_gid) {
        ASSERT_TRUE(acc->DeleteEdge(&edge).has_value());
        deleted_ab = true;
        break;
      }
    }
    ASSERT_TRUE(deleted_ab) << "setup bug: pre-fork A-[:KNOWS]->B edge not found for deletion";

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto historical = mem_storage->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  {
    auto &hist_acc = **historical;

    std::vector<std::pair<uint64_t, uint64_t>> seen;
    for (auto edge : hist_acc.Edges(knows_type, ms::View::OLD)) {
      EXPECT_EQ(edge.EdgeType(), knows_type) << "scan must only yield :KNOWS edges, never the :LIKES distractor";
      seen.emplace_back(edge.FromVertex().Gid().AsUint(), edge.ToVertex().Gid().AsUint());
    }

    EXPECT_THAT(seen,
                ::testing::UnorderedElementsAre(std::pair{a_gid.AsUint(), b_gid.AsUint()},
                                                std::pair{b_gid.AsUint(), c_gid.AsUint()}))
        << "expected exactly the 2 fork-time KNOWS edges (A->B, B->C): the post-fork-deleted A->B "
           "must still be visible, the post-fork-created A->C must not appear";
  }
  historical->reset();
  mem_storage->ReleaseForkPin(fork_ts);
}

}  // namespace

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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "flags/general.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "storage/v2/exceptions.hpp"
#include "storage/v2/indices/index_order.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/property_value.hpp"
#include "storage_test_utils.hpp"
#include "storage_v2_gc_metrics_fixture.hpp"
#include "tests/test_commit_args_helper.hpp"

namespace ms = memgraph::storage;

namespace {

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

// HistoricalAccess must force SNAPSHOT_ISOLATION: under an inherited READ_COMMITTED/READ_UNCOMMITTED
// ambient level the post-fork commit would be visible.
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

// A never-pinned fork_ts is rejected even while another pin is live.
TEST_F(VersioningHistoricalAccessTest, RejectsUnpinnedForkTimestamp) {
  auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());

  {
    auto acc = storage->Access(ms::WRITE);
    (void)acc->CreateVertex();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const uint64_t unrelated_pin = mem_storage->RegisterForkPin();

  // Never pinned and above the live pin, so a range check (fork_ts >= oldest pin) would wrongly accept it.
  auto result = mem_storage->HistoricalAccess(unrelated_pin + 1);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), ms::InMemoryStorage::HistoricalAccessError::ForkTimestampNotPinned);

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

// Transient historical readers must not mark the shared fork_ts finished in the commit log. Since
// RegisterForkPin does not increment timestamp_, `real_reader` starts at fork_ts and would lose its deltas to GC.
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

  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }

  EXPECT_GT(registration_.handles().unreleased_delta_objects.Value(), 0)
      << "real_reader (start_timestamp == fork_ts, still open) needs this history regardless of "
         "the fork pin; the transient historical reads above must not have finalized it";

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
// the history it needs: the reader's self-pin holds the floor.
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

  // Simulate DROP BRANCH: only the reader's self-pin protects this history now.
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

  historical->reset();
  EXPECT_EQ(mem_storage->OldestForkPin(), std::nullopt);
  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }
  EXPECT_EQ(0, registration_.handles().unreleased_delta_objects.Value());

  // Commit releases the reader's self-pin; a later Abort() or destruction must not release it again.
  const uint64_t fork_ts2 = mem_storage->RegisterForkPin();
  {
    auto reader = mem_storage->HistoricalAccess(fork_ts2);
    ASSERT_TRUE(reader.has_value());
    ASSERT_TRUE((*reader)->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    mem_storage->ReleaseForkPin(fork_ts2);
    EXPECT_EQ(mem_storage->OldestForkPin(), std::nullopt) << "commit must release the self-pin";
    (*reader)->Abort();
    reader->reset();
  }
  EXPECT_EQ(mem_storage->OldestForkPin(), std::nullopt);
}

// A commit minting commit_ts == fork_ts (writer opened pre-fork, committed post-fork) must stay invisible to readers
// at the pin, through later writes and GC; a '>=' off-by-one in the fast-discard gate would free the deltas.
TEST_F(VersioningHistoricalAccessGcTest, HistoricalReadStableAcrossCommitAtForkAndLaterWrites) {
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
  auto commit_prop = [&](int64_t value) {
    auto acc = storage->Access(st::WRITE);
    auto v = acc->FindVertex(v_gid, st::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->SetProperty(prop_id, st::PropertyValue(value)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };
  auto expect_fork_state = [&](st::Storage::Accessor &acc) {
    auto v = acc.FindVertex(v_gid, st::View::OLD);
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(v->GetProperty(prop_id, st::View::OLD).value_or(st::PropertyValue()), st::PropertyValue(1));
    EXPECT_EQ(CountVertices(acc, st::View::OLD), 1U);
  };

  auto writer = storage->Access(st::WRITE);
  {
    auto v = writer->FindVertex(v_gid, st::View::OLD);
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->SetProperty(prop_id, st::PropertyValue(2)).has_value());
    (void)writer->CreateVertex();
  }

  const uint64_t fork_ts = mem_storage->RegisterForkPin();
  auto historical = mem_storage->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  for (int i = 0; i < 3; ++i) expect_fork_state(**historical);

  ASSERT_TRUE(writer->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  writer.reset();
  for (int64_t value = 3; value < 6; ++value) {
    expect_fork_state(**historical);
    commit_prop(value);
  }

  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }
  EXPECT_GT(registration_.handles().unreleased_delta_objects.Value(), 0) << "the pin at P == C must keep the deltas";
  expect_fork_state(**historical);
  {
    auto fresh = mem_storage->HistoricalAccess(fork_ts);
    ASSERT_TRUE(fresh.has_value());
    expect_fork_state(**fresh);
  }

  historical->reset();
  mem_storage->ReleaseForkPin(fork_ts);
  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }
  EXPECT_EQ(0, registration_.handles().unreleased_delta_objects.Value());
}

// Main writers commit while historical readers open and read at one pin and GC runs: every historical read must
// return exactly the fork state.
TEST_F(VersioningHistoricalAccessGcTest, ConcurrentMainWritersAndHistoricalReadersSeeForkState) {
  namespace st = memgraph::storage;
  auto *mem_storage = static_cast<st::InMemoryStorage *>(storage.get());
  const auto prop_id = storage->NameToProperty("p");
  constexpr int kVertices = 4;
  constexpr int kWriters = 2;
  constexpr int kReaders = 3;
  constexpr int kWriterIterations = 200;

  std::vector<st::Gid> gids;
  {
    auto acc = storage->Access(st::WRITE);
    for (int i = 0; i < kVertices; ++i) {
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.SetProperty(prop_id, st::PropertyValue(i)).has_value());
      gids.push_back(v.Gid());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  const uint64_t fork_ts = mem_storage->RegisterForkPin();

  std::atomic<bool> stop{false};
  std::atomic<int> mismatches{0};
  std::atomic<int> reads{0};
  std::vector<std::jthread> threads;
  for (int w = 0; w < kWriters; ++w) {
    threads.emplace_back([&, w] {
      for (int it = 0; it < kWriterIterations; ++it) {
        auto acc = storage->Access(st::WRITE);
        // Disjoint vertices per writer, so no serialization conflicts.
        for (int i = w; i < kVertices; i += kWriters) {
          auto v = acc->FindVertex(gids[i], st::View::OLD);
          if (!v || !v->SetProperty(prop_id, st::PropertyValue(1000 + it)).has_value()) ++mismatches;
        }
        (void)acc->CreateVertex();
        if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) ++mismatches;
      }
    });
  }
  for (int r = 0; r < kReaders; ++r) {
    threads.emplace_back([&] {
      while (!stop.load(std::memory_order_acquire)) {
        auto historical = mem_storage->HistoricalAccess(fork_ts);
        if (!historical) {
          ++mismatches;
          continue;
        }
        bool ok = CountVertices(**historical, st::View::OLD) == kVertices;
        for (int i = 0; i < kVertices; ++i) {
          auto v = (*historical)->FindVertex(gids[i], st::View::OLD);
          ok = ok && v && v->GetProperty(prop_id, st::View::OLD).value_or(st::PropertyValue()) == st::PropertyValue(i);
        }
        if (!ok) ++mismatches;
        ++reads;
      }
    });
  }
  threads.emplace_back([&] {
    while (!stop.load(std::memory_order_acquire)) storage->FreeMemory();
  });
  for (int w = 0; w < kWriters; ++w) threads[w].join();
  stop.store(true, std::memory_order_release);
  threads.clear();

  EXPECT_EQ(mismatches.load(), 0);
  EXPECT_GT(reads.load(), 0);
  mem_storage->ReleaseForkPin(fork_ts);
  EXPECT_EQ(mem_storage->OldestForkPin(), std::nullopt);
}

// The GC floor must be the OLDEST pin (a newest-pin floor would unlink A's history). After releasing
// it, B's history stays reconstructible and A's timestamp is rejected.
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
  auto released = mem_storage->HistoricalAccess(pin_a);
  ASSERT_FALSE(released.has_value()) << "a released pin must be refused while another pin is live";
  EXPECT_EQ(released.error(), ms::InMemoryStorage::HistoricalAccessError::ForkTimestampNotPinned);
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

// Full GC under a live fork pin: every GC horizon (unlink gate, chain trim, index sweep) is clamped to the oldest
// pin, and the historical label+property scan returns exactly the fork state in index order (ASC and DESC).
TEST_F(VersioningHistoricalAccessGcTest, FullGcUnderForkPinKeepsForkStateVisibleAndIndexed) {
  namespace st = memgraph::storage;
  auto *mem_storage = static_cast<st::InMemoryStorage *>(storage.get());
  const auto label = storage->NameToLabel("Person");
  const auto age_prop = storage->NameToProperty("age");

  for (auto order : {st::IndexOrder::ASC, st::IndexOrder::DESC}) {
    auto index_acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(index_acc->CreateIndex(label, {st::PropertyPath{age_prop}}, order).has_value());
    ASSERT_TRUE(index_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Created out of order so that gid order != age order.
  st::Gid v20_gid;
  st::Gid v40_gid;
  {
    auto acc = storage->Access(st::WRITE);
    for (int64_t age : {50, 20, 40, 10, 30}) {
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(label).has_value());
      ASSERT_TRUE(v.SetProperty(age_prop, st::PropertyValue(age)).has_value());
      if (age == 20) v20_gid = v.Gid();
      if (age == 40) v40_gid = v.Gid();
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  const uint64_t fork_ts = mem_storage->RegisterForkPin();

  {
    auto acc = storage->Access(st::WRITE);

    auto v20 = acc->FindVertex(v20_gid, st::View::OLD);
    ASSERT_TRUE(v20.has_value());
    ASSERT_TRUE(v20->SetProperty(age_prop, st::PropertyValue(999)).has_value());

    auto v5 = acc->CreateVertex();
    ASSERT_TRUE(v5.AddLabel(label).has_value());
    ASSERT_TRUE(v5.SetProperty(age_prop, st::PropertyValue(5)).has_value());

    auto v40 = acc->FindVertex(v40_gid, st::View::OLD);
    ASSERT_TRUE(v40.has_value());
    ASSERT_TRUE(acc->DetachDeleteVertex(&*v40).has_value());

    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  for (int i = 0; i < 6; ++i) {
    storage->FreeMemory();
  }

  auto historical = mem_storage->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  {
    auto &hist_acc = **historical;

    auto v20 = hist_acc.FindVertex(v20_gid, st::View::OLD);
    ASSERT_TRUE(v20.has_value());
    auto prop = v20->GetProperty(age_prop, st::View::OLD);
    ASSERT_TRUE(prop.has_value());
    EXPECT_EQ(*prop, st::PropertyValue(20)) << "GC must keep the pre-fork property value";
    EXPECT_TRUE(hist_acc.FindVertex(v40_gid, st::View::OLD).has_value())
        << "GC must keep the vertex that was deleted after the fork";

    struct Case {
      st::IndexOrder order;
      std::vector<int64_t> expected;
    };

    const Case cases[] = {{st::IndexOrder::ASC, {10, 20, 30, 40, 50}}, {st::IndexOrder::DESC, {50, 40, 30, 20, 10}}};
    for (const auto &[order, expected] : cases) {
      SCOPED_TRACE(order == st::IndexOrder::ASC ? "ASC" : "DESC");

      std::vector<int64_t> ages;
      for (auto v : hist_acc.Vertices(label,
                                      std::array{st::PropertyPath{age_prop}},
                                      std::array{st::PropertyValueRange::IsNotNull()},
                                      st::View::OLD,
                                      order)) {
        auto p = v.GetProperty(age_prop, st::View::OLD);
        ASSERT_TRUE(p.has_value());
        ages.push_back(p->ValueInt());
      }

      EXPECT_THAT(ages, ::testing::ElementsAreArray(expected))
          << "GC must not sweep index entries the fork still needs; expected exactly the pre-fork ages in index "
             "order: age=20 must read 20 (not 999), no 5 (post-fork create), 40 present (post-fork delete), "
             "property order rather than gid order";
    }
  }
  historical->reset();
  mem_storage->ReleaseForkPin(fork_ts);
}

// Edge-type index scan through a historical accessor returns fork-state edges only.
TEST_F(VersioningHistoricalAccessTest, HistoricalEdgeTypeIndexScanReturnsForkState) {
  auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());
  const auto knows_type = storage->NameToEdgeType("KNOWS");
  const auto likes_type = storage->NameToEdgeType("LIKES");

  {
    auto index_acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(index_acc->CreateIndex(knows_type).has_value());
    ASSERT_TRUE(index_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // A-[:LIKES]->C is a distractor that must never appear in a KNOWS scan.
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
    for (auto edge : out_edges->edges) {
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

// A historical reader must not use an index committed after its fork_ts (the fork state was never indexed by it),
// while plain accessors and a reader pinned after the index commit do.
TEST_F(VersioningHistoricalAccessTest, IndexCommittedAfterForkIsNotReadyForHistoricalReader) {
  auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());
  const auto label = storage->NameToLabel("L");
  const auto edge_type = storage->NameToEdgeType("E");
  const auto prop = storage->NameToProperty("p");
  const std::array paths{ms::PropertyPath{prop}};

  const uint64_t pin_before = mem_storage->RegisterForkPin();
  auto create_index = [&](auto &&create) {
    auto index_acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(create(*index_acc).has_value());
    ASSERT_TRUE(index_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };
  create_index([&](ms::Storage::Accessor &acc) { return acc.CreateIndex(label); });
  create_index([&](ms::Storage::Accessor &acc) { return acc.CreateIndex(label, {ms::PropertyPath{prop}}); });
  create_index([&](ms::Storage::Accessor &acc) { return acc.CreateIndex(edge_type); });
  create_index([&](ms::Storage::Accessor &acc) { return acc.CreateIndex(edge_type, prop); });
  create_index([&](ms::Storage::Accessor &acc) { return acc.CreateGlobalEdgeIndex(prop); });
  create_index([&](ms::Storage::Accessor &acc) { return acc.CreateGlobalVertexIndex(prop); });
  const uint64_t pin_after = mem_storage->RegisterForkPin();

  auto expect_all_ready = [&](ms::Storage::Accessor &acc, bool expected) {
    EXPECT_EQ(acc.LabelIndexReady(label), expected);
    EXPECT_EQ(acc.LabelPropertyIndexReady(label, paths), expected);
    EXPECT_EQ(acc.EdgeTypeIndexReady(edge_type), expected);
    EXPECT_EQ(acc.EdgeTypePropertyIndexReady(edge_type, prop), expected);
    EXPECT_EQ(acc.EdgePropertyIndexReady(prop), expected);
    EXPECT_EQ(acc.VertexPropertyIndexReady(prop), expected);
  };

  {
    SCOPED_TRACE("plain accessor");
    auto acc = storage->Access(ms::READ);
    expect_all_ready(*acc, true);
  }
  {
    SCOPED_TRACE("historical reader pinned before the index commit");
    auto historical = mem_storage->HistoricalAccess(pin_before);
    ASSERT_TRUE(historical.has_value());
    expect_all_ready(**historical, false);
  }
  {
    SCOPED_TRACE("historical reader pinned after the index commit");
    auto historical = mem_storage->HistoricalAccess(pin_after);
    ASSERT_TRUE(historical.has_value());
    expect_all_ready(**historical, true);
  }

  mem_storage->ReleaseForkPin(pin_before);
  mem_storage->ReleaseForkPin(pin_after);
}

// An index whose commit lands exactly on fork_ts (C == P) is visible to the fork reader (IsVisible is <=). That is
// sound because the index was populated with writers excluded, i.e. from exactly the fork state.
TEST_F(VersioningHistoricalAccessTest, IndexCommittedExactlyAtForkServesForkState) {
  auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());
  const auto label = storage->NameToLabel("L");
  const auto prop = storage->NameToProperty("p");
  const std::array paths{ms::PropertyPath{prop}};

  ms::Gid v10_gid;
  ms::Gid v20_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    for (int64_t value : {20, 10}) {
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(label).has_value());
      ASSERT_TRUE(v.SetProperty(prop, ms::PropertyValue(value)).has_value());
      (value == 10 ? v10_gid : v20_gid) = v.Gid();
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  uint64_t fork_ts = 0;
  {
    auto index_acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(index_acc->CreateIndex(label, {ms::PropertyPath{prop}}).has_value());
    fork_ts = mem_storage->RegisterForkPin();
    ASSERT_TRUE(index_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  const uint64_t probe = mem_storage->RegisterForkPin();
  ASSERT_EQ(probe, fork_ts + 1) << "test precondition: the index commit minted exactly fork_ts";
  mem_storage->ReleaseForkPin(probe);

  {
    auto acc = storage->Access(ms::WRITE);
    auto v10 = acc->FindVertex(v10_gid, ms::View::OLD);
    ASSERT_TRUE(v10.has_value());
    ASSERT_TRUE(v10->SetProperty(prop, ms::PropertyValue(999)).has_value());
    auto v5 = acc->CreateVertex();
    ASSERT_TRUE(v5.AddLabel(label).has_value());
    ASSERT_TRUE(v5.SetProperty(prop, ms::PropertyValue(5)).has_value());
    auto v20 = acc->FindVertex(v20_gid, ms::View::OLD);
    ASSERT_TRUE(v20.has_value());
    ASSERT_TRUE(acc->DetachDeleteVertex(&*v20).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto historical = mem_storage->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  auto &hist_acc = **historical;
  EXPECT_TRUE(hist_acc.LabelPropertyIndexReady(label, paths));
  std::vector<int64_t> values;
  for (auto v : hist_acc.Vertices(label, paths, std::array{ms::PropertyValueRange::IsNotNull()}, ms::View::OLD)) {
    values.push_back(v.GetProperty(prop, ms::View::OLD).value_or(ms::PropertyValue()).ValueInt());
  }
  EXPECT_THAT(values, ::testing::ElementsAre(10, 20));
  historical->reset();
  mem_storage->ReleaseForkPin(fork_ts);
}

// Writes through a historical accessor are refused in release builds too, and every refused accessor still
// releases its self-pin. MVCC writes throw at the first delta; metadata-only writes are refused at commit.
TEST_F(VersioningHistoricalAccessTest, RefusesWritesAndReleasesSelfPin) {
  auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());
  const auto label = storage->NameToLabel("L");

  ms::Gid v_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    v_gid = acc->CreateVertex().Gid();
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto index_acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(index_acc->CreateIndex(label).has_value());
    ASSERT_TRUE(index_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  const uint64_t fork_ts = mem_storage->RegisterForkPin();

  {
    auto historical = mem_storage->HistoricalAccess(fork_ts);
    ASSERT_TRUE(historical.has_value());
    EXPECT_THROW((void)(*historical)->CreateVertex(), ms::WriteInHistoricalAccessorException);
    auto v = (*historical)->FindVertex(v_gid, ms::View::OLD);
    ASSERT_TRUE(v.has_value());
    EXPECT_THROW((void)v->AddLabel(label), ms::WriteInHistoricalAccessorException);
  }
  {
    auto historical = mem_storage->HistoricalAccess(fork_ts);
    ASSERT_TRUE(historical.has_value());
    ASSERT_TRUE((*historical)->DropIndex(label).has_value());
    EXPECT_THROW((void)(*historical)->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()),
                 ms::WriteInHistoricalAccessorException);
  }

  EXPECT_EQ(mem_storage->OldestForkPin(), std::optional<uint64_t>{fork_ts});
  {
    auto acc = storage->Access(ms::READ);
    EXPECT_TRUE(acc->LabelIndexReady(label)) << "a refused historical commit must not drop main's index";
    EXPECT_EQ(CountVertices(*acc, ms::View::OLD), 1U);
    auto v = acc->FindVertex(v_gid, ms::View::OLD);
    ASSERT_TRUE(v.has_value());
    EXPECT_FALSE(v->HasLabel(label, ms::View::OLD).value_or(true));
  }
  mem_storage->ReleaseForkPin(fork_ts);
  EXPECT_EQ(mem_storage->OldestForkPin(), std::nullopt) << "a refused historical accessor leaked its self-pin";
}

// Only IN_MEMORY_TRANSACTIONAL keeps MVCC history; elsewhere a historical read would silently see live data.
TEST_F(VersioningHistoricalAccessTest, RejectsNonTransactionalStorageMode) {
  auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());
  const uint64_t fork_ts = mem_storage->RegisterForkPin();
  mem_storage->SetStorageMode(ms::StorageMode::IN_MEMORY_ANALYTICAL);

  auto result = mem_storage->HistoricalAccess(fork_ts);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), ms::InMemoryStorage::HistoricalAccessError::StorageModeNotTransactional);
  EXPECT_EQ(mem_storage->OldestForkPin(), std::optional<uint64_t>{fork_ts}) << "the refused open kept its self-pin";
  mem_storage->ReleaseForkPin(fork_ts);
  EXPECT_EQ(mem_storage->OldestForkPin(), std::nullopt) << "the refused open leaked its self-pin";
}

// Point and vector indexes hold only main's current state, so a historical reader must not use them: the point
// index is reported absent (the planner scans instead) and vector search is refused.
TEST_F(VersioningHistoricalAccessTest, PointAndVectorIndexesAreNotUsedByHistoricalReader) {
  auto *mem_storage = static_cast<ms::InMemoryStorage *>(storage.get());
  const auto label = storage->NameToLabel("L");
  const auto loc = storage->NameToProperty("loc");
  const auto emb = storage->NameToProperty("emb");
  constexpr std::string_view kVectorIndex = "vidx";
  {
    auto unique_acc = storage->UniqueAccess();
    ASSERT_TRUE(unique_acc->CreatePointIndex(label, loc).has_value());
    ASSERT_TRUE(unique_acc
                    ->CreateVectorIndex(ms::VectorIndexSpec{
                        .index_name = std::string{kVectorIndex},
                        .label_filter = ms::VectorLabelFilter{.mode = ms::VectorMatchMode::SINGLE, .ids = {label}},
                        .property = emb,
                        .metric_kind = unum::usearch::metric_kind_t::l2sq_k,
                        .dimension = 2,
                        .resize_coefficient = 2,
                        .capacity = 10,
                        .scalar_kind = unum::usearch::scalar_kind_t::f32_k})
                    .has_value());
    ASSERT_TRUE(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  ms::Gid a_gid;
  {
    auto acc = storage->Access(ms::WRITE);
    auto a = acc->CreateVertex();
    a_gid = a.Gid();
    ASSERT_TRUE(a.AddLabel(label).has_value());
    ASSERT_TRUE(a.SetProperty(loc, ms::PropertyValue(ms::Point2d{ms::CoordinateReferenceSystem::Cartesian_2d, 0, 0}))
                    .has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  const uint64_t fork_ts = mem_storage->RegisterForkPin();
  {
    auto acc = storage->Access(ms::WRITE);
    auto a = acc->FindVertex(a_gid, ms::View::OLD);
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(a->SetProperty(loc, ms::PropertyValue(ms::Point2d{ms::CoordinateReferenceSystem::Cartesian_2d, 9, 9}))
                    .has_value());
    auto b = acc->CreateVertex();
    ASSERT_TRUE(b.AddLabel(label).has_value());
    const auto index_id = acc->GetNameIdMapper()->NameToId(kVectorIndex);
    ASSERT_TRUE(b.SetProperty(emb,
                              ms::PropertyValue(ms::PropertyValue::VectorIndexIdData{
                                  .ids = memgraph::utils::small_vector<uint64_t>{index_id},
                                  .vector = memgraph::utils::small_vector<float>{1.0F, 0.0F}}))
                    .has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  EXPECT_TRUE(storage->Access(ms::READ)->PointIndexExists(label, loc));
  auto historical = mem_storage->HistoricalAccess(fork_ts);
  ASSERT_TRUE(historical.has_value());
  EXPECT_FALSE((*historical)->PointIndexExists(label, loc)) << "the point index only holds a's post-fork location";
  EXPECT_THROW((void)(*historical)->VectorIndexSearchOnNodes(std::string{kVectorIndex}, 10, {1.0F, 0.0F}),
               ms::VectorSearchException)
      << "the vector index would return b, created after the fork";
  historical->reset();
  mem_storage->ReleaseForkPin(fork_ts);
}

}  // namespace

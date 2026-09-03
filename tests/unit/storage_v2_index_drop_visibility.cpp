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

// An index drop mutates the live catalogue the moment the statement runs and publishes nothing at
// commit: it registers an abort callback and a log record. Every constraint drop in the same file
// defers publication to a commit callback instead, and its comment gives this as the reason. These
// tests cover the two consequences of the difference.
//
// Neither test depends on timing. Holding the dropping transaction open is enough to place the
// other operation inside the window, so both are deterministic.

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <optional>
#include <semaphore>
#include <string>
#include <thread>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/file.hpp"

using memgraph::storage::Config;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::View;

namespace {

class IndexDropVisibility : public ::testing::Test {
 protected:
  void SetUp() override { Clear(); }

  void TearDown() override { Clear(); }

  std::filesystem::path storage_directory{std::filesystem::temp_directory_path() /
                                          "MG_test_unit_storage_v2_index_drop_visibility"};

 private:
  void Clear() {
    if (std::filesystem::exists(storage_directory)) std::filesystem::remove_all(storage_directory);
  }
};

}  // namespace

// A drop evicts the index object and keeps hold of it so that an abort can reinstate the same
// object. Writers admitted during the window took their index set from the catalogue as it then
// stood, without the index, so they maintain nothing. Reinstating the object therefore reinstates it
// as it was at eviction, missing everything written since, and the index is live again and wrong: a
// scan through it returns fewer rows than the same scan without it.
//
// The interpreter takes read-only access for this in the analytical mode and its comment says the
// reason is exactly this. The transactional mode takes read.
TEST_F(IndexDropVisibility, AbortedDropLeavesIndexMissingConcurrentWrites) {
  Config config{};
  config.gc.type = Config::Gc::Type::NONE;

  auto store = std::make_unique<InMemoryStorage>(config);
  const auto label = store->NameToLabel("L");

  {
    auto acc = store->ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateIndex(label).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  // One labelled vertex before the window, which the index does contain.
  {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_TRUE(vertex.AddLabel(label).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  std::binary_semaphore applied{0};
  std::binary_semaphore may_abort{0};

  std::thread dropper([&] {
    auto acc = store->Access(memgraph::storage::READ);
    ASSERT_TRUE(acc->DropIndex(label).has_value());
    // The index is already gone from the catalogue here, with nothing committed.
    applied.release();
    may_abort.acquire();
    acc->Abort();
  });

  applied.acquire();
  // Begins inside the window, so its index set has no index to maintain.
  {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_TRUE(vertex.AddLabel(label).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  may_abort.release();
  dropper.join();

  // The drop was rolled back, so the index is live again and must answer for both vertices.
  auto acc = store->Access(memgraph::storage::READ);
  int64_t via_index = 0;
  for (auto vertex : acc->Vertices(label, View::OLD)) {
    (void)vertex;
    ++via_index;
  }
  int64_t via_scan = 0;
  for (auto vertex : acc->Vertices(View::OLD)) {
    auto has = vertex.HasLabel(label, View::OLD);
    ASSERT_TRUE(has.has_value());
    if (*has) ++via_scan;
  }

  ASSERT_EQ(via_scan, 2) << "the two labelled vertices are not both committed, so this test never set up the "
                            "comparison it exists to make";
  EXPECT_EQ(via_index, via_scan) << "a scan through the restored index returned " << via_index << " of " << via_scan
                                 << " labelled vertices. The rolled-back drop restored the index as it stood at "
                                    "eviction, without what was written while it was gone.";
}

// A snapshot whose transaction begins after the statement has run and before its transaction commits
// records the index as absent, while recording a durable timestamp below the drop's. Recovery then
// replays the drop onto a catalogue that never had the index, and refuses it, so the database does
// not come up.
//
// The snapshot fallback does not save it: the snapshot itself is a valid file and loads fine, and the
// failure happens later during log replay, which is outside the loop that tries older snapshots.
TEST_F(IndexDropVisibility, UncommittedDropSnapshotBlocksRecovery) {
  Config config{};
  config.durability.storage_directory = storage_directory;
  config.durability.recover_on_startup = false;
  config.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  config.gc.type = Config::Gc::Type::NONE;

  {
    auto store = std::make_unique<InMemoryStorage>(config);
    const auto label = store->NameToLabel("L");
    {
      auto acc = store->ReadOnlyAccess();
      ASSERT_TRUE(acc->CreateIndex(label).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    std::binary_semaphore applied{0};
    std::binary_semaphore may_commit{0};
    std::optional<bool> drop_ok;

    std::thread dropper([&] {
      auto acc = store->Access(memgraph::storage::READ);
      ASSERT_TRUE(acc->DropIndex(label).has_value());
      applied.release();
      may_commit.acquire();
      drop_ok = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value();
    });

    applied.acquire();
    // This snapshot's transaction begins inside the window: after the statement, before the commit.
    ASSERT_TRUE(store->CreateSnapshot(/*force=*/true).has_value());
    may_commit.release();
    dropper.join();
    ASSERT_TRUE(drop_ok.has_value());
    ASSERT_TRUE(*drop_ok);
    store.reset();
  }

  Config recover_config = config;
  recover_config.durability.recover_on_startup = true;

  std::string failure;
  try {
    auto recovered = std::make_unique<InMemoryStorage>(recover_config);
    if (recovered->IsBroken()) failure = "storage recovered into the broken state";
  } catch (std::exception const &e) {
    failure = e.what();
  }

  EXPECT_TRUE(failure.empty()) << "a snapshot taken while an index drop was applied but uncommitted produced a "
                                  "database that cannot be recovered: "
                               << failure;
}

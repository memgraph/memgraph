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

// An index drop mutates the live catalogue the moment the statement runs. A durability snapshot
// taken in the window between that and the commit therefore records the index as absent while
// recording a durable timestamp below the drop's, and the two halves disagree.
//
// The test does not depend on timing. Holding the dropping transaction open is enough to place the
// snapshot inside the window.

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

    // Read-only, which excludes writers but not the snapshot's own read accessor, so the window is
    // still open to it.
    std::thread dropper([&] {
      auto acc = store->ReadOnlyAccess();
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

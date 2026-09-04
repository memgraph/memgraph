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
// object. That is only exact if the index was maintained while it was gone, and what decides
// whether a concurrent writer maintains it is the index set that writer captured when it began.
//
// So the drop mutates the container as the statement runs and publishes nothing: the snapshot new
// transactions capture still holds the index until the drop commits. A writer admitted during the
// window therefore maintains the very object the abort restores, and the restore is exact without
// excluding anybody.
//
// The drop here takes an accessor that admits writers, and the writer commits inside the window
// rather than waiting for it, because that is the case the deferral exists to make safe.
TEST_F(IndexDropVisibility, AbortedDropLeavesIndexComplete) {
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
  std::binary_semaphore written{0};
  std::binary_semaphore may_abort{0};

  std::thread dropper([&] {
    auto acc = store->Access(memgraph::storage::READ);
    ASSERT_TRUE(acc->DropIndex(label).has_value());
    applied.release();
    may_abort.acquire();
    acc->Abort();
  });

  applied.acquire();

  // Begins and commits entirely inside the window, and is not made to wait for it. Ordering the
  // abort after this commit is what puts the write inside the window rather than after it.
  std::thread writer([&] {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_TRUE(vertex.AddLabel(label).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    written.release();
  });

  written.acquire();
  may_abort.release();
  dropper.join();
  writer.join();

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
                                 << " labelled vertices, so the rolled-back drop restored it without what was "
                                    "written while it was gone.";
}

// The same completeness claim, broken by a publisher that is not the dropper. This is the case
// that decides where the eviction belongs, and it is why withholding only the dropper's own
// publication is not enough.
//
// Publishing means publishing the catalogue. An eviction that happens when the statement runs is
// therefore visible to every other publisher, and registering an unrelated index publishes at
// statement time because a creation must. So the creation below hands out the uncommitted drop on
// the dropper's behalf, and the writer that begins next maintains nothing for the dropped index.
TEST_F(IndexDropVisibility, ConcurrentCreateDoesNotPublishAnUncommittedDrop) {
  Config config{};
  config.gc.type = Config::Gc::Type::NONE;

  auto store = std::make_unique<InMemoryStorage>(config);
  const auto dropped = store->NameToLabel("Dropped");
  const auto other = store->NameToLabel("Other");

  {
    auto acc = store->ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateIndex(dropped).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_TRUE(vertex.AddLabel(dropped).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  std::binary_semaphore applied{0};
  std::binary_semaphore may_abort{0};

  std::thread dropper([&] {
    auto acc = store->Access(memgraph::storage::READ);
    ASSERT_TRUE(acc->DropIndex(dropped).has_value());
    applied.release();
    may_abort.acquire();
    acc->Abort();
  });

  applied.acquire();

  // An unrelated index, created and committed entirely inside the dropper's window.
  {
    auto acc = store->ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateIndex(other).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Begins after that creation published, so its index set is whatever the creation handed out.
  {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto vertex = acc->CreateVertex();
    ASSERT_TRUE(vertex.AddLabel(dropped).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  may_abort.release();
  dropper.join();

  auto acc = store->Access(memgraph::storage::READ);
  int64_t via_index = 0;
  for (auto vertex : acc->Vertices(dropped, View::OLD)) {
    (void)vertex;
    ++via_index;
  }
  int64_t via_scan = 0;
  for (auto vertex : acc->Vertices(View::OLD)) {
    auto has = vertex.HasLabel(dropped, View::OLD);
    ASSERT_TRUE(has.has_value());
    if (*has) ++via_scan;
  }

  ASSERT_EQ(via_scan, 2) << "the two labelled vertices are not both committed, so this test never set up the "
                            "comparison it exists to make";
  EXPECT_EQ(via_index, via_scan) << "a scan through the restored index returned " << via_index << " of " << via_scan
                                 << " labelled vertices, so an unrelated index creation published the drop before "
                                    "the dropping transaction committed it.";
}

// A snapshot taken while a drop is pending must leave a database that starts.
//
// The snapshot's transaction captures the index set it can see, and the drop has not committed, so
// the index is recorded as present and replay applies the drop to a catalogue that has it. What
// makes this worth pinning is the arrangement it rules out: an eviction that happened when the
// statement ran would have the snapshot record the index as gone while recording a durable
// timestamp below the drop's, and replay would then apply the drop to a catalogue that never had
// it. The snapshot fallback would not save that, because the snapshot itself is a valid file that
// loads, and the refusal comes later during log replay, outside the loop that tries older
// snapshots.
TEST_F(IndexDropVisibility, SnapshotAcrossUncommittedDropStaysRecoverable) {
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

// Two transactions can both find the index and both record a drop of it, because a drop settles
// that the index exists when its statement runs and evicts when it commits. One evicts and the
// other finds nothing left, and the log holds two drops either way, so replay of the second meets a
// catalogue that no longer has the index. Recovery has to accept that rather than refuse to start.
//
// This is what keeps the tolerant replay honest. Without a case that reaches it, the branch is
// reachable only from snapshots written by older binaries, which no test here produces.
TEST_F(IndexDropVisibility, ConcurrentDropsOfOneIndexStayRecoverable) {
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

    // Both statements run before either commits, so both record a drop.
    auto first = store->Access(memgraph::storage::READ);
    auto second = store->Access(memgraph::storage::READ);
    ASSERT_TRUE(first->DropIndex(label).has_value());
    ASSERT_TRUE(second->DropIndex(label).has_value());
    ASSERT_TRUE(first->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ASSERT_TRUE(second->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    first.reset();
    second.reset();
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

  EXPECT_TRUE(failure.empty()) << "two logged drops of one index produced a database that cannot be recovered: "
                               << failure;
}

// Copyright 2025 Memgraph Ltd.
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

#include <unistd.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <thread>

#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"

namespace {
constexpr auto kTimeout = std::chrono::milliseconds(100);
// The bounded acquisition has to throw within this; pre-fix the disk path never throws at all.
constexpr auto kObserveTimeout = std::chrono::seconds(2);

std::filesystem::path MakeDiskConfig(memgraph::storage::Config &config) {
  auto dir =
      std::filesystem::temp_directory_path() / ("storage_v2_acc_disk_" + std::to_string(static_cast<int>(getpid())));
  std::filesystem::remove_all(dir);
  config.durability.storage_directory = dir;
  config.disk.main_storage_directory = dir / "rocksdb_main_storage";
  config.disk.label_index_directory = dir / "rocksdb_label_index";
  config.disk.label_property_index_directory = dir / "rocksdb_label_property_index";
  config.disk.unique_constraints_directory = dir / "rocksdb_unique_constraints";
  config.disk.name_id_mapper_directory = dir / "rocksdb_name_id_mapper";
  config.disk.id_name_mapper_directory = dir / "rocksdb_id_name_mapper";
  config.disk.durability_directory = dir / "rocksdb_durability";
  config.disk.wal_directory = dir / "rocksdb_wal";
  config.force_on_disk = true;
  return dir;
}

// Run `acquire` on a side thread while `holder` keeps the contended lock, and require it to throw
// `Expected` within kObserveTimeout. On the pre-fix disk path the timeout is discarded, so the call
// blocks until the holder is released; releasing it there lets the thread finish instead of wedging
// the test binary, and the failure is reported as the assertion it is.
template <typename Expected, typename Fn>
void ExpectAccessTimeout(Fn acquire, const char *what, std::unique_ptr<memgraph::storage::Storage::Accessor> &holder) {
  enum class Outcome : uint8_t { kBlocked, kExpected, kWrong };
  std::atomic<Outcome> outcome{Outcome::kBlocked};
  std::thread worker([&] {
    try {
      auto acc = acquire();
      (void)acc;
      outcome.store(Outcome::kWrong);
    } catch (const Expected &) {
      outcome.store(Outcome::kExpected);
    } catch (const std::exception &) {
      outcome.store(Outcome::kWrong);
    }
  });

  const auto deadline = std::chrono::steady_clock::now() + kObserveTimeout;
  while (outcome.load() == Outcome::kBlocked && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  const bool blocked = outcome.load() == Outcome::kBlocked;
  if (blocked) {
    ADD_FAILURE() << what
                  << " did not throw within 2s: DiskStorage ignores the caller's timeout instead of forwarding "
                     "it to AcquireGuardOrThrow";
    holder.reset();
  }
  worker.join();
  if (!blocked) {
    EXPECT_EQ(outcome.load(), Outcome::kExpected) << what << " threw the wrong exception type";
  }
}
}  // namespace

/// Test that unique/shared accessors can timeout and not deadlock
TEST(StorageV2Acc, Timeouts) {
  std::unique_ptr<memgraph::storage::Storage> storage(
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{}));

  {
    auto shared_acc = storage->Access(memgraph::storage::StorageAccessType::WRITE, {}, kTimeout);
    ASSERT_TRUE(shared_acc);
    ASSERT_THROW(storage->UniqueAccess({}, kTimeout), memgraph::storage::UniqueAccessTimeout);
    auto shared_acc2 = storage->Access(memgraph::storage::StorageAccessType::WRITE, {}, kTimeout);
    ASSERT_TRUE(shared_acc2);
  }
  {
    auto unique_acc = storage->UniqueAccess({}, kTimeout);
    ASSERT_TRUE(unique_acc);
    ASSERT_THROW(storage->Access(memgraph::storage::StorageAccessType::WRITE, {}, kTimeout),
                 memgraph::storage::SharedAccessTimeout);
    ASSERT_THROW(storage->UniqueAccess({}, kTimeout), memgraph::storage::UniqueAccessTimeout);
  }
}

/// Test read/write accessors
TEST(StorageV2Acc, RW) {
  std::unique_ptr<memgraph::storage::Storage> storage(
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{}));

  {  // Write should not block write or read, but should block read_only and unique
    auto shared_acc = storage->Access(memgraph::storage::StorageAccessType::WRITE, {}, kTimeout);
    ASSERT_TRUE(shared_acc);
    {
      auto shared_acc2 = storage->Access(memgraph::storage::StorageAccessType::WRITE, {}, kTimeout);
      ASSERT_TRUE(shared_acc2);
    }
    {
      auto shared_acc3 = storage->Access(memgraph::storage::StorageAccessType::READ, {}, kTimeout);
      ASSERT_TRUE(shared_acc3);
    }
    ASSERT_THROW(storage->ReadOnlyAccess({}, kTimeout), memgraph::storage::ReadOnlyAccessTimeout);
    ASSERT_THROW(storage->UniqueAccess({}, kTimeout), memgraph::storage::UniqueAccessTimeout);
  }
  {  // Read should not block read, write or read_only, but should block unique
    auto shared_acc = storage->Access(memgraph::storage::StorageAccessType::READ, {}, kTimeout);
    ASSERT_TRUE(shared_acc);
    {
      auto shared_acc2 = storage->Access(memgraph::storage::StorageAccessType::WRITE, {}, kTimeout);
      ASSERT_TRUE(shared_acc2);
    }
    {
      auto shared_acc3 = storage->Access(memgraph::storage::StorageAccessType::READ, {}, kTimeout);
      ASSERT_TRUE(shared_acc3);
    }
    ASSERT_THROW(storage->UniqueAccess({}, kTimeout), memgraph::storage::UniqueAccessTimeout);
    {
      auto shared_acc4 = storage->ReadOnlyAccess({}, kTimeout);
      ASSERT_TRUE(shared_acc4);
    }
  }
  {  // Read_only should not block read, read_only, but should block write and unique
    auto shared_acc = storage->ReadOnlyAccess({}, kTimeout);
    ASSERT_TRUE(shared_acc);
    ASSERT_THROW(storage->Access(memgraph::storage::StorageAccessType::WRITE, {}, kTimeout),
                 memgraph::storage::SharedAccessTimeout);
    {
      auto shared_acc3 = storage->Access(memgraph::storage::StorageAccessType::READ, {}, kTimeout);
      ASSERT_TRUE(shared_acc3);
    }
    {
      auto shared_acc4 = storage->ReadOnlyAccess({}, kTimeout);
      ASSERT_TRUE(shared_acc4);
    }
    ASSERT_THROW(storage->UniqueAccess({}, kTimeout), memgraph::storage::UniqueAccessTimeout);
  }
  {  // Read -> Read_only -> Read transitions
    auto shared_acc = storage->Access(memgraph::storage::StorageAccessType::READ, {}, kTimeout);
    ASSERT_TRUE(shared_acc);
    auto shared_acc2 = storage->ReadOnlyAccess({}, kTimeout);
    ASSERT_TRUE(shared_acc2);
    ASSERT_THROW(storage->Access(memgraph::storage::StorageAccessType::WRITE, {}, kTimeout),
                 memgraph::storage::SharedAccessTimeout);
    shared_acc2.reset();
    auto shared_acc3 = storage->Access(memgraph::storage::StorageAccessType::WRITE, {}, kTimeout);
    ASSERT_TRUE(shared_acc3);
  }
  {  // Unique should block all other accessors
    auto unique_acc = storage->UniqueAccess({}, kTimeout);
    ASSERT_TRUE(unique_acc);
    ASSERT_THROW(storage->Access(memgraph::storage::StorageAccessType::READ, {}, kTimeout),
                 memgraph::storage::SharedAccessTimeout);
    ASSERT_THROW(storage->Access(memgraph::storage::StorageAccessType::WRITE, {}, kTimeout),
                 memgraph::storage::SharedAccessTimeout);
    ASSERT_THROW(storage->ReadOnlyAccess({}, kTimeout), memgraph::storage::ReadOnlyAccessTimeout);
    ASSERT_THROW(storage->UniqueAccess({}, kTimeout), memgraph::storage::UniqueAccessTimeout);
  }
}

/// DiskStorage must forward the caller's timeout into AcquireGuardOrThrow; without the fix all three
/// access methods discard it, so a bounded acquisition silently blocks.
TEST(StorageV2AccDisk, Timeouts) {
  memgraph::storage::Config config{};
  const auto dir = MakeDiskConfig(config);
  std::unique_ptr<memgraph::storage::Storage> storage = std::make_unique<memgraph::storage::DiskStorage>(config);

  {  // WRITE held: UniqueAccess and ReadOnlyAccess must time out.
    auto holder = storage->Access(memgraph::storage::StorageAccessType::WRITE, {}, kTimeout);
    ASSERT_TRUE(holder);
    ExpectAccessTimeout<memgraph::storage::UniqueAccessTimeout>(
        [&] { return storage->UniqueAccess({}, kTimeout); }, "DiskStorage::UniqueAccess", holder);
  }
  {
    auto holder = storage->Access(memgraph::storage::StorageAccessType::WRITE, {}, kTimeout);
    ASSERT_TRUE(holder);
    ExpectAccessTimeout<memgraph::storage::ReadOnlyAccessTimeout>(
        [&] { return storage->ReadOnlyAccess({}, kTimeout); }, "DiskStorage::ReadOnlyAccess", holder);
  }
  {  // UNIQUE held: Access(WRITE) must time out.
    auto holder = storage->UniqueAccess({}, kTimeout);
    ASSERT_TRUE(holder);
    ExpectAccessTimeout<memgraph::storage::SharedAccessTimeout>(
        [&] { return storage->Access(memgraph::storage::StorageAccessType::WRITE, {}, kTimeout); },
        "DiskStorage::Access(WRITE)",
        holder);
  }

  storage.reset();
  std::filesystem::remove_all(dir);
}

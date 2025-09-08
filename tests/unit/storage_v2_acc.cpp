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

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"

namespace {
constexpr auto kTimeout = std::chrono::milliseconds(100);
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

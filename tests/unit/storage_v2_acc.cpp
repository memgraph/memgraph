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

namespace {
constexpr auto kTimeout = std::chrono::milliseconds(100);
}  // namespace

/// Test that unqiue/shared accessors can timeout and not deadlock
TEST(StorageV2Acc, Timeouts) {
  std::unique_ptr<memgraph::storage::Storage> storage(
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{}));

  {
    auto shared_acc = storage->Access({}, kTimeout);
    ASSERT_TRUE(shared_acc);
    ASSERT_THROW(storage->UniqueAccess({}, kTimeout), memgraph::storage::UniqueAccessTimeout);
    auto shared_acc2 = storage->Access({}, kTimeout);
    ASSERT_TRUE(shared_acc2);
  }
  {
    auto unique_acc = storage->UniqueAccess({}, kTimeout);
    ASSERT_TRUE(unique_acc);
    ASSERT_THROW(storage->Access({}, kTimeout), memgraph::storage::SharedAccessTimeout);
    ASSERT_THROW(storage->UniqueAccess({}, kTimeout), memgraph::storage::UniqueAccessTimeout);
  }
}

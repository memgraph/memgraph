// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>

#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/file.hpp"

class DiskStorageTest : public ::testing::TestWithParam<bool> {};

TEST_F(DiskStorageTest, CreateDiskStorageInDataDirectory) {
  memgraph::storage::Config config;
  auto storage = std::make_unique<memgraph::storage::DiskStorage>(config);
  ASSERT_TRUE(memgraph::utils::DirExists(config.disk.main_storage_directory));
}

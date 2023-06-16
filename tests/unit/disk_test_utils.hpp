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

#pragma once

#include <filesystem>
#include "storage/v2/config.hpp"
#include "storage/v2/disk/storage.hpp"

namespace disk_test_utils {

memgraph::storage::Config GenerateOnDiskConfig(const std::string &testName) {
  return {.disk = {.main_storage_directory = "rocksdb_" + testName + "_db",
                   .label_index_directory = "rocksdb_" + testName + "_label_index",
                   .label_property_index_directory = "rocksdb_" + testName + "_label_property_index",
                   .unique_constraints_directory = "rocksdb_" + testName + "_unique_constraints"}};
}

void RemoveRocksDbDirs(const std::string &testName) {
  std::filesystem::remove_all("rocksdb_" + testName + "_db");
  std::filesystem::remove_all("rocksdb_" + testName + "_label_index");
  std::filesystem::remove_all("rocksdb_" + testName + "_label_property_index");
  std::filesystem::remove_all("rocksdb_" + testName + "_unique_constraints");
}

uint64_t GetRealNumberOfEntriesInRocksDB(memgraph::storage::DiskStorage *disk_storage) {
  uint64_t num_keys = 0;
  disk_storage->GetRocksDBStorage()->db_->GetAggregatedIntProperty("rocksdb.estimate-num-keys", &num_keys);
  return num_keys;
}

}  // namespace disk_test_utils

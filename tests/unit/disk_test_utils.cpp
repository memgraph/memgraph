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

#include "disk_test_utils.hpp"

#include <rocksdb/utilities/transaction_db.h>
#include <filesystem>
#include <memory>

#include "dbms/constants.hpp"
#include "storage/v2/disk/storage.hpp"

namespace disk_test_utils {

memgraph::storage::Config GenerateOnDiskConfig(const std::string &testName) {
  return {.disk = {.main_storage_directory = "rocksdb_" + testName + "_db",
                   .label_index_directory = "rocksdb_" + testName + "_label_index",
                   .label_property_index_directory = "rocksdb_" + testName + "_label_property_index",
                   .unique_constraints_directory = "rocksdb_" + testName + "_unique_constraints",
                   .name_id_mapper_directory = "rocksdb_" + testName + "_name_id_mapper",
                   .id_name_mapper_directory = "rocksdb_" + testName + "_id_name_mapper",
                   .durability_directory = "rocksdb_" + testName + "_durability",
                   .wal_directory = "rocksdb_" + testName + "_wal"},
          .salient = {.name = memgraph::dbms::kDefaultDB}};
}

void RemoveRocksDbDirs(const std::string &testName) {
  std::filesystem::remove_all("rocksdb_" + testName + "_db");
  std::filesystem::remove_all("rocksdb_" + testName + "_label_index");
  std::filesystem::remove_all("rocksdb_" + testName + "_label_property_index");
  std::filesystem::remove_all("rocksdb_" + testName + "_unique_constraints");
  std::filesystem::remove_all("rocksdb_" + testName + "_name_id_mapper");
  std::filesystem::remove_all("rocksdb_" + testName + "_id_name_mapper");
  std::filesystem::remove_all("rocksdb_" + testName + "_durability");
  std::filesystem::remove_all("rocksdb_" + testName + "_wal");
}

uint64_t GetRealNumberOfEntriesInRocksDB(rocksdb::TransactionDB *disk_storage) {
  uint64_t num_keys = 0;
  disk_storage->GetAggregatedIntProperty("rocksdb.estimate-num-keys", &num_keys);
  return num_keys;
}

std::unique_ptr<memgraph::storage::Storage> CreateDiskStorage(memgraph::storage::Config config) {
  return std::make_unique<memgraph::storage::DiskStorage>(std::move(config));
}

}  // namespace disk_test_utils

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
#include <unistd.h>
#include <filesystem>
#include <memory>
#include <string>

#include "dbms/constants.hpp"
#include "storage/v2/disk/storage.hpp"

namespace disk_test_utils {

namespace {
// These are relative to the working directory and are removed wholesale, so the prefix has to be
// private to this process: test binaries run concurrently out of a shared working directory, and a
// name two of them agree on has one deleting the other's storage mid-use.
std::string DirectoryPrefix(const std::string &testName) {
  return "rocksdb_" + testName + "_" + std::to_string(static_cast<int>(getpid())) + "_";
}
}  // namespace

memgraph::storage::Config GenerateOnDiskConfig(const std::string &testName) {
  const auto prefix = DirectoryPrefix(testName);
  return {.disk = {.main_storage_directory = prefix + "db",
                   .label_index_directory = prefix + "label_index",
                   .label_property_index_directory = prefix + "label_property_index",
                   .unique_constraints_directory = prefix + "unique_constraints",
                   .name_id_mapper_directory = prefix + "name_id_mapper",
                   .id_name_mapper_directory = prefix + "id_name_mapper",
                   .durability_directory = prefix + "durability",
                   .wal_directory = prefix + "wal"},
          .salient = {.name = memgraph::dbms::kDefaultDB}};
}

void RemoveRocksDbDirs(const std::string &testName) {
  const auto prefix = DirectoryPrefix(testName);
  for (const auto *suffix : {"db",
                             "label_index",
                             "label_property_index",
                             "unique_constraints",
                             "name_id_mapper",
                             "id_name_mapper",
                             "durability",
                             "wal"}) {
    std::filesystem::remove_all(prefix + suffix);
  }
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

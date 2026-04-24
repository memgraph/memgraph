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

#pragma once

#include <cstdint>
#include <string>

#include "storage/v2/config.hpp"
#include "storage/v2/disk/storage.hpp"

namespace rocksdb {
class TransactionDB;
}  // namespace rocksdb

namespace disk_test_utils {

memgraph::storage::Config GenerateOnDiskConfig(const std::string &testName);

void RemoveRocksDbDirs(const std::string &testName);

uint64_t GetRealNumberOfEntriesInRocksDB(rocksdb::TransactionDB *disk_storage);

}  // namespace disk_test_utils

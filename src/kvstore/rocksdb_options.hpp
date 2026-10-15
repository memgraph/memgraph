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

#include <gflags/gflags_declare.h>
#include <rocksdb/options.h>

// Defined here rather than in mg-flags because mg-kvstore cannot depend on it (mg-flags -> mg-settings -> mg-kvstore).
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(storage_rocksdb_info_log_level);
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_uint64(storage_rocksdb_keep_log_file_num);

namespace memgraph::kvstore {

/// Apply the info log settings shared by every RocksDB instance Memgraph opens: the key-value stores backing auth,
/// triggers, streams, settings, replication/system state and the disk storage.
void ApplyRocksDBLogConfig(rocksdb::Options &options);

}  // namespace memgraph::kvstore

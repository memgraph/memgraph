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

#include <rocksdb/options.h>
#include <string_view>

namespace memgraph::storage {

/// Apply RocksDB tuning parameters to options.
void ApplyRocksDBConfig(rocksdb::Options &options, std::string_view info_log_level, bool enable_thread_tracking);

}  // namespace memgraph::storage

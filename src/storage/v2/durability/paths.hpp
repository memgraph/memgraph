// Copyright 2024 Memgraph Ltd.
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

#include "utils/timestamp.hpp"

namespace memgraph::storage::durability {

static const std::string kSnapshotDirectory{"snapshots"};
static const std::string kWalDirectory{"wal"};
static const std::string kBackupDirectory{".backup"};
static const std::string kLockFile{".lock"};

// This is the prefix used for Snapshot and WAL filenames. It is a timestamp
// format that equals to: YYYYmmddHHMMSSffffff
const std::string kTimestampFormat = "{:04d}{:02d}{:02d}{:02d}{:02d}{:02d}{:06d}";

// Generates the name for a snapshot in a well-defined sortable format with the
// start timestamp appended to the file name.
inline std::string MakeSnapshotName(uint64_t start_timestamp) {
  std::string date_str = utils::Timestamp::Now().ToString(kTimestampFormat);
  return date_str + "_timestamp_" + std::to_string(start_timestamp);
}

// Generates the name for a WAL file in a well-defined sortable format.
inline std::string MakeWalName() {
  std::string date_str = utils::Timestamp::Now().ToString(kTimestampFormat);
  return date_str + "_current";
}

// Generates the name for a WAL file in a well-defined sortable format with the
// range of timestamps contained [from, to] appended to the name.
inline std::string RemakeWalName(const std::string &current_name, uint64_t from_timestamp, uint64_t to_timestamp) {
  return current_name.substr(0, current_name.size() - 8) + "_from_" + std::to_string(from_timestamp) + "_to_" +
         std::to_string(to_timestamp);
}

}  // namespace memgraph::storage::durability

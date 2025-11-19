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

#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

#include "utils/timestamp.hpp"

namespace memgraph::storage::durability {

static constexpr std::string_view kSnapshotDirectory{"snapshots"};
static constexpr std::string_view kWalDirectory{"wal"};
static constexpr std::string_view kBackupDirectory{".backup"};
static constexpr std::string_view kLockFile{".lock"};
static constexpr std::string_view kReplicaDurabilityDirectory{"durability"};

// This is the prefix used for Snapshot and WAL filenames. It is a timestamp
// format that equals to: YYYYmmddHHMMSSffffff
const std::string kTimestampFormat = "{:04d}{:02d}{:02d}{:02d}{:02d}{:02d}{:06d}";

// Generates the name for a snapshot in a well-defined sortable format with the
// start timestamp appended to the file name.
inline auto MakeSnapshotName(uint64_t const last_durable_ts) -> std::string {
  auto const date_str = utils::Timestamp::Now().ToString(kTimestampFormat);
  return fmt::format("{}_timestamp_{}", date_str, std::to_string(last_durable_ts));
}

// Generates the name for a WAL file in a well-defined sortable format.
inline auto MakeWalName() -> std::string {
  auto const date_str = utils::Timestamp::Now().ToString(kTimestampFormat);
  return fmt::format("{}_current", date_str);
}

// Generates the path for a WAL file in a well-defined sortable format with the
// range of timestamps contained [from, to] appended to the name.
inline auto RemakeWalName(const std::filesystem::path &current_path, uint64_t const from_timestamp,
                          uint64_t const to_timestamp) -> std::string {
  auto const current_filename = current_path.filename().string();
  // 8 is the size of _current
  return fmt::format("{}/{}_from_{}_to_{}", current_path.parent_path().string(),
                     current_filename.substr(0, current_filename.size() - 8), std::to_string(from_timestamp),
                     std::to_string(to_timestamp));
}

}  // namespace memgraph::storage::durability

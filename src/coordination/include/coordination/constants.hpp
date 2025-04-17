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

#include <string>
#include <string_view>

using namespace std::string_view_literals;

namespace memgraph::coordination {

enum class LogStoreVersion : int {
  kV1 = 1,  // current version
  kV2 = 2,  // added last_committed_idx_
  // kv3 = 3,  // when new version is added
};

constexpr auto kActiveVersion = LogStoreVersion::kV2;

constexpr std::string_view kLogStoreVersion = "log_store_version";  // top level

constexpr std::string_view kLastCommitedIdx = "last_committed_idx_";  // top level

// snapshots
constexpr std::string_view kSnapshotIdPrefix = "snapshot_id_";  // top level
constexpr std::string_view kCoordClusterState = "coord_cluster_state";
constexpr std::string_view kLastLogIdx = "last_log_idx";
constexpr std::string_view kLastLogTerm = "last_log_term";
constexpr std::string_view kSize = "size";
constexpr std::string_view kLastConfig = "last_config";
constexpr std::string_view kType = "type";

// logs
constexpr std::string_view kLogEntryPrefix = "log_entry_";    // top level
constexpr std::string_view kLastLogEntry = "last_log_entry";  // top level
constexpr std::string_view kStartIdx = "start_idx";           // top level

const std::string kLogEntryDataKey = "data";
const std::string kLogEntryTermKey = "term";
const std::string kLogEntryValTypeKey = "val_type";

// routing policies
constexpr auto kEnabledReadsOnMain = "enabled_reads_on_main"sv;

// cluster state
constexpr int MAX_SNAPSHOTS = 3;
constexpr auto kUuid = "uuid"sv;
constexpr auto kDataInstances =
    "cluster_state"sv;  // called "cluster_state" because at the beginning data instances were considered cluster state
constexpr std::string_view kCoordinatorInstances = "coordinator_instances";
constexpr std::string_view kMainUUID = "current_main_uuid";
constexpr std::string_view kConfig{"config"};
constexpr std::string_view kStatus{"status"};

}  // namespace memgraph::coordination

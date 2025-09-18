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
#include <string>
#include <string_view>

namespace memgraph::coordination {

using namespace std::string_view_literals;

enum class LogStoreVersion : uint8_t {
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
constexpr int kInitialStartIdx = 1;
constexpr int kInitialLastLogEntry = 0;

const std::string kLogEntryDataKey = "data";
const std::string kLogEntryTermKey = "term";
const std::string kLogEntryValTypeKey = "val_type";

// routing policies
constexpr auto kEnabledReadsOnMain = "enabled_reads_on_main"sv;
constexpr auto kSyncFailoverOnly = "sync_failover_only"sv;
constexpr auto kMaxFailoverLagOnReplica = "max_failover_replica_lag"sv;

// cluster state
constexpr int MAX_SNAPSHOTS = 3;
constexpr auto kUuid = "uuid"sv;
constexpr std::string_view kDataInstances = "data_instances";
constexpr std::string_view kClusterState = "cluster_state";
constexpr std::string_view kCoordinatorInstances = "coordinator_instances";
constexpr std::string_view kMainUUID = "current_main_uuid";
constexpr std::string_view kConfig{"config"};
constexpr std::string_view kStatus{"status"};
// CoordinatorInstanceConfig
constexpr std::string_view kCoordinatorId{"coordinator_id"};
constexpr std::string_view kCoordinatorServer{"coordinator_server"};
constexpr std::string_view kBoltServer{"bolt_server"};
constexpr std::string_view kManagementServer{"management_server"};
constexpr std::string_view kCoordinatorHostname{"coordinator_hostname"};
// ReplicationClientInfo
constexpr std::string_view kInstanceName{"instance_name"};
constexpr std::string_view kReplicationMode{"replication_mode"};
constexpr std::string_view kReplicationServer{"replication_server"};
// DataInstanceConfig
constexpr std::string_view kMgtServer{"mgt_server"};
constexpr std::string_view kReplicationClientInfo{"replication_client_info"};

}  // namespace memgraph::coordination

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
#include <cstring>
#include <string>
#include <utility>

#include "rpc/messages.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"
#include "storage/v2/config.hpp"
#include "utils/enum.hpp"
#include "utils/uuid.hpp"

namespace memgraph::storage::replication {

struct AppendDeltasReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(AppendDeltasReq *self, memgraph::slk::Reader *reader);
  static void Save(const AppendDeltasReq &self, memgraph::slk::Builder *builder);
  AppendDeltasReq() = default;
  AppendDeltasReq(const utils::UUID &main_uuid, const utils::UUID &uuid, uint64_t previous_commit_timestamp,
                  uint64_t seq_num)
      : main_uuid{main_uuid}, uuid{uuid}, previous_commit_timestamp(previous_commit_timestamp), seq_num(seq_num) {}

  utils::UUID main_uuid;
  utils::UUID uuid;
  uint64_t previous_commit_timestamp;
  uint64_t seq_num;
};

struct AppendDeltasRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(AppendDeltasRes *self, memgraph::slk::Reader *reader);
  static void Save(const AppendDeltasRes &self, memgraph::slk::Builder *builder);
  AppendDeltasRes() = default;
  AppendDeltasRes(bool success, uint64_t current_commit_timestamp)
      : success(success), current_commit_timestamp(current_commit_timestamp) {}

  bool success;
  uint64_t current_commit_timestamp;
};

using AppendDeltasRpc = rpc::RequestResponse<AppendDeltasReq, AppendDeltasRes>;

struct HeartbeatReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(HeartbeatReq *self, memgraph::slk::Reader *reader);
  static void Save(const HeartbeatReq &self, memgraph::slk::Builder *builder);
  HeartbeatReq() = default;
  HeartbeatReq(const utils::UUID &main_uuid, const utils::UUID &uuid, uint64_t main_commit_timestamp,
               std::string epoch_id)
      : main_uuid(main_uuid), uuid{uuid}, main_commit_timestamp(main_commit_timestamp), epoch_id(std::move(epoch_id)) {}

  utils::UUID main_uuid;
  utils::UUID uuid;
  uint64_t main_commit_timestamp;
  std::string epoch_id;
};

struct HeartbeatRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(HeartbeatRes *self, memgraph::slk::Reader *reader);
  static void Save(const HeartbeatRes &self, memgraph::slk::Builder *builder);
  HeartbeatRes() = default;
  HeartbeatRes(bool success, uint64_t current_commit_timestamp, std::string epoch_id)
      : success(success), current_commit_timestamp(current_commit_timestamp), epoch_id(std::move(epoch_id)) {}

  bool success;
  uint64_t current_commit_timestamp;
  std::string epoch_id;
};

using HeartbeatRpc = rpc::RequestResponse<HeartbeatReq, HeartbeatRes>;

struct SnapshotReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(SnapshotReq *self, memgraph::slk::Reader *reader);
  static void Save(const SnapshotReq &self, memgraph::slk::Builder *builder);
  SnapshotReq() = default;
  explicit SnapshotReq(const utils::UUID &main_uuid, const utils::UUID &uuid) : main_uuid{main_uuid}, uuid{uuid} {}

  utils::UUID main_uuid;
  utils::UUID uuid;
};

struct SnapshotRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(SnapshotRes *self, memgraph::slk::Reader *reader);
  static void Save(const SnapshotRes &self, memgraph::slk::Builder *builder);
  SnapshotRes() = default;
  SnapshotRes(bool success, uint64_t current_commit_timestamp)
      : success(success), current_commit_timestamp(current_commit_timestamp) {}

  bool success;
  uint64_t current_commit_timestamp;
};

using SnapshotRpc = rpc::RequestResponse<SnapshotReq, SnapshotRes>;

struct WalFilesReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(WalFilesReq *self, memgraph::slk::Reader *reader);
  static void Save(const WalFilesReq &self, memgraph::slk::Builder *builder);
  WalFilesReq() = default;
  explicit WalFilesReq(const utils::UUID &main_uuid, const utils::UUID &uuid, uint64_t file_number)
      : main_uuid{main_uuid}, uuid{uuid}, file_number(file_number) {}

  utils::UUID main_uuid;
  utils::UUID uuid;
  uint64_t file_number;
};

struct WalFilesRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(WalFilesRes *self, memgraph::slk::Reader *reader);
  static void Save(const WalFilesRes &self, memgraph::slk::Builder *builder);
  WalFilesRes() = default;
  WalFilesRes(bool success, uint64_t current_commit_timestamp)
      : success(success), current_commit_timestamp(current_commit_timestamp) {}

  bool success;
  uint64_t current_commit_timestamp;
};

using WalFilesRpc = rpc::RequestResponse<WalFilesReq, WalFilesRes>;

struct CurrentWalReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(CurrentWalReq *self, memgraph::slk::Reader *reader);
  static void Save(const CurrentWalReq &self, memgraph::slk::Builder *builder);
  CurrentWalReq() = default;
  explicit CurrentWalReq(const utils::UUID &main_uuid, const utils::UUID &uuid) : main_uuid(main_uuid), uuid{uuid} {}

  utils::UUID main_uuid;
  utils::UUID uuid;
};

struct CurrentWalRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(CurrentWalRes *self, memgraph::slk::Reader *reader);
  static void Save(const CurrentWalRes &self, memgraph::slk::Builder *builder);
  CurrentWalRes() = default;
  CurrentWalRes(bool success, uint64_t current_commit_timestamp)
      : success(success), current_commit_timestamp(current_commit_timestamp) {}

  bool success;
  uint64_t current_commit_timestamp;
};

using CurrentWalRpc = rpc::RequestResponse<CurrentWalReq, CurrentWalRes>;

struct TimestampReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(TimestampReq *self, memgraph::slk::Reader *reader);
  static void Save(const TimestampReq &self, memgraph::slk::Builder *builder);
  TimestampReq() = default;
  explicit TimestampReq(const utils::UUID &main_uuid, const utils::UUID &uuid) : main_uuid(main_uuid), uuid{uuid} {}

  utils::UUID main_uuid;
  utils::UUID uuid;
};

struct TimestampRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(TimestampRes *self, memgraph::slk::Reader *reader);
  static void Save(const TimestampRes &self, memgraph::slk::Builder *builder);
  TimestampRes() = default;
  TimestampRes(bool success, uint64_t current_commit_timestamp)
      : success(success), current_commit_timestamp(current_commit_timestamp) {}

  bool success;
  uint64_t current_commit_timestamp;
};

using TimestampRpc = rpc::RequestResponse<TimestampReq, TimestampRes>;

struct CreateDatabaseReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(CreateDatabaseReq *self, memgraph::slk::Reader *reader);
  static void Save(const CreateDatabaseReq &self, memgraph::slk::Builder *builder);
  CreateDatabaseReq() = default;
  CreateDatabaseReq(const utils::UUID &main_uuid, std::string epoch_id, uint64_t expected_group_timestamp,
                    uint64_t new_group_timestamp, storage::SalientConfig config)
      : main_uuid(main_uuid),
        epoch_id(std::move(epoch_id)),
        expected_group_timestamp{expected_group_timestamp},
        new_group_timestamp(new_group_timestamp),
        config(std::move(config)) {}

  utils::UUID main_uuid;
  std::string epoch_id;
  uint64_t expected_group_timestamp;
  uint64_t new_group_timestamp;
  storage::SalientConfig config;
};

struct CreateDatabaseRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  enum class Result : uint8_t { SUCCESS, NO_NEED, FAILURE, /* Leave at end */ N };

  static void Load(CreateDatabaseRes *self, memgraph::slk::Reader *reader);
  static void Save(const CreateDatabaseRes &self, memgraph::slk::Builder *builder);
  CreateDatabaseRes() = default;
  explicit CreateDatabaseRes(Result res) : result(res) {}

  Result result;
};

using CreateDatabaseRpc = rpc::RequestResponse<CreateDatabaseReq, CreateDatabaseRes>;

struct DropDatabaseReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(DropDatabaseReq *self, memgraph::slk::Reader *reader);
  static void Save(const DropDatabaseReq &self, memgraph::slk::Builder *builder);
  DropDatabaseReq() = default;
  DropDatabaseReq(const utils::UUID &main_uuid, std::string epoch_id, uint64_t expected_group_timestamp,
                  uint64_t new_group_timestamp, const utils::UUID &uuid)
      : main_uuid(main_uuid),
        epoch_id(std::move(epoch_id)),
        expected_group_timestamp{expected_group_timestamp},
        new_group_timestamp(new_group_timestamp),
        uuid(uuid) {}

  utils::UUID main_uuid;
  std::string epoch_id;
  uint64_t expected_group_timestamp;
  uint64_t new_group_timestamp;
  utils::UUID uuid;
};

struct DropDatabaseRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  enum class Result : uint8_t { SUCCESS, NO_NEED, FAILURE, /* Leave at end */ N };

  static void Load(DropDatabaseRes *self, memgraph::slk::Reader *reader);
  static void Save(const DropDatabaseRes &self, memgraph::slk::Builder *builder);
  DropDatabaseRes() = default;
  explicit DropDatabaseRes(Result res) : result(res) {}

  Result result;
};

using DropDatabaseRpc = rpc::RequestResponse<DropDatabaseReq, DropDatabaseRes>;

struct SystemRecoveryReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(SystemRecoveryReq *self, memgraph::slk::Reader *reader);
  static void Save(const SystemRecoveryReq &self, memgraph::slk::Builder *builder);
  SystemRecoveryReq() = default;
  SystemRecoveryReq(const utils::UUID &main_uuid, uint64_t forced_group_timestamp,
                    std::vector<storage::SalientConfig> database_configs)
      : main_uuid(main_uuid),
        forced_group_timestamp{forced_group_timestamp},
        database_configs(std::move(database_configs)) {}

  utils::UUID main_uuid;
  uint64_t forced_group_timestamp;
  std::vector<storage::SalientConfig> database_configs;
};

struct SystemRecoveryRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  enum class Result : uint8_t { SUCCESS, NO_NEED, FAILURE, /* Leave at end */ N };

  static void Load(SystemRecoveryRes *self, memgraph::slk::Reader *reader);
  static void Save(const SystemRecoveryRes &self, memgraph::slk::Builder *builder);
  SystemRecoveryRes() = default;
  explicit SystemRecoveryRes(Result res) : result(res) {}

  Result result;
};

using SystemRecoveryRpc = rpc::RequestResponse<SystemRecoveryReq, SystemRecoveryRes>;

}  // namespace memgraph::storage::replication

// SLK serialization declarations
namespace memgraph::slk {

void Save(const memgraph::storage::replication::TimestampRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::TimestampRes *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::TimestampReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::TimestampReq *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::CurrentWalRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::CurrentWalRes *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::CurrentWalReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::CurrentWalReq *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::WalFilesRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::WalFilesRes *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::WalFilesReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::WalFilesReq *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::SnapshotRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::SnapshotRes *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::SnapshotReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::SnapshotReq *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::HeartbeatRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::HeartbeatRes *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::HeartbeatReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::HeartbeatReq *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::AppendDeltasRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::AppendDeltasRes *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::AppendDeltasReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::AppendDeltasReq *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::SalientConfig &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::SalientConfig *self, memgraph::slk::Reader *reader);

}  // namespace memgraph::slk

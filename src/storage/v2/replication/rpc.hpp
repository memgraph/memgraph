// Copyright 2023 Memgraph Ltd.
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

namespace memgraph::storage::replication {

struct AppendDeltasReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(AppendDeltasReq *self, memgraph::slk::Reader *reader);
  static void Save(const AppendDeltasReq &self, memgraph::slk::Builder *builder);
  AppendDeltasReq() = default;
  AppendDeltasReq(std::string name, uint64_t previous_commit_timestamp, uint64_t seq_num)
      : db_name(std::move(name)), previous_commit_timestamp(previous_commit_timestamp), seq_num(seq_num) {}

  std::string db_name;
  uint64_t previous_commit_timestamp;
  uint64_t seq_num;
};

struct AppendDeltasRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(AppendDeltasRes *self, memgraph::slk::Reader *reader);
  static void Save(const AppendDeltasRes &self, memgraph::slk::Builder *builder);
  AppendDeltasRes() = default;
  AppendDeltasRes(std::string name, bool success, uint64_t current_commit_timestamp)
      : db_name(std::move(name)), success(success), current_commit_timestamp(current_commit_timestamp) {}

  std::string db_name;
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
  HeartbeatReq(std::string name, uint64_t main_commit_timestamp, std::string epoch_id)
      : db_name(std::move(name)), main_commit_timestamp(main_commit_timestamp), epoch_id(std::move(epoch_id)) {}

  std::string db_name;
  uint64_t main_commit_timestamp;
  std::string epoch_id;
};

struct HeartbeatRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(HeartbeatRes *self, memgraph::slk::Reader *reader);
  static void Save(const HeartbeatRes &self, memgraph::slk::Builder *builder);
  HeartbeatRes() = default;
  HeartbeatRes(std::string name, bool success, uint64_t current_commit_timestamp, std::string epoch_id)
      : db_name(std::move(name)),
        success(success),
        current_commit_timestamp(current_commit_timestamp),
        epoch_id(std::move(epoch_id)) {}

  std::string db_name;
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
  explicit SnapshotReq(std::string name) : db_name(std::move(name)) {}

  std::string db_name;
};

struct SnapshotRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(SnapshotRes *self, memgraph::slk::Reader *reader);
  static void Save(const SnapshotRes &self, memgraph::slk::Builder *builder);
  SnapshotRes() = default;
  SnapshotRes(std::string name, bool success, uint64_t current_commit_timestamp)
      : db_name(std::move(name)), success(success), current_commit_timestamp(current_commit_timestamp) {}

  std::string db_name;
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
  explicit WalFilesReq(std::string name, uint64_t file_number) : db_name(std::move(name)), file_number(file_number) {}

  std::string db_name;
  uint64_t file_number;
};

struct WalFilesRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(WalFilesRes *self, memgraph::slk::Reader *reader);
  static void Save(const WalFilesRes &self, memgraph::slk::Builder *builder);
  WalFilesRes() = default;
  WalFilesRes(std::string name, bool success, uint64_t current_commit_timestamp)
      : db_name(std::move(name)), success(success), current_commit_timestamp(current_commit_timestamp) {}

  std::string db_name;
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
  explicit CurrentWalReq(std::string name) : db_name(std::move(name)) {}

  std::string db_name;
};

struct CurrentWalRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(CurrentWalRes *self, memgraph::slk::Reader *reader);
  static void Save(const CurrentWalRes &self, memgraph::slk::Builder *builder);
  CurrentWalRes() = default;
  CurrentWalRes(std::string name, bool success, uint64_t current_commit_timestamp)
      : db_name(std::move(name)), success(success), current_commit_timestamp(current_commit_timestamp) {}

  std::string db_name;
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
  explicit TimestampReq(std::string name) : db_name(std::move(name)) {}

  std::string db_name;
};

struct TimestampRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(TimestampRes *self, memgraph::slk::Reader *reader);
  static void Save(const TimestampRes &self, memgraph::slk::Builder *builder);
  TimestampRes() = default;
  TimestampRes(std::string name, bool success, uint64_t current_commit_timestamp)
      : db_name(std::move(name)), success(success), current_commit_timestamp(current_commit_timestamp) {}

  std::string db_name;
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
  CreateDatabaseReq(std::string epoch_id, uint64_t group_timestamp, storage::SalientConfig config)
      : epoch_id(std::move(epoch_id)), group_timestamp(group_timestamp), config(std::move(config)) {}

  std::string epoch_id;
  uint64_t group_timestamp;
  storage::SalientConfig config;
};

struct CreateDatabaseRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  enum class Result : uint8_t { SUCCESS, NO_NEED, FAILURE };

  static void Load(CreateDatabaseRes *self, memgraph::slk::Reader *reader);
  static void Save(const CreateDatabaseRes &self, memgraph::slk::Builder *builder);
  CreateDatabaseRes() = default;
  explicit CreateDatabaseRes(Result res) : result(res) {}

  Result result;
};

using CreateDatabaseRpc = rpc::RequestResponse<CreateDatabaseReq, CreateDatabaseRes>;

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

void Save(const memgraph::storage::replication::CreateDatabaseReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::CreateDatabaseReq *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::CreateDatabaseRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::CreateDatabaseRes *self, memgraph::slk::Reader *reader);

}  // namespace memgraph::slk

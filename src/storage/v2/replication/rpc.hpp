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
#include <utility>

#include "rpc/messages.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"
#include "storage/v2/config.hpp"
#include "utils/uuid.hpp"

namespace memgraph::storage::replication {

struct AppendDeltasReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(AppendDeltasReq *self, memgraph::slk::Reader *reader);
  static void Save(const AppendDeltasReq &self, memgraph::slk::Builder *builder);
  AppendDeltasReq() = default;
  AppendDeltasReq(const utils::UUID &main_uuid, const utils::UUID &uuid, uint64_t const seq_num)
      : main_uuid{main_uuid}, uuid{uuid}, seq_num(seq_num) {}

  utils::UUID main_uuid;
  utils::UUID uuid;
  uint64_t seq_num;
};

struct AppendDeltasRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(AppendDeltasRes *self, memgraph::slk::Reader *reader);
  static void Save(const AppendDeltasRes &self, memgraph::slk::Builder *builder);
  AppendDeltasRes() = default;

  explicit AppendDeltasRes(bool const success) : success(success) {}

  bool success;
};

struct InProgressRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  InProgressRes() = default;
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
  explicit SnapshotReq(const utils::UUID &main_uuid, const utils::UUID &storage_uuid)
      : main_uuid{main_uuid}, storage_uuid{storage_uuid} {}

  utils::UUID main_uuid;
  utils::UUID storage_uuid;
};

struct SnapshotRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(SnapshotRes *self, slk::Reader *reader);
  static void Save(const SnapshotRes &self, slk::Builder *builder);
  SnapshotRes() = default;

  explicit SnapshotRes(std::optional<uint64_t> current_commit_timestamp)
      : current_commit_timestamp(std::move(current_commit_timestamp)) {}

  std::optional<uint64_t> current_commit_timestamp;
};

using SnapshotRpc = rpc::RequestResponse<SnapshotReq, SnapshotRes>;

struct WalFilesReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(WalFilesReq *self, slk::Reader *reader);
  static void Save(const WalFilesReq &self, slk::Builder *builder);
  WalFilesReq() = default;
  explicit WalFilesReq(uint64_t const file_number, const utils::UUID &main_uuid, const utils::UUID &storage_uuid,
                       bool const reset_needed)
      : file_number(file_number), main_uuid{main_uuid}, uuid{storage_uuid}, reset_needed{reset_needed} {}

  uint64_t file_number;
  utils::UUID main_uuid;
  utils::UUID uuid;
  bool reset_needed;
};

struct WalFilesRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(WalFilesRes *self, memgraph::slk::Reader *reader);
  static void Save(const WalFilesRes &self, memgraph::slk::Builder *builder);
  WalFilesRes() = default;
  explicit WalFilesRes(std::optional<uint64_t> current_commit_timestamp)
      : current_commit_timestamp(current_commit_timestamp) {}

  std::optional<uint64_t> current_commit_timestamp;
};

using WalFilesRpc = rpc::RequestResponse<WalFilesReq, WalFilesRes>;

struct CurrentWalReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(CurrentWalReq *self, memgraph::slk::Reader *reader);
  static void Save(const CurrentWalReq &self, memgraph::slk::Builder *builder);
  CurrentWalReq() = default;
  explicit CurrentWalReq(const utils::UUID &main_uuid, const utils::UUID &uuid, bool const reset_needed)
      : main_uuid(main_uuid), uuid{uuid}, reset_needed{reset_needed} {}

  utils::UUID main_uuid;
  utils::UUID uuid;
  bool reset_needed;
};

struct CurrentWalRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(CurrentWalRes *self, memgraph::slk::Reader *reader);
  static void Save(const CurrentWalRes &self, memgraph::slk::Builder *builder);
  CurrentWalRes() = default;
  explicit CurrentWalRes(std::optional<uint64_t> current_commit_timestamp)
      : current_commit_timestamp(current_commit_timestamp) {}

  std::optional<uint64_t> current_commit_timestamp;
};

using CurrentWalRpc = rpc::RequestResponse<CurrentWalReq, CurrentWalRes>;

}  // namespace memgraph::storage::replication

// SLK serialization declarations
namespace memgraph::slk {

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

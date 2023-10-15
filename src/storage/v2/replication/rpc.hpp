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

#include "rpc/messages.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"

namespace memgraph {

namespace storage {

namespace replication {

struct AppendDeltasReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(AppendDeltasReq *self, memgraph::slk::Reader *reader);
  static void Save(const AppendDeltasReq &self, memgraph::slk::Builder *builder);
  AppendDeltasReq() {}
  AppendDeltasReq(uint64_t previous_commit_timestamp, uint64_t seq_num)
      : previous_commit_timestamp(previous_commit_timestamp), seq_num(seq_num) {}

  uint64_t previous_commit_timestamp;
  uint64_t seq_num;
};

struct AppendDeltasRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(AppendDeltasRes *self, memgraph::slk::Reader *reader);
  static void Save(const AppendDeltasRes &self, memgraph::slk::Builder *builder);
  AppendDeltasRes() {}
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
  HeartbeatReq() {}
  HeartbeatReq(uint64_t main_commit_timestamp, std::string epoch_id)
      : main_commit_timestamp(main_commit_timestamp), epoch_id(std::move(epoch_id)) {}

  uint64_t main_commit_timestamp;
  std::string epoch_id;
};

struct HeartbeatRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(HeartbeatRes *self, memgraph::slk::Reader *reader);
  static void Save(const HeartbeatRes &self, memgraph::slk::Builder *builder);
  HeartbeatRes() {}
  HeartbeatRes(bool success, uint64_t current_commit_timestamp, std::string epoch_id)
      : success(success), current_commit_timestamp(current_commit_timestamp), epoch_id(epoch_id) {}

  bool success;
  uint64_t current_commit_timestamp;
  std::string epoch_id;
};

using HeartbeatRpc = rpc::RequestResponse<HeartbeatReq, HeartbeatRes>;

struct FrequentHeartbeatReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(FrequentHeartbeatReq *self, memgraph::slk::Reader *reader);
  static void Save(const FrequentHeartbeatReq &self, memgraph::slk::Builder *builder);
  FrequentHeartbeatReq() {}
};

struct FrequentHeartbeatRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(FrequentHeartbeatRes *self, memgraph::slk::Reader *reader);
  static void Save(const FrequentHeartbeatRes &self, memgraph::slk::Builder *builder);
  FrequentHeartbeatRes() {}
  explicit FrequentHeartbeatRes(bool success) : success(success) {}

  bool success;
};

using FrequentHeartbeatRpc = rpc::RequestResponse<FrequentHeartbeatReq, FrequentHeartbeatRes>;

struct SnapshotReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(SnapshotReq *self, memgraph::slk::Reader *reader);
  static void Save(const SnapshotReq &self, memgraph::slk::Builder *builder);
  SnapshotReq() {}
};

struct SnapshotRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(SnapshotRes *self, memgraph::slk::Reader *reader);
  static void Save(const SnapshotRes &self, memgraph::slk::Builder *builder);
  SnapshotRes() {}
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
  WalFilesReq() {}
  explicit WalFilesReq(uint64_t file_number) : file_number(file_number) {}

  uint64_t file_number;
};

struct WalFilesRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(WalFilesRes *self, memgraph::slk::Reader *reader);
  static void Save(const WalFilesRes &self, memgraph::slk::Builder *builder);
  WalFilesRes() {}
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
  CurrentWalReq() {}
};

struct CurrentWalRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(CurrentWalRes *self, memgraph::slk::Reader *reader);
  static void Save(const CurrentWalRes &self, memgraph::slk::Builder *builder);
  CurrentWalRes() {}
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
  TimestampReq() {}
};

struct TimestampRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(TimestampRes *self, memgraph::slk::Reader *reader);
  static void Save(const TimestampRes &self, memgraph::slk::Builder *builder);
  TimestampRes() {}
  TimestampRes(bool success, uint64_t current_commit_timestamp)
      : success(success), current_commit_timestamp(current_commit_timestamp) {}

  bool success;
  uint64_t current_commit_timestamp;
};

using TimestampRpc = rpc::RequestResponse<TimestampReq, TimestampRes>;
}  // namespace replication
}  // namespace storage
}  // namespace memgraph

// SLK serialization declarations
#include "slk/serialization.hpp"
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

void Save(const memgraph::storage::replication::FrequentHeartbeatRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::FrequentHeartbeatRes *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::FrequentHeartbeatReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::FrequentHeartbeatReq *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::HeartbeatRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::HeartbeatRes *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::HeartbeatReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::HeartbeatReq *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::AppendDeltasRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::AppendDeltasRes *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::AppendDeltasReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::AppendDeltasReq *self, memgraph::slk::Reader *reader);

}  // namespace memgraph::slk

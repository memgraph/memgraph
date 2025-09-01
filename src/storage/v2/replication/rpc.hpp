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
#include <utility>

#include "rpc/messages.hpp"
#include "slk/serialization.hpp"
#include "storage/v2/config.hpp"
#include "utils/uuid.hpp"

namespace memgraph::storage::replication {

struct FinalizeCommitReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::FINALIZE_COMMIT_REQ, .name = "FinalizeCommitReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(FinalizeCommitReq *self, slk::Reader *reader);
  static void Save(const FinalizeCommitReq &self, slk::Builder *builder);
  FinalizeCommitReq() = default;

  FinalizeCommitReq(bool const decision_arg, const utils::UUID &main_uuid_arg, const utils::UUID &storage_uuid_arg,
                    uint64_t const durability_commit_timestamp_arg)
      : decision(decision_arg),
        main_uuid(main_uuid_arg),
        storage_uuid(storage_uuid_arg),
        durability_commit_timestamp(durability_commit_timestamp_arg) {}

  // If true, commit should be done, if false abort
  bool decision;
  utils::UUID main_uuid;
  utils::UUID storage_uuid;
  uint64_t durability_commit_timestamp;
};

struct FinalizeCommitRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::FINALIZE_COMMIT_RES, .name = "FinalizeCommitRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(FinalizeCommitRes *self, slk::Reader *reader);
  static void Save(const FinalizeCommitRes &self, slk::Builder *builder);
  FinalizeCommitRes() = default;

  explicit FinalizeCommitRes(bool const success_arg) : success(success_arg) {}

  bool success;
};

using FinalizeCommitRpc = rpc::RequestResponse<FinalizeCommitReq, FinalizeCommitRes>;

struct PrepareCommitReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::PREPARE_COMMIT_REQ, .name = "PrepareCommitReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(PrepareCommitReq *self, slk::Reader *reader);
  static void Save(const PrepareCommitReq &self, slk::Builder *builder);
  PrepareCommitReq() = default;
  PrepareCommitReq(const utils::UUID &main_uuid_arg, const utils::UUID &storage_uuid_arg,
                   uint64_t const previous_commit_timestamp_arg, bool const two_phase_commit_arg,
                   uint64_t const durability_commit_timestamp_arg)
      : main_uuid{main_uuid_arg},
        storage_uuid{storage_uuid_arg},
        previous_commit_timestamp(previous_commit_timestamp_arg),
        two_phase_commit(two_phase_commit_arg),
        durability_commit_timestamp(durability_commit_timestamp_arg) {}

  utils::UUID main_uuid;
  utils::UUID storage_uuid;
  uint64_t previous_commit_timestamp;
  bool two_phase_commit;
  uint64_t durability_commit_timestamp;
};

struct PrepareCommitRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::PREPARE_COMMIT_RES, .name = "PrepareCommitRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(PrepareCommitRes *self, slk::Reader *reader);
  static void Save(const PrepareCommitRes &self, slk::Builder *builder);
  PrepareCommitRes() = default;

  explicit PrepareCommitRes(bool const success) : success(success) {}

  bool success;
};

using PrepareCommitRpc = rpc::RequestResponse<PrepareCommitReq, PrepareCommitRes>;

// Special type of RPC message
struct InProgressRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_IN_PROGRESS_RES, .name = "InProgressRes"};
  static constexpr uint64_t kVersion{1};

  InProgressRes() = default;
};

struct HeartbeatReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_HEARTBEAT_REQ, .name = "HeartbeatReq"};
  static constexpr uint64_t kVersion{1};

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
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_HEARTBEAT_RES, .name = "HeartbeatRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(HeartbeatRes *self, slk::Reader *reader);
  static void Save(const HeartbeatRes &self, slk::Builder *builder);
  HeartbeatRes() = default;
  HeartbeatRes(bool const success, uint64_t const current_commit_timestamp, std::string epoch_id,
               uint64_t const num_txns_committed)
      : success_(success),
        current_commit_timestamp_(current_commit_timestamp),
        epoch_id_(std::move(epoch_id)),
        num_txns_committed_(num_txns_committed) {}

  bool success_;
  uint64_t current_commit_timestamp_;
  std::string epoch_id_;
  uint64_t num_txns_committed_;
};

using HeartbeatRpc = rpc::RequestResponse<HeartbeatReq, HeartbeatRes>;

struct SnapshotReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_SNAPSHOT_REQ, .name = "SnapshotReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(SnapshotReq *self, memgraph::slk::Reader *reader);
  static void Save(const SnapshotReq &self, memgraph::slk::Builder *builder);
  SnapshotReq() = default;
  explicit SnapshotReq(const utils::UUID &main_uuid, const utils::UUID &storage_uuid)
      : main_uuid{main_uuid}, storage_uuid{storage_uuid} {}

  utils::UUID main_uuid;
  utils::UUID storage_uuid;
};

struct SnapshotRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_SNAPSHOT_RES, .name = "SnapshotRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(SnapshotRes *self, slk::Reader *reader);
  static void Save(const SnapshotRes &self, slk::Builder *builder);
  SnapshotRes() = default;

  explicit SnapshotRes(std::optional<uint64_t> const current_commit_timestamp, uint64_t const num_txns_committed)
      : current_commit_timestamp_(current_commit_timestamp), num_txns_committed_(num_txns_committed) {}

  std::optional<uint64_t> current_commit_timestamp_;
  uint64_t num_txns_committed_;
};

using SnapshotRpc = rpc::RequestResponse<SnapshotReq, SnapshotRes>;

struct WalFilesReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_WALFILES_REQ, .name = "WalFilesReq"};
  static constexpr uint64_t kVersion{1};

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
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_WALFILES_RES, .name = "WalFilesRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(WalFilesRes *self, memgraph::slk::Reader *reader);
  static void Save(const WalFilesRes &self, memgraph::slk::Builder *builder);
  WalFilesRes() = default;
  explicit WalFilesRes(std::optional<uint64_t> const current_commit_timestamp, uint64_t const num_txns_committed)
      : current_commit_timestamp_(current_commit_timestamp), num_txns_committed_(num_txns_committed) {}

  std::optional<uint64_t> current_commit_timestamp_;
  uint64_t num_txns_committed_;
};

using WalFilesRpc = rpc::RequestResponse<WalFilesReq, WalFilesRes>;

struct CurrentWalReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_CURRENT_WAL_REQ, .name = "CurrentWalReq"};
  static constexpr uint64_t kVersion{1};

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
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_CURRENT_WAL_RES, .name = "CurrentWalRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(CurrentWalRes *self, memgraph::slk::Reader *reader);
  static void Save(const CurrentWalRes &self, memgraph::slk::Builder *builder);
  CurrentWalRes() = default;
  explicit CurrentWalRes(std::optional<uint64_t> const current_commit_timestamp, uint64_t const num_txns_committed)
      : current_commit_timestamp_(current_commit_timestamp), num_txns_committed_(num_txns_committed) {}

  std::optional<uint64_t> current_commit_timestamp_;
  uint64_t num_txns_committed_;
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

void Save(const memgraph::storage::replication::PrepareCommitRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::PrepareCommitRes *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::PrepareCommitReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::PrepareCommitReq *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::FinalizeCommitRes &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::FinalizeCommitRes *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::replication::FinalizeCommitReq &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::replication::FinalizeCommitReq *self, memgraph::slk::Reader *reader);

void Save(const memgraph::storage::SalientConfig &self, memgraph::slk::Builder *builder);

void Load(memgraph::storage::SalientConfig *self, memgraph::slk::Reader *reader);

}  // namespace memgraph::slk

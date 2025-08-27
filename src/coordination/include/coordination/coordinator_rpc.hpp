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

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/instance_state.hpp"
#include "coordination/instance_status.hpp"
#include "coordination/replication_lag_info.hpp"
#include "replication_coordination_glue/common.hpp"
#include "rpc/messages.hpp"
#include "utils/typeinfo.hpp"
#include "utils/uuid.hpp"

namespace memgraph::coordination {

struct PromoteToMainReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_FAILOVER_REQ, .name = "PromoteToMainReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(PromoteToMainReq *self, memgraph::slk::Reader *reader);
  static void Save(const PromoteToMainReq &self, memgraph::slk::Builder *builder);

  explicit PromoteToMainReq(const utils::UUID &uuid, std::vector<ReplicationClientInfo> replication_clients_info)
      : main_uuid(uuid), replication_clients_info(std::move(replication_clients_info)) {}
  PromoteToMainReq() = default;

  // get uuid here
  utils::UUID main_uuid;
  std::vector<ReplicationClientInfo> replication_clients_info;
};

struct PromoteToMainRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_FAILOVER_RES, .name = "PromoteToMainRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(PromoteToMainRes *self, memgraph::slk::Reader *reader);
  static void Save(const PromoteToMainRes &self, memgraph::slk::Builder *builder);

  explicit PromoteToMainRes(bool success) : success(success) {}
  PromoteToMainRes() = default;

  bool success;
};

using PromoteToMainRpc = rpc::RequestResponse<PromoteToMainReq, PromoteToMainRes>;

struct RegisterReplicaOnMainReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_REGISTER_REPLICA_ON_MAIN_REQ,
                                         .name = "RegisterReplicaOnMainReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(RegisterReplicaOnMainReq *self, memgraph::slk::Reader *reader);
  static void Save(const RegisterReplicaOnMainReq &self, memgraph::slk::Builder *builder);

  explicit RegisterReplicaOnMainReq(const utils::UUID &uuid, ReplicationClientInfo replication_client_info)
      : main_uuid(uuid), replication_client_info(std::move(replication_client_info)) {}
  RegisterReplicaOnMainReq() = default;

  utils::UUID main_uuid;
  ReplicationClientInfo replication_client_info;
};

struct RegisterReplicaOnMainRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_REGISTER_REPLICA_ON_MAIN_RES,
                                         .name = "RegisterReplicaOnMainRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(RegisterReplicaOnMainRes *self, memgraph::slk::Reader *reader);
  static void Save(const RegisterReplicaOnMainRes &self, memgraph::slk::Builder *builder);

  explicit RegisterReplicaOnMainRes(bool success) : success(success) {}
  RegisterReplicaOnMainRes() = default;

  bool success;
};

using RegisterReplicaOnMainRpc = rpc::RequestResponse<RegisterReplicaOnMainReq, RegisterReplicaOnMainRes>;

struct DemoteMainToReplicaReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_SET_REPL_MAIN_REQ,
                                         .name = "DemoteMainToReplicaReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(DemoteMainToReplicaReq *self, memgraph::slk::Reader *reader);
  static void Save(const DemoteMainToReplicaReq &self, memgraph::slk::Builder *builder);

  explicit DemoteMainToReplicaReq(ReplicationClientInfo replication_client_info)
      : replication_client_info(std::move(replication_client_info)) {}

  DemoteMainToReplicaReq() = default;

  ReplicationClientInfo replication_client_info;
};

struct DemoteMainToReplicaRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_SET_REPL_MAIN_RES,
                                         .name = "DemoteMainToReplicaRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(DemoteMainToReplicaRes *self, memgraph::slk::Reader *reader);
  static void Save(const DemoteMainToReplicaRes &self, memgraph::slk::Builder *builder);

  explicit DemoteMainToReplicaRes(bool success) : success(success) {}
  DemoteMainToReplicaRes() = default;

  bool success;
};

using DemoteMainToReplicaRpc = rpc::RequestResponse<DemoteMainToReplicaReq, DemoteMainToReplicaRes>;

struct UnregisterReplicaReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_UNREGISTER_REPLICA_REQ,
                                         .name = "UnregisterReplicaReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(UnregisterReplicaReq *self, memgraph::slk::Reader *reader);
  static void Save(UnregisterReplicaReq const &self, memgraph::slk::Builder *builder);

  explicit UnregisterReplicaReq(std::string_view inst_name) : instance_name(inst_name) {}

  UnregisterReplicaReq() = default;

  std::string instance_name;
};

struct UnregisterReplicaRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_UNREGISTER_REPLICA_RES,
                                         .name = "UnregisterReplicaRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(UnregisterReplicaRes *self, memgraph::slk::Reader *reader);
  static void Save(const UnregisterReplicaRes &self, memgraph::slk::Builder *builder);

  explicit UnregisterReplicaRes(bool success) : success(success) {}
  UnregisterReplicaRes() = default;

  bool success;
};

using UnregisterReplicaRpc = rpc::RequestResponse<UnregisterReplicaReq, UnregisterReplicaRes>;

struct EnableWritingOnMainReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_ENABLE_WRITING_ON_MAIN_REQ,
                                         .name = "EnableWritingOnMainReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(EnableWritingOnMainReq *self, memgraph::slk::Reader *reader);
  static void Save(EnableWritingOnMainReq const &self, memgraph::slk::Builder *builder);

  EnableWritingOnMainReq() = default;
};

struct EnableWritingOnMainRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_ENABLE_WRITING_ON_MAIN_RES,
                                         .name = "EnableWritingOnMainRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(EnableWritingOnMainRes *self, memgraph::slk::Reader *reader);
  static void Save(EnableWritingOnMainRes const &self, memgraph::slk::Builder *builder);

  explicit EnableWritingOnMainRes(bool const success) : success(success) {}
  EnableWritingOnMainRes() = default;

  bool success;
};

using EnableWritingOnMainRpc = rpc::RequestResponse<EnableWritingOnMainReq, EnableWritingOnMainRes>;

struct GetDatabaseHistoriesReqV1 {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_GET_INSTANCE_DATABASES_REQ,
                                         .name = "GetDatabaseHistoriesReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(GetDatabaseHistoriesReqV1 *self, memgraph::slk::Reader *reader);
  static void Save(const GetDatabaseHistoriesReqV1 &self, memgraph::slk::Builder *builder);

  GetDatabaseHistoriesReqV1() = default;
};

struct GetDatabaseHistoriesReq {
  // Type stays the same for all versions
  static constexpr utils::TypeInfo kType{GetDatabaseHistoriesReqV1::kType};
  static constexpr uint64_t kVersion{2};

  static void Load(GetDatabaseHistoriesReq *self, memgraph::slk::Reader *reader);
  static void Save(const GetDatabaseHistoriesReq &self, memgraph::slk::Builder *builder);

  static GetDatabaseHistoriesReq Upgrade(GetDatabaseHistoriesReqV1 const & /*prev*/) {
    return GetDatabaseHistoriesReq{};
  }

  GetDatabaseHistoriesReq() = default;
};

struct GetDatabaseHistoriesResV1 {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_GET_INSTANCE_DATABASES_RES,
                                         .name = "GetDatabaseHistoriesRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(GetDatabaseHistoriesResV1 *self, memgraph::slk::Reader *reader);
  static void Save(const GetDatabaseHistoriesResV1 &self, memgraph::slk::Builder *builder);

  explicit GetDatabaseHistoriesResV1(replication_coordination_glue::InstanceInfoV1 instance_info)
      : instance_info(std::move(instance_info)) {}
  GetDatabaseHistoriesResV1() = default;

  replication_coordination_glue::InstanceInfoV1 instance_info;
};

struct GetDatabaseHistoriesRes {
  static constexpr utils::TypeInfo kType{GetDatabaseHistoriesResV1::kType};
  static constexpr uint64_t kVersion{2};

  static void Load(GetDatabaseHistoriesRes *self, memgraph::slk::Reader *reader);
  static void Save(const GetDatabaseHistoriesRes &self, memgraph::slk::Builder *builder);

  explicit GetDatabaseHistoriesRes(replication_coordination_glue::InstanceInfo instance_info)
      : instance_info(std::move(instance_info)) {}
  GetDatabaseHistoriesRes() = default;

  // We cannot downgrade from GetDatabaseHistoriesRes, the caller should provide function for creating both responses
  // independently
  GetDatabaseHistoriesResV1 Downgrade() = delete;

  replication_coordination_glue::InstanceInfo instance_info;
};

using GetDatabaseHistoriesRpc = rpc::RequestResponse<GetDatabaseHistoriesReq, GetDatabaseHistoriesRes>;

struct ShowInstancesReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_SHOW_INSTANCES_REQ, .name = "ShowInstancesReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(ShowInstancesReq *self, memgraph::slk::Reader *reader);
  static void Save(const ShowInstancesReq &self, memgraph::slk::Builder *builder);

  ShowInstancesReq() = default;
};

struct ShowInstancesRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_SHOW_INSTANCES_RES, .name = "ShowInstancesRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(ShowInstancesRes *self, memgraph::slk::Reader *reader);
  static void Save(const ShowInstancesRes &self, memgraph::slk::Builder *builder);

  explicit ShowInstancesRes(std::optional<std::vector<InstanceStatus>> instances_status)
      : instances_status_(std::move(instances_status)) {}

  ShowInstancesRes() = default;

  std::optional<std::vector<InstanceStatus>> instances_status_;
};

using ShowInstancesRpc = rpc::RequestResponse<ShowInstancesReq, ShowInstancesRes>;

struct StateCheckReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_STATE_CHECK_REQ, .name = "StateCheckReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(StateCheckReq *self, memgraph::slk::Reader *reader);
  static void Save(const StateCheckReq &self, memgraph::slk::Builder *builder);
  StateCheckReq() = default;
};

struct StateCheckRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_STATE_CHECK_RES, .name = "StateCheckRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(StateCheckRes *self, memgraph::slk::Reader *reader);
  static void Save(const StateCheckRes &self, memgraph::slk::Builder *builder);

  StateCheckRes(bool const replica, std::optional<utils::UUID> const &req_uuid, bool writing_enabled)
      : state({.is_replica = replica, .uuid = req_uuid, .is_writing_enabled = writing_enabled}) {}
  StateCheckRes() = default;

  InstanceState state;
};

using StateCheckRpc = rpc::RequestResponse<StateCheckReq, StateCheckRes>;

struct ReplicationLagReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_GET_REPLICATION_LAG_REQ,
                                         .name = "ReplicationLagReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(ReplicationLagReq *self, memgraph::slk::Reader *reader);
  static void Save(const ReplicationLagReq &self, memgraph::slk::Builder *builder);
  ReplicationLagReq() = default;
};

struct ReplicationLagRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_GET_REPLICATION_LAG_RES,
                                         .name = "ReplicationLagRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(ReplicationLagRes *self, memgraph::slk::Reader *reader);
  static void Save(const ReplicationLagRes &self, memgraph::slk::Builder *builder);

  explicit ReplicationLagRes(std::optional<ReplicationLagInfo> lag_info) : lag_info_(std::move(lag_info)) {}
  ReplicationLagRes() = default;

  std::optional<ReplicationLagInfo> lag_info_;
};

using ReplicationLagRpc = rpc::RequestResponse<ReplicationLagReq, ReplicationLagRes>;

}  // namespace memgraph::coordination

// SLK serialization declarations
namespace memgraph::slk {

// PromoteToMainRpc
void Save(const memgraph::coordination::PromoteToMainRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::PromoteToMainRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::coordination::PromoteToMainReq &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::PromoteToMainReq *self, memgraph::slk::Reader *reader);

// RegisterReplicaOnMainRpc
void Save(const memgraph::coordination::RegisterReplicaOnMainReq &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::RegisterReplicaOnMainReq *self, memgraph::slk::Reader *reader);
void Save(const memgraph::coordination::RegisterReplicaOnMainRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::RegisterReplicaOnMainRes *self, memgraph::slk::Reader *reader);

// DemoteMainToReplicaRpc
void Save(const memgraph::coordination::DemoteMainToReplicaRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::DemoteMainToReplicaRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::coordination::DemoteMainToReplicaReq &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::DemoteMainToReplicaReq *self, memgraph::slk::Reader *reader);

// UnregisterReplicaRpc
void Save(memgraph::coordination::UnregisterReplicaRes const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::UnregisterReplicaRes *self, memgraph::slk::Reader *reader);
void Save(memgraph::coordination::UnregisterReplicaReq const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::UnregisterReplicaReq *self, memgraph::slk::Reader *reader);

// EnableWritingOnMainRpc
void Save(memgraph::coordination::EnableWritingOnMainRes const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::EnableWritingOnMainRes *self, memgraph::slk::Reader *reader);

// GetDatabaseHistoriesRpc
void Save(const memgraph::coordination::GetDatabaseHistoriesResV1 &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::GetDatabaseHistoriesResV1 *self, memgraph::slk::Reader *reader);
void Save(const memgraph::coordination::GetDatabaseHistoriesRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::GetDatabaseHistoriesRes *self, memgraph::slk::Reader *reader);

// ShowInstancesRpc
void Save(memgraph::coordination::ShowInstancesRes const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::ShowInstancesRes *self, memgraph::slk::Reader *reader);
void Save(memgraph::coordination::ShowInstancesReq const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::ShowInstancesReq *self, memgraph::slk::Reader *reader);

// StateCheckRpc
void Save(memgraph::coordination::StateCheckRes const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::StateCheckRes *self, memgraph::slk::Reader *reader);
void Save(memgraph::coordination::StateCheckReq const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::StateCheckReq *self, memgraph::slk::Reader *reader);

// ReplicationLagRpc
void Save(coordination::ReplicationLagRes const &self, slk::Builder *builder);
void Load(coordination::ReplicationLagRes *self, slk::Reader *reader);
void Save(coordination::ReplicationLagReq const &self, slk::Builder *builder);
void Load(coordination::ReplicationLagReq *self, slk::Reader *reader);

}  // namespace memgraph::slk

#endif

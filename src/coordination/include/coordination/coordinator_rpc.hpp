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

#include "utils/uuid.hpp"
#ifdef MG_ENTERPRISE

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/instance_state.hpp"
#include "coordination/instance_status.hpp"
#include "replication_coordination_glue/common.hpp"
#include "rpc/messages.hpp"

namespace memgraph::coordination {

struct PromoteReplicaToMainReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(PromoteReplicaToMainReq *self, memgraph::slk::Reader *reader);
  static void Save(const PromoteReplicaToMainReq &self, memgraph::slk::Builder *builder);

  explicit PromoteReplicaToMainReq(const utils::UUID &uuid, std::vector<ReplicationClientInfo> replication_clients_info)
      : main_uuid(uuid), replication_clients_info(std::move(replication_clients_info)) {}
  PromoteReplicaToMainReq() = default;

  // get uuid here
  utils::UUID main_uuid;
  std::vector<ReplicationClientInfo> replication_clients_info;
};

struct PromoteReplicaToMainRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(PromoteReplicaToMainRes *self, memgraph::slk::Reader *reader);
  static void Save(const PromoteReplicaToMainRes &self, memgraph::slk::Builder *builder);

  explicit PromoteReplicaToMainRes(bool success) : success(success) {}
  PromoteReplicaToMainRes() = default;

  bool success;
};

using PromoteReplicaToMainRpc = rpc::RequestResponse<PromoteReplicaToMainReq, PromoteReplicaToMainRes>;

struct RegisterReplicaOnMainReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(RegisterReplicaOnMainReq *self, memgraph::slk::Reader *reader);
  static void Save(const RegisterReplicaOnMainReq &self, memgraph::slk::Builder *builder);

  explicit RegisterReplicaOnMainReq(const utils::UUID &uuid, ReplicationClientInfo replication_client_info)
      : main_uuid(uuid), replication_client_info(std::move(replication_client_info)) {}
  RegisterReplicaOnMainReq() = default;

  utils::UUID main_uuid;
  ReplicationClientInfo replication_client_info;
};

struct RegisterReplicaOnMainRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(RegisterReplicaOnMainRes *self, memgraph::slk::Reader *reader);
  static void Save(const RegisterReplicaOnMainRes &self, memgraph::slk::Builder *builder);

  explicit RegisterReplicaOnMainRes(bool success) : success(success) {}
  RegisterReplicaOnMainRes() = default;

  bool success;
};

using RegisterReplicaOnMainRpc = rpc::RequestResponse<RegisterReplicaOnMainReq, RegisterReplicaOnMainRes>;

struct DemoteMainToReplicaReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(DemoteMainToReplicaReq *self, memgraph::slk::Reader *reader);
  static void Save(const DemoteMainToReplicaReq &self, memgraph::slk::Builder *builder);

  explicit DemoteMainToReplicaReq(ReplicationClientInfo replication_client_info)
      : replication_client_info(std::move(replication_client_info)) {}

  DemoteMainToReplicaReq() = default;

  ReplicationClientInfo replication_client_info;
};

struct DemoteMainToReplicaRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(DemoteMainToReplicaRes *self, memgraph::slk::Reader *reader);
  static void Save(const DemoteMainToReplicaRes &self, memgraph::slk::Builder *builder);

  explicit DemoteMainToReplicaRes(bool success) : success(success) {}
  DemoteMainToReplicaRes() = default;

  bool success;
};

using DemoteMainToReplicaRpc = rpc::RequestResponse<DemoteMainToReplicaReq, DemoteMainToReplicaRes>;

struct UnregisterReplicaReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(UnregisterReplicaReq *self, memgraph::slk::Reader *reader);
  static void Save(UnregisterReplicaReq const &self, memgraph::slk::Builder *builder);

  explicit UnregisterReplicaReq(std::string_view inst_name) : instance_name(inst_name) {}

  UnregisterReplicaReq() = default;

  std::string instance_name;
};

struct UnregisterReplicaRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(UnregisterReplicaRes *self, memgraph::slk::Reader *reader);
  static void Save(const UnregisterReplicaRes &self, memgraph::slk::Builder *builder);

  explicit UnregisterReplicaRes(bool success) : success(success) {}
  UnregisterReplicaRes() = default;

  bool success;
};

using UnregisterReplicaRpc = rpc::RequestResponse<UnregisterReplicaReq, UnregisterReplicaRes>;

struct EnableWritingOnMainReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(EnableWritingOnMainReq *self, memgraph::slk::Reader *reader);
  static void Save(EnableWritingOnMainReq const &self, memgraph::slk::Builder *builder);

  EnableWritingOnMainReq() = default;
};

struct EnableWritingOnMainRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(EnableWritingOnMainRes *self, memgraph::slk::Reader *reader);
  static void Save(EnableWritingOnMainRes const &self, memgraph::slk::Builder *builder);

  explicit EnableWritingOnMainRes(bool success) : success(success) {}
  EnableWritingOnMainRes() = default;

  bool success;
};

using EnableWritingOnMainRpc = rpc::RequestResponse<EnableWritingOnMainReq, EnableWritingOnMainRes>;

struct GetInstanceUUIDReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(GetInstanceUUIDReq *self, memgraph::slk::Reader *reader);
  static void Save(const GetInstanceUUIDReq &self, memgraph::slk::Builder *builder);

  GetInstanceUUIDReq() = default;
};

struct GetInstanceUUIDRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(GetInstanceUUIDRes *self, memgraph::slk::Reader *reader);
  static void Save(const GetInstanceUUIDRes &self, memgraph::slk::Builder *builder);

  explicit GetInstanceUUIDRes(std::optional<utils::UUID> uuid) : uuid(uuid) {}
  GetInstanceUUIDRes() = default;

  std::optional<utils::UUID> uuid;
};

using GetInstanceUUIDRpc = rpc::RequestResponse<GetInstanceUUIDReq, GetInstanceUUIDRes>;

struct GetDatabaseHistoriesReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(GetDatabaseHistoriesReq *self, memgraph::slk::Reader *reader);
  static void Save(const GetDatabaseHistoriesReq &self, memgraph::slk::Builder *builder);

  GetDatabaseHistoriesReq() = default;
};

struct GetDatabaseHistoriesRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(GetDatabaseHistoriesRes *self, memgraph::slk::Reader *reader);
  static void Save(const GetDatabaseHistoriesRes &self, memgraph::slk::Builder *builder);

  explicit GetDatabaseHistoriesRes(replication_coordination_glue::DatabaseHistories db_histories)
      : database_histories(std::move(db_histories)) {}
  GetDatabaseHistoriesRes() = default;

  replication_coordination_glue::DatabaseHistories database_histories;
};

using GetDatabaseHistoriesRpc = rpc::RequestResponse<GetDatabaseHistoriesReq, GetDatabaseHistoriesRes>;

struct ShowInstancesReq {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(ShowInstancesReq *self, memgraph::slk::Reader *reader);
  static void Save(const ShowInstancesReq &self, memgraph::slk::Builder *builder);

  ShowInstancesReq() = default;
};

struct ShowInstancesRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(ShowInstancesRes *self, memgraph::slk::Reader *reader);
  static void Save(const ShowInstancesRes &self, memgraph::slk::Builder *builder);

  explicit ShowInstancesRes(std::optional<std::vector<InstanceStatus>> instances_status)
      : instances_status_(std::move(instances_status)) {}

  ShowInstancesRes() = default;

  std::optional<std::vector<InstanceStatus>> instances_status_;
};

using ShowInstancesRpc = rpc::RequestResponse<ShowInstancesReq, ShowInstancesRes>;

struct StateCheckReq {
  static const utils::TypeInfo kType;  // TODO: make constexpr?
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(StateCheckReq *self, memgraph::slk::Reader *reader);
  static void Save(const StateCheckReq &self, memgraph::slk::Builder *builder);
  StateCheckReq() = default;
};

struct StateCheckRes {
  static const utils::TypeInfo kType;
  static const utils::TypeInfo &GetTypeInfo() { return kType; }

  static void Load(StateCheckRes *self, memgraph::slk::Reader *reader);
  static void Save(const StateCheckRes &self, memgraph::slk::Builder *builder);

  StateCheckRes(bool replica, std::optional<utils::UUID> req_uuid, bool writing_enabled)
      : state({.is_replica = replica, .uuid = req_uuid, .is_writing_enabled = writing_enabled}) {}
  StateCheckRes() = default;

  InstanceState state;
};

using StateCheckRpc = rpc::RequestResponse<StateCheckReq, StateCheckRes>;

}  // namespace memgraph::coordination

// SLK serialization declarations
namespace memgraph::slk {

// PromoteReplicaToMainRpc
void Save(const memgraph::coordination::PromoteReplicaToMainRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::PromoteReplicaToMainRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::coordination::PromoteReplicaToMainReq &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::PromoteReplicaToMainReq *self, memgraph::slk::Reader *reader);

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

// GetInstanceUUIDRpc
void Save(const memgraph::coordination::GetInstanceUUIDReq &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::GetInstanceUUIDReq *self, memgraph::slk::Reader *reader);
void Save(const memgraph::coordination::GetInstanceUUIDRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::GetInstanceUUIDRes *self, memgraph::slk::Reader *reader);

// UnregisterReplicaRpc
void Save(memgraph::coordination::UnregisterReplicaRes const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::UnregisterReplicaRes *self, memgraph::slk::Reader *reader);
void Save(memgraph::coordination::UnregisterReplicaReq const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::UnregisterReplicaReq *self, memgraph::slk::Reader *reader);

// EnableWritingOnMainRpc
void Save(memgraph::coordination::EnableWritingOnMainRes const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::EnableWritingOnMainRes *self, memgraph::slk::Reader *reader);

// GetDatabaseHistoriesRpc
void Save(const memgraph::coordination::GetDatabaseHistoriesRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::GetDatabaseHistoriesRes *self, memgraph::slk::Reader *reader);

// ShowInstancesRpc
void Save(memgraph::coordination::ShowInstancesRes const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::ShowInstancesRes *self, memgraph::slk::Reader *reader);
void Save(memgraph::coordination::ShowInstancesReq const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::ShowInstancesReq *self, memgraph::slk::Reader *reader);

void Save(memgraph::coordination::StateCheckRes const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::StateCheckRes *self, memgraph::slk::Reader *reader);
void Save(memgraph::coordination::StateCheckReq const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::StateCheckReq *self, memgraph::slk::Reader *reader);

}  // namespace memgraph::slk

#endif

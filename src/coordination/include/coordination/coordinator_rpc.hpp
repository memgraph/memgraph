// Copyright 2026 Memgraph Ltd.
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
#include "coordination/coordinator_slk.hpp"
#include "coordination/instance_state.hpp"
#include "coordination/instance_status.hpp"
#include "coordination/replication_lag_info.hpp"
#include "coordination/utils.hpp"
#include "replication_coordination_glue/common.hpp"
#include "rpc/messages.hpp"
#include "utils/fixed_string.hpp"
#include "utils/typeinfo.hpp"
#include "utils/uuid.hpp"

#define DECLARE_SLK_SERIALIZATION_FUNCTIONS(Type)               \
  void Save(const Type &self, memgraph::slk::Builder *builder); \
  void Load(Type *self, memgraph::slk::Reader *reader);

#define DECLARE_SLK_FREE_FUNCTIONS(Type)                        \
  void Save(Type::Response const &self, slk::Builder *builder); \
  void Load(Type::Response *self, slk::Reader *reader);         \
  void Save(Type::Request const &self, slk::Builder *builder);  \
  void Load(Type::Request *self, slk::Reader *reader);

namespace memgraph::coordination {

template <utils::TypeId Id, FixedString Name, uint64_t Version, typename ArgType>
struct SingleArgMsg {
  static constexpr utils::TypeInfo kType{.id = Id, .name = Name.c_str()};
  static constexpr uint64_t kVersion{Version};

  static void Save(SingleArgMsg const &self, memgraph::slk::Builder *builder) {
    memgraph::slk::Save(self.arg_, builder);
  }

  static void Load(SingleArgMsg *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(&self->arg_, reader); }

  SingleArgMsg(ArgType arg) : arg_(std::move(arg)) {}

  SingleArgMsg() = default;

  ArgType arg_;
};

template <rpc::RpcMessage PriorVersionType, utils::DowngradeableType ArgType>
struct DowngradeableSingleArgMsg {
  static constexpr utils::TypeInfo kType{PriorVersionType::kType};
  static constexpr uint64_t kVersion{PriorVersionType::kVersion + 1};

  static void Save(DowngradeableSingleArgMsg const &self, memgraph::slk::Builder *builder) {
    memgraph::slk::Save(self.arg_, builder);
  }

  static void Load(DowngradeableSingleArgMsg *self, memgraph::slk::Reader *reader) {
    memgraph::slk::Load(&self->arg_, reader);
  }

  DowngradeableSingleArgMsg(ArgType arg) : arg_(std::move(arg)) {}

  DowngradeableSingleArgMsg() = default;

  PriorVersionType Downgrade() const { return PriorVersionType{arg_.Downgrade()}; }

  ArgType arg_;
};

template <utils::TypeId Id, FixedString Name, uint64_t Version>
struct EmptyReq {
  static constexpr utils::TypeInfo kType{.id = Id, .name = Name.c_str()};
  static constexpr uint64_t kVersion{Version};

  static void Save(EmptyReq const & /*self*/, memgraph::slk::Builder * /*builder*/) {}

  static void Load(EmptyReq * /*self*/, memgraph::slk::Reader * /*reader*/) {}

  EmptyReq() = default;
};

template <rpc::RpcMessage PriorVersionType>
struct UpgradeableEmptyReq {
  static constexpr utils::TypeInfo kType{PriorVersionType::kType};
  static constexpr uint64_t kVersion{PriorVersionType::kVersion + 1};

  static void Save(UpgradeableEmptyReq const & /*self*/, memgraph::slk::Builder * /*builder*/) {}

  static void Load(UpgradeableEmptyReq * /*self*/, memgraph::slk::Reader * /*reader*/) {}

  static UpgradeableEmptyReq Upgrade(PriorVersionType const &) { return UpgradeableEmptyReq{}; }

  UpgradeableEmptyReq() = default;
};

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

  explicit PromoteToMainRes(bool success) : arg_(success) {}

  PromoteToMainRes() = default;

  bool arg_;
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

  explicit RegisterReplicaOnMainRes(bool success) : arg_(success) {}

  RegisterReplicaOnMainRes() = default;

  bool arg_;
};

using RegisterReplicaOnMainRpc = rpc::RequestResponse<RegisterReplicaOnMainReq, RegisterReplicaOnMainRes>;

struct DemoteMainToReplicaReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_SET_REPL_MAIN_REQ,
                                         .name = "DemoteMainToReplicaReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(DemoteMainToReplicaReq *self, memgraph::slk::Reader *reader);
  static void Save(const DemoteMainToReplicaReq &self, memgraph::slk::Builder *builder);

  // main uuid is provided when Demote is called from InstanceSuccessCallback because at that point we already know
  // what's next main uuid since the failover has already been done
  explicit DemoteMainToReplicaReq(ReplicationClientInfo replication_client_info,
                                  std::optional<utils::UUID> const &main_uuid = std::nullopt)
      : replication_client_info_(std::move(replication_client_info)), main_uuid_(main_uuid) {}

  DemoteMainToReplicaReq() = default;

  ReplicationClientInfo replication_client_info_;
  std::optional<utils::UUID> main_uuid_;
};

struct DemoteMainToReplicaRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_SET_REPL_MAIN_RES,
                                         .name = "DemoteMainToReplicaRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(DemoteMainToReplicaRes *self, memgraph::slk::Reader *reader);
  static void Save(const DemoteMainToReplicaRes &self, memgraph::slk::Builder *builder);

  explicit DemoteMainToReplicaRes(bool success) : arg_(success) {}

  DemoteMainToReplicaRes() = default;

  bool arg_;
};

using DemoteMainToReplicaRpc = rpc::RequestResponse<DemoteMainToReplicaReq, DemoteMainToReplicaRes>;

struct UnregisterReplicaReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_UNREGISTER_REPLICA_REQ,
                                         .name = "UnregisterReplicaReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(UnregisterReplicaReq *self, memgraph::slk::Reader *reader);
  static void Save(UnregisterReplicaReq const &self, memgraph::slk::Builder *builder);

  explicit UnregisterReplicaReq(std::string_view inst_name) : arg_(inst_name) {}

  UnregisterReplicaReq() = default;

  std::string arg_;
};

struct UnregisterReplicaRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_UNREGISTER_REPLICA_RES,
                                         .name = "UnregisterReplicaRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(UnregisterReplicaRes *self, memgraph::slk::Reader *reader);
  static void Save(const UnregisterReplicaRes &self, memgraph::slk::Builder *builder);

  explicit UnregisterReplicaRes(bool success) : arg_(success) {}

  UnregisterReplicaRes() = default;

  bool arg_;
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

  explicit EnableWritingOnMainRes(bool const success) : arg_(success) {}

  EnableWritingOnMainRes() = default;

  bool arg_;
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
      : arg_(std::move(instance_info)) {}

  GetDatabaseHistoriesResV1() = default;

  replication_coordination_glue::InstanceInfoV1 arg_;
};

struct GetDatabaseHistoriesRes {
  static constexpr utils::TypeInfo kType{GetDatabaseHistoriesResV1::kType};
  static constexpr uint64_t kVersion{2};

  static void Load(GetDatabaseHistoriesRes *self, memgraph::slk::Reader *reader);
  static void Save(const GetDatabaseHistoriesRes &self, memgraph::slk::Builder *builder);

  explicit GetDatabaseHistoriesRes(replication_coordination_glue::InstanceInfo instance_info)
      : arg_(std::move(instance_info)) {}

  GetDatabaseHistoriesRes() = default;

  // We cannot downgrade from GetDatabaseHistoriesRes, the caller should provide function for creating both responses
  // independently
  GetDatabaseHistoriesResV1 Downgrade() = delete;

  replication_coordination_glue::InstanceInfo arg_;
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
      : arg_(std::move(instances_status)) {}

  ShowInstancesRes() = default;

  std::optional<std::vector<InstanceStatus>> arg_;
};

using ShowInstancesRpc = rpc::RequestResponse<ShowInstancesReq, ShowInstancesRes>;

struct GetRoutingTableReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_GET_ROUTING_TABLE_REQ,
                                         .name = "GetRoutingTableReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(GetRoutingTableReq *self, memgraph::slk::Reader *reader);
  static void Save(const GetRoutingTableReq &self, memgraph::slk::Builder *builder);

  GetRoutingTableReq() = default;

  explicit GetRoutingTableReq(std::string arg) : arg_(std::move(arg)) {}

  std::string arg_;
};

struct GetRoutingTableRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_GET_ROUTING_TABLE_RES,
                                         .name = "GetRoutingTableRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(GetRoutingTableRes *self, memgraph::slk::Reader *reader);
  static void Save(const GetRoutingTableRes &self, memgraph::slk::Builder *builder);

  explicit GetRoutingTableRes(RoutingTable routing_table) : arg_(std::move(routing_table)) {}

  GetRoutingTableRes() = default;

  RoutingTable arg_;
};

using GetRoutingTableRpc = rpc::RequestResponse<GetRoutingTableReq, GetRoutingTableRes>;

constexpr FixedString<14> kStateCheckReq = "StateCheckReq";
using StateCheckReqV1 = EmptyReq<utils::TypeId::COORD_STATE_CHECK_REQ, kStateCheckReq, 1>;
using StateCheckReqV2 = UpgradeableEmptyReq<StateCheckReqV1>;
using StateCheckReq = UpgradeableEmptyReq<StateCheckReqV2>;

constexpr FixedString<14> kStateCheckRes = "StateCheckRes";

// Implemented as a struct before, that's why for this type using Type isn't used
struct StateCheckResV1 {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_STATE_CHECK_RES, .name = kStateCheckRes.c_str()};
  static constexpr uint64_t kVersion{1};

  static void Load(StateCheckResV1 *self, memgraph::slk::Reader *reader);
  static void Save(const StateCheckResV1 &self, memgraph::slk::Builder *builder);

  StateCheckResV1(bool const replica, std::optional<utils::UUID> const &req_uuid, bool const writing_enabled)
      : arg_({.is_replica = replica, .uuid = req_uuid, .is_writing_enabled = writing_enabled}) {}

  explicit StateCheckResV1(InstanceStateV1 const &rec_state) : arg_(rec_state) {}

  StateCheckResV1() = default;

  InstanceStateV1 arg_;
};

// Implemented as a struct before, that's why for this type using Type isn't used
struct StateCheckResV2 {
  static constexpr utils::TypeInfo kType{StateCheckResV1::kType};
  static constexpr uint64_t kVersion{2};

  static void Load(StateCheckResV2 *self, memgraph::slk::Reader *reader);
  static void Save(const StateCheckResV2 &self, memgraph::slk::Builder *builder);

  StateCheckResV2(
      bool const replica, std::optional<utils::UUID> const &req_uuid, bool const writing_enabled,
      std::optional<std::map<std::string, uint64_t>> const &maybe_main_num_txns,
      // instance -> (db -> lag)
      std::optional<std::map<std::string, std::map<std::string, int64_t>>> const &maybe_main_num_txns_replicas)
      : arg_({.is_replica = replica,
              .uuid = req_uuid,
              .is_writing_enabled = writing_enabled,
              .main_num_txns = maybe_main_num_txns,
              .replicas_num_txns = maybe_main_num_txns_replicas}) {}

  explicit StateCheckResV2(InstanceStateV2 const &rec_state) : arg_(rec_state) {}

  StateCheckResV2() = default;

  StateCheckResV1 Downgrade() const { return StateCheckResV1{arg_.Downgrade()}; }

  InstanceStateV2 arg_;
};

using StateCheckRes = DowngradeableSingleArgMsg<StateCheckResV2, InstanceState>;
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

  explicit ReplicationLagRes(std::optional<ReplicationLagInfo> lag_info) : arg_(std::move(lag_info)) {}

  ReplicationLagRes() = default;

  std::optional<ReplicationLagInfo> arg_;
};

using ReplicationLagRpc = rpc::RequestResponse<ReplicationLagReq, ReplicationLagRes>;

using AddCoordinatorReq =
    SingleArgMsg<utils::TypeId::COORD_ADD_COORD_REQ, "AddCoordinatorReq", 1, CoordinatorInstanceConfig>;
using AddCoordinatorRes = SingleArgMsg<utils::TypeId::COORD_ADD_COORD_RES, "AddCoordinatorRes", 1, bool>;
using AddCoordinatorRpc = rpc::RequestResponse<AddCoordinatorReq, AddCoordinatorRes>;

using RemoveCoordinatorReq = SingleArgMsg<utils::TypeId::COORD_REMOVE_COORD_REQ, "RemoveCoordinatorReq", 1, int>;
using RemoveCoordinatorRes = SingleArgMsg<utils::TypeId::COORD_REMOVE_COORD_RES, "RemoveCoordinatorRes", 1, bool>;
using RemoveCoordinatorRpc = rpc::RequestResponse<RemoveCoordinatorReq, RemoveCoordinatorRes>;

using RegisterInstanceReq =
    SingleArgMsg<utils::TypeId::COORD_REGISTER_INSTANCE_REQ, "RegisterInstanceReq", 1, DataInstanceConfig>;
using RegisterInstanceRes = SingleArgMsg<utils::TypeId::COORD_REGISTER_INSTANCE_RES, "RegisterInstanceRes", 1, bool>;
using RegisterInstanceRpc = rpc::RequestResponse<RegisterInstanceReq, RegisterInstanceRes>;

using UnregisterInstanceReq =
    SingleArgMsg<utils::TypeId::COORD_UNREGISTER_INSTANCE_REQ, "UnregisterInstanceReq", 1, std::string>;
using UnregisterInstanceRes =
    SingleArgMsg<utils::TypeId::COORD_UNREGISTER_INSTANCE_RES, "UnregisterInstanceRes", 1, bool>;
using UnregisterInstanceRpc = rpc::RequestResponse<UnregisterInstanceReq, UnregisterInstanceRes>;

using SetInstanceToMainReq =
    SingleArgMsg<utils::TypeId::COORD_SET_INSTANCE_TO_MAIN_REQ, "SetInstanceToMainReq", 1, std::string>;
using SetInstanceToMainRes =
    SingleArgMsg<utils::TypeId::COORD_SET_INSTANCE_TO_MAIN_RES, "SetInstanceToMainRes", 1, bool>;
using SetInstanceToMainRpc = rpc::RequestResponse<SetInstanceToMainReq, SetInstanceToMainRes>;

using DemoteInstanceReq = SingleArgMsg<utils::TypeId::COORD_DEMOTE_INSTANCE_REQ, "DemoteInstanceReq", 1, std::string>;
using DemoteInstanceRes = SingleArgMsg<utils::TypeId::COORD_DEMOTE_INSTANCE_RES, "DemoteInstanceRes", 1, bool>;
using DemoteInstanceRpc = rpc::RequestResponse<DemoteInstanceReq, DemoteInstanceRes>;

using ForceResetReq = EmptyReq<utils::TypeId::COORD_FORCE_RESET_REQ, "ForceResetReq", 1>;
using ForceResetRes = SingleArgMsg<utils::TypeId::COORD_FORCE_RESET_RES, "ForceResetRes", 1, bool>;
using ForceResetRpc = rpc::RequestResponse<ForceResetReq, ForceResetRes>;

using UpdateConfigReq =
    SingleArgMsg<utils::TypeId::COORD_UPDATE_CONFIG_REQ, "UpdateConfigReq", 1, UpdateInstanceConfig>;
using UpdateConfigRes = SingleArgMsg<utils::TypeId::COORD_UPDATE_CONFIG_RES, "UpdateConfigRes", 1, bool>;
using UpdateConfigRpc = rpc::RequestResponse<UpdateConfigReq, UpdateConfigRes>;

using CoordReplicationLagReq = EmptyReq<utils::TypeId::COORD_REPL_LAG_REQ, "CoordReplLagReq", 1>;
using CoordReplicationLagRes = SingleArgMsg<utils::TypeId::COORD_REPL_LAG_RES, "CoordReplLagRes", 1,
                                            std::map<std::string, std::map<std::string, ReplicaDBLagData>>>;
using CoordReplicationLagRpc = rpc::RequestResponse<CoordReplicationLagReq, CoordReplicationLagRes>;

// Use this RPC message for updating all data instance config managed by coordinator in the future. Currently only used
// for sending deltas_batch_progress_size
using UpdateDataInstanceConfigReq =
    SingleArgMsg<utils::TypeId::COORD_UPDATE_DATA_INSTANCE_CONFIG_REQ, "UpdateDataInstanceConfigReq", 1, uint64_t>;
using UpdateDataInstanceConfigRes =
    SingleArgMsg<utils::TypeId::COORD_UPDATE_DATA_INSTANCE_CONFIG_RES, "UpdateDataInstanceConfigRes", 1, bool>;
using UpdateDataInstanceConfigRpc = rpc::RequestResponse<UpdateDataInstanceConfigReq, UpdateDataInstanceConfigRes>;

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

// GetRoutingTableRpc
void Save(memgraph::coordination::GetRoutingTableRes const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::GetRoutingTableRes *self, memgraph::slk::Reader *reader);
void Save(memgraph::coordination::GetRoutingTableReq const &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::GetRoutingTableReq *self, memgraph::slk::Reader *reader);

// StateCheckRpc

// ReplicationLagRpc
void Save(coordination::ReplicationLagRes const &self, slk::Builder *builder);
void Load(coordination::ReplicationLagRes *self, slk::Reader *reader);
void Save(coordination::ReplicationLagReq const &self, slk::Builder *builder);
void Load(coordination::ReplicationLagReq *self, slk::Reader *reader);

DECLARE_SLK_FREE_FUNCTIONS(coordination::AddCoordinatorRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::RemoveCoordinatorRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::RegisterInstanceRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::UnregisterInstanceRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::SetInstanceToMainRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::DemoteInstanceRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::ForceResetRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::UpdateConfigRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::CoordReplicationLagRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::UpdateDataInstanceConfigRpc)

DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::StateCheckReqV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::StateCheckReqV2)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::StateCheckResV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::StateCheckResV2)

DECLARE_SLK_FREE_FUNCTIONS(coordination::StateCheckRpc)

}  // namespace memgraph::slk

#endif

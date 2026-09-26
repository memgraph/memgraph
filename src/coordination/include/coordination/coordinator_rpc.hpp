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

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_ops_status.hpp"
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
template <utils::Enum StatusEnum>
struct ForwardedStatus;
}  // namespace memgraph::coordination

// Declared ahead of the messages that carry a ForwardedStatus, which reach these by qualified name.
namespace memgraph::slk {
template <memgraph::utils::Enum StatusEnum>
void Save(memgraph::coordination::ForwardedStatus<StatusEnum> const &self, Builder *builder);

template <memgraph::utils::Enum StatusEnum>
void Load(memgraph::coordination::ForwardedStatus<StatusEnum> *self, Reader *reader);
}  // namespace memgraph::slk

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

// A request carried at the next version without changing what it carries. A client sends at its request's version
// and the server answers at that same version, so a response that gains a field needs its request to move with it.
template <rpc::RpcMessage PriorVersionType>
struct UpgradeableSingleArgMsg {
  static constexpr utils::TypeInfo kType{PriorVersionType::kType};
  static constexpr uint64_t kVersion{PriorVersionType::kVersion + 1};

  using ArgType = decltype(PriorVersionType::arg_);

  static void Save(UpgradeableSingleArgMsg const &self, memgraph::slk::Builder *builder) {
    memgraph::slk::Save(self.arg_, builder);
  }

  static void Load(UpgradeableSingleArgMsg *self, memgraph::slk::Reader *reader) {
    memgraph::slk::Load(&self->arg_, reader);
  }

  static UpgradeableSingleArgMsg Upgrade(PriorVersionType const &prior) { return UpgradeableSingleArgMsg{prior.arg_}; }

  UpgradeableSingleArgMsg(ArgType arg) : arg_(std::move(arg)) {}

  UpgradeableSingleArgMsg() = default;

  ArgType arg_;
};

/// Why a leader declined to serve a write a follower forwarded to it, or nothing if it never answered.
///
/// A follower cannot tell a reason that has already passed, such as a leader that was not ready, from one that will
/// never clear, such as an id naming no coordinator, unless the reason itself travels back. A peer that predates it
/// travelling reads a single flag instead, and every reason other than success reaches it as a failure.
///
/// New enumerators go last in the statuses carried this way: an older peer must keep decoding the values it knows.
template <utils::Enum StatusEnum>
struct ForwardedStatus {
  std::optional<StatusEnum> status_;

  bool has_value() const { return status_.has_value(); }

  StatusEnum operator*() const { return *status_; }

  bool Downgrade() const { return status_ == StatusEnum::SUCCESS; }
};

// PromoteToMainReq gained a `writing_enabled` flag in v2: the coordinator projects its global_read_only setting onto
// the promoted main (writing_enabled = !global_read_only) so read-only is honored across failover. v1 carried only the
// uuid and replicas; a v1 sender doesn't know about read-only mode, so Upgrade keeps writing enabled to preserve
// pre-feature behavior.
struct PromoteToMainReqV1 {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_FAILOVER_REQ, .name = "PromoteToMainReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(PromoteToMainReqV1 *self, memgraph::slk::Reader *reader);
  static void Save(const PromoteToMainReqV1 &self, memgraph::slk::Builder *builder);

  explicit PromoteToMainReqV1(const utils::UUID &uuid, std::vector<ReplicationClientInfo> replication_clients_info)
      : main_uuid(uuid), replication_clients_info(std::move(replication_clients_info)) {}

  PromoteToMainReqV1() = default;

  utils::UUID main_uuid;
  std::vector<ReplicationClientInfo> replication_clients_info;
};

struct PromoteToMainReq {
  static constexpr utils::TypeInfo kType{PromoteToMainReqV1::kType};
  static constexpr uint64_t kVersion{2};

  static void Load(PromoteToMainReq *self, memgraph::slk::Reader *reader);
  static void Save(const PromoteToMainReq &self, memgraph::slk::Builder *builder);

  PromoteToMainReq(const utils::UUID &uuid, std::vector<ReplicationClientInfo> replication_clients_info,
                   bool writing_enabled)
      : main_uuid(uuid),
        replication_clients_info(std::move(replication_clients_info)),
        writing_enabled(writing_enabled) {}

  PromoteToMainReq() = default;

  // A v1 sender doesn't know about read-only mode; keep writing enabled to preserve pre-feature behavior.
  static PromoteToMainReq Upgrade(PromoteToMainReqV1 prev) {
    return PromoteToMainReq{prev.main_uuid, std::move(prev.replication_clients_info), true};
  }

  PromoteToMainReqV1 Downgrade() const { return PromoteToMainReqV1{main_uuid, replication_clients_info}; }

  utils::UUID main_uuid;
  std::vector<ReplicationClientInfo> replication_clients_info;
  bool writing_enabled;
};

struct PromoteToMainResV1 {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_FAILOVER_RES, .name = "PromoteToMainRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(PromoteToMainResV1 *self, memgraph::slk::Reader *reader);
  static void Save(const PromoteToMainResV1 &self, memgraph::slk::Builder *builder);

  explicit PromoteToMainResV1(bool success) : arg_(success) {}

  PromoteToMainResV1() = default;

  bool arg_;
};

struct PromoteToMainRes {
  static constexpr utils::TypeInfo kType{PromoteToMainResV1::kType};
  static constexpr uint64_t kVersion{2};

  static void Load(PromoteToMainRes *self, memgraph::slk::Reader *reader);
  static void Save(const PromoteToMainRes &self, memgraph::slk::Builder *builder);

  explicit PromoteToMainRes(bool success) : arg_(success) {}

  PromoteToMainRes() = default;

  PromoteToMainResV1 Downgrade() const { return PromoteToMainResV1{arg_}; }

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

// Each write below is served by whichever coordinator leads, so a follower forwards it and the leader's answer says
// why it declined. Version 1 of each answer carries a single flag instead, and a peer speaking that version is sent
// the flag.
using AddCoordinatorReqV1 =
    SingleArgMsg<utils::TypeId::COORD_ADD_COORD_REQ, "AddCoordinatorReq", 1, CoordinatorInstanceConfig>;
using AddCoordinatorReq = UpgradeableSingleArgMsg<AddCoordinatorReqV1>;
using AddCoordinatorResV1 = SingleArgMsg<utils::TypeId::COORD_ADD_COORD_RES, "AddCoordinatorRes", 1, bool>;
using AddCoordinatorRes = DowngradeableSingleArgMsg<AddCoordinatorResV1, ForwardedStatus<AddCoordinatorInstanceStatus>>;
using AddCoordinatorRpc = rpc::RequestResponse<AddCoordinatorReq, AddCoordinatorRes>;

using RemoveCoordinatorReqV1 = SingleArgMsg<utils::TypeId::COORD_REMOVE_COORD_REQ, "RemoveCoordinatorReq", 1, int>;
using RemoveCoordinatorReq = UpgradeableSingleArgMsg<RemoveCoordinatorReqV1>;
using RemoveCoordinatorResV1 = SingleArgMsg<utils::TypeId::COORD_REMOVE_COORD_RES, "RemoveCoordinatorRes", 1, bool>;
using RemoveCoordinatorRes =
    DowngradeableSingleArgMsg<RemoveCoordinatorResV1, ForwardedStatus<RemoveCoordinatorInstanceStatus>>;
using RemoveCoordinatorRpc = rpc::RequestResponse<RemoveCoordinatorReq, RemoveCoordinatorRes>;

using RegisterInstanceReqV1 =
    SingleArgMsg<utils::TypeId::COORD_REGISTER_INSTANCE_REQ, "RegisterInstanceReq", 1, DataInstanceConfig>;
using RegisterInstanceReq = UpgradeableSingleArgMsg<RegisterInstanceReqV1>;
using RegisterInstanceResV1 = SingleArgMsg<utils::TypeId::COORD_REGISTER_INSTANCE_RES, "RegisterInstanceRes", 1, bool>;
using RegisterInstanceRes =
    DowngradeableSingleArgMsg<RegisterInstanceResV1, ForwardedStatus<RegisterInstanceCoordinatorStatus>>;
using RegisterInstanceRpc = rpc::RequestResponse<RegisterInstanceReq, RegisterInstanceRes>;

using UnregisterInstanceReqV1 =
    SingleArgMsg<utils::TypeId::COORD_UNREGISTER_INSTANCE_REQ, "UnregisterInstanceReq", 1, std::string>;
using UnregisterInstanceReq = UpgradeableSingleArgMsg<UnregisterInstanceReqV1>;
using UnregisterInstanceResV1 =
    SingleArgMsg<utils::TypeId::COORD_UNREGISTER_INSTANCE_RES, "UnregisterInstanceRes", 1, bool>;
using UnregisterInstanceRes =
    DowngradeableSingleArgMsg<UnregisterInstanceResV1, ForwardedStatus<UnregisterInstanceCoordinatorStatus>>;
using UnregisterInstanceRpc = rpc::RequestResponse<UnregisterInstanceReq, UnregisterInstanceRes>;

using SetInstanceToMainReqV1 =
    SingleArgMsg<utils::TypeId::COORD_SET_INSTANCE_TO_MAIN_REQ, "SetInstanceToMainReq", 1, std::string>;
using SetInstanceToMainReq = UpgradeableSingleArgMsg<SetInstanceToMainReqV1>;
using SetInstanceToMainResV1 =
    SingleArgMsg<utils::TypeId::COORD_SET_INSTANCE_TO_MAIN_RES, "SetInstanceToMainRes", 1, bool>;
using SetInstanceToMainRes =
    DowngradeableSingleArgMsg<SetInstanceToMainResV1, ForwardedStatus<SetInstanceToMainCoordinatorStatus>>;
using SetInstanceToMainRpc = rpc::RequestResponse<SetInstanceToMainReq, SetInstanceToMainRes>;

using DemoteInstanceReqV1 = SingleArgMsg<utils::TypeId::COORD_DEMOTE_INSTANCE_REQ, "DemoteInstanceReq", 1, std::string>;
using DemoteInstanceReq = UpgradeableSingleArgMsg<DemoteInstanceReqV1>;
using DemoteInstanceResV1 = SingleArgMsg<utils::TypeId::COORD_DEMOTE_INSTANCE_RES, "DemoteInstanceRes", 1, bool>;
using DemoteInstanceRes =
    DowngradeableSingleArgMsg<DemoteInstanceResV1, ForwardedStatus<DemoteInstanceCoordinatorStatus>>;
using DemoteInstanceRpc = rpc::RequestResponse<DemoteInstanceReq, DemoteInstanceRes>;

using ForceResetReqV1 = EmptyReq<utils::TypeId::COORD_FORCE_RESET_REQ, "ForceResetReq", 1>;
using ForceResetReq = UpgradeableEmptyReq<ForceResetReqV1>;
using ForceResetResV1 = SingleArgMsg<utils::TypeId::COORD_FORCE_RESET_RES, "ForceResetRes", 1, bool>;
using ForceResetRes = DowngradeableSingleArgMsg<ForceResetResV1, ForwardedStatus<ReconcileClusterStateStatus>>;
using ForceResetRpc = rpc::RequestResponse<ForceResetReq, ForceResetRes>;

using YieldLeadershipReqV1 = EmptyReq<utils::TypeId::COORD_YIELD_LEADERSHIP_REQ, "YieldLeadershipReq", 1>;
using YieldLeadershipReq = UpgradeableEmptyReq<YieldLeadershipReqV1>;
using YieldLeadershipResV1 = SingleArgMsg<utils::TypeId::COORD_YIELD_LEADERSHIP_RES, "YieldLeadershipRes", 1, bool>;
using YieldLeadershipRes = DowngradeableSingleArgMsg<YieldLeadershipResV1, ForwardedStatus<YieldLeadershipStatus>>;
using YieldLeadershipRpc = rpc::RequestResponse<YieldLeadershipReq, YieldLeadershipRes>;

using ShowCoordSettingsReq = EmptyReq<utils::TypeId::COORD_SHOW_COORD_SETTINGS_REQ, "ShowCoordSettingsReq", 1>;
// nullopt when the leader couldn't serve the request.
using ShowCoordSettingsRes = SingleArgMsg<utils::TypeId::COORD_SHOW_COORD_SETTINGS_RES, "ShowCoordSettingsRes", 1,
                                          std::optional<std::vector<std::pair<std::string, std::string>>>>;
using ShowCoordSettingsRpc = rpc::RequestResponse<ShowCoordSettingsReq, ShowCoordSettingsRes>;

using UpdateConfigReqV1 =
    SingleArgMsg<utils::TypeId::COORD_UPDATE_CONFIG_REQ, "UpdateConfigReq", 1, UpdateInstanceConfig>;
using UpdateConfigReq = UpgradeableSingleArgMsg<UpdateConfigReqV1>;
using UpdateConfigResV1 = SingleArgMsg<utils::TypeId::COORD_UPDATE_CONFIG_RES, "UpdateConfigRes", 1, bool>;
using UpdateConfigRes = DowngradeableSingleArgMsg<UpdateConfigResV1, ForwardedStatus<UpdateConfigStatus>>;
using UpdateConfigRpc = rpc::RequestResponse<UpdateConfigReq, UpdateConfigRes>;

// v1 answered with a bare map, where an empty map meant both "the leader has no lag data" and "the leader couldn't
// collect it". v2 answers with ReplicationLagResult so a forwarding follower can tell the two apart and report why.
// The request is bumped alongside it because the response version follows the request version.
using CoordReplicationLagReqV1 = EmptyReq<utils::TypeId::COORD_REPL_LAG_REQ, "CoordReplLagReq", 1>;
using CoordReplicationLagReq = UpgradeableEmptyReq<CoordReplicationLagReqV1>;
using CoordReplicationLagResV1 =
    SingleArgMsg<utils::TypeId::COORD_REPL_LAG_RES, "CoordReplLagRes", 1, ReplicationLagData>;
using CoordReplicationLagRes = DowngradeableSingleArgMsg<CoordReplicationLagResV1, ReplicationLagResult>;
using CoordReplicationLagRpc = rpc::RequestResponse<CoordReplicationLagReq, CoordReplicationLagRes>;

// RPC for updating data instance config managed by the coordinator. v1 carried only deltas_batch_progress_size; v2
// additionally carries disable_writing (the projection of the coordinator's global_read_only setting). The two config
// items always travel together.
struct UpdateDataInstanceConfigReqV1 {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_UPDATE_DATA_INSTANCE_CONFIG_REQ,
                                         .name = "UpdateDataInstanceConfigReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(UpdateDataInstanceConfigReqV1 *self, memgraph::slk::Reader *reader);
  static void Save(const UpdateDataInstanceConfigReqV1 &self, memgraph::slk::Builder *builder);

  explicit UpdateDataInstanceConfigReqV1(uint64_t deltas_batch_progress_size)
      : deltas_batch_progress_size(deltas_batch_progress_size) {}

  UpdateDataInstanceConfigReqV1() = default;

  uint64_t deltas_batch_progress_size;
};

struct UpdateDataInstanceConfigReq {
  static constexpr utils::TypeInfo kType{UpdateDataInstanceConfigReqV1::kType};
  static constexpr uint64_t kVersion{2};

  static void Load(UpdateDataInstanceConfigReq *self, memgraph::slk::Reader *reader);
  static void Save(const UpdateDataInstanceConfigReq &self, memgraph::slk::Builder *builder);

  UpdateDataInstanceConfigReq(uint64_t deltas_batch_progress_size, bool disable_writing)
      : deltas_batch_progress_size(deltas_batch_progress_size), disable_writing(disable_writing) {}

  UpdateDataInstanceConfigReq() = default;

  // A v1 sender doesn't know about read-only mode; keep writing enabled to preserve pre-feature behavior.
  static UpdateDataInstanceConfigReq Upgrade(UpdateDataInstanceConfigReqV1 const &prev) {
    return UpdateDataInstanceConfigReq{prev.deltas_batch_progress_size, false};
  }

  UpdateDataInstanceConfigReqV1 Downgrade() const { return UpdateDataInstanceConfigReqV1{deltas_batch_progress_size}; }

  uint64_t deltas_batch_progress_size;
  bool disable_writing;
};

struct UpdateDataInstanceConfigResV1 {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::COORD_UPDATE_DATA_INSTANCE_CONFIG_RES,
                                         .name = "UpdateDataInstanceConfigRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(UpdateDataInstanceConfigResV1 *self, memgraph::slk::Reader *reader);
  static void Save(const UpdateDataInstanceConfigResV1 &self, memgraph::slk::Builder *builder);

  explicit UpdateDataInstanceConfigResV1(bool success) : arg_(success) {}

  UpdateDataInstanceConfigResV1() = default;

  bool arg_;
};

struct UpdateDataInstanceConfigRes {
  static constexpr utils::TypeInfo kType{UpdateDataInstanceConfigResV1::kType};
  static constexpr uint64_t kVersion{2};

  static void Load(UpdateDataInstanceConfigRes *self, memgraph::slk::Reader *reader);
  static void Save(const UpdateDataInstanceConfigRes &self, memgraph::slk::Builder *builder);

  explicit UpdateDataInstanceConfigRes(bool success) : arg_(success) {}

  UpdateDataInstanceConfigRes() = default;

  UpdateDataInstanceConfigResV1 Downgrade() const { return UpdateDataInstanceConfigResV1{arg_}; }

  bool arg_;
};

using UpdateDataInstanceConfigRpc = rpc::RequestResponse<UpdateDataInstanceConfigReq, UpdateDataInstanceConfigRes>;

// Coordinator->leader role/privilege RPCs. Follower coordinators forward role and privilege queries to the leader so
// the query works from any coordinator. Write responses carry the leader's exact status so the follower reports the
// same reason as the leader (e.g. NO_SUCH_ROLE, or ROLE_ALREADY_EXISTS which CREATE ROLE IF NOT EXISTS must treat as
// a no-op). An empty optional means the RPC itself failed (dead leader), so the follower reports a forwarding error.
using CreateRoleReq = SingleArgMsg<utils::TypeId::COORD_CREATE_ROLE_REQ, "CreateRoleReq", 1, std::string>;
using CreateRoleRes =
    SingleArgMsg<utils::TypeId::COORD_CREATE_ROLE_RES, "CreateRoleRes", 1, std::optional<CreateRoleStatus>>;
using CreateRoleRpc = rpc::RequestResponse<CreateRoleReq, CreateRoleRes>;

using DropRoleReq = SingleArgMsg<utils::TypeId::COORD_DROP_ROLE_REQ, "DropRoleReq", 1, std::string>;
using DropRoleRes = SingleArgMsg<utils::TypeId::COORD_DROP_ROLE_RES, "DropRoleRes", 1, std::optional<DropRoleStatus>>;
using DropRoleRpc = rpc::RequestResponse<DropRoleReq, DropRoleRes>;

// GetRoles reads the committed role set from the leader. An empty optional response means the receiver is not the ready
// leader (or the RPC failed), so the follower reports a forwarding error.
using GetRolesReq = EmptyReq<utils::TypeId::COORD_GET_ROLES_REQ, "GetRolesReq", 1>;
using GetRolesRes =
    SingleArgMsg<utils::TypeId::COORD_GET_ROLES_RES, "GetRolesRes", 1, std::optional<std::vector<CoordinatorRole>>>;
using GetRolesRpc = rpc::RequestResponse<GetRolesReq, GetRolesRes>;

// Grant/Revoke carry (role name, privilege mask). Response mirrors the write-status convention above.
using GrantPrivilegeReq =
    SingleArgMsg<utils::TypeId::COORD_GRANT_PRIVILEGE_REQ, "GrantPrivilegeReq", 1, std::pair<std::string, uint64_t>>;
using GrantPrivilegeRes =
    SingleArgMsg<utils::TypeId::COORD_GRANT_PRIVILEGE_RES, "GrantPrivilegeRes", 1, std::optional<GrantPrivilegeStatus>>;
using GrantPrivilegeRpc = rpc::RequestResponse<GrantPrivilegeReq, GrantPrivilegeRes>;

using RevokePrivilegeReq =
    SingleArgMsg<utils::TypeId::COORD_REVOKE_PRIVILEGE_REQ, "RevokePrivilegeReq", 1, std::pair<std::string, uint64_t>>;
using RevokePrivilegeRes = SingleArgMsg<utils::TypeId::COORD_REVOKE_PRIVILEGE_RES, "RevokePrivilegeRes", 1,
                                        std::optional<RevokePrivilegeStatus>>;
using RevokePrivilegeRpc = rpc::RequestResponse<RevokePrivilegeReq, RevokePrivilegeRes>;

// GetRolePrivileges reads one role's mask from the leader. The response pair is {role_found, mask}; an empty optional
// means the RPC failed, so the follower reports a forwarding error.
using GetRolePrivilegesReq =
    SingleArgMsg<utils::TypeId::COORD_GET_ROLE_PRIVILEGES_REQ, "GetRolePrivilegesReq", 1, std::string>;
using GetRolePrivilegesRes = SingleArgMsg<utils::TypeId::COORD_GET_ROLE_PRIVILEGES_RES, "GetRolePrivilegesRes", 1,
                                          std::optional<std::pair<bool, uint64_t>>>;
using GetRolePrivilegesRpc = rpc::RequestResponse<GetRolePrivilegesReq, GetRolePrivilegesRes>;

// SetCoordinatorSetting carries (setting name, setting value). Follower coordinators forward the write to the leader so
// the query works from any coordinator; the response mirrors the write-status convention above, carrying the leader's
// exact status so the follower reports the same reason (e.g. UNKNOWN_SETTING, INVALID_ARGUMENT).
using SetCoordinatorSettingReq = SingleArgMsg<utils::TypeId::COORD_SET_COORDINATOR_SETTING_REQ,
                                              "SetCoordinatorSettingReq", 1, std::pair<std::string, std::string>>;
using SetCoordinatorSettingRes =
    SingleArgMsg<utils::TypeId::COORD_SET_COORDINATOR_SETTING_RES, "SetCoordinatorSettingRes", 1,
                 std::optional<SetCoordinatorSettingStatus>>;
using SetCoordinatorSettingRpc = rpc::RequestResponse<SetCoordinatorSettingReq, SetCoordinatorSettingRes>;

}  // namespace memgraph::coordination

// SLK serialization declarations
namespace memgraph::slk {

template <memgraph::utils::Enum StatusEnum>
void Save(memgraph::coordination::ForwardedStatus<StatusEnum> const &self, Builder *builder) {
  Save(self.status_, builder);
}

template <memgraph::utils::Enum StatusEnum>
void Load(memgraph::coordination::ForwardedStatus<StatusEnum> *self, Reader *reader) {
  Load(&self->status_, reader);
}

// PromoteToMainRpc
void Save(const memgraph::coordination::PromoteToMainResV1 &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::PromoteToMainResV1 *self, memgraph::slk::Reader *reader);
void Save(const memgraph::coordination::PromoteToMainRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::PromoteToMainRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::coordination::PromoteToMainReqV1 &self, memgraph::slk::Builder *builder);
void Load(memgraph::coordination::PromoteToMainReqV1 *self, memgraph::slk::Reader *reader);
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
DECLARE_SLK_FREE_FUNCTIONS(coordination::YieldLeadershipRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::ShowCoordSettingsRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::UpdateConfigRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::CoordReplicationLagRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::CreateRoleRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::DropRoleRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::GetRolesRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::GrantPrivilegeRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::RevokePrivilegeRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::GetRolePrivilegesRpc)
DECLARE_SLK_FREE_FUNCTIONS(coordination::SetCoordinatorSettingRpc)

DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::AddCoordinatorReqV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::AddCoordinatorResV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::RemoveCoordinatorReqV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::RemoveCoordinatorResV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::RegisterInstanceReqV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::RegisterInstanceResV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::UnregisterInstanceReqV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::UnregisterInstanceResV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::SetInstanceToMainReqV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::SetInstanceToMainResV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::DemoteInstanceReqV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::DemoteInstanceResV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::ForceResetReqV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::ForceResetResV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::YieldLeadershipReqV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::YieldLeadershipResV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::UpdateConfigReqV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::UpdateConfigResV1)

DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::UpdateDataInstanceConfigReqV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::UpdateDataInstanceConfigReq)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::UpdateDataInstanceConfigResV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::UpdateDataInstanceConfigRes)

DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::CoordReplicationLagReqV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::CoordReplicationLagResV1)

DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::StateCheckReqV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::StateCheckReqV2)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::StateCheckResV1)
DECLARE_SLK_SERIALIZATION_FUNCTIONS(coordination::StateCheckResV2)

DECLARE_SLK_FREE_FUNCTIONS(coordination::StateCheckRpc)

}  // namespace memgraph::slk

#endif

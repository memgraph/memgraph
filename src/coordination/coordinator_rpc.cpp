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

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_rpc.hpp"

#include "coordination/coordinator_slk.hpp"
#include "slk/serialization.hpp"

namespace memgraph {

namespace coordination {

void PromoteToMainReq::Save(const PromoteToMainReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void PromoteToMainReq::Load(PromoteToMainReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void PromoteToMainRes::Save(const PromoteToMainRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void PromoteToMainRes::Load(PromoteToMainRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void DemoteMainToReplicaReq::Save(const DemoteMainToReplicaReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void DemoteMainToReplicaReq::Load(DemoteMainToReplicaReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void DemoteMainToReplicaRes::Save(const DemoteMainToReplicaRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void DemoteMainToReplicaRes::Load(DemoteMainToReplicaRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void UnregisterReplicaReq::Save(UnregisterReplicaReq const &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void UnregisterReplicaReq::Load(UnregisterReplicaReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void UnregisterReplicaRes::Save(UnregisterReplicaRes const &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void UnregisterReplicaRes::Load(UnregisterReplicaRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void EnableWritingOnMainRes::Save(EnableWritingOnMainRes const &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void EnableWritingOnMainRes::Load(EnableWritingOnMainRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void EnableWritingOnMainReq::Save(EnableWritingOnMainReq const & /*self*/, memgraph::slk::Builder * /*builder*/) {}

void EnableWritingOnMainReq::Load(EnableWritingOnMainReq * /*self*/, memgraph::slk::Reader * /*reader*/) {}

// ShowInstances
void ShowInstancesReq::Save(const ShowInstancesReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void ShowInstancesReq::Load(ShowInstancesReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void ShowInstancesRes::Save(const ShowInstancesRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void ShowInstancesRes::Load(ShowInstancesRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

// StateCheck
void StateCheckReq::Save(const StateCheckReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void StateCheckReq::Load(StateCheckReq *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }

void StateCheckRes::Save(const StateCheckRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void StateCheckRes::Load(StateCheckRes *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }

// GetDatabaseHistoriesRpc

void GetDatabaseHistoriesReq::Save(const GetDatabaseHistoriesReq & /*self*/, memgraph::slk::Builder * /*builder*/) {
  /* nothing to serialize */
}

void GetDatabaseHistoriesReq::Load(GetDatabaseHistoriesReq * /*self*/, memgraph::slk::Reader * /*reader*/) {
  /* nothing to serialize */
}

void GetDatabaseHistoriesRes::Save(const GetDatabaseHistoriesRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void GetDatabaseHistoriesRes::Load(GetDatabaseHistoriesRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

// RegisterReplicaOnMain
void RegisterReplicaOnMainReq::Load(RegisterReplicaOnMainReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void RegisterReplicaOnMainReq::Save(const RegisterReplicaOnMainReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void RegisterReplicaOnMainRes::Load(RegisterReplicaOnMainRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void RegisterReplicaOnMainRes::Save(const RegisterReplicaOnMainRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

}  // namespace coordination

constexpr utils::TypeInfo coordination::PromoteToMainReq::kType{utils::TypeId::COORD_FAILOVER_REQ, "PromoteToMainReq",
                                                                nullptr};

constexpr utils::TypeInfo coordination::PromoteToMainRes::kType{utils::TypeId::COORD_FAILOVER_RES, "PromoteToMainRes",
                                                                nullptr};

constexpr utils::TypeInfo coordination::DemoteMainToReplicaReq::kType{utils::TypeId::COORD_SET_REPL_MAIN_REQ,
                                                                      "DemoteMainToReplicaReq", nullptr};

constexpr utils::TypeInfo coordination::DemoteMainToReplicaRes::kType{utils::TypeId::COORD_SET_REPL_MAIN_RES,

                                                                      "DemoteMainToReplicaRes", nullptr};

constexpr utils::TypeInfo coordination::UnregisterReplicaReq::kType{utils::TypeId::COORD_UNREGISTER_REPLICA_REQ,
                                                                    "UnregisterReplicaReq", nullptr};

constexpr utils::TypeInfo coordination::UnregisterReplicaRes::kType{utils::TypeId::COORD_UNREGISTER_REPLICA_RES,
                                                                    "UnregisterReplicaRes", nullptr};

constexpr utils::TypeInfo coordination::EnableWritingOnMainReq::kType{utils::TypeId::COORD_ENABLE_WRITING_ON_MAIN_REQ,
                                                                      "EnableWritingOnMainReq", nullptr};

constexpr utils::TypeInfo coordination::EnableWritingOnMainRes::kType{utils::TypeId::COORD_ENABLE_WRITING_ON_MAIN_RES,
                                                                      "EnableWritingOnMainRes", nullptr};

constexpr utils::TypeInfo coordination::GetDatabaseHistoriesReq::kType{utils::TypeId::COORD_GET_INSTANCE_DATABASES_REQ,
                                                                       "GetDatabaseHistoriesReq", nullptr};

constexpr utils::TypeInfo coordination::GetDatabaseHistoriesRes::kType{utils::TypeId::COORD_GET_INSTANCE_DATABASES_RES,
                                                                       "GetDatabaseHistoriesRes", nullptr};

constexpr utils::TypeInfo coordination::RegisterReplicaOnMainReq::kType{
    utils::TypeId::COORD_REGISTER_REPLICA_ON_MAIN_REQ, "RegisterReplicaOnMainReq", nullptr};

constexpr utils::TypeInfo coordination::RegisterReplicaOnMainRes::kType{
    utils::TypeId::COORD_REGISTER_REPLICA_ON_MAIN_RES, "RegisterReplicaOnMainRes", nullptr};

constexpr utils::TypeInfo coordination::ShowInstancesReq::kType{utils::TypeId::COORD_SHOW_INSTANCES_REQ,
                                                                "ShowInstancesReq", nullptr};

constexpr utils::TypeInfo coordination::ShowInstancesRes::kType{utils::TypeId::COORD_SHOW_INSTANCES_RES,
                                                                "ShowInstancesRes", nullptr};

constexpr utils::TypeInfo coordination::StateCheckReq::kType{utils::TypeId::COORD_STATE_CHECK_REQ, "StateCheckReq",
                                                             nullptr};

constexpr utils::TypeInfo coordination::StateCheckRes::kType{utils::TypeId::COORD_STATE_CHECK_RES, "StateCheckRes",
                                                             nullptr};

namespace slk {

// PromoteToMainRpc

void Save(const memgraph::coordination::PromoteToMainRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::coordination::PromoteToMainRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

void Save(const memgraph::coordination::PromoteToMainReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.replication_clients_info, builder);
}

void Load(memgraph::coordination::PromoteToMainReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->replication_clients_info, reader);
}

// DemoteMainToReplicaRpc
void Save(const memgraph::coordination::DemoteMainToReplicaReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.replication_client_info_, builder);
}

void Load(memgraph::coordination::DemoteMainToReplicaReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->replication_client_info_, reader);
}

void Save(const memgraph::coordination::DemoteMainToReplicaRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::coordination::DemoteMainToReplicaRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

// UnregisterReplicaRpc

void Save(memgraph::coordination::UnregisterReplicaReq const &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.instance_name, builder);
}

void Load(memgraph::coordination::UnregisterReplicaReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->instance_name, reader);
}

void Save(memgraph::coordination::UnregisterReplicaRes const &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::coordination::UnregisterReplicaRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

void Save(memgraph::coordination::EnableWritingOnMainRes const &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::coordination::EnableWritingOnMainRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

// GetDatabaseHistoriesRpc

void Save(const memgraph::coordination::GetDatabaseHistoriesRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.instance_info, builder);
}

void Load(memgraph::coordination::GetDatabaseHistoriesRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->instance_info, reader);
}

// RegisterReplicaOnMainRpc

void Save(const memgraph::coordination::RegisterReplicaOnMainRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::coordination::RegisterReplicaOnMainRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

void Save(const memgraph::coordination::RegisterReplicaOnMainReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.replication_client_info, builder);
}

void Load(memgraph::coordination::RegisterReplicaOnMainReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->replication_client_info, reader);
}

// ShowInstancesRpc
void Save(const memgraph::coordination::ShowInstancesReq &self, memgraph::slk::Builder *builder) { /*empty*/
}

void Load(memgraph::coordination::ShowInstancesReq *self, memgraph::slk::Reader *reader) { /*empty*/
}

void Save(const memgraph::coordination::ShowInstancesRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.instances_status_, builder);
}

void Load(memgraph::coordination::ShowInstancesRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->instances_status_, reader);
}

void Save(const memgraph::coordination::StateCheckReq &self, memgraph::slk::Builder *builder) { /*empty*/
}

void Load(memgraph::coordination::StateCheckReq *self, memgraph::slk::Reader *reader) { /*empty*/
}

void Save(const memgraph::coordination::StateCheckRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.state, builder);
}

void Load(memgraph::coordination::StateCheckRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->state, reader);
}

}  // namespace slk

}  // namespace memgraph

#endif

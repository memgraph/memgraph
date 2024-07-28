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

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_rpc.hpp"

#include "coordination/coordinator_slk.hpp"
#include "slk/serialization.hpp"

namespace memgraph {

namespace coordination {

void PromoteReplicaToMainReq::Save(const PromoteReplicaToMainReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void PromoteReplicaToMainReq::Load(PromoteReplicaToMainReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void PromoteReplicaToMainRes::Save(const PromoteReplicaToMainRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void PromoteReplicaToMainRes::Load(PromoteReplicaToMainRes *self, memgraph::slk::Reader *reader) {
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

// GetInstanceUUID
void GetInstanceUUIDReq::Save(const GetInstanceUUIDReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void GetInstanceUUIDReq::Load(GetInstanceUUIDReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void GetInstanceUUIDRes::Save(const GetInstanceUUIDRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void GetInstanceUUIDRes::Load(GetInstanceUUIDRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

// GetInstanceUUID
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

constexpr utils::TypeInfo coordination::PromoteReplicaToMainReq::kType{utils::TypeId::COORD_FAILOVER_REQ,
                                                                       "CoordPromoteReplicaToMainReq", nullptr};

constexpr utils::TypeInfo coordination::PromoteReplicaToMainRes::kType{utils::TypeId::COORD_FAILOVER_RES,
                                                                       "CoordPromoteReplicaToMainRes", nullptr};

constexpr utils::TypeInfo coordination::DemoteMainToReplicaReq::kType{utils::TypeId::COORD_SET_REPL_MAIN_REQ,
                                                                      "CoordDemoteToReplicaReq", nullptr};

constexpr utils::TypeInfo coordination::DemoteMainToReplicaRes::kType{utils::TypeId::COORD_SET_REPL_MAIN_RES,

                                                                      "CoordDemoteToReplicaRes", nullptr};

constexpr utils::TypeInfo coordination::UnregisterReplicaReq::kType{utils::TypeId::COORD_UNREGISTER_REPLICA_REQ,
                                                                    "UnregisterReplicaReq", nullptr};

constexpr utils::TypeInfo coordination::UnregisterReplicaRes::kType{utils::TypeId::COORD_UNREGISTER_REPLICA_RES,
                                                                    "UnregisterReplicaRes", nullptr};

constexpr utils::TypeInfo coordination::EnableWritingOnMainReq::kType{utils::TypeId::COORD_ENABLE_WRITING_ON_MAIN_REQ,
                                                                      "CoordEnableWritingOnMainReq", nullptr};

constexpr utils::TypeInfo coordination::EnableWritingOnMainRes::kType{utils::TypeId::COORD_ENABLE_WRITING_ON_MAIN_RES,
                                                                      "CoordEnableWritingOnMainRes", nullptr};

constexpr utils::TypeInfo coordination::GetInstanceUUIDReq::kType{utils::TypeId::COORD_GET_UUID_REQ, "CoordGetUUIDReq",
                                                                  nullptr};

constexpr utils::TypeInfo coordination::GetInstanceUUIDRes::kType{utils::TypeId::COORD_GET_UUID_RES, "CoordGetUUIDRes",
                                                                  nullptr};

constexpr utils::TypeInfo coordination::GetDatabaseHistoriesReq::kType{utils::TypeId::COORD_GET_INSTANCE_DATABASES_REQ,
                                                                       "GetInstanceDatabasesReq", nullptr};

constexpr utils::TypeInfo coordination::GetDatabaseHistoriesRes::kType{utils::TypeId::COORD_GET_INSTANCE_DATABASES_RES,
                                                                       "GetInstanceDatabasesRes", nullptr};

constexpr utils::TypeInfo coordination::RegisterReplicaOnMainReq::kType{
    utils::TypeId::COORD_REGISTER_REPLICA_ON_MAIN_REQ, "RegisterReplicaOnMainReq", nullptr};

constexpr utils::TypeInfo coordination::RegisterReplicaOnMainRes::kType{
    utils::TypeId::COORD_REGISTER_REPLICA_ON_MAIN_RES, "RegisterReplicaOnMainRes", nullptr};

constexpr utils::TypeInfo coordination::ShowInstancesReq::kType{utils::TypeId::COORD_SHOW_INSTANCES_REQ,
                                                                "ShowInstancesReq", nullptr};

constexpr utils::TypeInfo coordination::ShowInstancesRes::kType{utils::TypeId::COORD_SHOW_INSTANCES_RES,
                                                                "ShowInstancesRes", nullptr};

namespace slk {

// PromoteReplicaToMainRpc

void Save(const memgraph::coordination::PromoteReplicaToMainRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::coordination::PromoteReplicaToMainRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

void Save(const memgraph::coordination::PromoteReplicaToMainReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.replication_clients_info, builder);
}

void Load(memgraph::coordination::PromoteReplicaToMainReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->replication_clients_info, reader);
}

// DemoteMainToReplicaRpc
void Save(const memgraph::coordination::DemoteMainToReplicaReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.replication_client_info, builder);
}

void Load(memgraph::coordination::DemoteMainToReplicaReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->replication_client_info, reader);
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

// GetInstanceUUIDRpc

void Save(const memgraph::coordination::GetInstanceUUIDReq & /*self*/, memgraph::slk::Builder * /*builder*/) {
  /* nothing to serialize*/
}

void Load(memgraph::coordination::GetInstanceUUIDReq * /*self*/, memgraph::slk::Reader * /*reader*/) {
  /* nothing to serialize*/
}

void Save(const memgraph::coordination::GetInstanceUUIDRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.uuid, builder);
}

void Load(memgraph::coordination::GetInstanceUUIDRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->uuid, reader);
}

// GetInstanceTimestampsRpc

void Save(const memgraph::coordination::GetDatabaseHistoriesRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.database_histories, builder);
}

void Load(memgraph::coordination::GetDatabaseHistoriesRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->database_histories, reader);
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

// RegisterReplicaOnMainRpc

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

}  // namespace slk

}  // namespace memgraph

#endif

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

#include "replication_handler/system_rpc.hpp"

#include "auth/rpc.hpp"
#include "parameters/parameters.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"
#include "utils/enum.hpp"

namespace memgraph::slk {

// NOLINTNEXTLINE(misc-use-internal-linkage)
void Save(const memgraph::parameters::ParameterInfo &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.name, builder);
  memgraph::slk::Save(self.value, builder);
  memgraph::slk::Save(self.scope_context, builder);
}

// NOLINTNEXTLINE(misc-use-internal-linkage)
void Load(memgraph::parameters::ParameterInfo *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->name, reader);
  memgraph::slk::Load(&self->value, reader);
  memgraph::slk::Load(&self->scope_context, reader);
}

// Serialize code for storage::StorageInfo (flat POD; carried in SystemRecoveryReq V3 for COLD tenants).
void Save(const memgraph::storage::StorageInfo &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.vertex_count, builder);
  memgraph::slk::Save(self.edge_count, builder);
  memgraph::slk::Save(self.average_degree, builder);
  memgraph::slk::Save(self.memory_res, builder);
  memgraph::slk::Save(self.peak_memory_res, builder);
  memgraph::slk::Save(self.unreleased_delta_objects, builder);
  memgraph::slk::Save(self.disk_usage, builder);
  memgraph::slk::Save(self.label_indices, builder);
  memgraph::slk::Save(self.label_property_indices, builder);
  memgraph::slk::Save(self.text_indices, builder);
  memgraph::slk::Save(self.vector_indices, builder);
  memgraph::slk::Save(self.vector_edge_indices, builder);
  memgraph::slk::Save(self.existence_constraints, builder);
  memgraph::slk::Save(self.unique_constraints, builder);
  memgraph::slk::Save(self.type_constraints, builder);
  memgraph::slk::Save(std::to_underlying(self.storage_mode), builder);
  memgraph::slk::Save(std::to_underlying(self.isolation_level), builder);
  memgraph::slk::Save(self.durability_snapshot_enabled, builder);
  memgraph::slk::Save(self.durability_wal_enabled, builder);
  memgraph::slk::Save(self.property_store_compression_enabled, builder);
  memgraph::slk::Save(std::to_underlying(self.property_store_compression_level), builder);
  memgraph::slk::Save(self.schema_vertex_count, builder);
  memgraph::slk::Save(self.schema_edge_count, builder);
}

void Load(memgraph::storage::StorageInfo *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->vertex_count, reader);
  memgraph::slk::Load(&self->edge_count, reader);
  memgraph::slk::Load(&self->average_degree, reader);
  memgraph::slk::Load(&self->memory_res, reader);
  memgraph::slk::Load(&self->peak_memory_res, reader);
  memgraph::slk::Load(&self->unreleased_delta_objects, reader);
  memgraph::slk::Load(&self->disk_usage, reader);
  memgraph::slk::Load(&self->label_indices, reader);
  memgraph::slk::Load(&self->label_property_indices, reader);
  memgraph::slk::Load(&self->text_indices, reader);
  memgraph::slk::Load(&self->vector_indices, reader);
  memgraph::slk::Load(&self->vector_edge_indices, reader);
  memgraph::slk::Load(&self->existence_constraints, reader);
  memgraph::slk::Load(&self->unique_constraints, reader);
  memgraph::slk::Load(&self->type_constraints, reader);
  uint8_t sm = 0;
  memgraph::slk::Load(&sm, reader);
  if (!utils::NumToEnum(sm, self->storage_mode)) throw SlkReaderException("Unexpected result line:{}!", __LINE__);
  // IsolationLevel / CompressionLevel have no Enum::N sentinel (NumToEnum unusable); the value comes
  // from a same-version MAIN, so direct-cast the underlying integer.
  uint8_t il = 0;
  memgraph::slk::Load(&il, reader);
  self->isolation_level = static_cast<storage::IsolationLevel>(il);
  memgraph::slk::Load(&self->durability_snapshot_enabled, reader);
  memgraph::slk::Load(&self->durability_wal_enabled, reader);
  memgraph::slk::Load(&self->property_store_compression_enabled, reader);
  uint8_t cl = 0;
  memgraph::slk::Load(&cl, reader);
  self->property_store_compression_level = static_cast<utils::CompressionLevel>(cl);
  memgraph::slk::Load(&self->schema_vertex_count, reader);
  memgraph::slk::Load(&self->schema_edge_count, reader);
}

// Serialize code for SystemRecoveryReqV1
void Save(const memgraph::replication::SystemRecoveryReqV1 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.forced_group_timestamp, builder);
  memgraph::slk::Save(self.database_configs, builder);
  memgraph::slk::Save(self.auth_config, builder);
  memgraph::slk::Save(self.users, builder);
  memgraph::slk::Save(self.roles, builder);
  memgraph::slk::Save(self.profiles, builder);
}

void Load(memgraph::replication::SystemRecoveryReqV1 *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->forced_group_timestamp, reader);
  memgraph::slk::Load(&self->database_configs, reader);
  memgraph::slk::Load(&self->auth_config, reader);
  memgraph::slk::Load(&self->users, reader);
  memgraph::slk::Load(&self->roles, reader);
  memgraph::slk::Load(&self->profiles, reader);
}

// Serialize code for SystemRecoveryReqV2 (with parameters)
void Save(const memgraph::replication::SystemRecoveryReqV2 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.forced_group_timestamp, builder);
  memgraph::slk::Save(self.database_configs, builder);
  memgraph::slk::Save(self.auth_config, builder);
  memgraph::slk::Save(self.users, builder);
  memgraph::slk::Save(self.roles, builder);
  memgraph::slk::Save(self.profiles, builder);
  memgraph::slk::Save(self.parameters, builder);
}

void Load(memgraph::replication::SystemRecoveryReqV2 *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->forced_group_timestamp, reader);
  memgraph::slk::Load(&self->database_configs, reader);
  memgraph::slk::Load(&self->auth_config, reader);
  memgraph::slk::Load(&self->users, reader);
  memgraph::slk::Load(&self->roles, reader);
  memgraph::slk::Load(&self->profiles, reader);
  memgraph::slk::Load(&self->parameters, reader);
}

// Serialize code for SystemRecoveryReq (v3, with the hot/cold COLD set)
void Save(const memgraph::replication::SystemRecoveryReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.forced_group_timestamp, builder);
  memgraph::slk::Save(self.database_configs, builder);
  memgraph::slk::Save(self.auth_config, builder);
  memgraph::slk::Save(self.users, builder);
  memgraph::slk::Save(self.roles, builder);
  memgraph::slk::Save(self.profiles, builder);
  memgraph::slk::Save(self.parameters, builder);
  memgraph::slk::Save(self.cold_database_configs, builder);
  memgraph::slk::Save(self.cold_database_stats, builder);
}

void Load(memgraph::replication::SystemRecoveryReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->forced_group_timestamp, reader);
  memgraph::slk::Load(&self->database_configs, reader);
  memgraph::slk::Load(&self->auth_config, reader);
  memgraph::slk::Load(&self->users, reader);
  memgraph::slk::Load(&self->roles, reader);
  memgraph::slk::Load(&self->profiles, reader);
  memgraph::slk::Load(&self->parameters, reader);
  memgraph::slk::Load(&self->cold_database_configs, reader);
  memgraph::slk::Load(&self->cold_database_stats, reader);
}

// Serialize code for SystemRecoveryResV1 (same layout as Res)
void Save(const memgraph::replication::SystemRecoveryResV1 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.result, builder);
}

void Load(memgraph::replication::SystemRecoveryResV1 *self, memgraph::slk::Reader *reader) {
  uint8_t res = 0;
  memgraph::slk::Load(&res, reader);
  if (!utils::NumToEnum(res, self->result)) {
    throw SlkReaderException("Unexpected result line:{}!", __LINE__);
  }
}

// Serialize code for SystemRecoveryResV2 (same layout as Res)
void Save(const memgraph::replication::SystemRecoveryResV2 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.result, builder);
}

void Load(memgraph::replication::SystemRecoveryResV2 *self, memgraph::slk::Reader *reader) {
  uint8_t res = 0;
  memgraph::slk::Load(&res, reader);
  if (!utils::NumToEnum(res, self->result)) {
    throw SlkReaderException("Unexpected result line:{}!", __LINE__);
  }
}

// Serialize code for SystemRecoveryRes
void Save(const memgraph::replication::SystemRecoveryRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.result, builder);
}

void Load(memgraph::replication::SystemRecoveryRes *self, memgraph::slk::Reader *reader) {
  uint8_t res = 0;
  memgraph::slk::Load(&res, reader);
  if (!utils::NumToEnum(res, self->result)) {
    throw SlkReaderException("Unexpected result line:{}!", __LINE__);
  }
}

}  // namespace memgraph::slk

namespace memgraph::replication {

void SystemRecoveryReqV1::Save(const SystemRecoveryReqV1 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void SystemRecoveryReqV1::Load(SystemRecoveryReqV1 *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void SystemRecoveryReqV2::Save(const SystemRecoveryReqV2 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void SystemRecoveryReqV2::Load(SystemRecoveryReqV2 *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void SystemRecoveryReq::Save(const SystemRecoveryReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void SystemRecoveryReq::Load(SystemRecoveryReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void SystemRecoveryResV2::Save(const SystemRecoveryResV2 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void SystemRecoveryResV2::Load(SystemRecoveryResV2 *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void SystemRecoveryRes::Save(const SystemRecoveryRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void SystemRecoveryRes::Load(SystemRecoveryRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void SystemRecoveryResV1::Save(const SystemRecoveryResV1 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void SystemRecoveryResV1::Load(SystemRecoveryResV1 *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

}  // namespace memgraph::replication

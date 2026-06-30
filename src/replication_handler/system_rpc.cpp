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
// Driven by the single storage::StorageInfoForEachField list (storage.hpp) so adding a field updates
// this wire path AND the durability cold_stats JSON at once. Each field forwards to the generic
// slk::Save/Load, which already encodes an enum as its underlying integer (and on Load validates enums
// that carry an ::N sentinel, e.g. StorageMode, via NumToEnum) — see the enum overloads in
// serialization.hpp. No per-field enum dispatch is needed here.
//
// Wire-contract tripwire: those generic enum overloads write/read each enum as std::underlying_type_t<T>,
// and a peer must agree on that width. The encoding has always assumed a 1-byte base; widening any of
// these enums would silently change the wire width and desync a same-version peer. Make that a COMPILE
// error instead of a runtime corruption.
static_assert(sizeof(std::underlying_type_t<memgraph::storage::StorageMode>) == 1);
static_assert(sizeof(std::underlying_type_t<memgraph::storage::IsolationLevel>) == 1);
static_assert(sizeof(std::underlying_type_t<memgraph::utils::CompressionLevel>) == 1);

void Save(const memgraph::storage::StorageInfo &self, memgraph::slk::Builder *builder) {
  memgraph::storage::StorageInfoForEachField(
      self, [&](const char * /*key*/, const auto &v) { memgraph::slk::Save(v, builder); });
}

void Load(memgraph::storage::StorageInfo *self, memgraph::slk::Reader *reader) {
  memgraph::storage::StorageInfoForEachField(*self,
                                             [&](const char * /*key*/, auto &v) { memgraph::slk::Load(&v, reader); });
}

// One COLD tenant's recovery payload — salient + as-of-suspend stats.
void Save(const memgraph::storage::ColdTenantRecovery &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.salient, builder);
  memgraph::slk::Save(self.stats, builder);
}

void Load(memgraph::storage::ColdTenantRecovery *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->salient, reader);
  memgraph::slk::Load(&self->stats, reader);
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
  memgraph::slk::Save(self.cold_databases, builder);
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
  memgraph::slk::Load(&self->cold_databases, reader);
}

// Serialize code for SystemRecoveryResV1 (same layout as Res)
void Save(const memgraph::replication::SystemRecoveryResV1 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.result, builder);
}

void Load(memgraph::replication::SystemRecoveryResV1 *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->result, reader);
}

// Serialize code for SystemRecoveryResV2 (same layout as Res)
void Save(const memgraph::replication::SystemRecoveryResV2 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.result, builder);
}

void Load(memgraph::replication::SystemRecoveryResV2 *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->result, reader);
}

// Serialize code for SystemRecoveryRes
void Save(const memgraph::replication::SystemRecoveryRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.result, builder);
}

void Load(memgraph::replication::SystemRecoveryRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->result, reader);
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

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

namespace {

// SalientConfig wire format before storage_light_edge was added (5 fields).
// Used by V1 and V2 SystemRecoveryReq serialization for ISSU compatibility.
void SaveSalientConfigLegacy(const memgraph::storage::SalientConfig &cfg, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(*cfg.name.str_view(), builder);
  memgraph::slk::Save(cfg.uuid, builder);
  memgraph::slk::Save(std::to_underlying(cfg.storage_mode), builder);
  memgraph::slk::Save(cfg.items.properties_on_edges, builder);
  memgraph::slk::Save(cfg.items.enable_schema_metadata, builder);
}

void LoadSalientConfigLegacy(memgraph::storage::SalientConfig *cfg, memgraph::slk::Reader *reader) {
  std::string name;
  memgraph::slk::Load(&name, reader);
  cfg->name = std::move(name);
  memgraph::slk::Load(&cfg->uuid, reader);
  uint8_t sm = 0;
  memgraph::slk::Load(&sm, reader);
  if (!memgraph::utils::NumToEnum(sm, cfg->storage_mode)) {
    throw memgraph::slk::SlkReaderException("Unexpected result line:{}!", __LINE__);
  }
  memgraph::slk::Load(&cfg->items.properties_on_edges, reader);
  memgraph::slk::Load(&cfg->items.enable_schema_metadata, reader);
  cfg->items.storage_light_edge = false;
}

void SaveSalientConfigsLegacy(const std::vector<memgraph::storage::SalientConfig> &configs,
                              memgraph::slk::Builder *builder) {
  const uint64_t size = configs.size();
  memgraph::slk::Save(size, builder);
  for (const auto &cfg : configs) {
    SaveSalientConfigLegacy(cfg, builder);
  }
}

void LoadSalientConfigsLegacy(std::vector<memgraph::storage::SalientConfig> *configs, memgraph::slk::Reader *reader) {
  uint64_t size = 0;
  memgraph::slk::Load(&size, reader);
  configs->resize(size);
  for (auto &cfg : *configs) {
    LoadSalientConfigLegacy(&cfg, reader);
  }
}

}  // namespace

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

// Serialize code for SystemRecoveryReqV1 (no parameters, legacy SalientConfig)
void Save(const memgraph::replication::SystemRecoveryReqV1 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.forced_group_timestamp, builder);
  SaveSalientConfigsLegacy(self.database_configs, builder);
  memgraph::slk::Save(self.auth_config, builder);
  memgraph::slk::Save(self.users, builder);
  memgraph::slk::Save(self.roles, builder);
  memgraph::slk::Save(self.profiles, builder);
}

void Load(memgraph::replication::SystemRecoveryReqV1 *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->forced_group_timestamp, reader);
  LoadSalientConfigsLegacy(&self->database_configs, reader);
  memgraph::slk::Load(&self->auth_config, reader);
  memgraph::slk::Load(&self->users, reader);
  memgraph::slk::Load(&self->roles, reader);
  memgraph::slk::Load(&self->profiles, reader);
}

// Serialize code for SystemRecoveryReqV2 (adds parameters, legacy SalientConfig)
void Save(const memgraph::replication::SystemRecoveryReqV2 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.forced_group_timestamp, builder);
  SaveSalientConfigsLegacy(self.database_configs, builder);
  memgraph::slk::Save(self.auth_config, builder);
  memgraph::slk::Save(self.users, builder);
  memgraph::slk::Save(self.roles, builder);
  memgraph::slk::Save(self.profiles, builder);
  memgraph::slk::Save(self.parameters, builder);
}

void Load(memgraph::replication::SystemRecoveryReqV2 *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->forced_group_timestamp, reader);
  LoadSalientConfigsLegacy(&self->database_configs, reader);
  memgraph::slk::Load(&self->auth_config, reader);
  memgraph::slk::Load(&self->users, reader);
  memgraph::slk::Load(&self->roles, reader);
  memgraph::slk::Load(&self->profiles, reader);
  memgraph::slk::Load(&self->parameters, reader);
}

// Serialize code for SystemRecoveryReq V3 (current, SalientConfig with storage_light_edge)
void Save(const memgraph::replication::SystemRecoveryReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.forced_group_timestamp, builder);
  memgraph::slk::Save(self.database_configs, builder);
  memgraph::slk::Save(self.auth_config, builder);
  memgraph::slk::Save(self.users, builder);
  memgraph::slk::Save(self.roles, builder);
  memgraph::slk::Save(self.profiles, builder);
  memgraph::slk::Save(self.parameters, builder);
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
}

// Serialize code for SystemRecoveryResV1
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

// Serialize code for SystemRecoveryResV2
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

// Serialize code for SystemRecoveryRes V3
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

void SystemRecoveryResV1::Save(const SystemRecoveryResV1 &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}

void SystemRecoveryResV1::Load(SystemRecoveryResV1 *self, memgraph::slk::Reader *reader) {
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

}  // namespace memgraph::replication

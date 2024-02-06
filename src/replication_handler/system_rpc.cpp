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

#include "replication_handler/system_rpc.hpp"

#include <json/json.hpp>

#include "auth/rpc.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"
#include "storage/v2/replication/rpc.hpp"
#include "utils/enum.hpp"

namespace memgraph::slk {
// Serialize code for SystemHeartbeatRes
void Save(const memgraph::replication::SystemHeartbeatRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.system_timestamp, builder);
}
void Load(memgraph::replication::SystemHeartbeatRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->system_timestamp, reader);
}

// Serialize code for SystemHeartbeatReq
void Save(const memgraph::replication::SystemHeartbeatReq & /*self*/, memgraph::slk::Builder * /*builder*/) {
  /* Nothing to serialize */
}
void Load(memgraph::replication::SystemHeartbeatReq * /*self*/, memgraph::slk::Reader * /*reader*/) {
  /* Nothing to serialize */
}

// Serialize code for SystemRecoveryReq
void Save(const memgraph::replication::SystemRecoveryReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.forced_group_timestamp, builder);
  memgraph::slk::Save(self.database_configs, builder);
  memgraph::slk::Save(self.auth_config, builder);
  memgraph::slk::Save(self.users, builder);
  memgraph::slk::Save(self.roles, builder);
}

void Load(memgraph::replication::SystemRecoveryReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->forced_group_timestamp, reader);
  memgraph::slk::Load(&self->database_configs, reader);
  memgraph::slk::Load(&self->auth_config, reader);
  memgraph::slk::Load(&self->users, reader);
  memgraph::slk::Load(&self->roles, reader);
}

// Serialize code for SystemRecoveryRes
void Save(const memgraph::replication::SystemRecoveryRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(utils::EnumToNum<uint8_t>(self.result), builder);
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

constexpr utils::TypeInfo SystemHeartbeatReq::kType{utils::TypeId::REP_SYSTEM_HEARTBEAT_REQ, "SystemHeartbeatReq",
                                                    nullptr};

constexpr utils::TypeInfo SystemHeartbeatRes::kType{utils::TypeId::REP_SYSTEM_HEARTBEAT_RES, "SystemHeartbeatRes",
                                                    nullptr};

constexpr utils::TypeInfo SystemRecoveryReq::kType{utils::TypeId::REP_SYSTEM_RECOVERY_REQ, "SystemRecoveryReq",
                                                   nullptr};

constexpr utils::TypeInfo SystemRecoveryRes::kType{utils::TypeId::REP_SYSTEM_RECOVERY_RES, "SystemRecoveryRes",
                                                   nullptr};

void SystemHeartbeatReq::Save(const SystemHeartbeatReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void SystemHeartbeatReq::Load(SystemHeartbeatReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}
void SystemHeartbeatRes::Save(const SystemHeartbeatRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void SystemHeartbeatRes::Load(SystemHeartbeatRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void SystemRecoveryReq::Save(const SystemRecoveryReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void SystemRecoveryReq::Load(SystemRecoveryReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}
void SystemRecoveryRes::Save(const SystemRecoveryRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void SystemRecoveryRes::Load(SystemRecoveryRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

}  // namespace memgraph::replication

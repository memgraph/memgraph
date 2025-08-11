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

#include "replication_handler/system_rpc.hpp"

#include "auth/rpc.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"
#include "utils/enum.hpp"

namespace memgraph::slk {

// Serialize code for SystemRecoveryReq
void Save(const memgraph::replication::SystemRecoveryReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_uuid, builder);
  memgraph::slk::Save(self.forced_group_timestamp, builder);
  memgraph::slk::Save(self.database_configs, builder);
  memgraph::slk::Save(self.auth_config, builder);
  memgraph::slk::Save(self.users, builder);
  memgraph::slk::Save(self.roles, builder);
  memgraph::slk::Save(self.profiles, builder);
}

void Load(memgraph::replication::SystemRecoveryReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_uuid, reader);
  memgraph::slk::Load(&self->forced_group_timestamp, reader);
  memgraph::slk::Load(&self->database_configs, reader);
  memgraph::slk::Load(&self->auth_config, reader);
  memgraph::slk::Load(&self->users, reader);
  memgraph::slk::Load(&self->roles, reader);
  memgraph::slk::Load(&self->profiles, reader);
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

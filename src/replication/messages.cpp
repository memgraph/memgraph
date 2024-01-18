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
#include "replication/messages.hpp"
#include <json/json.hpp>
#include "auth/models.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"
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

// TODO Move
// Serialize code for auth::User
void Save(const auth::User &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.Serialize().dump(), builder);
}
// Deserialize code for auth::User
void Load(auth::User *self, memgraph::slk::Reader *reader) {
  std::string tmp;
  memgraph::slk::Load(&tmp, reader);
  const auto json = nlohmann::json::parse(tmp);
  *self = memgraph::auth::User::Deserialize(json);
}
// Serialize code for auth::Role
void Save(const auth::Role &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.Serialize().dump(), builder);
}
namespace {
auth::Role LoadAuthRole(memgraph::slk::Reader *reader) {
  std::string tmp;
  memgraph::slk::Load(&tmp, reader);
  const auto json = nlohmann::json::parse(tmp);
  return memgraph::auth::Role::Deserialize(json);
}
}  // namespace
// Deserialize code for auth::User
void Load(auth::Role *self, memgraph::slk::Reader *reader) {
  std::string tmp;
  memgraph::slk::Load(&tmp, reader);
  const auto json = nlohmann::json::parse(tmp);
  *self = LoadAuthRole(reader);
}
// Special case for optional<Role>
template <>
inline void Load<auth::Role>(std::optional<auth::Role> *obj, Reader *reader) {
  bool exists = false;
  Load(&exists, reader);
  if (exists) {
    obj->emplace(LoadAuthRole(reader));
  } else {
    *obj = std::nullopt;
  }
}

// Serialize code for UpdateAuthDataReq
void Save(const memgraph::replication::UpdateAuthDataReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.user, builder);
  memgraph::slk::Save(self.role, builder);
}
void Load(memgraph::replication::UpdateAuthDataReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->user, reader);
  memgraph::slk::Load(&self->role, reader);
}

// Serialize code for UpdateAuthDataRes
void Save(const memgraph::replication::UpdateAuthDataRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}
void Load(memgraph::replication::UpdateAuthDataRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

// Serialize code for DropAuthDataReq
void Save(const memgraph::replication::DropAuthDataReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(utils::EnumToNum<2, uint8_t>(self.type), builder);
  memgraph::slk::Save(self.name, builder);
}
void Load(memgraph::replication::DropAuthDataReq *self, memgraph::slk::Reader *reader) {
  uint8_t type_tmp = 0;
  memgraph::slk::Load(&type_tmp, reader);
  if (!utils::NumToEnum<2>(type_tmp, self->type)) {
    throw SlkReaderException("Unexpected result line:{}!", __LINE__);
  }
  memgraph::slk::Load(&self->name, reader);
}

// Serialize code for DropAuthDataRes
void Save(const memgraph::replication::DropAuthDataRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}
void Load(memgraph::replication::DropAuthDataRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

}  // namespace memgraph::slk

namespace memgraph::replication {
constexpr utils::TypeInfo SystemHeartbeatReq::kType{utils::TypeId::REP_SYSTEM_HEARTBEAT_REQ, "SystemHeartbeatReq",
                                                    nullptr};

constexpr utils::TypeInfo SystemHeartbeatRes::kType{utils::TypeId::REP_SYSTEM_HEARTBEAT_RES, "SystemHeartbeatRes",
                                                    nullptr};

constexpr utils::TypeInfo UpdateAuthDataReq::kType{utils::TypeId::REP_UPDATE_AUTH_DATA_REQ, "UpdateAuthDataReq",
                                                   nullptr};

constexpr utils::TypeInfo UpdateAuthDataRes::kType{utils::TypeId::REP_UPDATE_AUTH_DATA_RES, "UpdateAuthDataRes",
                                                   nullptr};

constexpr utils::TypeInfo DropAuthDataReq::kType{utils::TypeId::REP_DROP_AUTH_DATA_REQ, "DropAuthDataReq", nullptr};

constexpr utils::TypeInfo DropAuthDataRes::kType{utils::TypeId::REP_DROP_AUTH_DATA_RES, "DropAuthDataRes", nullptr};

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

void UpdateAuthDataReq::Save(const UpdateAuthDataReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void UpdateAuthDataReq::Load(UpdateAuthDataReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}
void UpdateAuthDataRes::Save(const UpdateAuthDataRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void UpdateAuthDataRes::Load(UpdateAuthDataRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}

void DropAuthDataReq::Save(const DropAuthDataReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void DropAuthDataReq::Load(DropAuthDataReq *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }
void DropAuthDataRes::Save(const DropAuthDataRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void DropAuthDataRes::Load(DropAuthDataRes *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }

}  // namespace memgraph::replication

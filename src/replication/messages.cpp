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
#include "auth/auth.hpp"
#include "auth/models.hpp"
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

// TODO Move
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
// Deserialize code for auth::Role
void Load(auth::Role *self, memgraph::slk::Reader *reader) { *self = LoadAuthRole(reader); }
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

// Serialize code for auth::User
void Save(const auth::User &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.Serialize().dump(), builder);
  std::optional<auth::Role> role{};
  if (const auto *role_ptr = self.role(); role_ptr) {
    role.emplace(*role_ptr);
  }
  memgraph::slk::Save(role, builder);
}
// Deserialize code for auth::User
void Load(auth::User *self, memgraph::slk::Reader *reader) {
  std::string tmp;
  memgraph::slk::Load(&tmp, reader);
  const auto json = nlohmann::json::parse(tmp);
  *self = memgraph::auth::User::Deserialize(json);
  std::optional<auth::Role> role{};
  memgraph::slk::Load(&role, reader);
  if (role)
    self->SetRole(*role);
  else
    self->ClearRole();
}

// Serialize code for auth::Auth::Config
void Save(const auth::Auth::Config &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.name_regex_str, builder);
  memgraph::slk::Save(self.password_regex_str, builder);
  memgraph::slk::Save(self.password_permit_null, builder);
}
// Deserialize code for auth::Auth::Config
void Load(auth::Auth::Config *self, memgraph::slk::Reader *reader) {
  std::string name_regex_str{};
  std::string password_regex_str{};
  bool password_permit_null{};

  memgraph::slk::Load(&name_regex_str, reader);
  memgraph::slk::Load(&password_regex_str, reader);
  memgraph::slk::Load(&password_permit_null, reader);

  *self = auth::Auth::Config{std::move(name_regex_str), std::move(password_regex_str), password_permit_null};
}

// Serialize code for UpdateAuthDataReq
void Save(const memgraph::replication::UpdateAuthDataReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.epoch_id, builder);
  memgraph::slk::Save(self.expected_group_timestamp, builder);
  memgraph::slk::Save(self.new_group_timestamp, builder);
  memgraph::slk::Save(self.user, builder);
  memgraph::slk::Save(self.role, builder);
}
void Load(memgraph::replication::UpdateAuthDataReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->epoch_id, reader);
  memgraph::slk::Load(&self->expected_group_timestamp, reader);
  memgraph::slk::Load(&self->new_group_timestamp, reader);
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
  memgraph::slk::Save(self.epoch_id, builder);
  memgraph::slk::Save(self.expected_group_timestamp, builder);
  memgraph::slk::Save(self.new_group_timestamp, builder);
  memgraph::slk::Save(utils::EnumToNum<2, uint8_t>(self.type), builder);
  memgraph::slk::Save(self.name, builder);
}
void Load(memgraph::replication::DropAuthDataReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->epoch_id, reader);
  memgraph::slk::Load(&self->expected_group_timestamp, reader);
  memgraph::slk::Load(&self->new_group_timestamp, reader);
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

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

#pragma once

#include <vector>

#include "auth/auth.hpp"
#include "auth/models.hpp"
#include "auth/profiles/user_profiles.hpp"
#include "rpc/messages.hpp"
#include "storage/v2/config.hpp"

namespace memgraph::replication {

struct SystemRecoveryReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_SYSTEM_RECOVERY_REQ, .name = "SystemRecoveryReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(SystemRecoveryReq *self, memgraph::slk::Reader *reader);
  static void Save(const SystemRecoveryReq &self, memgraph::slk::Builder *builder);
  SystemRecoveryReq() = default;
  SystemRecoveryReq(const utils::UUID &main_uuid, uint64_t forced_group_timestamp,
                    std::vector<storage::SalientConfig> database_configs, auth::Auth::Config auth_config,
                    std::vector<auth::User> users, std::vector<auth::Role> roles,
                    std::vector<auth::UserProfiles::Profile> profiles)
      : main_uuid(main_uuid),
        forced_group_timestamp{forced_group_timestamp},
        database_configs(std::move(database_configs)),
        auth_config(std::move(auth_config)),
        users{std::move(users)},
        roles{std::move(roles)},
        profiles{std::move(profiles)} {}

  utils::UUID main_uuid;
  uint64_t forced_group_timestamp;
  std::vector<storage::SalientConfig> database_configs;
  auth::Auth::Config auth_config;
  std::vector<auth::User> users;
  std::vector<auth::Role> roles;
  std::vector<auth::UserProfiles::Profile> profiles;
};

struct SystemRecoveryRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_SYSTEM_RECOVERY_RES, .name = "SystemRecoveryRes"};
  static constexpr uint64_t kVersion{1};

  enum class Result : uint8_t { SUCCESS, NO_NEED, FAILURE, /* Leave at end */ N };

  static void Load(SystemRecoveryRes *self, memgraph::slk::Reader *reader);
  static void Save(const SystemRecoveryRes &self, memgraph::slk::Builder *builder);
  SystemRecoveryRes() = default;
  explicit SystemRecoveryRes(Result res) : result(res) {}

  Result result;
};

using SystemRecoveryRpc = rpc::RequestResponse<SystemRecoveryReq, SystemRecoveryRes>;

}  // namespace memgraph::replication

namespace memgraph::slk {
void Save(const memgraph::replication::SystemRecoveryReq &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication::SystemRecoveryReq *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication::SystemRecoveryRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication::SystemRecoveryRes *self, memgraph::slk::Reader *reader);
}  // namespace memgraph::slk

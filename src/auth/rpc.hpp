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

#pragma once

#include <optional>
#include <variant>
#include <vector>

#include "auth/auth.hpp"
#include "auth/models.hpp"
#include "auth/ops.hpp"
#include "auth/profiles/user_profiles.hpp"
#include "rpc/messages.hpp"
#include "system/action.hpp"

namespace memgraph::replication {

struct UpdateAuthDataReqV1 {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_UPDATE_AUTH_DATA_REQ, .name = "UpdateAuthDataReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(UpdateAuthDataReqV1 *self, memgraph::slk::Reader *reader);
  static void Save(const UpdateAuthDataReqV1 &self, memgraph::slk::Builder *builder);
  UpdateAuthDataReqV1() = default;

  UpdateAuthDataReqV1(const utils::UUID &main_uuid, uint64_t const expected_ts, uint64_t const new_ts, auth::User user)
      : main_uuid(main_uuid),
        expected_group_timestamp{expected_ts},
        new_group_timestamp{new_ts},
        user{std::move(user)} {}

  UpdateAuthDataReqV1(const utils::UUID &main_uuid, uint64_t const expected_ts, uint64_t const new_ts, auth::Role role)
      : main_uuid(main_uuid),
        expected_group_timestamp{expected_ts},
        new_group_timestamp{new_ts},
        role{std::move(role)} {}

  UpdateAuthDataReqV1(const utils::UUID &main_uuid, uint64_t expected_ts, uint64_t new_ts,
                      auth::UserProfiles::Profile profile)
      : main_uuid(main_uuid),
        expected_group_timestamp{expected_ts},
        new_group_timestamp{new_ts},
        profile{std::move(profile)} {}

  utils::UUID main_uuid;
  uint64_t expected_group_timestamp{};
  uint64_t new_group_timestamp{};
  std::optional<auth::User> user;
  std::optional<auth::Role> role;
  std::optional<auth::UserProfiles::Profile> profile{};
};

/// An auth transaction's operations as one request, so a replica applies all of them or none. V1 carried at most
/// one record, which meant a transaction of several statements arrived as several requests and could be applied
/// in part.
struct UpdateAuthDataReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_UPDATE_AUTH_DATA_REQ, .name = "UpdateAuthDataReq"};
  static constexpr uint64_t kVersion{2};

  static void Load(UpdateAuthDataReq *self, memgraph::slk::Reader *reader);
  static void Save(const UpdateAuthDataReq &self, memgraph::slk::Builder *builder);
  UpdateAuthDataReq() = default;

  UpdateAuthDataReq(const utils::UUID &main_uuid, uint64_t const expected_ts, uint64_t const new_ts,
                    std::vector<AuthOp> ops)
      : main_uuid(main_uuid), expected_group_timestamp{expected_ts}, new_group_timestamp{new_ts}, ops{std::move(ops)} {}

  /// An older main sends one record per request, which is a batch of one.
  static UpdateAuthDataReq Upgrade(UpdateAuthDataReqV1 const &v1) {
    std::vector<AuthOp> ops;
    if (v1.user) ops.emplace_back(AuthUpdateOp{*v1.user});
    if (v1.role) ops.emplace_back(AuthUpdateOp{*v1.role});
    if (v1.profile) ops.emplace_back(AuthUpdateOp{*v1.profile});
    return UpdateAuthDataReq{v1.main_uuid, v1.expected_group_timestamp, v1.new_group_timestamp, std::move(ops)};
  }

  utils::UUID main_uuid;
  uint64_t expected_group_timestamp{};
  uint64_t new_group_timestamp{};
  std::vector<AuthOp> ops;
};

struct UpdateAuthDataResV1 {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_UPDATE_AUTH_DATA_RES, .name = "UpdateAuthDataRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(UpdateAuthDataResV1 *self, memgraph::slk::Reader *reader);
  static void Save(const UpdateAuthDataResV1 &self, memgraph::slk::Builder *builder);
  UpdateAuthDataResV1() = default;

  explicit UpdateAuthDataResV1(bool success) : success{success} {}

  bool success{};
};

/// Same content as V1. The server answers at the request's version, so the response moves with the request.
struct UpdateAuthDataRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_UPDATE_AUTH_DATA_RES, .name = "UpdateAuthDataRes"};
  static constexpr uint64_t kVersion{2};

  static void Load(UpdateAuthDataRes *self, memgraph::slk::Reader *reader);
  static void Save(const UpdateAuthDataRes &self, memgraph::slk::Builder *builder);
  UpdateAuthDataRes() = default;

  explicit UpdateAuthDataRes(bool success) : success{success} {}

  UpdateAuthDataResV1 Downgrade() const { return UpdateAuthDataResV1{success}; }

  bool success{};
};

using UpdateAuthDataRpc = rpc::RequestResponse<UpdateAuthDataReq, UpdateAuthDataRes>;
using UpdateAuthDataRpcV1 = rpc::RequestResponse<UpdateAuthDataReqV1, UpdateAuthDataResV1>;

struct DropAuthDataReq {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_DROP_AUTH_DATA_REQ, .name = "DropAuthDataReq"};
  static constexpr uint64_t kVersion{1};

  static void Load(DropAuthDataReq *self, memgraph::slk::Reader *reader);
  static void Save(const DropAuthDataReq &self, memgraph::slk::Builder *builder);
  DropAuthDataReq() = default;

  enum class DataType : uint8_t { USER, ROLE, PROFILE, /* Leave at end */ N };

  DropAuthDataReq(const utils::UUID &main_uuid, uint64_t const expected_ts, uint64_t const new_ts, DataType const type,
                  std::string_view const name)
      : main_uuid(main_uuid),
        expected_group_timestamp{expected_ts},
        new_group_timestamp{new_ts},
        type{type},
        name{name} {}

  utils::UUID main_uuid;
  uint64_t expected_group_timestamp;
  uint64_t new_group_timestamp;
  DataType type;
  std::string name;
};

struct DropAuthDataRes {
  static constexpr utils::TypeInfo kType{.id = utils::TypeId::REP_DROP_AUTH_DATA_RES, .name = "DropAuthDataRes"};
  static constexpr uint64_t kVersion{1};

  static void Load(DropAuthDataRes *self, memgraph::slk::Reader *reader);
  static void Save(const DropAuthDataRes &self, memgraph::slk::Builder *builder);
  DropAuthDataRes() = default;

  explicit DropAuthDataRes(bool const success) : success{success} {}

  bool success;
};

using DropAuthDataRpc = rpc::RequestResponse<DropAuthDataReq, DropAuthDataRes>;

}  // namespace memgraph::replication

#ifdef MG_ENTERPRISE
namespace memgraph::auth {

/// One auth transaction's replication, as a single request. A replica applies every operation in it or none, so
/// it cannot be left holding part of a transaction -- a user without the grant that accompanied it, say. A
/// statement outside a transaction takes the same path with a batch of one.
struct BatchedAuthAction final : memgraph::system::ISystemAction {
  explicit BatchedAuthAction(PendingActions ops) : ops_{std::move(ops)} {}

  void DoDurability() override { /* Done during Auth execution */ }

  bool ShouldReplicateInCommunity() const override { return false; }

  // system::Transaction is only forward-declared here, so reading its timestamps happens in the .cpp.
  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     memgraph::system::Transaction const &txn) const override;

  void PostReplication(replication::RoleMainData & /*main_data*/) const override {}

 private:
  PendingActions ops_;
};

}  // namespace memgraph::auth
#endif

namespace memgraph::slk {

void Save(const auth::Role &self, memgraph::slk::Builder *builder);
void Load(auth::Role *self, memgraph::slk::Reader *reader);
void Save(const auth::User &self, memgraph::slk::Builder *builder);
void Load(auth::User *self, memgraph::slk::Reader *reader);
void Save(const auth::UserProfiles::Profile &self, memgraph::slk::Builder *builder);
void Load(auth::UserProfiles::Profile *self, memgraph::slk::Reader *reader);
void Save(const auth::Auth::Config &self, memgraph::slk::Builder *builder);
void Load(auth::Auth::Config *self, memgraph::slk::Reader *reader);

void Save(const memgraph::replication::AuthUpdateOp &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication::AuthUpdateOp *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication::AuthDropOp &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication::AuthDropOp *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication::UpdateAuthDataReqV1 &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication::UpdateAuthDataReqV1 *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication::UpdateAuthDataReq &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication::UpdateAuthDataReq *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication::UpdateAuthDataResV1 &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication::UpdateAuthDataResV1 *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication::UpdateAuthDataRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication::UpdateAuthDataRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication::DropAuthDataRes &self, memgraph::slk::Builder *builder);
void Load(memgraph::replication::DropAuthDataRes *self, memgraph::slk::Reader *reader);
void Save(const memgraph::replication::DropAuthDataReq & /*self*/, memgraph::slk::Builder * /*builder*/);
void Load(memgraph::replication::DropAuthDataReq * /*self*/, memgraph::slk::Reader * /*reader*/);
}  // namespace memgraph::slk

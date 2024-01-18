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

#pragma once

#include <memory>
#include <optional>
#include "auth/models.hpp"
#include "storage/v2/config.hpp"

namespace memgraph::dbms {
struct SystemTransaction {
  struct Delta {
    enum class Action {
      CREATE_DATABASE,
      DROP_DATABASE,
      UPDATE_AUTH_DATA,
      DROP_AUTH_DATA,
      /**
       *
       * CREATE USER user_name [IDENTIFIED BY 'password'];
       * SET PASSWORD FOR user_name TO 'new_password';
       * ^ SaveUser
       *
       * DROP USER user_name;
       * ^ Directly on KVStore
       *
       * CREATE ROLE role_name;
       * ^ SaveRole
       *
       * DROP ROLE
       * ^ RemoveRole
       *
       * SET ROLE FOR user_name TO role_name;
       * CLEAR ROLE FOR user_name;
       * ^ Do stuff then do SaveUser
       *
       * GRANT privilege_list TO user_or_role;
       * DENY AUTH, INDEX TO moderator:
       * REVOKE AUTH, INDEX TO moderator:
       * GRANT permission_level ON (LABELS | EDGE_TYPES) label_list TO user_or_role;
       * REVOKE (LABELS | EDGE_TYPES) label_or_edge_type_list FROM user_or_role
       * DENY (LABELS | EDGE_TYPES) label_or_edge_type_list TO user_or_role
       * ^ all of these are EditPermissions <-> SaveUser/Role
       *
       * Multi-tenant TODO Doc;
       * ^ Should all call SaveUser
       *
       */
    };

    static constexpr struct CreateDatabase {
    } create_database;
    static constexpr struct DropDatabase {
    } drop_database;
    static constexpr struct UpdateAuthData {
    } update_auth_data;
    static constexpr struct DropAuthData {
    } drop_auth_data;

    enum class AuthData { USER, ROLE };

    // Multi-tenancy
    Delta(CreateDatabase /*tag*/, storage::SalientConfig config)
        : action(Action::CREATE_DATABASE), config(std::move(config)) {}
    Delta(DropDatabase /*tag*/, const utils::UUID &uuid) : action(Action::DROP_DATABASE), uuid(uuid) {}

    // Auth
    Delta(UpdateAuthData /*tag*/, std::optional<auth::User> user)
        : action(Action::UPDATE_AUTH_DATA), auth_data{std::move(user), std::nullopt} {}
    Delta(UpdateAuthData /*tag*/, std::optional<auth::Role> role)
        : action(Action::UPDATE_AUTH_DATA), auth_data{std::nullopt, std::move(role)} {}
    Delta(DropAuthData /*tag*/, AuthData type, std::string_view name)
        : action(Action::DROP_AUTH_DATA),
          auth_data_key{
              .type = type,
              .name = std::string{name},
          } {}

    // Generic
    Delta(const Delta &) = delete;
    Delta(Delta &&) = delete;
    Delta &operator=(const Delta &) = delete;
    Delta &operator=(Delta &&) = delete;

    ~Delta() {
      switch (action) {
        case Action::CREATE_DATABASE:
          std::destroy_at(&config);
          break;
        case Action::DROP_DATABASE:
          std::destroy_at(&uuid);
          break;
        case Action::UPDATE_AUTH_DATA:
          std::destroy_at(&auth_data);
          break;
        case Action::DROP_AUTH_DATA:
          std::destroy_at(&auth_data_key);
          break;
      }
    }

    Action action;
    union {
      storage::SalientConfig config;
      utils::UUID uuid;
      struct {
        std::optional<auth::User> user;
        std::optional<auth::Role> role;
      } auth_data;
      struct {
        AuthData type;
        std::string name;
      } auth_data_key;
    };
  };

  explicit SystemTransaction(uint64_t timestamp) : system_timestamp(timestamp) {}

  // Currently system transitions support a single delta
  std::optional<Delta> delta{};  // TODO Vector
  uint64_t system_timestamp;
};

}  // namespace memgraph::dbms

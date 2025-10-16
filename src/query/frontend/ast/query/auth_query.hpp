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

#include "query/frontend/ast/query/expression.hpp"
#include "query/frontend/ast/query/query.hpp"

#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace memgraph::query {
class AuthQuery : public memgraph::query::Query {
 public:
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const override { return kType; }

  enum class Action {
    CREATE_ROLE,
    DROP_ROLE,
    SHOW_ROLES,
    CREATE_USER,
    SET_PASSWORD,
    CHANGE_PASSWORD,
    DROP_USER,
    SHOW_CURRENT_USER,
    SHOW_CURRENT_ROLE,
    SHOW_USERS,
    SET_ROLE,
    CLEAR_ROLE,
    GRANT_PRIVILEGE,
    DENY_PRIVILEGE,
    REVOKE_PRIVILEGE,
    SHOW_PRIVILEGES,
    SHOW_ROLE_FOR_USER,
    SHOW_USERS_FOR_ROLE,
    GRANT_DATABASE_TO_USER,
    DENY_DATABASE_FROM_USER,
    REVOKE_DATABASE_FROM_USER,
    SHOW_DATABASE_PRIVILEGES,
    SET_MAIN_DATABASE,
    GRANT_IMPERSONATE_USER,
    DENY_IMPERSONATE_USER,
  };

  enum class Privilege {
    CREATE,
    DELETE,
    MATCH,
    MERGE,
    SET,
    REMOVE,
    INDEX,
    STATS,
    AUTH,
    CONSTRAINT,
    DUMP,
    REPLICATION,
    DURABILITY,
    READ_FILE,
    FREE_MEMORY,
    TRIGGER,
    CONFIG,
    STREAM,
    MODULE_READ,
    MODULE_WRITE,
    WEBSOCKET,
    STORAGE_MODE,
    TRANSACTION_MANAGEMENT,
    MULTI_DATABASE_EDIT,
    MULTI_DATABASE_USE,
    COORDINATOR,
    IMPERSONATE_USER,
    PROFILE_RESTRICTION,
  };

  enum class FineGrainedPrivilege { NOTHING, READ, UPDATE, CREATE, DELETE };

  enum class DatabaseSpecification {
    NONE,     // No database specification (non-enterprise)
    MAIN,     // MAIN database (enterprise)
    CURRENT,  // CURRENT database (enterprise)
    DATABASE  // Specific database name (enterprise)
  };

  AuthQuery() = default;

  DEFVISITABLE(QueryVisitor<void>);

  memgraph::query::AuthQuery::Action action_;
  std::string user_;
  std::vector<std::string> roles_;
  std::string user_or_role_;
  std::unordered_set<std::string> role_databases_;
  memgraph::query::Expression *old_password_{nullptr};
  memgraph::query::Expression *new_password_{nullptr};
  bool if_not_exists_;
  memgraph::query::Expression *password_{nullptr};
  std::string database_;
  std::vector<memgraph::query::AuthQuery::Privilege> privileges_;
  std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
      label_privileges_;
  std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
      edge_type_privileges_;
  std::vector<std::string> impersonation_targets_;

  // Database specification for SHOW PRIVILEGES query
  DatabaseSpecification database_specification_{DatabaseSpecification::NONE};

  AuthQuery *Clone(AstStorage *storage) const override {
    auto *object = storage->Create<AuthQuery>();
    object->action_ = action_;
    object->user_ = user_;
    object->roles_ = roles_;
    object->user_or_role_ = user_or_role_;
    object->role_databases_ = role_databases_;
    object->old_password_ = old_password_;
    object->new_password_ = new_password_;
    object->if_not_exists_ = if_not_exists_;
    object->password_ = password_ ? password_->Clone(storage) : nullptr;
    object->database_ = database_;
    object->privileges_ = privileges_;
    object->label_privileges_ = label_privileges_;
    object->edge_type_privileges_ = edge_type_privileges_;
    object->impersonation_targets_ = impersonation_targets_;
    object->database_specification_ = database_specification_;
    return object;
  }

 protected:
  AuthQuery(Action action, std::string user, std::vector<std::string> roles, std::string user_or_role,
            bool if_not_exists, Expression *password, std::string database, std::vector<Privilege> privileges,
            std::vector<std::unordered_map<FineGrainedPrivilege, std::vector<std::string>>> label_privileges,
            std::vector<std::unordered_map<FineGrainedPrivilege, std::vector<std::string>>> edge_type_privileges,
            std::vector<std::string> impersonation_targets,
            DatabaseSpecification database_specification = DatabaseSpecification::NONE,
            std::unordered_set<std::string> role_databases = {})
      : action_(action),
        user_(std::move(user)),
        roles_(std::move(roles)),
        user_or_role_(std::move(user_or_role)),
        role_databases_(std::move(role_databases)),
        if_not_exists_(if_not_exists),
        password_(password),
        database_(std::move(database)),
        privileges_(std::move(privileges)),
        label_privileges_(std::move(label_privileges)),
        edge_type_privileges_(std::move(edge_type_privileges)),
        impersonation_targets_(std::move(impersonation_targets)),
        database_specification_(database_specification) {}

 private:
  friend class AstStorage;
};

/// Constant that holds all available privileges.
const std::vector<AuthQuery::Privilege> kPrivilegesAll = {
    AuthQuery::Privilege::CREATE,
    AuthQuery::Privilege::DELETE,
    AuthQuery::Privilege::MATCH,
    AuthQuery::Privilege::MERGE,
    AuthQuery::Privilege::SET,
    AuthQuery::Privilege::REMOVE,
    AuthQuery::Privilege::INDEX,
    AuthQuery::Privilege::STATS,
    AuthQuery::Privilege::AUTH,
    AuthQuery::Privilege::CONSTRAINT,
    AuthQuery::Privilege::DUMP,
    AuthQuery::Privilege::REPLICATION,
    AuthQuery::Privilege::READ_FILE,
    AuthQuery::Privilege::DURABILITY,
    AuthQuery::Privilege::FREE_MEMORY,
    AuthQuery::Privilege::TRIGGER,
    AuthQuery::Privilege::CONFIG,
    AuthQuery::Privilege::STREAM,
    AuthQuery::Privilege::MODULE_READ,
    AuthQuery::Privilege::MODULE_WRITE,
    AuthQuery::Privilege::WEBSOCKET,
    AuthQuery::Privilege::TRANSACTION_MANAGEMENT,
    AuthQuery::Privilege::STORAGE_MODE,
    AuthQuery::Privilege::MULTI_DATABASE_EDIT,
    AuthQuery::Privilege::MULTI_DATABASE_USE,
    AuthQuery::Privilege::COORDINATOR,
    AuthQuery::Privilege::IMPERSONATE_USER,
    AuthQuery::Privilege::PROFILE_RESTRICTION,
};

}  // namespace memgraph::query

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
    GRANT_ROLE,
    REVOKE_ROLE,
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
    GRANT_PROPERTY_PERMISSION,
    DENY_PROPERTY_PERMISSION,
    REVOKE_PROPERTY_PERMISSION,
  };

  enum class PropertyPermissionType : uint8_t { NONE = 0, READ = 1, WRITE = 2 };

  friend constexpr PropertyPermissionType operator|(PropertyPermissionType a, PropertyPermissionType b) {
    return static_cast<PropertyPermissionType>(std::to_underlying(a) | std::to_underlying(b));
  }

  friend constexpr PropertyPermissionType operator&(PropertyPermissionType a, PropertyPermissionType b) {
    return static_cast<PropertyPermissionType>(std::to_underlying(a) & std::to_underlying(b));
  }

  friend constexpr PropertyPermissionType &operator|=(PropertyPermissionType &a, PropertyPermissionType b) {
    return a = a | b;
  }

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
    IMPERSONATE_USER,
    PROFILE_RESTRICTION,
    PARALLEL_EXECUTION,
    SERVER_SIDE_PARAMETERS,
    SERVER_SIDE_DESCRIPTIONS,
    RELOAD_TLS,
    // Coordinator-only privileges (READ / WRITE in the grammar). Not part of kPrivilegesAll, so GRANT ALL PRIVILEGES on
    // a data instance does not grant them; they gate queries only on coordinators.
    COORDINATOR_READ,
    COORDINATOR_WRITE
  };

  enum class FineGrainedPrivilege {
    READ,
    UPDATE,
    SET_LABEL,
    REMOVE_LABEL,
    SET_PROPERTY,
    CREATE,
    DELETE,
    DELETE_EDGE,
    CREATE_EDGE,
    ALL
  };

  enum class LabelMatchingMode { ANY, EXACTLY };

  enum class UserOrRoleType {
    UNSPECIFIED,  // Neither USER nor ROLE was explicitly specified; both are checked and an error is thrown if
                  // ambiguous.
    USER,         // Explicitly specified as a USER; only the user namespace is checked.
    ROLE,         // Explicitly specified as a ROLE; only the role namespace is checked.
  };

  enum class PropertyEntityKind { NODE, EDGE };

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
  // True for GRANT/DENY/REVOKE ALL PRIVILEGES. On coordinators this maps to both COORDINATOR_READ and
  // COORDINATOR_WRITE; privileges_ still carries the data-instance kPrivilegesAll expansion for other instances.
  bool all_privileges_{false};
  std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
      label_privileges_;
  std::vector<memgraph::query::AuthQuery::LabelMatchingMode> label_matching_modes_;
  std::vector<std::unordered_map<memgraph::query::AuthQuery::FineGrainedPrivilege, std::vector<std::string>>>
      edge_type_privileges_;
  std::vector<std::string> impersonation_targets_;
  std::vector<std::string> property_permissions_;
  std::vector<std::string> property_entity_names_;
  PropertyEntityKind property_entity_kind_{PropertyEntityKind::NODE};
  LabelMatchingMode property_matching_mode_{LabelMatchingMode::ANY};
  PropertyPermissionType property_permission_types_{PropertyPermissionType::NONE};

  // Database specification for SHOW PRIVILEGES query
  DatabaseSpecification database_specification_{DatabaseSpecification::NONE};
  UserOrRoleType entity_type_{UserOrRoleType::UNSPECIFIED};

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
    object->all_privileges_ = all_privileges_;
    object->label_privileges_ = label_privileges_;
    object->label_matching_modes_ = label_matching_modes_;
    object->edge_type_privileges_ = edge_type_privileges_;
    object->impersonation_targets_ = impersonation_targets_;
    object->property_permissions_ = property_permissions_;
    object->property_entity_names_ = property_entity_names_;
    object->property_entity_kind_ = property_entity_kind_;
    object->property_matching_mode_ = property_matching_mode_;
    object->property_permission_types_ = property_permission_types_;
    object->database_specification_ = database_specification_;
    object->entity_type_ = entity_type_;
    return object;
  }

 protected:
  AuthQuery(Action action, std::string user, std::vector<std::string> roles, std::string user_or_role,
            bool if_not_exists, Expression *password, std::string database, std::vector<Privilege> privileges,
            std::vector<std::unordered_map<FineGrainedPrivilege, std::vector<std::string>>> label_privileges,
            std::vector<LabelMatchingMode> label_matching_modes,
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
        label_matching_modes_(std::move(label_matching_modes)),
        edge_type_privileges_(std::move(edge_type_privileges)),
        impersonation_targets_(std::move(impersonation_targets)),
        database_specification_(database_specification) {}

 private:
  friend class AstStorage;
};

/// The two coordinator-only privileges (READ / WRITE in the grammar).
inline bool IsCoordinatorPrivilege(AuthQuery::Privilege privilege) {
  return privilege == AuthQuery::Privilege::COORDINATOR_READ || privilege == AuthQuery::Privilege::COORDINATOR_WRITE;
}

/// Coordinators expose only a small slice of the whole auth surface:
///   - role management: CREATE ROLE, DROP ROLE, SHOW ROLES;
///   - coordinator privilege management on roles: GRANT/REVOKE COORDINATOR_READ|COORDINATOR_WRITE|ALL PRIVILEGES,
///     SHOW PRIVILEGES FOR ROLE <role>.
/// GRANT/REVOKE are permitted only for the coordinator privileges (COORDINATOR_READ/COORDINATOR_WRITE, or
/// ALL PRIVILEGES which maps to both) and never target a USER; fine-grained access control (label/edge-type entity
/// privileges) is rejected. SHOW PRIVILEGES likewise never targets a USER (coordinators have no users). Every other
/// auth query -- DENY in any form, GRANT DATABASE, property permissions, ... -- is rejected on a coordinator.
inline bool IsCoordinatorPermittedAuthQuery(AuthQuery const &query) {
  switch (query.action_) {
    case AuthQuery::Action::CREATE_ROLE:
    case AuthQuery::Action::DROP_ROLE:
    case AuthQuery::Action::SHOW_ROLES:
      return true;
    case AuthQuery::Action::GRANT_PRIVILEGE:
    case AuthQuery::Action::REVOKE_PRIVILEGE: {
      if (query.entity_type_ == AuthQuery::UserOrRoleType::USER) {
        return false;
      }
      // Reject fine-grained access control (GRANT ... ON NODES/EDGES ...); coordinators have no graph.
      if (!query.label_privileges_.empty() || !query.edge_type_privileges_.empty()) {
        return false;
      }
      // ALL PRIVILEGES maps to both coordinator privileges.
      if (query.all_privileges_) {
        return true;
      }
      if (query.privileges_.empty()) {
        return false;
      }
      for (auto const privilege : query.privileges_) {
        if (!IsCoordinatorPrivilege(privilege)) {
          return false;
        }
      }
      return true;
    }
    case AuthQuery::Action::SHOW_PRIVILEGES:
      return query.entity_type_ != AuthQuery::UserOrRoleType::USER;
    default:
      return false;
  }
}

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
    AuthQuery::Privilege::IMPERSONATE_USER,
    AuthQuery::Privilege::PROFILE_RESTRICTION,
    AuthQuery::Privilege::PARALLEL_EXECUTION,
    AuthQuery::Privilege::SERVER_SIDE_PARAMETERS,
    AuthQuery::Privilege::SERVER_SIDE_DESCRIPTIONS,
    AuthQuery::Privilege::RELOAD_TLS,
};

}  // namespace memgraph::query

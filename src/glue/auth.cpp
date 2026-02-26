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

#include "glue/auth.hpp"
#include "auth/models.hpp"
#include "query/exceptions.hpp"

namespace memgraph::glue {

auth::Permission PrivilegeToPermission(query::AuthQuery::Privilege privilege) {
  switch (privilege) {
    case query::AuthQuery::Privilege::MATCH:
      return auth::Permission::MATCH;
    case query::AuthQuery::Privilege::CREATE:
      return auth::Permission::CREATE;
    case query::AuthQuery::Privilege::MERGE:
      return auth::Permission::MERGE;
    case query::AuthQuery::Privilege::DELETE:
      return auth::Permission::DELETE;
    case query::AuthQuery::Privilege::SET:
      return auth::Permission::SET;
    case query::AuthQuery::Privilege::REMOVE:
      return auth::Permission::REMOVE;
    case query::AuthQuery::Privilege::INDEX:
      return auth::Permission::INDEX;
    case query::AuthQuery::Privilege::STATS:
      return auth::Permission::STATS;
    case query::AuthQuery::Privilege::CONSTRAINT:
      return auth::Permission::CONSTRAINT;
    case query::AuthQuery::Privilege::DUMP:
      return auth::Permission::DUMP;
    case query::AuthQuery::Privilege::REPLICATION:
      return auth::Permission::REPLICATION;
    case query::AuthQuery::Privilege::DURABILITY:
      return auth::Permission::DURABILITY;
    case query::AuthQuery::Privilege::READ_FILE:
      return auth::Permission::READ_FILE;
    case query::AuthQuery::Privilege::FREE_MEMORY:
      return auth::Permission::FREE_MEMORY;
    case query::AuthQuery::Privilege::TRIGGER:
      return auth::Permission::TRIGGER;
    case query::AuthQuery::Privilege::CONFIG:
      return auth::Permission::CONFIG;
    case query::AuthQuery::Privilege::AUTH:
      return auth::Permission::AUTH;
    case query::AuthQuery::Privilege::STREAM:
      return auth::Permission::STREAM;
    case query::AuthQuery::Privilege::MODULE_READ:
      return auth::Permission::MODULE_READ;
    case query::AuthQuery::Privilege::MODULE_WRITE:
      return auth::Permission::MODULE_WRITE;
    case query::AuthQuery::Privilege::WEBSOCKET:
      return auth::Permission::WEBSOCKET;
    case query::AuthQuery::Privilege::STORAGE_MODE:
      return auth::Permission::STORAGE_MODE;
    case query::AuthQuery::Privilege::TRANSACTION_MANAGEMENT:
      return auth::Permission::TRANSACTION_MANAGEMENT;
    case query::AuthQuery::Privilege::MULTI_DATABASE_EDIT:
      return auth::Permission::MULTI_DATABASE_EDIT;
    case query::AuthQuery::Privilege::MULTI_DATABASE_USE:
      return auth::Permission::MULTI_DATABASE_USE;
    case query::AuthQuery::Privilege::COORDINATOR:
      return auth::Permission::COORDINATOR;
    case query::AuthQuery::Privilege::IMPERSONATE_USER:
      return auth::Permission::IMPERSONATE_USER;
    case query::AuthQuery::Privilege::PROFILE_RESTRICTION:
      return auth::Permission::PROFILE_RESTRICTION;
    case query::AuthQuery::Privilege::PARALLEL_EXECUTION:
      return auth::Permission::PARALLEL_EXECUTION;
    case query::AuthQuery::Privilege::SERVER_SIDE_PARAMETERS:
      return auth::Permission::SERVER_SIDE_PARAMETERS;
  }
}

#ifdef MG_ENTERPRISE
auth::FineGrainedPermission FineGrainedPrivilegeToFineGrainedPermission(
    query::AuthQuery::FineGrainedPrivilege const fine_grained_privilege, FineGrainedPermissionType const type) {
  switch (fine_grained_privilege) {
    case query::AuthQuery::FineGrainedPrivilege::READ:
      return auth::FineGrainedPermission::READ;
    case query::AuthQuery::FineGrainedPrivilege::UPDATE:
      // UPDATE is a grammar shorthand. For labels, this expands to
      // SET_LABEL | REMOVE_LABEL | SET_PROPERTY | DELETE_EDGE. For edge types,
      // it is a synonym for SET_PROPERTY.
      if (type == FineGrainedPermissionType::LABEL) {
        return auth::FineGrainedPermission::SET_LABEL | auth::FineGrainedPermission::REMOVE_LABEL |
               auth::FineGrainedPermission::SET_PROPERTY | auth::FineGrainedPermission::DELETE_EDGE;
      }
      return auth::FineGrainedPermission::SET_PROPERTY;
    case query::AuthQuery::FineGrainedPrivilege::SET_LABEL:
      return auth::FineGrainedPermission::SET_LABEL;
    case query::AuthQuery::FineGrainedPrivilege::REMOVE_LABEL:
      return auth::FineGrainedPermission::REMOVE_LABEL;
    case query::AuthQuery::FineGrainedPrivilege::SET_PROPERTY:
      return auth::FineGrainedPermission::SET_PROPERTY;
    case query::AuthQuery::FineGrainedPrivilege::CREATE:
      return auth::FineGrainedPermission::CREATE;
    case query::AuthQuery::FineGrainedPrivilege::DELETE:
      return auth::FineGrainedPermission::DELETE;
    case query::AuthQuery::FineGrainedPrivilege::DELETE_EDGE:
      return auth::FineGrainedPermission::DELETE_EDGE;
    case query::AuthQuery::FineGrainedPrivilege::ALL:
      return type == FineGrainedPermissionType::LABEL ? auth::kAllLabelPermissions : auth::kAllEdgeTypePermissions;
  }
}
#endif
}  // namespace memgraph::glue

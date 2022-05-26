// Copyright 2022 Memgraph Ltd.
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
    case query::AuthQuery::Privilege::SCHEMA:
      return auth::Permission::SCHEMA;
  }
}
}  // namespace memgraph::glue

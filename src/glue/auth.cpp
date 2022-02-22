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

auth::Permission PrivilegeToPermission(memgraph::query::AuthQuery::Privilege privilege) {
  switch (privilege) {
    case memgraph::query::AuthQuery::Privilege::MATCH:
      return auth::Permission::MATCH;
    case memgraph::query::AuthQuery::Privilege::CREATE:
      return auth::Permission::CREATE;
    case memgraph::query::AuthQuery::Privilege::MERGE:
      return auth::Permission::MERGE;
    case memgraph::query::AuthQuery::Privilege::DELETE:
      return auth::Permission::DELETE;
    case memgraph::query::AuthQuery::Privilege::SET:
      return auth::Permission::SET;
    case memgraph::query::AuthQuery::Privilege::REMOVE:
      return auth::Permission::REMOVE;
    case memgraph::query::AuthQuery::Privilege::INDEX:
      return auth::Permission::INDEX;
    case memgraph::query::AuthQuery::Privilege::STATS:
      return auth::Permission::STATS;
    case memgraph::query::AuthQuery::Privilege::CONSTRAINT:
      return auth::Permission::CONSTRAINT;
    case memgraph::query::AuthQuery::Privilege::DUMP:
      return auth::Permission::DUMP;
    case memgraph::query::AuthQuery::Privilege::REPLICATION:
      return auth::Permission::REPLICATION;
    case memgraph::query::AuthQuery::Privilege::DURABILITY:
      return auth::Permission::DURABILITY;
    case memgraph::query::AuthQuery::Privilege::READ_FILE:
      return auth::Permission::READ_FILE;
    case memgraph::query::AuthQuery::Privilege::FREE_MEMORY:
      return auth::Permission::FREE_MEMORY;
    case memgraph::query::AuthQuery::Privilege::TRIGGER:
      return auth::Permission::TRIGGER;
    case memgraph::query::AuthQuery::Privilege::CONFIG:
      return auth::Permission::CONFIG;
    case memgraph::query::AuthQuery::Privilege::AUTH:
      return auth::Permission::AUTH;
    case memgraph::query::AuthQuery::Privilege::STREAM:
      return auth::Permission::STREAM;
    case memgraph::query::AuthQuery::Privilege::MODULE_READ:
      return auth::Permission::MODULE_READ;
    case memgraph::query::AuthQuery::Privilege::MODULE_WRITE:
      return auth::Permission::MODULE_WRITE;
    case memgraph::query::AuthQuery::Privilege::WEBSOCKET:
      return auth::Permission::WEBSOCKET;
  }
}
}  // namespace memgraph::glue

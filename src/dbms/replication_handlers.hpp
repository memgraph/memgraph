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

#include "dbms/dbms_handler.hpp"
#include "replication/state.hpp"
#include "slk/streams.hpp"
#include "system/state.hpp"

namespace memgraph::dbms {
#ifdef MG_ENTERPRISE
// RPC handlers
void CreateDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                           DbmsHandler &dbms_handler, slk::Reader *req_reader, slk::Builder *res_builder);
void DropDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access, DbmsHandler &dbms_handler,
                         slk::Reader *req_reader, slk::Builder *res_builder);
bool SystemRecoveryHandler(DbmsHandler &dbms_handler, const std::vector<storage::SalientConfig> &database_configs);

// RPC registration
void Register(replication::RoleReplicaData const &data, system::ReplicaHandlerAccessToState &system_state_access,
              dbms::DbmsHandler &dbms_handler);
#endif
}  // namespace memgraph::dbms

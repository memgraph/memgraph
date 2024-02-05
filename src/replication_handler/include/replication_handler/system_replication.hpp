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

#include "auth/auth.hpp"
#include "dbms/dbms_handler.hpp"
#include "slk/streams.hpp"
#include "system/state.hpp"

namespace memgraph::replication {
#ifdef MG_ENTERPRISE
void SystemHeartbeatHandler(uint64_t ts, slk::Reader *req_reader, slk::Builder *res_builder);
void SystemRecoveryHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                           dbms::DbmsHandler &dbms_handler, auth::SynchedAuth &auth, slk::Reader *req_reader,
                           slk::Builder *res_builder);
void Register(replication::RoleReplicaData const &data, dbms::DbmsHandler &dbms_handler, auth::SynchedAuth &auth);
bool StartRpcServer(dbms::DbmsHandler &dbms_handler, const replication::RoleReplicaData &data, auth::SynchedAuth &auth);
#else
bool StartRpcServer(dbms::DbmsHandler &dbms_handler, const replication::RoleReplicaData &data);
#endif

}  // namespace memgraph::replication

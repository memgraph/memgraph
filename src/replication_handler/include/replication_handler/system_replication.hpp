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

#include "auth/auth.hpp"
#include "dbms/dbms_handler.hpp"
#include "system/state.hpp"

namespace memgraph::replication {

inline void LogWrongMain(const std::optional<utils::UUID> &current_main_uuid, const utils::UUID &main_req_id,
                         std::string_view rpc_req) {
  spdlog::error("Received {} with main_uuid: {} != current_main_uuid: {}",
                rpc_req,
                std::string(main_req_id),
                current_main_uuid.has_value() ? std::string(current_main_uuid.value()) : "");
}

#ifdef MG_ENTERPRISE
void SystemRecoveryHandler(system::ReplicaHandlerAccessToState &system_state_access,
                           const std::optional<utils::UUID> &current_main_uuid, dbms::DbmsHandler &dbms_handler,
                           auth::SynchedAuth &auth, parameters::Parameters &parameters, uint64_t request_version,
                           slk::Reader *req_reader, slk::Builder *res_builder);
#else
void SystemRecoveryHandler(system::ReplicaHandlerAccessToState &system_state_access,
                           const std::optional<utils::UUID> &current_main_uuid, parameters::Parameters &parameters,
                           uint64_t request_version, slk::Reader *req_reader, slk::Builder *res_builder);
#endif

void FinalizeSystemTxHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                             const std::optional<utils::UUID> &current_main_uuid, uint64_t request_version,
                             slk::Reader *req_reader, slk::Builder *res_builder);

#ifdef MG_ENTERPRISE
void Register(RoleReplicaData const &data, system::System &system, dbms::DbmsHandler &dbms_handler,
              auth::SynchedAuth &auth, parameters::Parameters &parameters);
#else
void Register(RoleReplicaData const &data, system::System &system, parameters::Parameters &parameters);
#endif

#ifdef MG_ENTERPRISE
bool StartRpcServer(
    dbms::DbmsHandler &dbms_handler,
    memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> &repl_state,
    RoleReplicaData &data, auth::SynchedAuth &auth, system::System &system, parameters::Parameters &parameters);
#else
bool StartRpcServer(
    dbms::DbmsHandler &dbms_handler,
    memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> &repl_state,
    RoleReplicaData &data, system::System &system, parameters::Parameters &parameters);
#endif

}  // namespace memgraph::replication

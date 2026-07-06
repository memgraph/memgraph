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

#include "dbms/dbms_handler.hpp"
#include "replication/state.hpp"
#include "system/state.hpp"

namespace memgraph::dbms {

#ifdef MG_ENTERPRISE

inline void LogWrongMain(const std::optional<utils::UUID> &current_main_uuid, const utils::UUID &main_req_id,
                         std::string_view rpc_req) {
  spdlog::error("Received {} with main_id: {} != current_main_uuid: {}",
                rpc_req,
                std::string(main_req_id),
                current_main_uuid.has_value() ? std::string(current_main_uuid.value()) : "");
}

// RPC handlers
void CreateDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                           const std::optional<utils::UUID> &current_main_uuid, DbmsHandler &dbms_handler,
                           uint64_t request_version, slk::Reader *req_reader, slk::Builder *res_builder);
void DropDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                         const std::optional<utils::UUID> &current_main_uuid, DbmsHandler &dbms_handler,
                         uint64_t request_version, slk::Reader *req_reader, slk::Builder *res_builder);
void ResetDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                           const std::optional<utils::UUID> &current_main_uuid, DbmsHandler &dbms_handler,
                           uint64_t request_version, slk::Reader *req_reader, slk::Builder *res_builder);
void RenameDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                           const std::optional<utils::UUID> &current_main_uuid, DbmsHandler &dbms_handler,
                           uint64_t request_version, slk::Reader *req_reader, slk::Builder *res_builder);
void SuspendDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                            const std::optional<utils::UUID> &current_main_uuid, DbmsHandler &dbms_handler,
                            uint64_t request_version, slk::Reader *req_reader, slk::Builder *res_builder);
void ResumeDatabaseHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                           const std::optional<utils::UUID> &current_main_uuid, DbmsHandler &dbms_handler,
                           uint64_t request_version, slk::Reader *req_reader, slk::Builder *res_builder);
// Reconcile this replica to MAIN's authoritative hot/cold sets. database_configs = HOT salient
// configs; cold_databases = the COLD set (each a salient + MAIN's as-of-suspend stats + epoch metadata);
// reset_uuids = tenants MAIN reset to empty (a replica that missed ResetDatabaseRpc resets them).
// Converges {HOT ∪ COLD} to match MAIN. Returns false on a non-transient failure.
bool SystemRecoveryHandler(DbmsHandler &dbms_handler, const std::vector<storage::SalientConfig> &database_configs,
                           const std::vector<storage::ColdTenantRecovery> &cold_databases,
                           const std::vector<utils::UUID> &reset_uuids);

// RPC registration
void Register(replication::RoleReplicaData const &data, system::ReplicaHandlerAccessToState &system_state_access,
              dbms::DbmsHandler &dbms_handler);
#endif
}  // namespace memgraph::dbms

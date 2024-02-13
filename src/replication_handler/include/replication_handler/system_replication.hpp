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

inline void LogWrongMain(const std::optional<utils::UUID> &current_main_uuid, const utils::UUID &main_req_id,
                         std::string_view rpc_req) {
  spdlog::error("Received {} with main_id: {} != current_main_uuid: {}", rpc_req, std::string(main_req_id),
                current_main_uuid.has_value() ? std::string(current_main_uuid.value()) : "");
}

#ifdef MG_ENTERPRISE
void SystemHeartbeatHandler(uint64_t ts, const std::optional<utils::UUID> &current_main_uuid, slk::Reader *req_reader,
                            slk::Builder *res_builder);

void SystemRecoveryHandler(memgraph::system::ReplicaHandlerAccessToState &system_state_access,
                           std::optional<utils::UUID> &current_main_uuid, dbms::DbmsHandler &dbms_handler,
                           auth::SynchedAuth &auth, slk::Reader *req_reader, slk::Builder *res_builder);

void Register(replication::RoleReplicaData const &data, system::System &system, dbms::DbmsHandler &dbms_handler,
              auth::SynchedAuth &auth);

bool StartRpcServer(dbms::DbmsHandler &dbms_handler, replication::RoleReplicaData &data, auth::SynchedAuth &auth,
                    system::System &system);
#else
bool StartRpcServer(dbms::DbmsHandler &dbms_handler, replication::RoleReplicaData &data);
#endif

}  // namespace memgraph::replication

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

#ifdef MG_ENTERPRISE

#include "coordination/data_instance_management_server.hpp"
#include "replication_handler/replication_handler.hpp"

import memgraph.coordination.coordinator_communication_config;
import memgraph.coordination.coordinator_rpc;

namespace memgraph::dbms {

class DbmsHandler;

class DataInstanceManagementServerHandlers {
 public:
  static void Register(coordination::DataInstanceManagementServer &server,
                       replication::ReplicationHandler &replication_handler);

 private:
  static void StateCheckHandler(const replication::ReplicationHandler &replication_handler, uint64_t request_version,
                                slk::Reader *req_reader, slk::Builder *res_builder);

  static void PromoteToMainHandler(replication::ReplicationHandler &replication_handler, uint64_t request_version,
                                   slk::Reader *req_reader, slk::Builder *res_builder);
  static void RegisterReplicaOnMainHandler(replication::ReplicationHandler &replication_handler,
                                           uint64_t request_version, slk::Reader *req_reader,
                                           slk::Builder *res_builder);
  static void DemoteMainToReplicaHandler(replication::ReplicationHandler &replication_handler, uint64_t request_version,
                                         slk::Reader *req_reader, slk::Builder *res_builder);
  static void SwapMainUUIDHandler(replication::ReplicationHandler &replication_handler, uint64_t request_version,
                                  slk::Reader *req_reader, slk::Builder *res_builder);

  static void UnregisterReplicaHandler(replication::ReplicationHandler &replication_handler, uint64_t request_version,
                                       slk::Reader *req_reader, slk::Builder *res_builder);
  static void EnableWritingOnMainHandler(replication::ReplicationHandler &replication_handler, uint64_t request_version,
                                         slk::Reader *req_reader, slk::Builder *res_builder);

  static void GetDatabaseHistoriesHandler(replication::ReplicationHandler const &replication_handler,
                                          uint64_t request_version, slk::Reader *req_reader, slk::Builder *res_builder);

  static void GetReplicationLagHandler(replication::ReplicationHandler const &replication_handler,
                                       uint64_t request_version, slk::Reader *req_reader, slk::Builder *res_builder);

  static auto DoRegisterReplica(replication::ReplicationHandler &replication_handler,
                                coordination::ReplicationClientInfo const &config) -> bool;
};

}  // namespace memgraph::dbms

#endif

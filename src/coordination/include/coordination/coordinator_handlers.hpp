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

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_communication_config.hpp"
#include "coordination/coordinator_rpc.hpp"
#include "coordination/coordinator_server.hpp"
#include "replication_handler/replication_handler.hpp"
#include "slk/streams.hpp"

namespace memgraph::dbms {

class DbmsHandler;

class CoordinatorHandlers {
 public:
  static void Register(memgraph::coordination::CoordinatorServer &server,
                       replication::ReplicationHandler &replication_handler);

 private:
  static void PromoteReplicaToMainHandler(replication::ReplicationHandler &replication_handler, slk::Reader *req_reader,
                                          slk::Builder *res_builder);
  static void RegisterReplicaOnMainHandler(replication::ReplicationHandler &replication_handler,
                                           slk::Reader *req_reader, slk::Builder *res_builder);
  static void DemoteMainToReplicaHandler(replication::ReplicationHandler &replication_handler, slk::Reader *req_reader,
                                         slk::Builder *res_builder);
  static void SwapMainUUIDHandler(replication::ReplicationHandler &replication_handler, slk::Reader *req_reader,
                                  slk::Builder *res_builder);

  static void UnregisterReplicaHandler(replication::ReplicationHandler &replication_handler, slk::Reader *req_reader,
                                       slk::Builder *res_builder);
  static void EnableWritingOnMainHandler(replication::ReplicationHandler &replication_handler, slk::Reader *req_reader,
                                         slk::Builder *res_builder);

  static void GetInstanceUUIDHandler(replication::ReplicationHandler &replication_handler, slk::Reader *req_reader,
                                     slk::Builder *res_builder);

  static void GetDatabaseHistoriesHandler(replication::ReplicationHandler &replication_handler, slk::Reader *req_reader,
                                          slk::Builder *res_builder);

  template <typename TResponse>
  static auto DoRegisterReplica(replication::ReplicationHandler &replication_handler,
                                coordination::ReplicationClientInfo const &config, slk::Builder *res_builder) -> bool {
    auto const converter = [](const auto &repl_info_config) {
      return replication::ReplicationClientConfig{
          .name = repl_info_config.instance_name,
          .mode = repl_info_config.replication_mode,
          .ip_address = repl_info_config.replication_server.address,
          .port = repl_info_config.replication_server.port,
      };
    };

    auto instance_client = replication_handler.RegisterReplica(converter(config));
    if (instance_client.HasError()) {
      using enum memgraph::replication::RegisterReplicaError;
      switch (instance_client.GetError()) {
        // Can't happen, checked on the coordinator side
        case memgraph::query::RegisterReplicaError::NAME_EXISTS:
          spdlog::error("Replica with the same name already exists!");
          slk::Save(TResponse{false}, res_builder);
          return false;
        // Can't happen, checked on the coordinator side
        case memgraph::query::RegisterReplicaError::ENDPOINT_EXISTS:
          spdlog::error("Replica with the same endpoint already exists!");
          slk::Save(TResponse{false}, res_builder);
          return false;
        // We don't handle disk issues
        case memgraph::query::RegisterReplicaError::COULD_NOT_BE_PERSISTED:
          spdlog::error("Registered replica could not be persisted!");
          slk::Save(TResponse{false}, res_builder);
          return false;
        case memgraph::query::RegisterReplicaError::ERROR_ACCEPTING_MAIN:
          spdlog::error("Replica didn't accept change of main!");
          slk::Save(TResponse{false}, res_builder);
          return false;
        case memgraph::query::RegisterReplicaError::CONNECTION_FAILED:
          // Connection failure is not a fatal error
          break;
      }
    }
    return true;
  }
};

}  // namespace memgraph::dbms

#endif

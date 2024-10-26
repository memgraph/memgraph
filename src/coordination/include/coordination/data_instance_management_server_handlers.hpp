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
#include "coordination/data_instance_management_server.hpp"
#include "replication_handler/replication_handler.hpp"
#include "slk/streams.hpp"

namespace memgraph::dbms {

class DbmsHandler;

class DataInstanceManagementServerHandlers {
 public:
  static void Register(memgraph::coordination::DataInstanceManagementServer &server,
                       replication::ReplicationHandler &replication_handler);

 private:
  static void StateCheckHandler(replication::ReplicationHandler &replication_handler, slk::Reader *req_reader,
                                slk::Builder *res_builder);

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
    auto const converter = [&config](const auto &repl_info_config) {
      return replication::ReplicationClientConfig{.name = repl_info_config.instance_name,
                                                  .mode = repl_info_config.replication_mode,
                                                  .repl_server_endpoint = config.replication_server};
    };

    auto instance_client = replication_handler.RegisterReplica(converter(config));
    if (instance_client.HasError()) {
      using memgraph::query::RegisterReplicaError;
      switch (instance_client.GetError()) {
        case RegisterReplicaError::NOT_MAIN: {
          spdlog::error("Error when registering instance {} as replica. Instance not main anymore.",
                        config.instance_name);
          slk::Save(TResponse{false}, res_builder);
          return false;
        }
        case RegisterReplicaError::NAME_EXISTS: {
          spdlog::error(
              "Error when registering instance {} as replica. Instance with the same name already registered.",
              config.instance_name);
          slk::Save(TResponse{false}, res_builder);
          return false;
        }
        case RegisterReplicaError::ENDPOINT_EXISTS: {
          spdlog::error(
              "Error when registering instance {} as replica. Instance with the same endpoint already exists.",
              config.instance_name);
          slk::Save(TResponse{false}, res_builder);
          return false;
        }
        case RegisterReplicaError::COULD_NOT_BE_PERSISTED: {
          spdlog::error("Error when registering instance {} as replica. Registering instance could not be persisted.",
                        config.instance_name);
          slk::Save(TResponse{false}, res_builder);
          return false;
        }
        case RegisterReplicaError::ERROR_ACCEPTING_MAIN: {
          spdlog::error("Error when registering instance {} as replica. Instance couldn't accept change of main.",
                        config.instance_name);
          slk::Save(TResponse{false}, res_builder);
          return false;
        }
        case RegisterReplicaError::CONNECTION_FAILED: {
          spdlog::error(
              "Error when registering instance {} as replica. Instance couldn't register all databases successfully.",
              config.instance_name);
          slk::Save(TResponse{false}, res_builder);
          return false;
        }
      }
    }
    spdlog::trace("Instance {} successfully registered as replica.", config.instance_name);
    return true;
  }
};

}  // namespace memgraph::dbms

#endif

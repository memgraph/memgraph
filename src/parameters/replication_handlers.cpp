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

#include "parameters/replication_handlers.hpp"

#include <spdlog/spdlog.h>

#include "parameters/parameters.hpp"
#include "parameters/parameters_rpc.hpp"
#include "replication_handler/system_replication.hpp"
#include "rpc/utils.hpp"

namespace memgraph::parameters {

namespace {

void SetParameterHandler(system::ReplicaHandlerAccessToState &system_state_access,
                         const std::optional<utils::UUID> &current_main_uuid, Parameters &parameters,
                         uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  using storage::replication::SetParameterRes;
  SetParameterRes res(false);

  storage::replication::SetParameterReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  if (current_main_uuid != req.main_uuid) [[unlikely]] {
    replication::LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::SetParameterReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("SetParameterHandler: bad expected timestamp {},{}",
                  req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  if (parameters.SetParameter(req.parameter.name, req.parameter.value, req.parameter.scope_context)) {
    res = SetParameterRes(true);
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

void UnsetParameterHandler(system::ReplicaHandlerAccessToState &system_state_access,
                           const std::optional<utils::UUID> &current_main_uuid, Parameters &parameters,
                           uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  using storage::replication::UnsetParameterRes;
  UnsetParameterRes res(false);

  storage::replication::UnsetParameterReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  if (current_main_uuid != req.main_uuid) [[unlikely]] {
    replication::LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::UnsetParameterReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("UnsetParameterHandler: bad expected timestamp {},{}",
                  req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  if (parameters.UnsetParameter(req.name, req.scope_context)) {
    res = UnsetParameterRes(true);
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

void DeleteAllParametersHandler(system::ReplicaHandlerAccessToState &system_state_access,
                                const std::optional<utils::UUID> &current_main_uuid, Parameters &parameters,
                                uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  using storage::replication::DeleteAllParametersRes;
  DeleteAllParametersRes res(false);

  storage::replication::DeleteAllParametersReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  if (current_main_uuid != req.main_uuid) [[unlikely]] {
    replication::LogWrongMain(
        current_main_uuid, req.main_uuid, storage::replication::DeleteAllParametersReq::kType.name);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  if (req.expected_group_timestamp != system_state_access.LastCommitedTS()) {
    spdlog::debug("DeleteAllParametersHandler: bad expected timestamp {},{}",
                  req.expected_group_timestamp,
                  system_state_access.LastCommitedTS());
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  if (parameters.DeleteAllParameters()) {
    res = DeleteAllParametersRes(true);
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

}  // namespace

bool SystemRecoveryHandler(Parameters &parameters, const std::vector<ParameterInfo> &params) {
  return parameters.ApplyRecovery(params);
}

void Register(replication::RoleReplicaData const &data, system::ReplicaHandlerAccessToState &system_state_access,
              Parameters &parameters) {
  data.server->rpc_server_.Register<storage::replication::SetParameterRpc>(
      [&data, system_state_access, &parameters](std::optional<rpc::FileReplicationHandler> const &,
                                                uint64_t const request_version,
                                                auto *req_reader,
                                                auto *res_builder) mutable {
        SetParameterHandler(system_state_access, data.uuid_, parameters, request_version, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::UnsetParameterRpc>(
      [&data, system_state_access, &parameters](std::optional<rpc::FileReplicationHandler> const &,
                                                uint64_t const request_version,
                                                auto *req_reader,
                                                auto *res_builder) mutable {
        UnsetParameterHandler(system_state_access, data.uuid_, parameters, request_version, req_reader, res_builder);
      });
  data.server->rpc_server_.Register<storage::replication::DeleteAllParametersRpc>(
      [&data, system_state_access, &parameters](std::optional<rpc::FileReplicationHandler> const &,
                                                uint64_t const request_version,
                                                auto *req_reader,
                                                auto *res_builder) mutable {
        DeleteAllParametersHandler(
            system_state_access, data.uuid_, parameters, request_version, req_reader, res_builder);
      });
}

}  // namespace memgraph::parameters

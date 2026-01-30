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

#include "utils/replication_handlers.hpp"

#include <spdlog/spdlog.h>

#include "replication_handler/system_replication.hpp"
#include "utils/parameters.hpp"
#include "utils/parameters_rpc.hpp"
#include "rpc/utils.hpp"

namespace memgraph::utils {

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

  if (parameters.SetParameter(req.parameter.name, req.parameter.value, req.parameter.scope)) {
    system_state_access.SetLastCommitedTS(req.new_group_timestamp);
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

  if (parameters.UnsetParameter(req.name, req.scope)) {
    system_state_access.SetLastCommitedTS(req.new_group_timestamp);
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
    system_state_access.SetLastCommitedTS(req.new_group_timestamp);
    res = DeleteAllParametersRes(true);
  }

  rpc::SendFinalResponse(res, request_version, res_builder);
}

}  // namespace

bool ApplyParametersRecovery(Parameters &parameters, const std::vector<ParameterInfo> &params) {
  for (const auto &p : params) {
    parameters.SetParameter(p.name, p.value, p.scope);
  }
  return true;
}

std::vector<ParameterInfo> GetParametersSnapshotForRecovery(Parameters &parameters) {
  std::vector<ParameterInfo> out;
  for (const auto scope : {ParameterScope::GLOBAL, ParameterScope::DATABASE, ParameterScope::SESSION}) {
    for (const auto &p : parameters.GetAllParameters(scope)) {
      out.push_back(p);
    }
  }
  return out;
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

}  // namespace memgraph::utils

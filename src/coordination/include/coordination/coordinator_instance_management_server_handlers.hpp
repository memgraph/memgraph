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

#ifdef MG_ENTERPRISE

#include "coordination/coordinator_instance.hpp"
#include "coordination/coordinator_instance_management_server.hpp"

#include "coordination/coordinator_rpc.hpp"

#include "rpc/utils.hpp"

namespace memgraph::coordination {
class CoordinatorInstanceManagementServerHandlers {
 public:
  static void Register(CoordinatorInstanceManagementServer &server, CoordinatorInstance &coordinator_instance);

 private:
  template <rpc::IsRpc Rpc, typename F>
  static void FwdRequestHandler(F const &f, uint64_t request_version, slk::Reader *req_reader,
                                slk::Builder *res_builder) {
    typename Rpc::Request req;
    rpc::LoadWithUpgrade(req, request_version, req_reader);
    if constexpr (std::is_invocable_v<F>) {
      rpc::SendFinalResponse(typename Rpc::Response{f()}, request_version, res_builder);
    } else {
      rpc::SendFinalResponse(typename Rpc::Response{f(req.arg_)}, request_version, res_builder);
    }
  }

  // Answers a forwarded write with the reason the leader declined it, so the follower can tell a reason that has
  // already passed from one that will never clear. A peer too old to read the reason is sent a flag instead.
  template <rpc::IsRpc Rpc, ForwardableStatus StatusEnum, typename F>
  static void FwdRequestHandler(F const &f, uint64_t request_version, slk::Reader *req_reader,
                                slk::Builder *res_builder) {
    typename Rpc::Request req;
    rpc::LoadWithUpgrade(req, request_version, req_reader);
    if constexpr (std::is_invocable_v<F>) {
      rpc::SendFinalResponse(typename Rpc::Response{ForwardedStatus<StatusEnum>{f()}}, request_version, res_builder);
    } else {
      rpc::SendFinalResponse(
          typename Rpc::Response{ForwardedStatus<StatusEnum>{f(req.arg_)}}, request_version, res_builder);
    }
  }
};

}  // namespace memgraph::coordination

#endif

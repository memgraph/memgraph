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

#ifdef MG_ENTERPRISE

#pragma once

#include "coordination/coordinator_instance_client.hpp"

namespace memgraph::coordination {

class CoordinatorInstanceConnector {
 public:
  explicit CoordinatorInstanceConnector(ManagementServerConfig const &config) : client_{config} {}

  template <rpc::IsRpc Rpc, typename... Args>
  auto SendRpc(Args &&...args) {
    using ReturnType = decltype(std::declval<typename Rpc::Response>().arg_);
    auto stream{client_.RpcClient().Stream<Rpc>(std::forward<Args>(args)...)};
    if (!stream.has_value()) {
      spdlog::error(
          "Failed to receive response to {}: {}", Rpc::Request::kType.name, utils::GetRpcErrorMsg(stream.error()));
      return ReturnType{};
    }
    auto res = stream.value().SendAndWait();

    if (res.has_value()) {
      return res.value().arg_;
    }
    spdlog::error("Failed to receive response to {}: {}", Rpc::Request::kType.name, utils::GetRpcErrorMsg(res.error()));
    return ReturnType{};
  }

 private:
  mutable CoordinatorInstanceClient client_;
};

}  // namespace memgraph::coordination
#endif

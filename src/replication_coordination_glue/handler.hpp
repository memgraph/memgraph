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

#include "rpc/client.hpp"
#include "utils/uuid.hpp"

#include "messages.hpp"
#include "rpc/messages.hpp"

namespace memgraph::replication_coordination_glue {
inline bool SendSwapMainUUIDRpc(memgraph::rpc::Client &rpc_client_, const memgraph::utils::UUID &uuid) {
  try {
    auto stream{rpc_client_.Stream<SwapMainUUIDRpc>(uuid)};
    if (!stream.AwaitResponse().success) {
      spdlog::error("Failed to receive successful RPC swapping of uuid response!");
      return false;
    }
    return true;
  } catch (const memgraph::rpc::RpcFailedException &) {
    spdlog::error("RPC error occurred while sending swapping uuid RPC!");
  }
  return false;
}

inline void FrequentHeartbeatHandler(slk::Reader *req_reader, slk::Builder *res_builder) {
  FrequentHeartbeatReq req;
  FrequentHeartbeatReq::Load(&req, req_reader);
  memgraph::slk::Load(&req, req_reader);
  FrequentHeartbeatRes res{};
  memgraph::slk::Save(res, res_builder);
}
}  // namespace memgraph::replication_coordination_glue

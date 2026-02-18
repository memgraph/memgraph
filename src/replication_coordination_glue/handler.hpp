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

#include "rpc/client.hpp"
#include "utils/event_counter.hpp"
#include "utils/uuid.hpp"

#include "messages.hpp"
#include "rpc/messages.hpp"
#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen

namespace memgraph::metrics {
extern const Event SwapMainUUIDRpcSuccess;
extern const Event SwapMainUUIDRpcFail;
}  // namespace memgraph::metrics

namespace memgraph::replication_coordination_glue {

inline bool SendSwapMainUUIDRpc(rpc::Client &rpc_client_, const utils::UUID &uuid) {
  auto const res = std::invoke([&rpc_client_, &uuid]() -> std::expected<SwapMainUUIDRes, rpc::RpcError> {
    auto stream{rpc_client_.Stream<SwapMainUUIDRpc>(uuid)};
    if (!stream.has_value()) return std::unexpected{stream.error()};
    return stream.value().SendAndWait();
  });

  if (res.has_value() && res.value().success) {
    metrics::IncrementCounter(metrics::SwapMainUUIDRpcSuccess);
    return true;
  }

  metrics::IncrementCounter(metrics::SwapMainUUIDRpcFail);
  spdlog::error("Received unsuccessful response to SwapMainUUIDReq");
  return false;
}

inline void FrequentHeartbeatHandler(uint64_t const request_version, slk::Reader *req_reader,
                                     slk::Builder *res_builder) {
  FrequentHeartbeatReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  rpc::SendFinalResponse(FrequentHeartbeatRes{}, request_version, res_builder);
}
}  // namespace memgraph::replication_coordination_glue

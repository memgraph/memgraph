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

#include "metrics/prometheus_metrics.hpp"
#include "rpc/client.hpp"
#include "utils/uuid.hpp"

#include "messages.hpp"
#include "rpc/messages.hpp"
#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen

namespace memgraph::replication_coordination_glue {

inline bool SendSwapMainUUIDRpc(rpc::Client &rpc_client_, const utils::UUID &uuid) {
  try {
    if (auto stream{rpc_client_.Stream<SwapMainUUIDRpc>(uuid)}; !stream.SendAndWait().success) {
      spdlog::error("Received unsuccessful response to SwapMainUUIDReq");
      metrics::Metrics().global.swap_main_uuid_rpc_fail->Increment();
      return false;
    }
    metrics::Metrics().global.swap_main_uuid_rpc_success->Increment();
    return true;
  } catch (const rpc::RpcFailedException &e) {
    spdlog::error("Failed to receive response to SwapMainUUIDReq. Error occurred: {}", e.what());
    metrics::Metrics().global.swap_main_uuid_rpc_fail->Increment();
  }
  return false;
}

inline void FrequentHeartbeatHandler(uint64_t const request_version, slk::Reader *req_reader,
                                     slk::Builder *res_builder) {
  FrequentHeartbeatReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  rpc::SendFinalResponse(FrequentHeartbeatRes{}, request_version, res_builder);
}
}  // namespace memgraph::replication_coordination_glue

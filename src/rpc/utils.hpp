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

#include "rpc/version.hpp"
#include "slk/serialization.hpp"

#include "spdlog/spdlog.h"

namespace memgraph::rpc {

template <typename TResponse>
void SendFinalResponse(TResponse const &res, slk::Builder *builder, std::string description = "") {
  slk::Save(TResponse::kType.id, builder);
  slk::Save(rpc::current_protocol_version, builder);
  slk::Save(res, builder);
  builder->Finalize();
  spdlog::trace("[RpcServer] sent {}. {}", TResponse::kType.name, description);
}

inline void SendInProgressMsg(slk::Builder *builder) {
  if (!builder->IsEmpty()) {
    throw slk::SlkBuilderException("InProgress RPC message can only be sent when the builder's buffer is empty.");
  }
  Save(storage::replication::InProgressRes::kType.id, builder);
  Save(rpc::current_protocol_version, builder);
  builder->Finalize();
  spdlog::trace("[RpcServer] sent {}", storage::replication::InProgressRes::kType.name);
}

}  // namespace memgraph::rpc

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
#include "utils/function_traits.hpp"

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

// T must be the newest type in the sequence of requests
template <typename T>
void LoadWithUpgrade(T &in, uint64_t const request_version, slk::Reader *reader) {
  if (request_version == T::kVersion) {
    slk::Load(&in, reader);
    return;
  }
  if constexpr (requires { &T::Upgrade; }) {
    using prev_t = typename utils::function_traits<decltype(&T::Upgrade)>::template argument<0>;
    prev_t prev{};
    LoadWithUpgrade(prev, request_version, reader);
    in = T::Upgrade(prev);
  } else {
    throw std::runtime_error("No upgrade path available for this type");
  }
}

}  // namespace memgraph::rpc

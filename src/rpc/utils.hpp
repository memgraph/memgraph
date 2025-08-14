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

template <typename T>
concept HasDowngrade = requires(const std::remove_cvref_t<T> &res) {
  {res.Downgrade()};
  requires RpcMessage<T> && RpcMessage<std::remove_cvref_t<decltype(res.Downgrade())>>;
};

template <RpcMessage TResponse>
void SaveWithUpgrade(TResponse const &res, uint64_t const response_version, slk::Builder *builder) {
  if (response_version == TResponse::kVersion) {
    slk::Save(res, builder);
    return;
  }
  if constexpr (HasDowngrade<TResponse>) {
    auto prev_res = res.Downgrade();
    static_assert(decltype(prev_res)::kVersion == TResponse::kVersion - 1, "Wrong response version path");
    SaveWithUpgrade(prev_res, response_version, builder);
  } else {
    throw std::runtime_error("No downgrade path available for this type of response");
  }
}

template <RpcMessage TResponse>
void SendFinalResponse(TResponse const &res, uint64_t const response_version, slk::Builder *builder,
                       std::string description = "") {
  ProtocolMessageHeader const message_header{.protocol_version = current_protocol_version,
                                             .message_id = TResponse::kType.id,
                                             .message_version = response_version};
  SaveMessageHeader(message_header, builder);
  SaveWithUpgrade(res, response_version, builder);
  builder->Finalize();
  spdlog::trace("[RpcServer] sent {}, version {}. {}", TResponse::kType.name, response_version, description);
}

inline void SendInProgressMsg(slk::Builder *builder) {
  if (!builder->IsEmpty()) {
    throw slk::SlkBuilderException("InProgress RPC message can only be sent when the builder's buffer is empty.");
  }
  constexpr ProtocolMessageHeader message_header{.protocol_version = current_protocol_version,
                                                 .message_id = storage::replication::InProgressRes::kType.id,
                                                 .message_version = storage::replication::InProgressRes::kVersion};
  SaveMessageHeader(message_header, builder);
  builder->Finalize();
  spdlog::trace("[RpcServer] sent {}", storage::replication::InProgressRes::kType.name);
}

// T must be the newest type in the sequence of requests
template <RpcMessage TRequest>
void LoadWithUpgrade(TRequest &req, uint64_t const request_version, slk::Reader *reader) {
  if (request_version == TRequest::kVersion) {
    slk::Load(&req, reader);
    return;
  }
  if constexpr (requires { &TRequest::Upgrade; }) {
    using prev_req_t = typename utils::function_traits<decltype(&TRequest::Upgrade)>::template argument<0>;
    static_assert(prev_req_t::kVersion == TRequest::kVersion - 1, "Wrong request version path");
    prev_req_t prev_req{};
    LoadWithUpgrade(prev_req, request_version, reader);
    req = TRequest::Upgrade(prev_req);
  } else {
    throw std::runtime_error("No upgrade path available for this type of request");
  }
}

}  // namespace memgraph::rpc

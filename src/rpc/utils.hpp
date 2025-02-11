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
void SendFinalResponse(TResponse const &res, slk::Builder *builder) {
  slk::Save(TResponse::kType.id, builder);
  slk::Save(rpc::current_version, builder);
  slk::Save(res, builder);
  builder->Finalize();
  spdlog::trace("[RpcServer] sent {}", TResponse::kType.name);
}

}  // namespace memgraph::rpc

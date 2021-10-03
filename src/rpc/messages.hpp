// Copyright 2021 Memgraph Ltd.
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

#include <cstdint>
#include <memory>

#include "utils/typeinfo.hpp"

namespace rpc {

using MessageSize = uint32_t;

/// Each RPC is defined via this struct.
///
/// `TRequest` and `TResponse` are required to be classes which have a static
/// member `kType` of `utils::TypeInfo` type. This is used for proper
/// registration and deserialization of RPC types. Additionally, both `TRequest`
/// and `TResponse` are required to define the following serialization functions:
///   * static void Save(const TRequest|TResponse &, slk::Builder *, ...)
///   * static void Load(TRequest|TResponse *, slk::Reader *, ...)
template <typename TRequest, typename TResponse>
struct RequestResponse {
  using Request = TRequest;
  using Response = TResponse;
};

}  // namespace rpc

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

#include <concepts>
#include <memory>
#include <type_traits>

#include "storage/v2/property_constants.hpp"
#include "storage/v2/replication/serialization.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::storage::replication {
struct FinalizeCommitRes;
}  // namespace memgraph::storage::replication

namespace memgraph::slk {
class Reader;
class Builder;
}  // namespace memgraph::slk

namespace memgraph::rpc {

using MessageSize = uint32_t;

template <typename T>
concept RpcMessage = requires {
  requires std::same_as<decltype(T::kType), const utils::TypeInfo>;
  requires std::same_as<decltype(T::kVersion), const uint64_t>;
  { T::Load(std::declval<T *>(), std::declval<slk::Reader *>()) } -> std::same_as<void>;
  { T::Save(std::declval<const T &>(), std::declval<slk::Builder *>()) } -> std::same_as<void>;
};

/// Each RPC is defined via this struct.
///
/// `TRequest` and `TResponse` are required to be classes which have a static
/// member `kType` of `utils::TypeInfo` type. This is used for proper
/// registration and deserialization of RPC types. Additionally, both `TRequest`
/// and `TResponse` are required to define the following serialization functions:
///   * static void Save(const TRequest|TResponse &, slk::Builder *, ...)
///   * static void Load(TRequest|TResponse *, slk::Reader *, ...)
template <RpcMessage TRequest, RpcMessage TResponse>
struct RequestResponse {
  using Request = TRequest;
  using Response = TResponse;
};

template <typename T>
concept IsRpc = requires {
  typename T::Request;
  typename T::Response;
};

}  // namespace memgraph::rpc

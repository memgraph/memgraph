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

namespace memgraph::storage::replication {
struct FinalizeCommitRes;
}
namespace memgraph::rpc {

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

template <typename T>
concept IsRpc = requires {
  typename T::Request;
  typename T::Response;
};

// Option I: The user needs to remember for any request bump to also bump the response, not great.
template <typename T>
concept HasActiveVersion = requires {
  std::same_as<decltype(T::kActiveVersion), const uint64_t>;
};

template <HasActiveVersion TRequest, HasActiveVersion TResponse>
struct NewRequestResponse {
  using Request = TRequest;
  using Response = TResponse;
};

// Option II: This would require new TypeId for each version of the message, new Load and Save messages...
template <typename TRequest, typename TResponse, uint64_t TVersion>
struct RequestResponseV2 {
  using Request = TRequest;
  using Response = TResponse;
  static constexpr uint64_t Version = TVersion;
};

template <typename TRequest, typename TResponse>
struct ResponseFactory {
  static TResponse make(uint64_t version) {
    static_assert(sizeof(TResponse) == 0, "No ResponseFactory specialization for this response type");
  }
};

// request ID, how to retrieve the correct request type?
// Based on type ID or subtype
// Search response type based on version and request id
// What to do for each new modification of request?
// I know exactly which request type I am handling and which response version I need to return
// Somehow based on the type ID I need to create a concrete type

}  // namespace memgraph::rpc

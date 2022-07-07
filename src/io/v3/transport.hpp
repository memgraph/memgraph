// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// TODO chrono::microseconds instead of std::time_t

#pragma once

#include <concepts>
#include <variant>

#include "utils/result.hpp"

#include "address.hpp"
#include "errors.hpp"
#include "future.hpp"

using memgraph::utils::BasicResult;

class SimulatorHandle;

template <typename T>
concept Message = requires(T a, uint8_t *ptr, size_t len) {
  // These are placeholders and will be replaced
  // by some concept that identifies Thrift-generated
  // messages.
  { a.Serialize() } -> std::same_as<std::vector<uint8_t>>;
  { T::Deserialize(ptr, len) } -> std::same_as<T>;
};

template <Message M>
struct ResponseEnvelope {
  M message;
  uint64_t request_id;
  Address from;
};

template <Message M>
using ResponseResult = BasicResult<Timeout, ResponseEnvelope<M>>;

template <Message M>
using ResponseFuture = MgFuture<ResponseResult<M>>;

template <Message... Ms>
struct RequestEnvelope {
  std::variant<Ms...> message;
  uint64_t request_id;
  Address from;
};

template <Message... Ms>
using RequestResult = BasicResult<Timeout, RequestEnvelope<Ms...>>;

template <typename I>
class Io {
 public:
  Io(I io, Address address) : implementation_(io), address_(address) {}

  /// Set the default timeout for all requests that are issued
  /// without an explicit timeout set.
  void SetDefaultTimeoutMicroseconds(uint64_t timeout_microseconds) {
    default_timeout_microseconds_ = timeout_microseconds;
  }

  /// Issue a request with an explicit timeout in microseconds provided.
  template <Message Request, Message Response>
  ResponseFuture<Response> RequestWithTimeout(Address address, Request request, uint64_t timeout_microseconds) {
    uint64_t request_id = ++request_id_counter_;
    return implementation_.template Request<Request, Response>(address, request_id, request, timeout_microseconds);
  }

  /// Issue a request that times out after the default timeout.
  template <Message Request, Message Response>
  ResponseFuture<Response> Request(Address address, Request request) {
    uint64_t request_id = ++request_id_counter_;
    uint64_t timeout_microseconds = default_timeout_microseconds_;
    return implementation_.template Request<Request, Response>(address, request_id, request, timeout_microseconds);
  }

  /// Wait for an explicit number of microseconds for a request of one of the
  /// provided types to arrive.
  template <Message... Ms>
  RequestResult<Ms...> ReceiveWithTimeout(uint64_t timeout_microseconds) {
    return implementation_.template Receive<Ms...>(timeout_microseconds);
  }

  /// Wait the default number of microseconds for a request of one of the
  /// provided types to arrive.
  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive() {
    uint64_t timeout_microseconds = default_timeout_microseconds_;
    return implementation_.template Receive<Ms...>(timeout_microseconds);
  }

  /// Send a message in a best-effort fashion. If you need reliable delivery,
  /// this must be built on-top. TCP is not enough for most use cases.
  template <Message M>
  void Send(Address address, uint64_t request_id, M message) {
    return implementation_.template Send<M>(address, request_id, message);
  }

  /// The current system time. This time source should be preferred over
  /// any other time source.
  std::time_t Now() { return implementation_.Now(); }

  /// Returns true of the system should shut-down.
  bool ShouldShutDown() { return implementation_.ShouldShutDown(); }

 private:
  I implementation_;
  Address address_;
  uint64_t request_id_counter_ = 0;
  uint64_t default_timeout_microseconds_ = 50 * 1000;
};

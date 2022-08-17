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

#pragma once

#include <chrono>
#include <concepts>
#include <random>
#include <variant>

#include "io/address.hpp"
#include "io/errors.hpp"
#include "io/future.hpp"
#include "io/time.hpp"
#include "utils/result.hpp"

namespace memgraph::io {

using memgraph::utils::BasicResult;

// TODO(tyler) ensure that Message continues to represent
// reasonable constraints around message types over time,
// as we adapt things to use Thrift-generated message types.
template <typename T>
concept Message = std::same_as<T, std::decay_t<T>>;

template <Message M>
struct ResponseEnvelope {
  M message;
  uint64_t request_id;
  Address from_address;
};

template <Message M>
using ResponseResult = BasicResult<TimedOut, ResponseEnvelope<M>>;

template <Message M>
using ResponseFuture = memgraph::io::Future<ResponseResult<M>>;

template <Message M>
using ResponsePromise = memgraph::io::Promise<ResponseResult<M>>;

template <Message... Ms>
struct RequestEnvelope {
  std::variant<Ms...> message;
  uint64_t request_id;
  Address from_address;
};

template <Message... Ms>
using RequestResult = BasicResult<TimedOut, RequestEnvelope<Ms...>>;

template <typename I>
class Io {
  I implementation_;
  Address address_;
  uint64_t request_id_counter_ = 0;
  Duration default_timeout_ = std::chrono::microseconds{50000};

 public:
  Io(I io, Address address) : implementation_(io), address_(address) {}

  /// Set the default timeout for all requests that are issued
  /// without an explicit timeout set.
  void SetDefaultTimeout(Duration timeout) { default_timeout_ = timeout; }

  /// Returns the current default timeout for this Io instance.
  Duration GetDefaultTimeout() { return default_timeout_; }

  /// Issue a request with an explicit timeout in microseconds provided. This tends to be used by clients.
  template <Message Request, Message Response>
  ResponseFuture<Response> RequestWithTimeout(Address address, Request request, Duration timeout) {
    const uint64_t request_id = ++request_id_counter_;
    return implementation_.template Request<Request, Response>(address, request_id, request, timeout);
  }

  /// Issue a request that times out after the default timeout. This tends
  /// to be used by clients.
  template <Message Request, Message Response>
  ResponseFuture<Response> Request(Address address, Request request) {
    const uint64_t request_id = ++request_id_counter_;
    const Duration timeout = default_timeout_;
    return implementation_.template Request<Request, Response>(address, request_id, std::move(request), timeout);
  }

  /// Wait for an explicit number of microseconds for a request of one of the
  /// provided types to arrive. This tends to be used by servers.
  template <Message... Ms>
  RequestResult<Ms...> ReceiveWithTimeout(Duration timeout) {
    return implementation_.template Receive<Ms...>(timeout);
  }

  /// Wait the default number of microseconds for a request of one of the
  /// provided types to arrive. This tends to be used by servers.
  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive() {
    const Duration timeout = default_timeout_;
    return implementation_.template Receive<Ms...>(timeout);
  }

  /// Send a message in a best-effort fashion. This is used for messaging where
  /// responses are not necessarily expected, and for servers to respond to requests.
  /// If you need reliable delivery, this must be built on-top. TCP is not enough for most use cases.
  template <Message M>
  void Send(Address address, uint64_t request_id, M message) {
    return implementation_.template Send<M>(address, request_id, std::move(message));
  }

  /// The current system time. This time source should be preferred over any other,
  /// because it lets us deterministically control clocks from tests for making
  /// things like timeouts deterministic.
  Time Now() const { return implementation_.Now(); }

  /// Returns true if the system should shut-down.
  bool ShouldShutDown() const { return implementation_.ShouldShutDown(); }

  /// Returns a random number within the specified distribution.
  template <class D = std::poisson_distribution<>, class Return = uint64_t>
  Return Rand(D distrib) {
    return implementation_.template Rand<D, Return>(distrib);
  }

  Address GetAddress() { return address_; }
};
};  // namespace memgraph::io

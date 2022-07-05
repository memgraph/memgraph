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
struct MessageAndAddress {
  M message;
  uint64_t request_id;
  Address from;
};

template <Message M>
using ResponseResult = BasicResult<Timeout, MessageAndAddress<M>>;

template <Message M>
using ResponseFuture = MgFuture<ResponseResult<M>>;

template <Message... Ms>
struct MessageVariantAndSenderAddress {
  std::variant<Ms...> message;
  uint64_t request_id;
  Address from;
};

template <Message... Ms>
using RequestResult = BasicResult<Timeout, MessageVariantAndSenderAddress<Ms...>>;

template <typename I>
class Io {
 public:
  Io(I io, Address address) : implementation_(io), address_(address) {}

  void SetDefaultTimeoutMicroseconds(uint64_t timeout_microseconds) {
    default_timeout_microseconds_ = timeout_microseconds;
  }

  /*
    template <Message Request, Message Response>
    ResponseFuture<Response> RequestTimeout(Address address, Request request, uint64_t timeout_microseconds) {
      uint64_t request_id = ++request_id_counter_;
      return implementation_.template RequestTimeout<Request, Response>(address, request_id, request,
                                                                        timeout_microseconds);
    }
    */

  template <Message Request, Message Response>
  ResponseFuture<Response> RequestTimeout(Address address, Request request) {
    uint64_t request_id = ++request_id_counter_;
    uint64_t timeout_microseconds = default_timeout_microseconds_;
    return implementation_.template RequestTimeout<Request, Response>(address, request_id, request,
                                                                      timeout_microseconds);
  }

  /*
    template <Message... Ms>
    RequestResult<Ms...> ReceiveTimeout(uint64_t timeout_microseconds) {
      return implementation_.template ReceiveTimeout<Ms...>(timeout_microseconds);
    }

    template <Message... Ms>
    RequestResult<Ms...> ReceiveTimeout() {
      uint64_t timeout_microseconds = default_timeout_microseconds_;
      return implementation_.template ReceiveTimeout<Ms...>(timeout_microseconds);
    }
    template <Message M>
    void Send(Address address, uint64_t request_id, M message) {
      return implementation_.template Send<M>(address, request_id, message);
    }
    */

  std::time_t Now() { return implementation_.Now(); }

  bool ShouldShutDown() { return implementation_.ShouldShutDown(); }

 private:
  I implementation_;
  Address address_;
  uint64_t request_id_counter_ = 0;
  uint64_t default_timeout_microseconds_ = 50 * 1000;
};

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

#include "io/address.hpp"
#include "io/message_conversion.hpp"
#include "io/protobuf_transport/protobuf_transport_handle.hpp"
#include "io/transport.hpp"

namespace memgraph::io::protobuf_transport {

class ProtobufTransport {
  std::shared_ptr<ProtobufTransportHandle> protobuf_transport_handle_;

 public:
  explicit ProtobufTransport(std::shared_ptr<ProtobufTransportHandle> protobuf_transport_handle)
      : protobuf_transport_handle_(std::move(protobuf_transport_handle)) {}

  template <Message RequestT, Message ResponseT>
  ResponseFuture<ResponseT> Request(Address to_address, Address from_address, RequestId request_id, RequestT request,
                                    Duration timeout) {
    auto [future, promise] = memgraph::io::FuturePromisePair<ResponseResult<ResponseT>>();

    protobuf_transport_handle_->SubmitRequest(to_address, from_address, request_id, std::move(request), timeout,
                                              std::move(promise));

    return std::move(future);
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(Address receiver_address, Duration timeout) {
    return protobuf_transport_handle_->template Receive<Ms...>(receiver_address, timeout);
  }

  template <Message M>
  void Send(Address to_address, Address from_address, RequestId request_id, M &&message) {
    return protobuf_transport_handle_->template Send<M>(to_address, from_address, request_id, std::forward<M>(message));
  }

  Time Now() const { return protobuf_transport_handle_->Now(); }

  bool ShouldShutDown() const { return protobuf_transport_handle_->ShouldShutDown(); }

  template <class D = std::poisson_distribution<>, class Return = uint64_t>
  Return Rand(D distrib) {
    std::random_device rng;
    return distrib(rng);
  }
};

};  // namespace memgraph::io::protobuf_transport

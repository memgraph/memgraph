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

#include <iostream>
#include <optional>
#include <vector>

#include "io/address.hpp"
#include "io/errors.hpp"
#include "io/rsm/raft.hpp"
#include "utils/result.hpp"

namespace memgraph::io::rsm {

using memgraph::io::Address;
using memgraph::io::Duration;
using memgraph::io::ResponseEnvelope;
using memgraph::io::ResponseFuture;
using memgraph::io::ResponseResult;
using memgraph::io::Time;
using memgraph::io::TimedOut;
using memgraph::io::rsm::ReadRequest;
using memgraph::io::rsm::ReadResponse;
using memgraph::io::rsm::WriteRequest;
using memgraph::io::rsm::WriteResponse;
using memgraph::utils::BasicResult;

template <typename IoImpl, typename WriteRequestT, typename WriteResponseT, typename ReadRequestT,
          typename ReadResponseT>
class RsmClient {
  using ServerPool = std::vector<Address>;

  Io<IoImpl> io_;
  Address leader_;
  ServerPool server_addrs_;

  /// State for single async read/write operations. In the future this could become a map
  /// of async operations that can be accessed via an ID etc...
  Time async_read_before_;
  std::optional<ResponseFuture<ReadResponse<ReadResponseT>>> async_read_;
  Time async_write_before_;
  std::optional<ResponseFuture<ReadResponse<WriteResponseT>>> async_write_;

  template <typename ResponseT>
  bool PossiblyRedirectLeader(const ResponseT &response) {
    if (response.retry_leader) {
      MG_ASSERT(!response.success, "retry_leader should never be set for successful responses");
      leader_ = response.retry_leader.value();
      spdlog::debug("client redirected to leader server {}", leader_.ToString());
      return true;
    } else if (!response.success) {
      std::uniform_int_distribution<size_t> addr_distrib(0, (server_addrs_.size() - 1));
      size_t addr_index = io_.Rand(addr_distrib);
      leader_ = server_addrs_[addr_index];

      spdlog::debug(
          "client NOT redirected to leader server despite our success failing to be processed (it probably was sent to "
          "a RSM Candidate) trying a random one at index {} with address {}",
          addr_index, leader_.ToString());
      return true;
    }
    return false;
  }

 public:
  RsmClient(Io<IoImpl> io, Address leader, ServerPool server_addrs)
      : io_{io}, leader_{leader}, server_addrs_{server_addrs} {}

  RsmClient(const RsmClient &) = delete;
  RsmClient &operator=(const RsmClient &) = delete;
  RsmClient(RsmClient &&) noexcept = default;
  RsmClient &operator=(RsmClient &&) noexcept = default;

  RsmClient() = delete;

  BasicResult<TimedOut, WriteResponseT> SendWriteRequest(WriteRequestT req) {
    WriteRequest<WriteRequestT> client_req;
    client_req.operation = req;

    const Duration overall_timeout = io_.GetDefaultTimeout();
    const Time before = io_.Now();

    do {
      spdlog::debug("client sending WriteRequest to Leader {}", leader_.ToString());
      ResponseFuture<WriteResponse<WriteResponseT>> response_future =
          io_.template Request<WriteRequest<WriteRequestT>, WriteResponse<WriteResponseT>>(leader_, client_req);
      ResponseResult<WriteResponse<WriteResponseT>> response_result = std::move(response_future).Wait();

      if (response_result.HasError()) {
        spdlog::debug("client timed out while trying to communicate with leader server {}", leader_.ToString());
        return response_result.GetError();
      }

      ResponseEnvelope<WriteResponse<WriteResponseT>> &&response_envelope = std::move(response_result.GetValue());
      WriteResponse<WriteResponseT> &&write_response = std::move(response_envelope.message);

      if (write_response.success) {
        return std::move(write_response.write_return);
      }

      PossiblyRedirectLeader(write_response);
    } while (io_.Now() < before + overall_timeout);

    return TimedOut{};
  }

  BasicResult<TimedOut, ReadResponseT> SendReadRequest(ReadRequestT req) {
    ReadRequest<ReadRequestT> read_req;
    read_req.operation = req;

    const Duration overall_timeout = io_.GetDefaultTimeout();
    const Time before = io_.Now();

    do {
      spdlog::debug("client sending ReadRequest to Leader {}", leader_.ToString());

      ResponseFuture<ReadResponse<ReadResponseT>> get_response_future =
          io_.template Request<ReadRequest<ReadRequestT>, ReadResponse<ReadResponseT>>(leader_, read_req);

      // receive response
      ResponseResult<ReadResponse<ReadResponseT>> get_response_result = std::move(get_response_future).Wait();

      if (get_response_result.HasError()) {
        spdlog::debug("client timed out while trying to communicate with leader server {}", leader_.ToString());
        return get_response_result.GetError();
      }

      ResponseEnvelope<ReadResponse<ReadResponseT>> &&get_response_envelope = std::move(get_response_result.GetValue());
      ReadResponse<ReadResponseT> &&read_get_response = std::move(get_response_envelope.message);

      if (read_get_response.success) {
        return std::move(read_get_response.read_return);
      }

      PossiblyRedirectLeader(read_get_response);
    } while (io_.Now() < before + overall_timeout);

    return TimedOut{};
  }

  /// This method submits a request to the underlying Io interface
  /// and stores it as this client's async read.
  void SendAsyncReadRequest(ReadRequestT read_req) {
    MG_ASSERT(!async_read_);

    async_read_before_ = io_.Now();
    async_read_ = io_.template Request<ReadRequest<ReadRequestT>, ReadResponse<ReadResponseT>>(leader_, read_req);
  }

  /// Non-blocking. See if a response or a timeout has occurred for our async read request, and if so,
  /// handle it in a similar way as SendReadRequest does.
  std::optional<BasicResult<TimedOut, ReadResponseT>> PollAsyncReadRequest() {
    MG_ASSERT(async_read_);

    bool can_act = async_read_.IsReady();

    if (!can_act) {
      return std::nullopt;
    }

    const Duration overall_timeout = io_.GetDefaultTimeout();

    do {
      std::optional<ResponseResult<ReadResponse<ReadResponseT>>> get_response_result = async_read_.TryGet();
      if (!get_response_result) {
        // Inner result is not ready yet.
        // TODO(gvolfing) -VERIFY- is this actually needed?
        return std::nullopt;
      }

      if (get_response_result.HasError()) {
        spdlog::debug("client timed out while trying to communicate with leader server {}", leader_.ToString());
        return get_response_result.GetError();
      }

      ResponseEnvelope<ReadResponse<ReadResponseT>> &&get_response_envelope = std::move(get_response_result.GetValue());
      ReadResponse<ReadResponseT> &&read_get_response = std::move(get_response_envelope.message);

      if (PossiblyRedirectLeader(read_get_response)) {
        return std::nullopt;
      }
    } while (io_.Now() < async_read_before_ + overall_timeout);

    return TimedOut{};
    // we have either received a timeout, a redirection, or a returnable high-level response
  }

  /// Blocking. Drive the underlying async_read_ request until its request returns or times out, in a similar way that
  /// SendReadRequest does. This will block until timeout or response, but may still return std::nullopt if we were
  /// redirected.
  std::optional<BasicResult<TimedOut, ReadResponseT>> AwaitAsyncReadRequest() {
    MG_ASSERT(async_read_);
    // TODO(Gabor) call Wait on the underlying future and then handle it as-if can_act is true in the above method

    const Duration overall_timeout = io_.GetDefaultTimeout();

    do {
      ResponseResult<ReadResponse<ReadResponseT>> get_response_result = async_read_.Wait();
      if (get_response_result.HasError()) {
        spdlog::debug("client timed out while trying to communicate with leader server {}", leader_.ToString());
        return get_response_result.GetError();
      }

      ResponseEnvelope<ReadResponse<ReadResponseT>> &&get_response_envelope = std::move(get_response_result.GetValue());
      ReadResponse<ReadResponseT> &&read_get_response = std::move(get_response_envelope.message);

      if (read_get_response.success) {
        return std::move(read_get_response.read_return);
      }

      if (PossiblyRedirectLeader(read_get_response)) {
        return std::nullopt;
      }
    } while (io_.Now() < async_read_before_ + overall_timeout);

    return TimedOut{};
  }

  // TODO(Gabor) do the same 3 methods for AsyncWriteRequest
};

}  // namespace memgraph::io::rsm

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
#include <type_traits>
#include <unordered_map>
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

class AsyncRequestToken {
  size_t id_;

 public:
  AsyncRequestToken(size_t id) : id_(id) {}
  size_t GetId() const { return id_; }
};

template <typename RequestT, typename ResponseT>
struct AsyncRequest {
  Time start_time;
  RequestT request;
  ResponseFuture<ResponseT> future;
};

template <typename IoImpl, typename WriteRequestT, typename WriteResponseT, typename ReadRequestT,
          typename ReadResponseT>
class RsmClient {
  using ServerPool = std::vector<Address>;

  Io<IoImpl> io_;
  Address leader_;
  ServerPool server_addrs_;

  /// State for single async read/write operations. In the future this could become a map
  /// of async operations that can be accessed via an ID etc...
  std::unordered_map<size_t, AsyncRequest<ReadRequestT, ReadResponse<ReadResponseT>>> async_reads_;
  std::unordered_map<size_t, AsyncRequest<WriteRequestT, WriteResponse<WriteResponseT>>> async_writes_;

  size_t async_token_generator_ = 0;

  void SelectRandomLeader() {
    std::uniform_int_distribution<size_t> addr_distrib(0, (server_addrs_.size() - 1));
    size_t addr_index = io_.Rand(addr_distrib);
    leader_ = server_addrs_[addr_index];

    spdlog::debug(
        "client NOT redirected to leader server despite our success failing to be processed (it probably was sent to "
        "a RSM Candidate) trying a random one at index {} with address {}",
        addr_index, leader_.ToString());
  }

  template <typename ResponseT>
  void PossiblyRedirectLeader(const ResponseT &response) {
    if (response.retry_leader) {
      MG_ASSERT(!response.success, "retry_leader should never be set for successful responses");
      leader_ = response.retry_leader.value();
      spdlog::debug("client redirected to leader server {}", leader_.ToString());
    }
    if (!response.success) {
      SelectRandomLeader();
    }
  }

 public:
  RsmClient(Io<IoImpl> io, Address leader, ServerPool server_addrs)
      : io_{io}, leader_{leader}, server_addrs_{server_addrs} {}

  RsmClient(const RsmClient &) = delete;
  RsmClient &operator=(const RsmClient &) = delete;
  RsmClient(RsmClient &&) noexcept = default;
  RsmClient &operator=(RsmClient &&) noexcept = default;

  RsmClient() = delete;
  ~RsmClient() = default;

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

  /// AsyncRead methods
  AsyncRequestToken SendAsyncReadRequest(const ReadRequestT &req) {
    size_t token = async_token_generator_++;

    ReadRequest<ReadRequestT> read_req = {.operation = req};

    AsyncRequest<ReadRequestT, ReadResponse<ReadResponseT>> async_request{
        .start_time = io_.Now(),
        .request = std::move(req),
        .future = io_.template Request<ReadRequest<ReadRequestT>, ReadResponse<ReadResponseT>>(leader_, read_req),
    };

    async_reads_.emplace(token, std::move(async_request));

    return AsyncRequestToken(token);
  }

  void ResendAsyncReadRequest(AsyncRequestToken &token) {
    auto &async_request = async_reads_.at(token.GetId());

    ReadRequest<ReadRequestT> read_req = {.operation = async_request.request};

    async_request.future =
        io_.template Request<ReadRequest<ReadRequestT>, ReadResponse<ReadResponseT>>(leader_, read_req);
  }

  std::optional<BasicResult<TimedOut, ReadResponseT>> PollAsyncReadRequest(AsyncRequestToken &token) {
    auto &async_request = async_reads_.at(token.GetId());

    if (!async_request.future.IsReady()) {
      return std::nullopt;
    }

    return AwaitAsyncReadRequest();
  }

  std::optional<BasicResult<TimedOut, ReadResponseT>> AwaitAsyncReadRequest(AsyncRequestToken &token) {
    auto &async_request = async_reads_.at(token.GetId());
    ResponseResult<ReadResponse<ReadResponseT>> get_response_result = std::move(async_request.future).Wait();

    const Duration overall_timeout = io_.GetDefaultTimeout();
    const bool past_time_out = io_.Now() > async_request.start_time + overall_timeout;
    const bool result_has_error = get_response_result.HasError();

    if (result_has_error && past_time_out) {
      // TODO static assert the exact type of error.
      spdlog::debug("client timed out while trying to communicate with leader server {}", leader_.ToString());
      async_reads_.erase(token.GetId());
      return TimedOut{};
    }

    if (!result_has_error) {
      ResponseEnvelope<ReadResponse<ReadResponseT>> &&get_response_envelope = std::move(get_response_result.GetValue());
      ReadResponse<ReadResponseT> &&read_get_response = std::move(get_response_envelope.message);

      PossiblyRedirectLeader(read_get_response);

      if (read_get_response.success) {
        async_reads_.erase(token.GetId());
        return std::move(read_get_response.read_return);
      }
    } else {
      SelectRandomLeader();
    }

    ResendAsyncReadRequest(token);

    return std::nullopt;
  }

  /// AsyncWrite methods
  AsyncRequestToken SendAsyncWriteRequest(const WriteRequestT &req) {
    size_t token = async_token_generator_++;

    WriteRequest<WriteRequestT> write_req = {.operation = req};

    AsyncRequest<WriteRequestT, WriteResponse<WriteResponseT>> async_request{
        .start_time = io_.Now(),
        .request = std::move(req),
        .future = io_.template Request<WriteRequest<WriteRequestT>, WriteResponse<WriteResponseT>>(leader_, write_req),
    };

    async_writes_.emplace(token, std::move(async_request));

    return AsyncRequestToken(token);
  }

  void ResendAsyncWriteRequest(AsyncRequestToken &token) {
    auto &async_request = async_writes_.at(token.GetId());

    WriteRequest<WriteRequestT> write_req = {.operation = async_request.request};

    async_request.future =
        io_.template Request<WriteRequest<WriteRequestT>, WriteResponse<WriteResponseT>>(leader_, write_req);
  }

  std::optional<BasicResult<TimedOut, WriteResponseT>> PollAsyncWriteRequest(AsyncRequestToken &token) {
    auto &async_request = async_writes_.at(token.GetId());

    if (!async_request.future.IsReady()) {
      return std::nullopt;
    }

    return AwaitAsyncWriteRequest();
  }

  std::optional<BasicResult<TimedOut, WriteResponseT>> AwaitAsyncWriteRequest(AsyncRequestToken &token) {
    auto &async_request = async_writes_.at(token.GetId());
    ResponseResult<WriteResponse<WriteResponseT>> get_response_result = std::move(async_request.future).Wait();

    const Duration overall_timeout = io_.GetDefaultTimeout();
    const bool past_time_out = io_.Now() > async_request.start_time + overall_timeout;
    const bool result_has_error = get_response_result.HasError();

    if (result_has_error && past_time_out) {
      // TODO static assert the exact type of error.
      spdlog::debug("client timed out while trying to communicate with leader server {}", leader_.ToString());
      async_writes_.erase(token.GetId());
      return TimedOut{};
    }

    if (!result_has_error) {
      ResponseEnvelope<WriteResponse<WriteResponseT>> &&get_response_envelope =
          std::move(get_response_result.GetValue());
      WriteResponse<WriteResponseT> &&write_get_response = std::move(get_response_envelope.message);

      PossiblyRedirectLeader(write_get_response);

      if (write_get_response.success) {
        async_writes_.erase(token.GetId());
        return std::move(write_get_response.write_return);
      }
    } else {
      SelectRandomLeader();
    }

    ResendAsyncWriteRequest(token);

    return std::nullopt;
  }
};

}  // namespace memgraph::io::rsm

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

#include <iostream>
#include <optional>
#include <vector>

#include "io/address.hpp"
#include "io/rsm/raft.hpp"

using memgraph::io::Address;
using memgraph::io::ResponseEnvelope;
using memgraph::io::ResponseFuture;
using memgraph::io::ResponseResult;
using memgraph::io::rsm::ReadRequest;
using memgraph::io::rsm::ReadResponse;
using memgraph::io::rsm::WriteRequest;
using memgraph::io::rsm::WriteResponse;

template <typename IoImpl, typename WriteRequestT, typename WriteResponseT, typename ReadRequestT,
          typename ReadResponseT>
class RsmClient {
  using ServerPool = std::vector<Address>;

  IoImpl io_;
  Address leader_;

  std::mt19937 cli_rng_{0};
  ServerPool server_addrs_;

  template <typename ResponseT>
  std::optional<ResponseT> CheckForCorrectLeader(ResponseT response) {
    if (response.retry_leader) {
      MG_ASSERT(!response.success, "retry_leader should never be set for successful responses");
      leader_ = response.retry_leader.value();
      std::cout << "client redirected to leader server " << leader_.last_known_port << std::endl;
    } else if (!response.success) {
      std::uniform_int_distribution<size_t> addr_distrib(0, (server_addrs_.size() - 1));
      size_t addr_index = addr_distrib(cli_rng_);
      leader_ = server_addrs_[addr_index];

      std::cout << "client NOT redirected to leader server, trying a random one at index " << addr_index
                << " with port " << leader_.last_known_port << std::endl;
      return std::nullopt;
    }

    return response;
  }

 public:
  RsmClient(IoImpl io, Address leader, ServerPool server_addrs)
      : io_{io}, leader_{leader}, server_addrs_{server_addrs} {}

  RsmClient() = delete;

  std::optional<WriteResponse<WriteResponseT>> SendWriteRequest(WriteRequestT req) {
    WriteRequest<WriteRequestT> client_req;
    client_req.operation = req;

    std::cout << "client sending CasRequest to Leader " << leader_.last_known_port << std::endl;
    ResponseFuture<WriteResponse<WriteResponseT>> response_future =
        io_.template Request<WriteRequest<WriteRequestT>, WriteResponse<WriteResponseT>>(leader_, client_req);
    ResponseResult<WriteResponse<WriteResponseT>> response_result = std::move(response_future).Wait();

    if (response_result.HasError()) {
      std::cout << "client timed out while trying to communicate with leader server " << std::endl;
      // continue;
      return std::nullopt;
    }

    ResponseEnvelope<WriteResponse<WriteResponseT>> response_envelope = response_result.GetValue();
    WriteResponse<WriteResponseT> write_response = response_envelope.message;

    return CheckForCorrectLeader(write_response);
  }

  std::optional<ReadResponse<ReadResponseT>> SendReadRequest(ReadRequestT req) {
    ReadRequest<ReadRequestT> read_req;
    read_req.operation = req;

    std::cout << "client sending GetRequest to Leader " << leader_.last_known_port << std::endl;

    ResponseFuture<ReadResponse<ReadResponseT>> get_response_future =
        io_.template Request<ReadRequest<ReadRequestT>, ReadResponse<ReadResponseT>>(leader_, read_req);

    // receive response
    ResponseResult<ReadResponse<ReadResponseT>> get_response_result = std::move(get_response_future).Wait();

    if (get_response_result.HasError()) {
      std::cout << "client timed out while trying to communicate with leader server " << std::endl;
      return std::nullopt;
    }

    ResponseEnvelope<ReadResponse<ReadResponseT>> get_response_envelope = get_response_result.GetValue();
    ReadResponse<ReadResponseT> read_get_response = get_response_envelope.message;

    // if (!read_get_response.success) {
    //   // sent to a non-leader
    //   return {};
    // }

    return CheckForCorrectLeader(read_get_response);
  }
};

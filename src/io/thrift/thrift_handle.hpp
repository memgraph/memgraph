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

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>

#include <boost/asio/ip/tcp.hpp>
#include <boost/lexical_cast.hpp>

#include <folly/init/Init.h>
#include <folly/io/SocketOptionMap.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/net/NetworkSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

// From generated code
#include "interface/gen-cpp2/UberServer.h"
#include "interface/gen-cpp2/UberServerAsyncClient.h"

#include "io/errors.hpp"
#include "io/message_conversion.hpp"
#include "io/transport.hpp"

namespace memgraph::io::thrift {

using namespace apache::thrift;
using namespace folly;

using memgraph::io::Address;
using memgraph::io::OpaqueMessage;
using memgraph::io::OpaquePromise;
using memgraph::io::TimedOut;
using RequestId = uint64_t;
class ThriftHandle {
  mutable std::mutex mu_{};
  mutable std::condition_variable cv_;
  const Address address_ = Address::TestAddress(0);

  EventBase base_;

  // the responses to requests that are being waited on
  std::map<PromiseKey, DeadlineAndOpaquePromise> promises_;

  // messages that are sent to servers that may later receive them
  std::vector<OpaqueMessage> can_receive_;

  // TODO(tyler) thrift clients for each outbound address combination
  // AsyncClient does not offer default init so they are optional atm.
  std::map<Address, std::optional<cpp2::UberServerAsyncClient>> clients_;

  // TODO(gabor) make this to a threadpool
  // uuid of the address -> port number where the given rsm is residing.
  // TODO(gabor) The RSM map should not be a part of this class.
  // std::map<boost::uuids::uuid, uint16_t /*this should be the actual RSM*/> rsm_map_;

  cpp2::Address convertToUberAddress(const memgraph::io::Address &address) {
    cpp2::Address ret_address;
    ret_address.unique_id_ref() = boost::uuids::to_string(address.unique_id);
    ret_address.last_known_ip_ref() = address.last_known_ip.to_string();
    ret_address.last_known_port_ref() = static_cast<int32_t>(address.last_known_port);
    return ret_address;
  }

 public:
  explicit ThriftHandle(Address our_address) : address_(our_address) {}

  Time Now() const {
    auto nano_time = std::chrono::system_clock::now();
    return std::chrono::time_point_cast<std::chrono::microseconds>(nano_time);
  }

  template <Message M>
  void DeliverMessage(Address to_address, Address from_address, RequestId request_id, M &&message) {
    std::any message_any(std::move(message));
    OpaqueMessage opaque_message{
        .from_address = from_address, .request_id = request_id, .message = std::move(message_any)};

    PromiseKey promise_key{.requester_address = to_address,
                           .request_id = opaque_message.request_id,
                           .replier_address = opaque_message.from_address};

    {
      std::unique_lock<std::mutex> lock(mu_);

      if (promises_.contains(promise_key)) {
        // complete waiting promise if it's there
        DeadlineAndOpaquePromise dop = std::move(promises_.at(promise_key));
        promises_.erase(promise_key);

        dop.promise.Fill(std::move(opaque_message));
      } else {
        can_receive_.emplace_back(std::move(opaque_message));
      }
    }  // lock dropped

    cv_.notify_all();
  }

  template <Message Request, Message Response>
  void SubmitRequest(Address to_address, Address from_address, RequestId request_id, Request &&request,
                     Duration timeout, ResponsePromise<Response> &&promise) {
    const Time deadline = Now() + timeout;
    Address our_address = address_;

    PromiseKey promise_key{.requester_address = from_address, .request_id = request_id, .replier_address = to_address};
    OpaquePromise opaque_promise(std::move(promise).ToUnique());
    DeadlineAndOpaquePromise dop{.deadline = deadline, .promise = std::move(opaque_promise)};
    promises_.emplace(std::move(promise_key), std::move(dop));

    cv_.notify_all();

    bool port_matches = to_address.last_known_port == our_address.last_known_port;
    bool ip_matches = to_address.last_known_ip == our_address.last_known_ip;

    if (port_matches && ip_matches) {
      // hairpin routing optimization
      DeliverMessage(to_address, from_address, request_id, std::move(request));
    } else {
      // send using a thrift client to remove service
      Send(to_address, from_address, request_id, request);
    }
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(Duration timeout) {
    // TODO(tyler) block for the specified duration on the Inbox's receipt of a message of this type.
    std::unique_lock lock(mu_);

    Time before = Now();

    while (can_receive_.empty()) {
      Time now = Now();

      // protection against non-monotonic timesources
      auto maxed_now = std::max(now, before);
      auto elapsed = maxed_now - before;

      if (timeout < elapsed) {
        return TimedOut{};
      }

      Duration relative_timeout = timeout - elapsed;

      auto cv_status_value = cv_.wait_for(lock, relative_timeout);

      if (cv_status_value == std::cv_status::timeout) {
        return TimedOut{};
      }
    }

    auto current_message = std::move(can_receive_.back());
    can_receive_.pop_back();

    auto m_opt = std::move(current_message).Take<Ms...>();

    return std::move(m_opt).value();
  }

  // This method is used for communication for in-between different
  // machines and processes, its exact functionality will be
  // implemented later after the shape of the Thrift generated
  // UberMessage is specified as this is not needed for M1.
  template <Message M>
  void Send(Address to_address, Address from_address, RequestId request_id, M message) {
    MG_ASSERT(false, "Communication in-between different machines and processes is not yet implemented!");

    //   // TODO(tyler) call thrift client for address (or create one if it doesn't exist yet)
    //   cpp2::UberMessage uber_message;

    //   uber_message.to_address_ref() = convertToUberAddress(to_address);
    //   uber_message.from_address_ref() = convertToUberAddress(from_address);
    //   uber_message.request_id_ref() = static_cast<int64_t>(request_id);
    //   uber_message.high_level_union_ref() = message;

    //   // cpp2::UberMessage uber_message = {
    //   //   .to_address = convertToUberAddress(to_address),
    //   //   .from_address = convertToUberAddress(from_address),
    //   //   .request_id = static_cast<int64_t>(request_id),
    //   //   .high_level_union = message
    //   // };

    //   if (clients_.contains(to_address)) {
    //     auto &client = clients_[to_address];
    //     client->sync_ReceiveUberMessage(uber_message);
    //   } else {
    //     // maybe make this into a member var
    //     const auto &other_ip = to_address.last_known_ip.to_string();
    //     const auto &other_port = to_address.last_known_port;
    //     auto socket(folly::AsyncSocket::newSocket(&base_, other_ip, other_port));
    //     auto client_channel = HeaderClientChannel::newChannel(std::move(socket));
    //     // Create a client object
    //     cpp2::UberServerAsyncClient client(std::move(client_channel));

    //     client.sync_ReceiveUberMessage(uber_message);
    //   }
  }
};

class UberMessageService final : cpp2::UberServerSvIf {
  std::shared_ptr<ThriftHandle> handle_;

  memgraph::io::Address convertToMgAddress(const cpp2::Address &address) {
    memgraph::io::Address ret_address;
    ret_address = {.unique_id{boost::lexical_cast<boost::uuids::uuid>(address.get_unique_id())},
                   .last_known_ip{boost::asio::ip::make_address(address.get_last_known_ip())},
                   .last_known_port = static_cast<uint16_t>(address.get_last_known_port())};
    return ret_address;
  }

 public:
  explicit UberMessageService(std::shared_ptr<ThriftHandle> handle) : handle_{handle} {}

  void ReceiveUberMessage(const cpp2::UberMessage &uber_message) override {
    const auto &to_address = uber_message.get_to_address();
    const auto &from_address = uber_message.get_from_address();
    const auto &request_id = uber_message.get_request_id();
    auto message = uber_message.get_high_level_union();

    const auto mg_to_address = convertToMgAddress(to_address);
    const auto mg_from_address = convertToMgAddress(from_address);
    // Castint int64_t -> uint64_t
    // FBThrift only provides us with signed integers. If someone
    // wishes to use signed integers then the go to solution seems to
    // be to use the one-bigger signed version. Unfortunately FBThrift
    // does not provide a uint128_t so we have to use the 64 bit one
    // for now.
    // TODO(gvolfing) Investigate and try to get around this problem
    // with Varint or some other Thrift type.
    const auto mg_request_id = static_cast<uint64_t>(request_id);

    // Transform high_level_union into something usable if needed(?).
    handle_->DeliverMessage(mg_to_address, mg_from_address, mg_request_id, std::move(message));
  }
};

}  // namespace memgraph::io::thrift

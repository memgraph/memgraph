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

#include <chrono>
#include <string>
#include <thread>

#include "gtest/gtest.h"

#include "io/address.hpp"
#include "io/future.hpp"
#include "io/thrift/thrift_handle.hpp"
#include "io/transport.hpp"
#include "utils/logging.hpp"

using memgraph::io::Address;
using memgraph::io::Duration;
using memgraph::io::FuturePromisePair;
using memgraph::io::RequestEnvelope;
using memgraph::io::ResponseEnvelope;
using memgraph::io::ResponseResult;
using memgraph::io::Time;
using memgraph::io::thrift::ThriftHandle;

namespace {
struct TestMessage {
  int value;
};
}  // namespace

TEST(Thrift, ThriftHandleTimeout) {
  auto our_address = Address::TestAddress(0);
  auto handle = ThriftHandle{our_address};

  // assert timeouts fire
  Duration zero_timeout = Duration{};
  auto should_timeout_1 = handle.Receive<TestMessage>(zero_timeout);
  MG_ASSERT(should_timeout_1.HasError());

  Duration ten_ms = std::chrono::microseconds{10000};
  Time before = handle.Now();

  auto should_timeout_2 = handle.Receive<TestMessage>(ten_ms);
  MG_ASSERT(should_timeout_2.HasError());

  Time after = handle.Now();

  MG_ASSERT(after - before >= ten_ms);
}

TEST(Thrift, ThriftHandleReceive) {
  auto our_address = Address::TestAddress(0);
  auto handle = ThriftHandle{our_address};

  // assert we can send and receive
  auto to_address = Address::TestAddress(0);
  auto from_address = Address::TestAddress(1);
  auto request_id = 0;
  auto message = TestMessage{
      .value = 777,
  };

  handle.DeliverMessage(to_address, from_address, request_id, std::move(message));

  auto should_have_message = handle.Receive<TestMessage>(Duration{});
  MG_ASSERT(should_have_message.HasValue());

  RequestEnvelope<TestMessage> re = should_have_message.GetValue();
  TestMessage request = std::get<TestMessage>(std::move(re.message));
  MG_ASSERT(request.value == 777);
}

/// this test "sends" a TestMessage to a server and expects to receive
/// a TestMessage back with the same value.
TEST(Thrift, ThriftHandleRequestReceive) {
  // use the same address for now, to rely on loopback optimization
  auto our_address = Address::TestAddress(0);
  auto cli_address = our_address;
  auto srv_address = cli_address;

  auto handle = ThriftHandle{our_address};

  auto timeout = Duration{};
  auto request_id = 1;
  auto expected_value = 323;
  auto request = TestMessage{};
  request.value = expected_value;

  auto [future, promise] = FuturePromisePair<ResponseResult<TestMessage>>();

  handle.SubmitRequest(srv_address, cli_address, request_id, std::move(request), timeout, std::move(promise));

  // TODO(tyler) do actual socket stuff in the future maybe

  ResponseResult<TestMessage> response_result = std::move(future).Wait();
  MG_ASSERT(response_result.HasValue());
  ResponseEnvelope<TestMessage> response_envelope = response_result.GetValue();
  TestMessage response = response_envelope.message;
  MG_ASSERT(response.value == expected_value);
}

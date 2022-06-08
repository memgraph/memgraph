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

#include <folly/init/Init.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>
#include "interface/gen-cpp2/PingPong.h"  // From generated code
#include "interface/gen-cpp2/PingPongAsyncClient.h"

using namespace apache::thrift;
using namespace cpp2;
using namespace folly;

int main(int argc, char **argv) {
  EventBase base;
  static constexpr int port = 6666;  // The port on which server is listening
  folly::init(&argc, &argv);

  // Create a async client socket and connect it. Change
  // the ip address to where the server is listening

  auto socket(folly::AsyncSocket::newSocket(&base, "127.0.0.1", port));

  // Create a HeaderClientChannel object which is used in creating
  // client object
  auto client_channel = HeaderClientChannel::newChannel(std::move(socket));
  // Create a client object
  PingPongAsyncClient client(std::move(client_channel));
  // Invoke the add function on the server. As we are doing async
  // invocation of the function, we do not immediately get
  // the result. Instead we get a future object.
  Ping req{};
  req.message_ref() = "I am here!";
  Pong result{};
  LOG(ERROR) << "Sending...";
  client.sync_ping(result, req);
  LOG(ERROR) << result.get_message();

  return 0;
}
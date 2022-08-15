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

#include <string>
#include <thread>

#include "gtest/gtest.h"

#include <folly/init/Init.h>
#include <folly/io/SocketOptionMap.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/net/NetworkSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "interface/gen-cpp2/Echo.h"  // From generated code
#include "interface/gen-cpp2/EchoAsyncClient.h"

#include "io/thrift/thrift_transport.hpp"

using namespace memgraph::io;

using namespace apache::thrift;
using namespace cpp2;
using namespace folly;

using namespace std::chrono_literals;

namespace {

// static constexpr int port = 6666;  // The port on which server is listening

class EchoSvc : public EchoSvIf {
  inline static const std::string prefix_{"0"};

  std::string current_message_;
  // bool has_message_{false};

 public:
  virtual ~EchoSvc() {}

  // The Thrift handle method
  void ReceiveSend(const EchoMessage &m) override {
    // m.get_message();
    LOG(ERROR) << "Received\n";
    current_message_ = prefix_ + m.get_message();
    // SendOutMessage(6665);
    //  LOG(ERROR) << "Sent\n";
  }

  void SendOneShotMessage(int other_port, const std::string &message_str) {
    EventBase base;
    auto socket(folly::AsyncSocket::newSocket(&base, "127.0.0.1", other_port));

    // Create a HeaderClientChannel object which is used in creating
    // client object
    auto client_channel = HeaderClientChannel::newChannel(std::move(socket));
    // Create a client object
    EchoAsyncClient client(std::move(client_channel));

    EchoMessage message;
    message.message_ref() = message_str;
    client.sync_ReceiveSend(message);
  }

  void SendOutMessage(int other_port) {
    EventBase base;
    auto socket(folly::AsyncSocket::newSocket(&base, "127.0.0.1", other_port));

    // Create a HeaderClientChannel object which is used in creating
    // client object
    auto client_channel = HeaderClientChannel::newChannel(std::move(socket));
    // Create a client object
    EchoAsyncClient client(std::move(client_channel));

    EchoMessage message;
    message.message_ref() = current_message_;
    client.sync_ReceiveSend(message);
  }

  std::string GetCurrentMessage() { return current_message_; }
};

}  // namespace

TEST(ThriftTransport, Echo) {
  // TODO(tyler and gabor) use thrift-generated echo, and thrift transport, to send, reply, and receive the response for
  // a thrift-defined message
  int argc = 0;
  char **argv;
  folly::init(&argc, &argv);

  auto ptr1 = std::make_shared<EchoSvc>();
  auto ptr2 = std::make_shared<EchoSvc>();

  auto server_thread2 = std::jthread([&ptr2] {
    ThriftServer *s = new ThriftServer();
    s->setInterface(ptr2);
    s->setPort(6666);
    s->serve();
  });

  auto server_thread1 = std::jthread([&ptr1] {
    ThriftServer *s = new ThriftServer();
    s->setInterface(ptr1);
    s->setPort(6665);
    s->serve();
  });

  // Wait some time...
  std::this_thread::sleep_for(4000ms);

  ptr1->SendOneShotMessage(6666, "original");
  // Wait some time...
  std::this_thread::sleep_for(4000ms);

  ptr2->SendOutMessage(6665);
  // Wait some time...
  std::this_thread::sleep_for(4000ms);

  auto result = ptr1->GetCurrentMessage();

  ASSERT_EQ(result, std::string("00original"));
}

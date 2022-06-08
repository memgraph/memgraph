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
#include <folly/io/SocketOptionMap.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/net/NetworkSocket.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "interface/gen-cpp2/PingPong.h"  // This is included from generated code

using namespace apache::thrift;
using namespace cpp2;

const std::string prefix{"Prefix"};

// The thrift has generated service interface with name CalculatorSvIf
// At server side we have to implement this interface
class PingPongSvc : public PingPongSvIf {
 public:
  virtual ~PingPongSvc() {}
  // We have to implement async_tm_add to implement the add function
  // of the Calculator service which we defined in calculator.thrift file
  void ping(Pong &res, const Ping &req) {
    LOG(ERROR) << "Received\n";
    res.message_ref() = prefix + req.get_message();
    LOG(ERROR) << "Sent\n";
  }
};

int main(int argc, char **argv) {
  folly::init(&argc, &argv);
  // To create a server, we need to first create server handler object.
  // The server handler object contains the implementation of the service.
  std::shared_ptr<PingPongSvc> ptr(new PingPongSvc());
  // Create a thrift server
  ThriftServer *s = new ThriftServer();

  // Set server handler object
  s->setInterface(ptr);
  // Set the server port
  s->setPort(6666);
  // Start the server to serve!!
  s->serve();

  return 0;
}
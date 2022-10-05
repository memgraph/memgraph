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
#include <limits>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "io/address.hpp"
#include "io/protobuf_transport/protobuf_transport.hpp"
#include "io/transport.hpp"
#include "protobuf/messages.pb.cc"
#include "protobuf/messages.pb.h"

namespace memgraph::io::tests {

using memgraph::io::protobuf_transport::ProtobufTransport;
using MgAddress = memgraph::io::Address;
using PbAddress = memgraph::protobuf::Address;
using memgraph::protobuf::TestRequest;
using memgraph::protobuf::UberMessage;

TEST(ProtobufTransport, Echo) {
  uint16_t cli_port = 6000;
  uint16_t srv_port = 7000;

  MgAddress cli_addr = MgAddress::TestAddress(cli_port);
  MgAddress srv_addr = MgAddress::TestAddress(srv_port);

  ProtobufTransport cli_pt{cli_port};
  ProtobufTransport srv_pt{srv_port};

  Io<ProtobufTransport> cli_io{cli_pt, cli_addr};
  Io<ProtobufTransport> srv_io{srv_pt, srv_addr};

  auto response_result_future = cli_io.Request<UberMessage, UberMessage>(srv_addr, UberMessage{});

  auto request_result = srv_io.Receive<UberMessage>();

  auto request_envelope = request_result.GetValue();

  UberMessage request = std::get<UberMessage>(request_envelope.message);

  // send it back as an echo
  srv_io.Send(request_envelope.from_address, request_envelope.request_id, request);

  // client receives it
  auto response_result = std::move(response_result_future).Wait();
  auto response_envelope = response_result.GetValue();
  UberMessage response = response_envelope.message;

  PbAddress to_addr;
  to_addr.set_last_known_port(1);

  PbAddress from_addr;
  to_addr.set_last_known_port(2);

  auto req = new TestRequest{};
  req->set_content("ping");

  UberMessage um;
  um.set_request_id(1);
  um.set_allocated_test_request(req);

  std::string out;

  bool success = um.SerializeToString(&out);

  MG_ASSERT(success);

  TestRequest rt;
  rt.ParseFromString(out);
}

}  // namespace memgraph::io::tests

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

#include "io/protobuf_transport/protobuf_transport.hpp"
#include "protobuf/echo_test.pb.cc"
#include "protobuf/echo_test.pb.h"

namespace memgraph::io::tests {

using memgraph::protobuf::TestRequest;
using memgraph::protobuf::TestResponse;

TEST(ProtobufTransport, Echo) {
  spdlog::error("ayo");

  std::string out;

  TestRequest req;
  req.set_request_id(1);
  req.set_content("ping");

  bool success = req.SerializeToString(&out);

  MG_ASSERT(success);

  TestRequest rt;
  rt.ParseFromString(out);

  MG_ASSERT(rt.content() == req.content());
}

}  // namespace memgraph::io::tests

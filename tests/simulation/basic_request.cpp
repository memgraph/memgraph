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

//#include <gtest/gtest.h>

#include <string>

#include "io/v3/simulator.hpp"
#include "io/v3/transport.hpp"
#include "utils/logging.hpp"

struct Request {
  std::string data;

  std::vector<uint8_t> Serialize() { return std::vector<uint8_t>(); }

  static Request Deserialize(uint8_t *ptr, size_t len) { return Request{}; }
};

struct Response {
  std::string data;

  std::vector<uint8_t> Serialize() { return std::vector<uint8_t>(); }

  static Response Deserialize(uint8_t *ptr, size_t len) { return Response{}; }
};

int main() {
  auto simulator = Simulator();
  auto addr_1 = Address::TestAddress(1);
  auto addr_2 = Address::TestAddress(2);

  auto sim_io_1 = simulator.Register(addr_1, true);
  auto sim_io_2 = simulator.Register(addr_2, true);

  // send request
  auto response_future = sim_io_1.RequestTimeout<Request, Response>(addr_2, Request{});

  // receive request
  RequestResult<Request> request_result = sim_io_2.ReceiveTimeout<Request>();
  auto req_envelope = request_result.GetValue();
  Request req = std::get<Request>(req_envelope.message);

  auto srv_res = Response{req.data};

  // send response
  sim_io_2.Send(req_envelope.from, req_envelope.request_id, srv_res);

  // receive response
  auto response_result = response_future.Wait();
  auto res = response_result.GetValue();

  return 0;
}

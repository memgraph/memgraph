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
#include <vector>

#include "io/v3/simulator.hpp"
#include "utils/terminate_handler.hpp"

struct RequestMsg {
  std::string data;

  std::vector<uint8_t> Serialize() { return std::vector<uint8_t>(); }

  static RequestMsg Deserialize(uint8_t *ptr, size_t len) { return RequestMsg{}; }
};

struct ResponseMsg {
  std::string data;

  std::vector<uint8_t> Serialize() { return std::vector<uint8_t>(); }

  static ResponseMsg Deserialize(uint8_t *ptr, size_t len) { return ResponseMsg{}; }
};

int main() {
  auto simulator = Simulator();
  auto cli_addr = Address::TestAddress(1);
  auto srv_addr = Address::TestAddress(2);

  Io<SimulatorTransport> cli_io = simulator.Register(cli_addr, true);
  Io<SimulatorTransport> srv_io = simulator.Register(srv_addr, true);

  // send request
  RequestMsg cli_req;
  ResponseFuture<ResponseMsg> response_future = cli_io.template Request<RequestMsg, ResponseMsg>(srv_addr, cli_req);

  // receive request
  RequestResult<RequestMsg> request_result = srv_io.template Receive<RequestMsg>();
  //  auto req_envelope = request_result.GetValue();
  //  Request req = std::get<Request>(req_envelope.message);
  //
  //  auto srv_res = ResponseMsg{req.data};
  //
  //  // send response
  //  sim_io_2.Send(req_envelope.from, req_envelope.request_id, srv_res);
  //
  //  // receive response
  //  auto response_result = response_future.Wait();
  //  auto res = response_result.GetValue();

  return 0;
}

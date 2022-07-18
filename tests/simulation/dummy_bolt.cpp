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

// TODO(gitbuda): Document thoroughly all BoltSession methods.

#include <map>
#include <string>
#include <vector>

// TODO(gitbuda): This should move to the utils/logging + refactor where needed.
#include <spdlog/sinks/daily_file_sink.h>

#include "communication/bolt/v1/session.hpp"
#include "communication/bolt/v1/value.hpp"
#include "communication/context.hpp"
#include "communication/v2/server.hpp"
#include "communication/v2/session.hpp"
#include "io/v3/simulator.hpp"
#include "utils/logging.hpp"

// TODO: BoltValue  --A--> TransportLayerValue(FBThriftValue) --B--> TypedValue
// TODO: TypedValue --C--> TransportLayerValue(FBThriftValue) --D--> BoltValue

struct InterpretRequest {
  std::string query;
  std::map<std::string, memgraph::communication::bolt::Value> params;
};

struct InterpretResponse {
  std::vector<std::string> header;
  int query_id;
};

struct PullRequest {
  int batch_size;
  int query_id;
};

struct PullResponse {
  std::vector<memgraph::communication::bolt::Value> batch_result;
  std::optional<std::map<std::string, memgraph::communication::bolt::Value>> summary;
};

template <typename TTransport>
struct SessionData {
  Io<TTransport> io;
  Address cypher_srv_addr;
};

template <typename TTransport>
class BoltSession final : public memgraph::communication::bolt::Session<memgraph::communication::v2::InputStream,
                                                                        memgraph::communication::v2::OutputStream> {
 public:
  BoltSession(SessionData<TTransport> *data, const memgraph::communication::v2::ServerEndpoint &endpoint,
              memgraph::communication::v2::InputStream *input_stream,
              memgraph::communication::v2::OutputStream *output_stream)
      : memgraph::communication::bolt::Session<memgraph::communication::v2::InputStream,
                                               memgraph::communication::v2::OutputStream>(input_stream, output_stream),
        session_data_(data),
        endpoint_(endpoint) {}

  using memgraph::communication::bolt::Session<memgraph::communication::v2::InputStream,
                                               memgraph::communication::v2::OutputStream>::TEncoder;

  void BeginTransaction() override { std::cout << "BEGIN TX HANDLER" << std::endl; }

  void CommitTransaction() override { std::cout << "COMMIT TX HANDLER" << std::endl; }

  void RollbackTransaction() override { std::cout << "ROLLBACK TX HANDLER" << std::endl; }

  std::pair<std::vector<std::string>, std::optional<int>> Interpret(
      const std::string &query, const std::map<std::string, memgraph::communication::bolt::Value> &params) override {
    std::cout << "INTERPRET QUERY HANDLER: " << query << std::endl;
    InterpretRequest cli_req;
    cli_req.query = query;
    cli_req.params = {};
    auto res_f = session_data_->io.template RequestWithTimeout<InterpretRequest, InterpretResponse>(
        session_data_->cypher_srv_addr, cli_req, 1000);
    auto res_rez = res_f.Wait();
    if (!res_rez.HasError()) {
      std::cout << "[CLIENT] Got a valid response" << std::endl;
      auto env = res_rez.GetValue();
      std::cout << "Query id: " << env.message.query_id << " interpreted with" << env.message.header.size()
                << " headers" << std::endl;
    } else {
      std::cout << "[CLIENT] Got an error" << std::endl;
    }
    return {{}, 0};
  }

  std::map<std::string, memgraph::communication::bolt::Value> Pull(TEncoder *encoder, std::optional<int> n,
                                                                   std::optional<int> qid) override {
    return {};
  }

  std::map<std::string, memgraph::communication::bolt::Value> Discard(std::optional<int> n,
                                                                      std::optional<int> qid) override {
    return {};
  }

  void Abort() override { std::cout << "ABORT HANDLE" << std::endl; }

  bool Authenticate(const std::string &username, const std::string &password) override {
    std::cout << "AUTH HANDLER" << std::endl;
    return true;
  }

  std::optional<std::string> GetServerNameForInit() override { return std::nullopt; }

  SessionData<TTransport> *session_data_;
  memgraph::communication::v2::ServerEndpoint endpoint_;
};

template <typename TTransport>
using ServerT = memgraph::communication::v2::Server<BoltSession<TTransport>, SessionData<TTransport>>;
using memgraph::communication::ServerContext;

template <typename TTransport>
void run_bolt_server(Io<TTransport> io, Address srv_addr) {
  auto server_endpoint = memgraph::communication::v2::ServerEndpoint{boost::asio::ip::address::from_string("0.0.0.0"),
                                                                     static_cast<uint16_t>(7687)};
  SessionData<TTransport> session_data{.io = std::move(io), .cypher_srv_addr = srv_addr};
  ServerContext context;
  ServerT<TTransport> server(server_endpoint, &session_data, &context, 1800, "Bolt", 4);

  MG_ASSERT(server.Start(), "Couldn't start the Bolt server!");
  server.AwaitShutdown();
}

template <typename TTransport>
void run_cypher_server(Io<TTransport> io) {
  int highest_query_id = 0;
  while (!io.ShouldShutDown()) {
    auto request_result = io.template ReceiveWithTimeout<InterpretRequest>(10000);
    if (request_result.HasError()) {
      std::cout << "[SERVER] Is receiving..." << std::endl;
      continue;
    }

    auto request_envelope = request_result.GetValue();
    auto req = std::get<InterpretRequest>(request_envelope.message);
    std::cout << "Interpreter got the following query: " << req.query << " with " << req.params.size() << " params"
              << std::endl;
    highest_query_id += 1;
    auto srv_res = InterpretResponse{{}, highest_query_id};
    request_envelope.Reply(srv_res, io);
  }
}

int main() {
  auto config = SimulatorConfig{
      .drop_percent = 0,
      .perform_timeouts = true,
      .scramble_messages = true,
      .rng_seed = 0,
  };
  auto simulator = Simulator(config);

  auto cli_addr = Address::TestAddress(1);
  auto srv_addr = Address::TestAddress(2);

  Io<SimulatorTransport> cli_io = simulator.Register(cli_addr);
  Io<SimulatorTransport> srv_io = simulator.Register(srv_addr);

  auto cli_thread = std::jthread(run_bolt_server<SimulatorTransport>, std::move(cli_io), srv_addr);
  simulator.IncrementServerCountAndWaitForQuiescentState(cli_addr);

  auto srv_thread = std::jthread(run_cypher_server<SimulatorTransport>, std::move(srv_io));
  simulator.IncrementServerCountAndWaitForQuiescentState(srv_addr);

  return 0;
}

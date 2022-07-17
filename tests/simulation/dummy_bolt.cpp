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

#include <map>

// TODO(gitbuda): This should move to the utils/logging + refactor where needed.
#include <spdlog/sinks/daily_file_sink.h>

#include "communication/bolt/v1/session.hpp"
#include "communication/bolt/v1/value.hpp"
#include "communication/context.hpp"
#include "communication/v2/server.hpp"
#include "communication/v2/session.hpp"
#include "utils/logging.hpp"

struct SessionData {};

class BoltSession final : public memgraph::communication::bolt::Session<memgraph::communication::v2::InputStream,
                                                                        memgraph::communication::v2::OutputStream> {
 public:
  BoltSession(SessionData *data, const memgraph::communication::v2::ServerEndpoint &endpoint,
              memgraph::communication::v2::InputStream *input_stream,
              memgraph::communication::v2::OutputStream *output_stream)
      : memgraph::communication::bolt::Session<memgraph::communication::v2::InputStream,
                                               memgraph::communication::v2::OutputStream>(input_stream, output_stream),
        endpoint_(endpoint) {}

  using memgraph::communication::bolt::Session<memgraph::communication::v2::InputStream,
                                               memgraph::communication::v2::OutputStream>::TEncoder;

  void BeginTransaction() override { std::cout << "BEGIN TX HANDLER" << std::endl; }

  void CommitTransaction() override { std::cout << "COMMIT TX HANDLER" << std::endl; }

  void RollbackTransaction() override { std::cout << "ROLLBACK TX HANDLER" << std::endl; }

  std::pair<std::vector<std::string>, std::optional<int>> Interpret(
      const std::string &query, const std::map<std::string, memgraph::communication::bolt::Value> &params) override {
    std::cout << "INTERPRET QUERY HANDLER: " << query << std::endl;
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

  memgraph::communication::v2::ServerEndpoint endpoint_;
};

using ServerT = memgraph::communication::v2::Server<BoltSession, SessionData>;
using memgraph::communication::ServerContext;

int main() {
  auto server_endpoint = memgraph::communication::v2::ServerEndpoint{boost::asio::ip::address::from_string("0.0.0.0"),
                                                                     static_cast<uint16_t>(7687)};
  SessionData session_data;
  ServerContext context;
  ServerT server(server_endpoint, &session_data, &context, 1800, "Bolt", 4);

  MG_ASSERT(server.Start(), "Couldn't start the Bolt server!");
  server.AwaitShutdown();

  return 0;
}

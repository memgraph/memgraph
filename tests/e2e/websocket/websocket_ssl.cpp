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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <fmt/core.h>
#include <gflags/gflags.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include "common.hpp"
#include "utils/logging.hpp"

DEFINE_uint64(bolt_port, 7687, "Bolt port");

class WebsocketSSLClient {
 public:
  WebsocketSSLClient() { session_ = {std::make_shared<Session<true>>(ioc_, ctx_, received_messages_)}; }

  explicit WebsocketSSLClient(Credentials creds) {
    session_ = std::make_shared<Session<true>>(creds, ioc_, ctx_, received_messages_);
  }

  void Connect(const std::string host, const std::string port) {
    session_->Run(host, port);
    bg_thread_ = std::jthread([this]() { ioc_.run(); });
  }

  void Close() { ioc_.stop(); }

  void AwaitClose() {
    MG_ASSERT(bg_thread_.joinable());
    bg_thread_.join();
  }

  std::vector<std::string> GetReceivedMessages() { return received_messages_; }

 private:
  std::vector<std::string> received_messages_;
  ssl::context ctx_{ssl::context::tlsv12_client};
  net::io_context ioc_;
  std::jthread bg_thread_;
  std::shared_ptr<Session<true>> session_;
};

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E websocket SSL!");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  MG_ASSERT(FLAGS_bolt_port != 0);
  logging::RedirectToStderr();

  auto mg_client = GetBoltClient(static_cast<uint16_t>(FLAGS_bolt_port), true);
  mg::Client::Init();

  TestWebsocketWithoutAnyUsers<WebsocketSSLClient>(mg_client);
  TestWebsocketWithAuthentication<WebsocketSSLClient>(mg_client);
  TestWebsocketWithoutBeingAuthorized<WebsocketSSLClient>(mg_client);

  return 0;
}

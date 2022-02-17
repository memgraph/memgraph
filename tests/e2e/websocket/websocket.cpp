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
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <spdlog/spdlog.h>
#include <unistd.h>
#include <mgclient.hpp>

#include "common.hpp"
#include "utils/logging.hpp"

DEFINE_uint64(bolt_port, 7687, "Bolt port");
DEFINE_uint64(monitoring_port, 7444, "Monitoring port");

class WebsocketClient {
 public:
  WebsocketClient() : session_{std::make_shared<Session<false>>(ioc_, received_messages_)} {}

  explicit WebsocketClient(Credentials creds)
      : session_{std::make_shared<Session<false>>(creds, ioc_, received_messages_)} {}

  void Connect(const std::string_view host, const std::string_view port) {
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
  net::io_context ioc_;
  std::jthread bg_thread_;
  std::shared_ptr<Session<false>> session_;
};

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E websocket!");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  MG_ASSERT(FLAGS_bolt_port != 0);
  MG_ASSERT(FLAGS_monitoring_port != 0);
  logging::RedirectToStderr();

  mg::Client::Init();
  auto mg_client = GetBoltClient(static_cast<uint16_t>(FLAGS_bolt_port), false);

  RunTestCases<WebsocketClient>(mg_client, std::to_string(FLAGS_monitoring_port));

  return 0;
}

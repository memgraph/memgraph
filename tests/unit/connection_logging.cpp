// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "communication/v2/session.hpp"
#include "utils/json_log_formatter.hpp"
#include "utils/structured_logging.hpp"

#include <gtest/gtest.h>
#include <spdlog/async_logger.h>
#include <spdlog/details/thread_pool.h>
#include <spdlog/sinks/ostream_sink.h>

#include <chrono>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

struct TestContext {
  bool fail_on_execute{false};

  template <typename F, typename P>
  void AddTask(F &&task, P priority) {
    std::forward<F>(task)(priority);
  }
};

class TestSession {
 public:
  TestSession(TestContext &context, memgraph::communication::v2::InputStream *,
              memgraph::communication::v2::OutputStream *)
      : context_(context) {}

  bool Execute() {
    if (context_.fail_on_execute) throw std::runtime_error("test processing failure");
    return false;
  }

  void HandleError() {}

  auto ApproximateQueryPriority() const { return memgraph::utils::Priority::HIGH; }

  std::string UUID() const { return "structured-connection-test"; }

 private:
  TestContext &context_;
};

class ConnectionLoggingTest : public ::testing::TestWithParam<bool> {
 protected:
  using tcp = boost::asio::ip::tcp;
  using Session = memgraph::communication::v2::Session<TestSession, TestContext>;

  void SetUp() override {
    previous_logger_ = spdlog::default_logger();
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(output_);
    sink->set_formatter(std::make_unique<memgraph::logging::JsonLogFormatter>());
    std::shared_ptr<spdlog::logger> base;
    if (GetParam()) {
      pool_ = std::make_shared<spdlog::details::thread_pool>(128, 1);
      base = std::make_shared<spdlog::async_logger>("memgraph_log", sink, pool_, spdlog::async_overflow_policy::block);
    } else {
      base = std::make_shared<spdlog::logger>("memgraph_log", sink);
    }
    base->set_level(spdlog::level::trace);
    base->flush_on(spdlog::level::trace);
    logger_ = memgraph::logging::EnableStructuredEvents(std::move(base));
    spdlog::set_default_logger(logger_);
  }

  void TearDown() override {
    boost::system::error_code error;
    client_.close(error);
    io_.restart();
    io_.run_for(std::chrono::seconds{1});
    session_.reset();
    FinishLogging();
  }

  void OpenConnection() {
    tcp::acceptor acceptor{io_, tcp::endpoint{boost::asio::ip::make_address("127.0.0.1"), 0}};
    client_.connect(acceptor.local_endpoint());
    client_port_ = client_.local_endpoint().port();
    session_ = Session::Create(acceptor.accept(), &context_, server_context_, "Bolt");
    ASSERT_TRUE(session_->Start());
  }

  void FinishLogging() {
    spdlog::set_default_logger(previous_logger_);
    logger_.reset();
    pool_.reset();
  }

  std::vector<nlohmann::json> Events() {
    FinishLogging();
    std::vector<nlohmann::json> events;
    std::istringstream input{output_.str()};
    for (std::string line; std::getline(input, line);) {
      auto record = nlohmann::json::parse(line);
      if (record.contains("event")) events.push_back(std::move(record));
    }
    return events;
  }

  boost::asio::io_context io_;
  tcp::socket client_{io_};
  memgraph::communication::ServerContext server_context_;
  TestContext context_;
  std::shared_ptr<Session> session_;
  uint16_t client_port_{0};
  std::ostringstream output_;
  std::shared_ptr<spdlog::logger> previous_logger_;
  std::shared_ptr<spdlog::logger> logger_;
  std::shared_ptr<spdlog::details::thread_pool> pool_;
};

TEST_P(ConnectionLoggingTest, CorrelatesAcceptedAndPeerClosedConnections) {
  OpenConnection();
  client_.close();
  io_.run_for(std::chrono::seconds{2});
  EXPECT_FALSE(session_->IsConnected());
  const auto events = Events();
  ASSERT_EQ(events.size(), 2);
  EXPECT_EQ(events[0].at("event").at("name"), "connection.accepted");
  EXPECT_EQ(events[1].at("event").at("name"), "connection.closed");
  for (const auto &event : events) {
    const auto &attributes = event.at("attributes");
    EXPECT_EQ(attributes.at("session.id"), "structured-connection-test");
    EXPECT_EQ(attributes.at("network.protocol.name"), "Bolt");
    EXPECT_EQ(attributes.at("client.address"), "127.0.0.1");
    EXPECT_EQ(attributes.at("client.port"), client_port_);
  }
}

TEST_P(ConnectionLoggingTest, RecordsServerTerminationOnce) {
  OpenConnection();
  session_->RequestTermination();
  io_.run_for(std::chrono::seconds{2});
  EXPECT_FALSE(session_->IsConnected());
  const auto events = Events();
  ASSERT_EQ(events.size(), 2);
  EXPECT_EQ(events[1].at("event").at("name"), "connection.closed");
}

TEST_P(ConnectionLoggingTest, RecordsProcessingErrorsWithoutChangingExceptionMessages) {
  context_.fail_on_execute = true;
  OpenConnection();
  boost::asio::write(client_, boost::asio::buffer("ping", 4));
  io_.run_for(std::chrono::seconds{2});
  EXPECT_FALSE(session_->IsConnected());
  const auto events = Events();
  ASSERT_EQ(events.size(), 3);
  EXPECT_EQ(events[1].at("event").at("name"), "connection.error");
  EXPECT_EQ(events[1].at("attributes").at("error.type"), "session_processing_error");
  EXPECT_EQ(
      events[1].at("message"),
      "Exception was thrown while processing event in Bolt session associated with " +
          fmt::format("{}", std::optional{tcp::endpoint{boost::asio::ip::make_address("127.0.0.1"), client_port_}}));
  EXPECT_EQ(events[2].at("event").at("name"), "connection.closed");
}

INSTANTIATE_TEST_SUITE_P(LoggerTypes, ConnectionLoggingTest, ::testing::Values(false, true));

}  // namespace

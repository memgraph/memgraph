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

#include "utils/structured_logging.hpp"
#include "utils/json_log_formatter.hpp"
#include "utils/session_context.hpp"

#include <gtest/gtest.h>
#include <spdlog/async_logger.h>
#include <spdlog/details/os.h>
#include <spdlog/details/thread_pool.h>
#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>

#include <limits>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

class StructuredEventTest : public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    previous_logger_ = spdlog::default_logger();
    auto json_sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(json_output_);
    json_sink->set_formatter(std::make_unique<memgraph::logging::JsonLogFormatter>());
    auto text_sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(text_output_);
    text_sink->set_pattern("%v");
    const std::vector<spdlog::sink_ptr> sinks{json_sink, text_sink};
    std::shared_ptr<spdlog::logger> base;
    if (GetParam()) {
      pool_ = std::make_shared<spdlog::details::thread_pool>(1024, 1);
      base = std::make_shared<spdlog::async_logger>(
          "memgraph_log", sinks.begin(), sinks.end(), pool_, spdlog::async_overflow_policy::block);
    } else {
      base = std::make_shared<spdlog::logger>("memgraph_log", sinks.begin(), sinks.end());
    }
    base->set_level(spdlog::level::trace);
    base->flush_on(spdlog::level::trace);
    logger_ = memgraph::logging::EnableStructuredEvents(std::move(base));
    spdlog::set_default_logger(logger_);
  }

  void TearDown() override { Finish(); }

  void Finish() {
    spdlog::set_default_logger(previous_logger_);
    logger_.reset();
    pool_.reset();
  }

  std::vector<nlohmann::json> Records() {
    Finish();
    std::vector<nlohmann::json> records;
    std::istringstream input{json_output_.str()};
    for (std::string line; std::getline(input, line);) records.push_back(nlohmann::json::parse(line));
    return records;
  }

  std::shared_ptr<spdlog::logger> previous_logger_;
  std::shared_ptr<spdlog::logger> logger_;
  std::shared_ptr<spdlog::details::thread_pool> pool_;
  std::ostringstream json_output_;
  std::ostringstream text_output_;
};

TEST_P(StructuredEventTest, PreservesTypedAttributesAndLegacyMessage) {
  const auto thread_id = spdlog::details::os::thread_id();
  const std::string message = "original \"message\"\nsecond line\\path";
  const nlohmann::json attributes{{"session.id", "session-1"},
                                  {"transaction.id", std::numeric_limits<uint64_t>::max()},
                                  {"duration_ms", 123},
                                  {"sampled", false}};
  memgraph::logging::LogEvent(spdlog::level::warn, "query.slow", message, attributes);
  const auto records = Records();
  ASSERT_EQ(records.size(), 1);
  EXPECT_EQ(records[0].at("event").at("name"), "query.slow");
  EXPECT_EQ(records[0].at("attributes"), attributes);
  EXPECT_EQ(records[0].at("message"), message);
  EXPECT_EQ(records[0].at("logger"), "memgraph_log");
  EXPECT_EQ(records[0].at("thread_id"), thread_id);
  EXPECT_EQ(text_output_.str(), message + "\n");
}

TEST_P(StructuredEventTest, KeepsOriginalLoggerIdentityAndAsyncType) {
  EXPECT_EQ(memgraph::logging::EnableStructuredEvents(logger_), logger_);
  EXPECT_EQ(std::dynamic_pointer_cast<spdlog::async_logger>(logger_) != nullptr, GetParam());
}

TEST_P(StructuredEventTest, SnapshotsAttributesAndDoesNotContaminateOrdinaryLogs) {
  nlohmann::json attributes{{"session.id", "first"}};
  memgraph::logging::LogEvent(spdlog::level::info, "query.failed", "first", attributes);
  attributes["session.id"] = "second";
  spdlog::info("ordinary");
  memgraph::logging::LogEvent(spdlog::level::info, "query.failed", "second", attributes);
  const auto records = Records();
  ASSERT_EQ(records.size(), 3);
  EXPECT_EQ(records[0].at("attributes").at("session.id"), "first");
  EXPECT_FALSE(records[1].contains("event"));
  EXPECT_FALSE(records[1].contains("attributes"));
  EXPECT_EQ(records[2].at("attributes").at("session.id"), "second");
}

TEST_P(StructuredEventTest, FollowsRuntimeLevelChangesOnTheMainLogger) {
  logger_->set_level(spdlog::level::err);
  memgraph::logging::LogEvent(spdlog::level::info, "query.slow", "filtered", nlohmann::json::object());
  logger_->set_level(spdlog::level::trace);
  memgraph::logging::LogEvent(spdlog::level::debug, "query.slow", "visible", nlohmann::json::object());
  const auto records = Records();
  ASSERT_EQ(records.size(), 1);
  EXPECT_EQ(records[0].at("message"), "visible");
}

TEST_P(StructuredEventTest, PreservesInvalidUtf8OnLegacySinks) {
  std::string message = "invalid \xff";
  message.push_back('\0');
  memgraph::logging::LogEvent(spdlog::level::warn, "query.failed", message, {{"db.query.text", message}});
  const auto records = Records();
  ASSERT_EQ(records.size(), 1);
  std::string expected = "invalid \xef\xbf\xbd";
  expected.push_back('\0');
  EXPECT_EQ(records[0].at("message"), expected);
  EXPECT_EQ(records[0].at("attributes").at("db.query.text"), expected);
  EXPECT_EQ(text_output_.str(), message + "\n");
}

TEST_P(StructuredEventTest, RejectsInvalidEventShapes) {
  EXPECT_THROW(memgraph::logging::LogEvent(spdlog::level::info, "", "message", nlohmann::json::object()),
               std::invalid_argument);
  EXPECT_THROW(memgraph::logging::LogEvent(spdlog::level::info, "event", "message", nlohmann::json::array()),
               std::invalid_argument);
  EXPECT_TRUE(Records().empty());
}

TEST_P(StructuredEventTest, KeepsConcurrentSessionAttributesIsolated) {
  std::vector<std::thread> producers;
  for (int i = 0; i < 16; ++i) {
    producers.emplace_back([i] {
      memgraph::logging::LogEvent(
          spdlog::level::info, "query.failed", std::to_string(i), {{"session.id", std::to_string(i)}});
      spdlog::info("ordinary {}", i);
    });
  }
  for (auto &producer : producers) producer.join();
  const auto records = Records();
  ASSERT_EQ(records.size(), 32);
  size_t event_count = 0;
  for (const auto &record : records) {
    if (record.contains("event")) {
      ++event_count;
      EXPECT_EQ(record.at("message"), record.at("attributes").at("session.id"));
    } else {
      EXPECT_FALSE(record.contains("attributes"));
    }
  }
  EXPECT_EQ(event_count, 16);
}

TEST_P(StructuredEventTest, SlowQueryKeepsCapturedTransactionAfterCommit) {
  memgraph::logging::SessionLogContext session;
  session.SetSessionUuid("session-1");
  session.SetTxId(77);
  const auto context = session.QueryContext(2);
  session.ClearTxId();
  memgraph::logging::EmitSlowQueryLog("alice", "memgraph", "RETURN 1", 42, "Once\nProduce", context);
  const auto records = Records();
  ASSERT_EQ(records.size(), 1);
  EXPECT_EQ(records[0].at("event").at("name"), "query.slow");
  const auto &attributes = records[0].at("attributes");
  EXPECT_EQ(attributes.at("session.id"), "session-1");
  EXPECT_EQ(attributes.at("transaction.id"), 77);
  EXPECT_EQ(attributes.at("query.id"), 2);
  EXPECT_EQ(attributes.at("duration_ms"), 42);
  EXPECT_EQ(attributes.at("query.plan"), "Once\nProduce");
  EXPECT_EQ(attributes.at("outcome"), "success");
  EXPECT_FALSE(session.QueryContext().transaction_id);
}

TEST_P(StructuredEventTest, FailedQueryHasTypedContextAndNoInventedIdentifiers) {
  memgraph::logging::EmitFailedQueryLog("alice",
                                        "memgraph",
                                        "invalid query",
                                        "bad syntax",
                                        {.session_id = "session-1", .phase = "parse", .error_type = "SyntaxException"});
  const auto records = Records();
  ASSERT_EQ(records.size(), 1);
  EXPECT_EQ(records[0].at("event").at("name"), "query.failed");
  const auto &attributes = records[0].at("attributes");
  EXPECT_EQ(attributes.at("user.name"), "alice");
  EXPECT_EQ(attributes.at("db.namespace"), "memgraph");
  EXPECT_EQ(attributes.at("db.query.text"), "invalid query");
  EXPECT_EQ(attributes.at("error.type"), "SyntaxException");
  EXPECT_EQ(attributes.at("phase"), "parse");
  EXPECT_FALSE(attributes.contains("transaction.id"));
  EXPECT_FALSE(attributes.contains("query.id"));
}

TEST_P(StructuredEventTest, AuthenticationFailureContainsOnlySafeCorrelationAndReason) {
  memgraph::logging::EmitAuthenticationFailure("session-1", "unauthenticated");
  const auto records = Records();
  ASSERT_EQ(records.size(), 1);
  EXPECT_EQ(records[0].at("event").at("name"), "authentication.failed");
  EXPECT_EQ(records[0].at("attributes"),
            (nlohmann::json{{"session.id", "session-1"}, {"outcome", "failure"}, {"reason", "unauthenticated"}}));
}

INSTANTIATE_TEST_SUITE_P(LoggerTypes, StructuredEventTest, ::testing::Values(false, true));

TEST(StructuredLogging, PlainLoggerKeepsOriginalMessageAndSilentAuthRejections) {
  const auto previous = spdlog::default_logger();
  std::ostringstream stream;
  auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(stream);
  sink->set_pattern("%v");
  auto logger = std::make_shared<spdlog::logger>("plain", sink);
  spdlog::set_default_logger(logger);
  memgraph::logging::LogEvent(spdlog::level::warn, "query.failed", "original", nlohmann::json::object());
  memgraph::logging::EmitAuthenticationFailure("session-1", "unauthenticated");
  spdlog::set_default_logger(previous);
  EXPECT_EQ(stream.str(), "original\n");
}

TEST(StructuredLogging, RejectsMissingLogger) {
  EXPECT_THROW(memgraph::logging::EnableStructuredEvents(nullptr), std::invalid_argument);
}

}  // namespace

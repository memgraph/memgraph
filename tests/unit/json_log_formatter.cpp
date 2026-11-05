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

#include "utils/json_log_formatter.hpp"

#include <gtest/gtest.h>
#include <spdlog/async_logger.h>
#include <spdlog/details/os.h>
#include <spdlog/details/thread_pool.h>
#include <spdlog/sinks/ostream_sink.h>

#include <algorithm>
#include <chrono>
#include <sstream>
#include <string>
#include <utility>

namespace {

spdlog::details::log_msg MakeMessage(std::string_view message) {
  spdlog::details::log_msg msg{"memgraph_log", spdlog::level::info, {message.data(), message.size()}};
  msg.time = std::chrono::sys_days{std::chrono::year{2026} / 10 / 30} + std::chrono::hours{12} +
             std::chrono::minutes{34} + std::chrono::seconds{56} + std::chrono::microseconds{789123};
  msg.thread_id = 42;
  return msg;
}

std::string Format(spdlog::formatter &formatter, const spdlog::details::log_msg &msg) {
  spdlog::memory_buf_t buffer;
  formatter.format(msg, buffer);
  return {buffer.data(), buffer.size()};
}

TEST(JsonLogFormatter, EmitsDocumentedFields) {
  memgraph::logging::JsonLogFormatter formatter;
  const auto output = Format(formatter, MakeMessage("Session closed by peer"));
  const auto record = nlohmann::json::parse(output);
  EXPECT_EQ(record,
            (nlohmann::json{{"timestamp", "2026-10-30T12:34:56.789Z"},
                            {"level", "info"},
                            {"logger", "memgraph_log"},
                            {"message", "Session closed by peer"},
                            {"thread_id", 42}}));
  EXPECT_EQ(output.back(), '\n');
  EXPECT_EQ(std::count(output.begin(), output.end(), '\n'), 1);
}

TEST(JsonLogFormatter, EscapesPayloadWithoutAddingPhysicalLines) {
  memgraph::logging::JsonLogFormatter formatter;
  std::string message = "quotes \" backslash \\ newline\n carriage\r tab\t";
  message.push_back('\0');
  message.push_back('\x1b');
  message += " UTF-8 \xe2\x98\x83";
  const auto output = Format(formatter, MakeMessage(message));
  EXPECT_EQ(nlohmann::json::parse(output).at("message"), message);
  EXPECT_EQ(std::count(output.begin(), output.end(), '\n'), 1);
  EXPECT_EQ(output.find('\x1b'), std::string::npos);
}

TEST(JsonLogFormatter, ReplacesInvalidUtf8) {
  memgraph::logging::JsonLogFormatter formatter;
  auto msg = MakeMessage("invalid \xff");
  msg.logger_name = "logger\xff";
  const auto record = nlohmann::json::parse(Format(formatter, msg));
  EXPECT_EQ(record.at("message"), "invalid \xef\xbf\xbd");
  EXPECT_EQ(record.at("logger"), "logger\xef\xbf\xbd");
}

TEST(JsonLogFormatter, HandlesEmptyMessage) {
  memgraph::logging::JsonLogFormatter formatter;
  EXPECT_EQ(nlohmann::json::parse(Format(formatter, MakeMessage(""))).at("message"), "");
}

TEST(JsonLogFormatter, PreservesSeverityNames) {
  memgraph::logging::JsonLogFormatter formatter;
  for (const auto &[level, name] : {std::pair{spdlog::level::trace, "trace"},
                                    std::pair{spdlog::level::debug, "debug"},
                                    std::pair{spdlog::level::info, "info"},
                                    std::pair{spdlog::level::warn, "warning"},
                                    std::pair{spdlog::level::err, "error"},
                                    std::pair{spdlog::level::critical, "critical"}}) {
    auto msg = MakeMessage("message");
    msg.level = level;
    EXPECT_EQ(nlohmann::json::parse(Format(formatter, msg)).at("level"), name);
  }
}

TEST(JsonLogFormatter, FormatsTimeBeforeEpochInUtc) {
  memgraph::logging::JsonLogFormatter formatter;
  auto msg = MakeMessage("message");
  msg.time = spdlog::log_clock::time_point{} - std::chrono::milliseconds{1};
  EXPECT_EQ(nlohmann::json::parse(Format(formatter, msg)).at("timestamp"), "1969-12-31T23:59:59.999Z");
}

TEST(JsonLogFormatter, ClonePreservesOutput) {
  memgraph::logging::JsonLogFormatter formatter;
  auto clone = formatter.clone();
  const auto msg = MakeMessage("message");
  EXPECT_EQ(Format(formatter, msg), Format(*clone, msg));
}

TEST(JsonLogFormatter, AsyncLoggingPreservesProducerThreadAndPayload) {
  std::ostringstream stream;
  const auto producer_thread = spdlog::details::os::thread_id();
  const std::string message = "async\nmessage";
  {
    auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(stream);
    sink->set_formatter(std::make_unique<memgraph::logging::JsonLogFormatter>());
    auto pool = std::make_shared<spdlog::details::thread_pool>(16, 1);
    auto logger = std::make_shared<spdlog::async_logger>("async_log", sink, pool, spdlog::async_overflow_policy::block);
    logger->info("{}", message);
    logger->flush();
  }
  const auto record = nlohmann::json::parse(stream.str());
  EXPECT_EQ(record.at("message"), message);
  EXPECT_EQ(record.at("logger"), "async_log");
  EXPECT_EQ(record.at("thread_id"), producer_thread);
}

}  // namespace

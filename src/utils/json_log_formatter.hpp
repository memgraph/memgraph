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

#pragma once

#include <chrono>
#include <memory>
#include <string_view>

#include <fmt/chrono.h>
#include <spdlog/formatter.h>
#include <nlohmann/json.hpp>

#include "utils/structured_logging.hpp"

namespace memgraph::logging {

class JsonLogFormatter final : public spdlog::formatter {
 public:
  void format(const spdlog::details::log_msg &msg, spdlog::memory_buf_t &dest) override {
    const auto seconds = std::chrono::floor<std::chrono::seconds>(msg.time);
    const auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(msg.time - seconds).count();
    const auto level = spdlog::level::to_string_view(msg.level);
    nlohmann::json record{
        {"timestamp",
         fmt::format("{:%Y-%m-%dT%H:%M:%S}.{:03}Z", fmt::gmtime(spdlog::log_clock::to_time_t(seconds)), milliseconds)},
        {"level", std::string_view{level.data(), level.size()}},
        {"logger", std::string_view{msg.logger_name.data(), msg.logger_name.size()}},
        {"message", std::string_view{msg.payload.data(), msg.payload.size()}},
        {"thread_id", msg.thread_id}};
    AddStructuredEventFields(msg, record);
    // Log payloads can contain arbitrary bytes; preserve a valid JSON record even for invalid UTF-8.
    const auto serialized = record.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
    dest.append(serialized.data(), serialized.data() + serialized.size());
    dest.push_back('\n');
  }

  std::unique_ptr<spdlog::formatter> clone() const override { return std::make_unique<JsonLogFormatter>(); }
};

}  // namespace memgraph::logging

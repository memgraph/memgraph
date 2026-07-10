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

#include <gtest/gtest.h>

#include <spdlog/spdlog.h>
#include <utils/logging.hpp>

namespace {

constexpr std::string_view kLogLevelArg = "--log-level=";
constexpr std::string_view kEnvLogLevel = "SPDLOG_LEVEL";

constexpr std::pair<std::string_view, spdlog::level::level_enum> kLogLevels[] = {
    {"trace", spdlog::level::trace},
    {"debug", spdlog::level::debug},
    {"info", spdlog::level::info},
    {"warn", spdlog::level::warn},
    {"err", spdlog::level::err},
    {"off", spdlog::level::off},
};

constexpr spdlog::level::level_enum kDefaultLogLevel = spdlog::level::trace;

spdlog::level::level_enum ParseLevel(std::string_view level_str) {
  for (auto [name, level] : kLogLevels) {
    if (level_str == name) return level;
  }
  return spdlog::level::off;
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  spdlog::level::level_enum log_level = kDefaultLogLevel;

  for (int i = 1; i < argc; ++i) {
    std::string_view arg{argv[i]};
    if (arg.starts_with(kLogLevelArg)) {
      std::string_view level_str = arg.substr(kLogLevelArg.size());
      log_level = ParseLevel(level_str);
    }
  }

  char *env_log = std::getenv(kEnvLogLevel.data());
  if (env_log) {
    log_level = ParseLevel(std::string_view(env_log));
  }

  memgraph::logging::RedirectToStderr();
  spdlog::set_level(log_level);
  return RUN_ALL_TESTS();
}

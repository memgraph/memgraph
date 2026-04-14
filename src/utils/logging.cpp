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

#include "utils/logging.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>

#include <regex>

namespace {
constexpr std::string_view kRegexFmt = "$1****$3";
}  // namespace

std::string memgraph::logging::MaskSensitiveInformation(std::string_view const input) {
  static const std::regex re_all(
      R"((password\s*:\s*['"]|pas+word\s+to\s*['"]|re?pl?ac?e?\s*['"]|identified\s+by\s*['"]|pas+word\s+for\s+\w+\s+to\s*['"]|['"]?aws_access_key['"]?\s*[:=]\s*['"]|['"]?aws_secret_key['"]?\s*[:=]\s*['"]|['"]?aws[._-]?access[._-]?key['"]?\s+to\s*['"]|['"]?aws[._-]?secret[._-]?key['"]?\s+to\s*['"])([^'"]*)(['"]))",
      std::regex_constants::icase | std::regex_constants::optimize);

  auto str = std::string{input};
  return std::regex_replace(str, re_all, std::string{kRegexFmt});
}

// It is possible if using asynchronous logger that this log line won't be seen because there is no way force flush
// messages when using asynchronous queue except calling spdlog::shutdown. Calling spdlog::shutdown() is not necessary
// for synchronous logger but it is the only way to flush messages when using async logger. The reason why we don't use
// spdlog::shudown is because there is then a time window between the invocation of spdlog::shutdown and std::abort
// which means that the program could segfault at any logging place in the codebase. In the core dump, it would
// therefore be hard to see the proper reason of the core dump
void memgraph::logging::AssertFailed(std::source_location const loc, char const *expr, std::string const &message) {
  spdlog::critical(
      "\nAssertion failed in file {} at line {}."
      "\n\tExpression: '{}'"
      "{}",
      loc.file_name(),
      loc.line(),
      expr,
      !message.empty() ? fmt::format("\n\tMessage: '{}'", message) : "");
  std::terminate();
}

void memgraph::logging::RedirectToStderr() { spdlog::set_default_logger(spdlog::stderr_color_mt("stderr")); }

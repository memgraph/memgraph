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

#include <fmt/format.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <ctre.hpp>

#include <iostream>

namespace {
constexpr std::string_view kRedaction = "****";

// Matches only as far as the quote that opens the value. Every alternative
// ends with that quote, so the last character of a match is the one that has
// to close the value.
//
// The value itself is deliberately left unmatched: which character ends it
// depends on the quote that opened it, and a regular expression cannot carry
// that without a backreference.
constexpr auto kCredentialClause = ctre::search<
    R"(password\s*:\s*['"]|pas+word\s+to\s*['"]|re?pl?ac?e?\s*['"]|identified\s+by\s*['"]|pas+word\s+for\s+\w+\s+to\s*['"]|['"]?aws_access_key['"]?\s*[:=]\s*['"]|['"]?aws_secret_key['"]?\s*[:=]\s*['"]|['"]?aws[._\-]?access[._\-]?key['"]?\s+to\s*['"]|['"]?aws[._\-]?secret[._\-]?key['"]?\s+to\s*['"])",
    ctre::case_insensitive>;

// Offset of the quote closing a value that `quote` opened, or npos if it is
// never closed. Only the same quote closes the value, and a backslash escapes
// the character after it.
size_t EndOfValue(std::string_view const value, char const quote) {
  for (size_t i = 0; i < value.size(); ++i) {
    if (value[i] == '\\') {
      ++i;
      continue;
    }
    if (value[i] == quote) return i;
  }
  return std::string_view::npos;
}
}  // namespace

std::optional<std::string> memgraph::logging::MaskSensitiveInformation(std::string_view const input) {
  auto rest = input;
  auto clause = kCredentialClause(rest);
  if (!clause) return std::nullopt;

  std::string masked;
  // A value can be shorter than the redaction replacing it.
  masked.reserve(input.size() + kRedaction.size());

  for (; clause; clause = kCredentialClause(rest)) {
    auto const matched = clause.to_view();
    auto const quote = matched.back();
    auto const value_at = static_cast<size_t>(matched.data() - rest.data()) + matched.size();

    masked.append(rest.substr(0, value_at));
    masked.append(kRedaction);

    auto const value = rest.substr(value_at);
    auto const closing = EndOfValue(value, quote);
    if (closing == std::string_view::npos) return masked;

    masked.push_back(quote);
    rest = value.substr(closing + 1);
  }

  masked.append(rest);
  return masked;
}

// It is possible if using asynchronous logger that this log line won't be seen because there is no way force flush
// messages when using asynchronous queue except calling spdlog::shutdown. That's why we also log err msg on std::cerr.
// Calling spdlog::shutdown() is not necessary for synchronous logger but it is the only way to flush messages when
// using async logger. The reason why we don't use spdlog::shutdown is because there is then a time window between the
// invocation of spdlog::shutdown and std::abort which means that the program could segfault at any logging place in the
// codebase. In the core dump, it would therefore be hard to see the proper reason of the core dump
void memgraph::logging::AssertFailed(std::source_location const loc, char const *expr, std::string const &message) {
  auto const msg = fmt::format(
      "\nAssertion failed in file {} at line {}."
      "\n\tExpression: '{}'"
      "{}",
      loc.file_name(),
      loc.line(),
      expr,
      !message.empty() ? fmt::format("\n\tMessage: '{}'", message) : "");
  spdlog::critical("{}", msg);
  if (std::dynamic_pointer_cast<spdlog::async_logger>(spdlog::default_logger())) {
    std::cerr << msg << '\n';
  }
  std::terminate();
}

void memgraph::logging::RedirectToStderr() { spdlog::set_default_logger(spdlog::stderr_color_mt("stderr")); }

// Copyright 2024 Memgraph Ltd.
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

#undef SPDLOG_ACTIVE_LEVEL
#ifndef NDEBUG
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#else
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_OFF
#endif
#include <array>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <regex>
#include <string>

#include <fmt/format.h>
// NOTE: fmt 9+ introduced fmt/std.h, it's important because of, e.g., std::path formatting. toolchain-v4 has fmt 8,
// the guard is here because of fmt 8 compatibility.
#if FMT_VERSION > 90000
#include <fmt/std.h>
#endif
#include <spdlog/fmt/ostr.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <boost/preprocessor/comparison/equal.hpp>
#include <boost/preprocessor/control/if.hpp>
#include <boost/preprocessor/variadic/size.hpp>

namespace memgraph::logging {

// TODO (antonio2368): Replace with std::source_location when it's supported by
// compilers
[[noreturn]] inline void AssertFailed(const char *file_name, int line_num, const char *expr,
                                      const std::string &message) {
  spdlog::critical(
      "\nAssertion failed in file {} at line {}."
      "\n\tExpression: '{}'"
      "{}",
      file_name, line_num, expr, !message.empty() ? fmt::format("\n\tMessage: '{}'", message) : "");
  std::terminate();
}

#define GET_MESSAGE(...) \
  BOOST_PP_IF(BOOST_PP_EQUAL(BOOST_PP_VARIADIC_SIZE(__VA_ARGS__), 0), "", fmt::format(__VA_ARGS__))

#define MG_ASSERT(expr, ...)                                                                  \
  do {                                                                                        \
    if (expr) [[likely]] {                                                                    \
      (void)0;                                                                                \
    } else {                                                                                  \
      ::memgraph::logging::AssertFailed(__FILE__, __LINE__, #expr, GET_MESSAGE(__VA_ARGS__)); \
    }                                                                                         \
  } while (false)

#ifndef NDEBUG
#define DMG_ASSERT(expr, ...) MG_ASSERT(expr, __VA_ARGS__)
#else
#define DMG_ASSERT(...) \
  do {                  \
  } while (false)
#endif

template <typename... Args>
void Fatal(const char *msg, const Args &...msg_args) {
  spdlog::critical(msg, msg_args...);
  std::terminate();
}

#define LOG_FATAL(...)             \
  do {                             \
    spdlog::critical(__VA_ARGS__); \
    std::terminate();              \
  } while (0)

#ifndef NDEBUG
#define DLOG_FATAL(...) LOG_FATAL(__VA_ARGS__)
#else
#define DLOG_FATAL(...) \
  do {                  \
  } while (false)
#endif

inline void RedirectToStderr() { spdlog::set_default_logger(spdlog::stderr_color_mt("stderr")); }

// /// Use it for operations that must successfully finish.
inline void AssertRocksDBStatus(const auto &status) { MG_ASSERT(status.ok(), "rocksdb: {}", status.ToString()); }

inline bool CheckRocksDBStatus(const auto &status) {
  if (!status.ok()) [[unlikely]] {
    spdlog::error("rocksdb: {}", status.ToString());
  }
  return status.ok();
}

inline std::string MaskSensitiveInformation(const std::string &input) {
  // Regex patterns for sensitive information and node properties
  std::regex nodePattern(R"(\(\w+:\w+\s*\{[^}]*\})");
  std::regex sensitivePattern(
      R"((password\s*:\s*'[^']*')|([Pp][Aa][Ss]*[Ss]*[Ww]*[Oo]*[Rr]*[Dd]*\s+[Tt][Oo]\s*'[^']*')|([Rr][Ee]?[Pp][Ll]?[Aa]?[Cc]?[Ee]?\s*'[^']*')|([Ii][Dd]?[Ee]?[Nn]?[Tt]?[Ii]?[Ff]?[Ii]?[Ee]?[Dd]*\s+[Bb][Yy]\s*'[^']*')|([Pp][Aa]*[Ss]*[Ss]*[Ww]*[Oo]*[Rr]*[Dd]*\s+[Ff][Oo][Rr]\s+\w+\s+[Tt][Oo]\s*'[^']*'))",
      std::regex_constants::icase);

  std::string result;
  size_t last_pos = 0;
  size_t current_pos = 0;

  // Process the string by replacing sensitive information first and handling nodes separately
  while (current_pos < input.size()) {
    std::smatch node_match;
    bool found = std::regex_search(input.cbegin() + current_pos, input.cend(), node_match, nodePattern);

    if (found) {
      size_t node_start = node_match.position() + current_pos;
      size_t node_end = node_start + node_match.length();

      // Process the part before the node match
      std::string non_node_part = input.substr(last_pos, node_start - last_pos);
      std::string masked_non_node_part;
      std::sregex_iterator it(non_node_part.begin(), non_node_part.end(), sensitivePattern);
      std::sregex_iterator end;

      size_t prev_end = 0;
      for (; it != end; ++it) {
        masked_non_node_part.append(non_node_part, prev_end, it->position() - prev_end);
        std::string replacement = it->str();
        size_t startPos = replacement.find("'");
        if (startPos != std::string::npos) {
          size_t endPos = replacement.find("'", startPos + 1);
          if (endPos != std::string::npos) {
            replacement.replace(startPos + 1, endPos - startPos - 1, "****");
          }
        }
        masked_non_node_part.append(replacement);
        prev_end = it->position() + it->length();
      }
      masked_non_node_part.append(non_node_part, prev_end, non_node_part.length() - prev_end);

      // Append the masked non-node part and the node match
      result.append(masked_non_node_part);
      result.append(node_match.str());

      // Update positions
      last_pos = node_end;
      current_pos = last_pos;
    } else {
      break;
    }
  }

  // Append and mask any remaining text after the last node match
  std::string remaining_part = input.substr(last_pos, input.size() - last_pos);
  std::string masked_remaining_part;
  std::sregex_iterator it(remaining_part.begin(), remaining_part.end(), sensitivePattern);
  std::sregex_iterator end;

  size_t prev_end = 0;
  for (; it != end; ++it) {
    masked_remaining_part.append(remaining_part, prev_end, it->position() - prev_end);
    std::string replacement = it->str();
    size_t startPos = replacement.find("'");
    if (startPos != std::string::npos) {
      size_t endPos = replacement.find("'", startPos + 1);
      if (endPos != std::string::npos) {
        replacement.replace(startPos + 1, endPos - startPos - 1, "****");
      }
    }
    masked_remaining_part.append(replacement);
    prev_end = it->position() + it->length();
  }
  masked_remaining_part.append(remaining_part, prev_end, remaining_part.length() - prev_end);

  result.append(masked_remaining_part);

  return result;
}
}  // namespace memgraph::logging

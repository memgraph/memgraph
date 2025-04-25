// Copyright 2025 Memgraph Ltd.
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

std::string memgraph::logging::MaskSensitiveInformation(std::string_view input) {
  static std::regex const nodePattern(R"(\(\w+:\w+\s*\{[^}]*\})");
  static std::regex const sensitivePattern(
      R"((password\s*:\s*['"][^'"]*['"])|([Pp][Aa][Ss]*[Ss]*[Ww]*[Oo]*[Rr]*[Dd]*\s+[Tt][Oo]\s*['"][^'"]*['"])|([Rr][Ee]?[Pp][Ll]?[Aa]?[Cc]?[Ee]?\s*['"][^'"]*['"])|([Ii][Dd]?[Ee]?[Nn]?[Tt]?[Ii]?[Ff]?[Ii]?[Ee]?[Dd]*\s+[Bb][Yy]\s*['"][^'"]*['"])|([Pp][Aa]*[Ss]*[Ss]*[Ww]*[Oo]*[Rr]*[Dd]*\s+[Ff][Oo][Rr]\s+\w+\s+[Tt][Oo]\s*['"][^'"]*['"]))",
      std::regex_constants::icase);

  std::string result;
  std::string_view remaining = input;

  // Process the string by replacing sensitive information first and handling nodes separately
  while (!remaining.empty()) {
    std::match_results<std::string_view::const_iterator> node_match;
    bool const found = std::regex_search(remaining.cbegin(), remaining.cend(), node_match, nodePattern);
    if (!found) break;

    size_t const node_start = node_match.position();
    size_t const node_end = node_start + node_match.length();

    // Process the part before the node match
    auto non_node_part = remaining.substr(0, node_start);
    std::string masked_non_node_part;

    // Declare the iterator for the regex search on sensitive patterns
    auto it = std::regex_iterator{non_node_part.begin(), non_node_part.end(), sensitivePattern};
    const auto end = std::regex_iterator<std::string_view::const_iterator>{};

    size_t prev_end = 0;
    for (; it != end; ++it) {
      masked_non_node_part.append(non_node_part, prev_end, it->position() - prev_end);
      std::string replacement = it->str();
      size_t const startPos = replacement.find_first_of("'\"");
      if (startPos != std::string::npos) {
        size_t const endPos = replacement.find_first_of("'\"", startPos + 1);
        if (endPos != std::string::npos) {
          // Mask the sensitive data between the quotes (single or double)
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
    remaining = remaining.substr(node_end);
  }

  // Append and mask any remaining text after the last node match
  std::string masked_remaining_part;

  // Declare the iterator for the regex search on sensitive patterns in remaining part
  auto it = std::regex_iterator{remaining.begin(), remaining.end(), sensitivePattern};
  const auto end = std::regex_iterator<std::string_view::const_iterator>{};

  size_t prev_end = 0;
  for (; it != end; ++it) {
    masked_remaining_part.append(remaining, prev_end, it->position() - prev_end);
    std::string replacement = it->str();
    size_t const startPos = replacement.find_first_of("'\"");
    if (startPos != std::string::npos) {
      size_t const endPos = replacement.find_first_of("'\"", startPos + 1);
      if (endPos != std::string::npos) {
        // Mask the sensitive data between the quotes (single or double)
        replacement.replace(startPos + 1, endPos - startPos - 1, "****");
      }
    }
    masked_remaining_part.append(replacement);
    prev_end = it->position() + it->length();
  }
  masked_remaining_part.append(remaining, prev_end, remaining.length() - prev_end);

  result.append(masked_remaining_part);

  return result;
}

void memgraph::logging::AssertFailed(char const *file_name, int line_num, char const *expr,
                                     std::string const &message) {
  spdlog::critical(
      "\nAssertion failed in file {} at line {}."
      "\n\tExpression: '{}'"
      "{}",
      file_name, line_num, expr, !message.empty() ? fmt::format("\n\tMessage: '{}'", message) : "");
  std::terminate();
}

void memgraph::logging::RedirectToStderr() { spdlog::set_default_logger(spdlog::stderr_color_mt("stderr")); }

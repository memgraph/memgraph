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
  static std::regex const nodePattern(R"(\(\w+:\w+\s*\{[^}]*\}\))");
  static std::regex const sensitivePattern(
      R"((password\s*:\s*['"][^'"]*['"])|([Pp][Aa][Ss]*[Ss]*[Ww]*[Oo]*[Rr]*[Dd]*\s+[Tt][Oo]\s*['"][^'"]*['"])|([Rr][Ee]?[Pp][Ll]?[Aa]?[Cc]?[Ee]?\s*['"][^'"]*['"])|([Ii][Dd]?[Ee]?[Nn]?[Tt]?[Ii]?[Ff]?[Ii]?[Ee]?[Dd]*\s+[Bb][Yy]\s*['"][^'"]*['"])|([Pp][Aa]*[Ss]*[Ss]*[Ww]*[Oo]*[Rr]*[Dd]*\s+[Ff][Oo][Rr]\s+\w+\s+[Tt][Oo]\s*['"][^'"]*['"])|(['"]?aws[_-]?access[_-]?key['"]?\s*[:=]\s*['"][^'"]*['"])|(['"]?aws[_-]?secret[_-]?key['"]?\s*[:=]\s*['"][^'"]*['"])|(['"]?aws[._-]?access[._-]?key['"]?\s+[Tt][Oo]\s*['"][^'"]*['"])|(['"]?aws[._-]?secret[._-]?key['"]?\s+[Tt][Oo]\s*['"][^'"]*['"]))",
      std::regex_constants::icase);

  // helper that masks the *last* quoted segment in `s`
  auto mask_last_quoted = [](std::string &s) {
    // find last quote of either kind
    auto last_single = s.rfind('\'');
    auto last_double = s.rfind('"');

    size_t last_pos;
    char quote_char;
    if (last_single == std::string::npos && last_double == std::string::npos) return;  // nothing to mask

    if (last_single == std::string::npos) {
      last_pos = last_double;
      quote_char = '"';
    } else if (last_double == std::string::npos) {
      last_pos = last_single;
      quote_char = '\'';
    } else if (last_single > last_double) {
      last_pos = last_single;
      quote_char = '\'';
    } else {
      last_pos = last_double;
      quote_char = '"';
    }

    if (last_pos == 0) return;
    // find matching opening quote of the same type, before last_pos
    auto open_pos = s.rfind(quote_char, last_pos - 1);
    if (open_pos == std::string::npos) return;

    // replace inside the quotes
    s.replace(open_pos + 1, last_pos - open_pos - 1, "****");
  };

  std::string result;
  std::string_view remaining = input;

  while (!remaining.empty()) {
    std::match_results<std::string_view::const_iterator> node_match;
    bool const found = std::regex_search(remaining.cbegin(), remaining.cend(), node_match, nodePattern);
    if (!found) break;

    size_t const node_start = node_match.position();
    size_t const node_end = node_start + node_match.length();

    auto non_node_part = remaining.substr(0, node_start);
    std::string masked_non_node_part;

    auto it = std::regex_iterator{non_node_part.begin(), non_node_part.end(), sensitivePattern};
    const auto end = std::regex_iterator<std::string_view::const_iterator>{};

    size_t prev_end = 0;
    for (; it != end; ++it) {
      masked_non_node_part.append(non_node_part, prev_end, it->position() - prev_end);
      std::string replacement = it->str();

      mask_last_quoted(replacement);

      masked_non_node_part.append(replacement);
      prev_end = it->position() + it->length();
    }
    masked_non_node_part.append(non_node_part, prev_end, non_node_part.length() - prev_end);

    result.append(masked_non_node_part);
    result.append(node_match.str());  // keep node intact

    remaining = remaining.substr(node_end);
  }

  // mask remaining tail
  std::string masked_remaining_part;
  auto it = std::regex_iterator{remaining.begin(), remaining.end(), sensitivePattern};
  const auto end = std::regex_iterator<std::string_view::const_iterator>{};

  size_t prev_end = 0;
  for (; it != end; ++it) {
    masked_remaining_part.append(remaining, prev_end, it->position() - prev_end);
    std::string replacement = it->str();

    mask_last_quoted(replacement);

    masked_remaining_part.append(replacement);
    prev_end = it->position() + it->length();
  }
  masked_remaining_part.append(remaining, prev_end, remaining.length() - prev_end);

  result.append(masked_remaining_part);

  return result;
}

void memgraph::logging::AssertFailed(std::source_location const loc, char const *expr, std::string const &message) {
  spdlog::critical(
      "\nAssertion failed in file {} at line {}."
      "\n\tExpression: '{}'"
      "{}",
      loc.file_name(), loc.line(), expr, !message.empty() ? fmt::format("\n\tMessage: '{}'", message) : "");
  std::terminate();
}

void memgraph::logging::RedirectToStderr() { spdlog::set_default_logger(spdlog::stderr_color_mt("stderr")); }

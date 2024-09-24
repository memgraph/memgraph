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

#include "utils/logging.hpp"

#include <regex>

std::string memgraph::logging::MaskSensitiveInformation(std::string const &input) {
  // Regex patterns for sensitive information and node properties
  static std::regex nodePattern(R"(\(\w+:\w+\s*\{[^}]*\})");
  static std::regex sensitivePattern(
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

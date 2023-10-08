// Copyright 2023 Memgraph Ltd.
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

#include <iostream>
#include <map>
#include <regex>
#include <sstream>
#include <string>

namespace memgraph::utils {

static inline std::regex GetRegexKey(const std::string &key) {
  return std::regex("\"" + key + "\"\\s*:\\s*\"([^\"]+)\"|\"" + key + "\"\\s*:\\s*([^,\\}]+)");
}

// Function to parse JSON string and return a value by key as a string.
static inline bool GetJsonValue(const std::string &jsonString, const std::string &key, std::string &out) {
  const auto regex = GetRegexKey(key);
  std::smatch match;

  if (std::regex_search(jsonString, match, regex)) {
    out = match[1].str();
    return true;
  }
  return false;
}

// Overloaded GetJsonValue function for different types.
template <typename T>
static inline bool GetJsonValue(const std::string &jsonString, const std::string &key, T &out) {
  const auto regex = GetRegexKey(key);
  std::smatch match;

  if (std::regex_search(jsonString, match, regex)) {
    for (size_t i = 1; i < match.size(); ++i) {
      const auto &m = match[i].str();
      if (m.empty()) continue;
      std::istringstream ss(m);
      ss >> out;
      return true;
    }
  }
  return false;
}

};  // namespace memgraph::utils

// Copyright 2022 Memgraph Ltd.
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

#include <optional>
#include <string>

#ifdef HAS_READLINE
// TODO: This should probably be moved to some utils file.

#include "readline/history.h"
#include "readline/readline.h"

/**
 * Helper function that reads a line from the
 * standard input using the 'readline' lib.
 * Adds support for history and reverse-search.
 *
 * @param prompt The prompt to display.
 * @return  A single command the user entered, or nullopt on EOF.
 */
inline std::optional<std::string> ReadLine(const std::string &prompt) {
  char *line = readline(prompt.c_str());
  if (!line) return std::nullopt;

  if (*line) add_history(line);
  std::string r_val(line);
  free(line);
  return r_val;
}

#else

inline std::optional<std::string> ReadLine(const std::string &prompt) {
  std::cout << prompt;
  std::string line;
  std::getline(std::cin, line);
  if (std::cin.eof()) return std::nullopt;
  return line;
}

#endif  // HAS_READLINE

// Repeats the prompt untile the user inputs an integer.
inline int64_t ReadInt(const std::string &prompt) {
  int64_t val = 0;
  std::stringstream ss;
  do {
    auto line = ReadLine(prompt);
    if (!line) continue;
    ss.str(*line);
    ss.clear();
    ss >> val;
  } while (ss.fail() || !ss.eof());
  return val;
}

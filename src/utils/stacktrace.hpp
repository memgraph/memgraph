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

#include <cxxabi.h>
#include <execinfo.h>
#include <fmt/format.h>
#include <stdexcept>
#include <utility>

#include "utils/on_scope_exit.hpp"

namespace memgraph::utils {

class Stacktrace {
 public:
  class Line {
   public:
    // cppcheck-suppress noExplicitConstructor
    explicit Line(std::string original) : original(std::move(original)) {}

    Line(std::string original, std::string function, std::string location)
        : original(std::move(original)), function(std::move(function)), location(std::move(location)) {}

    std::string original, function, location;
  };

  static constexpr size_t stacktrace_depth = 128;

  Stacktrace() {
    void *addresses[stacktrace_depth];
    auto depth = backtrace(addresses, stacktrace_depth);

    // will this leak if backtrace_symbols throws?
    char **symbols = nullptr;
    utils::OnScopeExit on_exit([&symbols]() { free(symbols); });

    symbols = backtrace_symbols(addresses, depth);

    // skip the first one since it will be Stacktrace::Stacktrace()
    for (int i = 1; i < depth; ++i) lines.emplace_back(format(symbols[i]));
  }

  auto begin() { return lines.begin(); }
  auto begin() const { return lines.begin(); }
  auto cbegin() const { return lines.cbegin(); }

  auto end() { return lines.end(); }
  auto end() const { return lines.end(); }
  auto cend() const { return lines.cend(); }

  const Line &operator[](size_t idx) const { return lines[idx]; }

  size_t size() const { return lines.size(); }

  template <class Stream>
  void dump(Stream &stream) {
    stream << dump();
  }

  std::string dump() {
    std::string message;
    for (size_t i = 0; i < size(); i++) {
      message.append(fmt::format("at {} ({}) \n", lines[i].function, lines[i].location));
    }
    return message;
  }

 private:
  std::vector<Line> lines;

  Line format(const std::string &original) {
    using namespace abi;
    auto line = original;

    auto begin = line.find('(');
    auto end = line.find('+');

    if (begin == std::string::npos || end == std::string::npos) return Line{original};

    line[end] = '\0';

    int s;
    auto demangled = __cxa_demangle(line.data() + begin + 1, nullptr, nullptr, &s);

    auto location = line.substr(0, begin);

    auto function =
        demangled ? std::string(demangled) : fmt::format("{}()", original.substr(begin + 1, end - begin - 1));

    return {original, function, location};
  }
};

}  // namespace memgraph::utils

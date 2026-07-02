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

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace memgraph::query::fuzz {

class QueryGenerator {
 public:
  QueryGenerator(uint8_t const *data, size_t size) : data_(data), size_(size) {}

  std::string Generate() {
    std::string q;
    auto const num_clauses = 1 + pick(3);
    for (auto i = 0u; i < num_clauses && HasBytes(); ++i) {
      if (i > 0) q += ' ';
      switch (pick(3)) {
        case 0:
          q += Create();
          break;
        case 1:
          q += Match();
          break;
        default:
          q += Return();
          break;
      }
    }
    if (q.find("RETURN") == std::string::npos) {
      q += " RETURN *";
    }
    return q;
  }

 private:
  bool HasBytes() const { return size_ > 0; }

  uint8_t next() {
    if (size_ == 0) return 0;
    auto const b = *data_++;
    --size_;
    return b;
  }

  uint8_t pick(uint8_t max) { return next() % max; }

  static constexpr char const *kVars[] = {"n", "m", "x", "y"};
  static constexpr size_t kNumVars = 4;
  static constexpr char const *kLabels[] = {"Employee", "Manager", "Department", "Project"};
  static constexpr size_t kNumLabels = 4;
  static constexpr char const *kRelTypes[] = {"WORKS_IN", "MANAGES", "ASSIGNED_TO"};
  static constexpr size_t kNumRelTypes = 3;
  static constexpr char const *kProps[] = {"name", "age", "id", "active"};
  static constexpr size_t kNumProps = 4;

  std::string NodePattern() {
    std::string s = "(";
    bool const has_var = pick(2);
    if (has_var) s += kVars[pick(kNumVars)];
    if (pick(2)) {
      s += ':';
      s += kLabels[pick(kNumLabels)];
    }
    if (pick(3) == 0) {
      s += " {";
      s += kProps[pick(kNumProps)];
      s += ": ";
      s += Literal();
      s += '}';
    }
    s += ')';
    return s;
  }

  std::string PatternElement() {
    auto s = NodePattern();
    if (pick(2)) {
      s += "-[";
      if (pick(2)) s += kVars[pick(kNumVars)];
      if (pick(2)) {
        s += ':';
        s += kRelTypes[pick(kNumRelTypes)];
      }
      s += "]->";
      s += NodePattern();
    }
    return s;
  }

  std::string Create() { return "CREATE " + PatternElement(); }

  std::string Match() {
    std::string s = "MATCH " + PatternElement();
    if (pick(3) == 0) {
      s += " WHERE ";
      s += kVars[pick(kNumVars)];
      s += '.';
      s += kProps[pick(kNumProps)];
      switch (pick(3)) {
        case 0:
          s += " = ";
          break;
        case 1:
          s += " > ";
          break;
        default:
          s += " <> ";
          break;
      }
      s += Literal();
    }
    return s;
  }

  std::string Return() {
    if (pick(2)) return "RETURN *";
    std::string s = "RETURN ";
    s += kVars[pick(kNumVars)];
    if (pick(2)) {
      s += ", ";
      s += kVars[pick(kNumVars)];
    }
    return s;
  }

  std::string Literal() {
    switch (pick(3)) {
      case 0:
        return std::to_string(pick(100));
      case 1:
        return pick(2) ? "true" : "false";
      default:
        return "'val_" + std::to_string(pick(10)) + "'";
    }
  }

  uint8_t const *data_;
  size_t size_;
};

}  // namespace memgraph::query::fuzz

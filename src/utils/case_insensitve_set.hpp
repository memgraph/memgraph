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

#include <string_view>
#include "absl/container/flat_hash_set.h"
#include "boost/functional/hash.hpp"

namespace memgraph::utils {

namespace {

// this is faster than calling C++ `std::tolower`, as it can be inlined
constexpr char AsciiToLower(char c) { return ('A' <= c && c <= 'Z') ? (c + ('a' - 'A')) : c; };

struct CaseInsensitiveHash {
  size_t operator()(std::string_view s) const {
    size_t seed = 0;
    for (char c : s) {
      boost::hash_combine(seed, AsciiToLower(c));
    }
    return seed;
  }
  using is_transparent = void;
};

struct CaseInsensitiveEqual {
  bool operator()(std::string_view lhs, std::string_view rhs) const {
    return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(),
                      [](auto c1, auto c2) { return AsciiToLower(c1) == AsciiToLower(c2); });
  }
  using is_transparent = void;
};
}  // namespace

// Note: only correct for ASCII strings
// TODO: correct for utf-8
struct CaseInsensitiveSet {
  explicit CaseInsensitiveSet(std::initializer_list<std::string> members) : set_{members} {}

  bool contains(auto const &value) const { return set_.contains(value); }

 private:
  absl::flat_hash_set<std::string, CaseInsensitiveHash, CaseInsensitiveEqual> set_;
};

}  // namespace memgraph::utils

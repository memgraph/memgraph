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

#include <map>
#include <memory>
#include <vector>

namespace memgraph::utils::print_helpers {

template <typename T>
std::ostream &operator<<(std::ostream &in, const std::vector<T> &vector) {
  in << "[";
  bool first = true;
  for (const auto &item : vector) {
    if (!first) {
      in << ", ";
    }
    first = false;
    in << item;
  }
  in << "]";
  return in;
}

template <typename K, typename V>
std::ostream &operator<<(std::ostream &in, const std::map<K, V> &map) {
  in << "{";
  bool first = true;
  for (const auto &[a, b] : map) {
    if (!first) {
      in << ", ";
    }
    first = false;
    in << a;
    in << ": ";
    in << b;
  }
  in << "}";
  return in;
}

template <typename K, typename V>
std::ostream &operator<<(std::ostream &in, const std::pair<K, V> &pair) {
  const auto &[a, b] = pair;
  in << "(";
  in << a;
  in << ", ";
  in << b;
  in << ")";
  return in;
}

}  // namespace memgraph::utils::print_helpers

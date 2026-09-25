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

#include <cstdint>
#include <deque>
#include <string>

#include <gtest/gtest.h>

#include "storage/v2/property_name_order.hpp"

namespace memgraph::test {

/// Points this thread at a name_order naming every identifier below @p upto.
///
/// For a test that builds stored maps directly, with no database to have
/// interned their keys. A comparison of two such maps reads where their keys'
/// names sort, and these keys have no names, so this gives them one.
///
/// The names are the identifiers themselves, padded so that they sort the way
/// the identifiers do. A test that was written when a map's order came from its
/// identifiers therefore keeps the order it was written against.
///
/// @pre called from one thread at a time, which a test's setup gives.
inline void UseANameOrderOverIdentifiers(std::uint32_t upto = 4096) {
  static memgraph::storage::PropertyNameOrder name_order;
  // A deque, because the name_order keeps a view of each name and a vector would
  // move them as it grew.
  static std::deque<std::string> names;
  static std::uint32_t named = 0;

  for (; named < upto; ++named) {
    auto padded = std::to_string(named);
    names.push_back(std::string(10 - padded.size(), '0') + padded);
    name_order.Add(named, names.back());
  }

  memgraph::storage::PointThisThreadAt(name_order);
}

/// Installs it before any test runs. Declare one at namespace scope in a test
/// that builds stored maps of its own:
///
///     auto const *kNameOrder = ::testing::AddGlobalTestEnvironment(new memgraph::test::NameOrderOverIdentifiers);
struct NameOrderOverIdentifiers : ::testing::Environment {
  void SetUp() override { UseANameOrderOverIdentifiers(); }
};

}  // namespace memgraph::test

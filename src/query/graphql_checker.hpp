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

#include <array>
#include <string>
#include <string_view>

namespace memgraph::query {

// TODO -1- (gvolfing) - check what happens if we can not provide
// the same signature for any given functionality
// TODO -2- (gvolfing) - test this once we have some facilities.
// TODO -3- (gvolfing) - check if we should read in the mapping from
// some files instead.
// TODO -4- (gvolfing) - fill out the remaining of the mappings
// TODO -5- (gvolfing) - consider if the query namespace is good for
// this facility

class GraphqlChecker {
 public:
  GraphqlChecker() = default;

  GraphqlChecker(const GraphqlChecker &) = delete;
  GraphqlChecker &operator=(const GraphqlChecker &) = delete;
  GraphqlChecker(GraphqlChecker &&) = delete;
  GraphqlChecker &operator=(GraphqlChecker &&) = delete;

  ~GraphqlChecker() = default;

  void operator()(std::string &query) const;

 private:
  static constexpr std::array<std::pair<std::string_view, std::string_view>, 1> mapping_ = {{

      {"dbms.components", "mgps.components"}

  }};
};

}  // namespace memgraph::query

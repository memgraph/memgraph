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
#include <cstring>
#include <functional>
#include <optional>
#include <string>
#include <string_view>

#include <fstream>

namespace memgraph::query::procedure {

// TODO -1- (gvolfing) - check what happens if we can not provide
// the same signature for any given functionality
// TODO -2- (gvolfing) - test this once we have some facilities.
// TODO -3- (gvolfing) - check if we should read in the mapping from
// some files instead.
// TODO -4- (gvolfing) - fill out the remaining of the mappings
// TODO -5- (gvolfing) - consider if the query namespace is good for
// this facility
[[nodiscard]] std::optional<std::string_view> FindApocReplacement(std::string_view apoc_name);

}  // namespace memgraph::query::procedure

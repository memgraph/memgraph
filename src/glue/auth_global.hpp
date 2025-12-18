// Copyright 2025 Memgraph Ltd.
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

namespace memgraph::glue {
inline constexpr std::string_view kDefaultUserRoleRegex = "[a-zA-Z0-9_.+-@]+";
static constexpr std::string_view kDefaultPasswordRegex = ".+";
}  // namespace memgraph::glue

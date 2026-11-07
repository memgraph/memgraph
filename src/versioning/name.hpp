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

#include <optional>
#include <string>
#include <string_view>

namespace memgraph::versioning {

// Returns a user-facing error if `name` is not a valid branch name; std::nullopt when valid.
// Policy (bounded like database identifiers elsewhere): 1..63 characters of [A-Za-z0-9._-],
// starting and ending with an alphanumeric; exact "main" (case-sensitive) is reserved; '@' is
// forbidden as the <database>@<branch> separator.
std::optional<std::string> ValidateBranchName(std::string_view name);

}  // namespace memgraph::versioning

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

// Pure predicate for branch-name validity, per specs/graph-versioning.md §4.1:
//   "A version name ... must be non-empty, cannot be `main` (reserved), cannot start with `.`,
//    and cannot contain `/` or `\`."
// Returns an end-user-facing error message when `name` is invalid; std::nullopt when valid.
// Deliberately takes a std::string_view (not AST/Query types) so callers (grammar visitor,
// unit tests, and later the version-store registry) can invoke it without any parser machinery.
std::optional<std::string> ValidateBranchName(std::string_view name);

}  // namespace memgraph::versioning

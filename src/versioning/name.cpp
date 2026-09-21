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

#include "versioning/name.hpp"

namespace memgraph::versioning {

std::optional<std::string> ValidateBranchName(std::string_view name) {
  if (name.empty()) {
    return "Branch name must not be empty.";
  }
  if (name == "main") {
    return "'main' is a reserved branch name and cannot be created, checked out, or dropped explicitly.";
  }
  if (name.front() == '.') {
    return "Branch name must not start with '.'.";
  }
  if (name.find('/') != std::string_view::npos || name.find('\\') != std::string_view::npos) {
    return "Branch name must not contain '/' or '\\'.";
  }
  return std::nullopt;
}

}  // namespace memgraph::versioning

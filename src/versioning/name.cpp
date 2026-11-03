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

#include <fmt/format.h>

namespace memgraph::versioning {

namespace {
// Branch-name policy, bounded like database identifiers elsewhere:
//  - PostgreSQL: identifiers are letters/digits/underscores, max 63 bytes (NAMEDATALEN - 1);
//  - git-check-ref-format: no leading/trailing '-', no trailing '.', no spaces or control chars;
//  - Kubernetes labels: DNS-style lowercase alphanumerics + '-'/'.'/'_', 63-char bound.
// '@' stays forbidden: it is the <database>@<branch> separator (Bolt db pin).
constexpr size_t kMaxBranchNameLength = 63;

bool IsAlphaNum(char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'); }

bool IsMiddleChar(char c) { return IsAlphaNum(c) || c == '.' || c == '_' || c == '-'; }
}  // namespace

std::optional<std::string> ValidateBranchName(std::string_view name) {
  if (name.empty()) {
    return "Branch name must not be empty.";
  }
  if (name == "main") {
    return "'main' is a reserved branch name.";
  }
  if (name.size() > kMaxBranchNameLength) {
    return fmt::format("Branch name must be at most {} characters.", kMaxBranchNameLength);
  }
  if (name.find('@') != std::string_view::npos) {
    return "Branch name must not contain '@': it names a branch as <database>@<branch>.";
  }
  // The charset rule first: it rejects separators like '/' anywhere, while the ends rule only
  // covers the characters the charset allows.
  for (const char c : name) {
    if (!IsMiddleChar(c)) {
      return "Branch name may contain only letters, digits, '.', '_' and '-'.";
    }
  }
  if (!IsAlphaNum(name.front()) || !IsAlphaNum(name.back())) {
    return "Branch name must start and end with an alphanumeric character.";
  }
  return std::nullopt;
}

}  // namespace memgraph::versioning

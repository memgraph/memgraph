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

#include <sys/resource.h>

#include <optional>

namespace memgraph::utils {

/// RLIMIT_NOFILE as inherited at process start, before RaiseNoFileSoftLimit().
/// Child processes that run foreign code should be restored to this.
inline std::optional<rlimit> &OriginalNoFileLimit() {
  static std::optional<rlimit> original;
  return original;
}

/// Raises the soft RLIMIT_NOFILE to the hard limit and returns the resulting limits.
/// Records the pre-raise limits in OriginalNoFileLimit(). Failures are non-fatal:
/// the limits actually in effect are returned (nullopt if getrlimit itself fails).
inline std::optional<rlimit> RaiseNoFileSoftLimit() {
  rlimit limit{};
  if (getrlimit(RLIMIT_NOFILE, &limit) != 0) return std::nullopt;
  OriginalNoFileLimit() = limit;
  if (limit.rlim_cur < limit.rlim_max) {
    rlimit raised = limit;
    raised.rlim_cur = limit.rlim_max;
    if (setrlimit(RLIMIT_NOFILE, &raised) == 0) limit = raised;
  }
  return limit;
}

}  // namespace memgraph::utils

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

#include <cerrno>
#include <optional>

namespace memgraph::utils {

/// RLIMIT_NOFILE as inherited at process start, before RaiseNoFileSoftLimit().
/// Child processes that run foreign code should be restored to this.
inline std::optional<rlimit> &OriginalNoFileLimit() {
  static std::optional<rlimit> original;
  return original;
}

struct NoFileRaiseResult {
  rlimit limit{};      // limits in effect after the attempt
  int raise_errno{0};  // errno of a failed setrlimit(), 0 otherwise
};

/// Raises the soft RLIMIT_NOFILE to the hard limit; failure is non-fatal and reported via raise_errno.
inline std::optional<NoFileRaiseResult> RaiseNoFileSoftLimit() {
  rlimit limit{};
  if (getrlimit(RLIMIT_NOFILE, &limit) != 0) return std::nullopt;
  OriginalNoFileLimit() = limit;
  NoFileRaiseResult result{.limit = limit};
  if (limit.rlim_cur < limit.rlim_max) {
    rlimit raised = limit;
    raised.rlim_cur = limit.rlim_max;
    if (setrlimit(RLIMIT_NOFILE, &raised) == 0) {
      result.limit = raised;
    } else {
      result.raise_errno = errno;
    }
  }
  return result;
}

}  // namespace memgraph::utils

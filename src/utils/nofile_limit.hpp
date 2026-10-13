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

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <optional>

namespace memgraph::utils {

/// RLIMIT_NOFILE as inherited at process start, before RaiseNoFileSoftLimit().
/// Child processes that run foreign code should be restored to this.
inline std::optional<rlimit> &OriginalNoFileLimit() {
  static std::optional<rlimit> original;
  return original;
}

/// Kernel ceiling for RLIMIT_NOFILE (fs.nr_open); setrlimit() fails with EPERM above it.
inline rlim_t ReadNrOpen() {
  constexpr rlim_t kDefaultNrOpen = 1048576;
  rlim_t value = kDefaultNrOpen;
  if (std::FILE *f = std::fopen("/proc/sys/fs/nr_open", "r")) {
    unsigned long long parsed = 0;
    if (std::fscanf(f, "%llu", &parsed) == 1 && parsed > 0) value = static_cast<rlim_t>(parsed);
    std::fclose(f);
  }
  return value;
}

struct NoFileRaiseResult {
  rlimit limit{};      // limits in effect after the attempt
  int raise_errno{0};  // errno of a failed setrlimit(), 0 otherwise
};

/// Raises the soft RLIMIT_NOFILE to min(hard, nr_open). Records the pre-raise limits in
/// OriginalNoFileLimit(). Failures are non-fatal: the limits actually in effect are returned together
/// with the errno of a failed setrlimit(). nullopt if getrlimit itself fails.
inline std::optional<NoFileRaiseResult> RaiseNoFileSoftLimit() {
  rlimit limit{};
  if (getrlimit(RLIMIT_NOFILE, &limit) != 0) return std::nullopt;
  OriginalNoFileLimit() = limit;
  NoFileRaiseResult result{.limit = limit};
  const rlim_t target = std::min(limit.rlim_max, ReadNrOpen());
  if (limit.rlim_cur < target) {
    rlimit raised = limit;
    raised.rlim_cur = target;
    if (setrlimit(RLIMIT_NOFILE, &raised) == 0) {
      result.limit = raised;
    } else {
      result.raise_errno = errno;
    }
  }
  return result;
}

}  // namespace memgraph::utils

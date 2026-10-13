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

#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>

#include "gtest/gtest.h"
#include "utils/nofile_limit.hpp"

namespace {

constexpr rlim_t kStartSoft = 256;
constexpr rlim_t kStartHard = 4096;

// Runs `body` in a forked child so the test process limits stay untouched; returns the child's exit code.
template <typename F>
int RunInChild(F body) {
  const pid_t pid = fork();
  if (pid == 0) _exit(body());
  int status = 0;
  waitpid(pid, &status, 0);
  return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

}  // namespace

TEST(NoFileLimit, RaisesSoftToMinOfHardAndNrOpen) {
  EXPECT_EQ(0, RunInChild([] {
              const rlimit start{kStartSoft, kStartHard};
              if (setrlimit(RLIMIT_NOFILE, &start) != 0) return 1;
              const auto result = memgraph::utils::RaiseNoFileSoftLimit();
              if (!result || result->raise_errno != 0) return 2;
              const rlim_t expected = std::min(kStartHard, memgraph::utils::ReadNrOpen());
              if (result->limit.rlim_cur != expected || result->limit.rlim_max != kStartHard) return 3;
              rlimit now{};
              if (getrlimit(RLIMIT_NOFILE, &now) != 0 || now.rlim_cur != expected) return 4;
              const auto &original = memgraph::utils::OriginalNoFileLimit();
              if (!original || original->rlim_cur != kStartSoft || original->rlim_max != kStartHard) return 5;
              return 0;
            }));
}

TEST(NoFileLimit, NoOpWhenSoftEqualsHard) {
  EXPECT_EQ(0, RunInChild([] {
              const rlimit start{kStartHard, kStartHard};
              if (setrlimit(RLIMIT_NOFILE, &start) != 0) return 1;
              const auto result = memgraph::utils::RaiseNoFileSoftLimit();
              if (!result || result->raise_errno != 0) return 2;
              if (result->limit.rlim_cur != kStartHard || result->limit.rlim_max != kStartHard) return 3;
              const auto &original = memgraph::utils::OriginalNoFileLimit();
              if (!original || original->rlim_cur != kStartHard) return 4;
              return 0;
            }));
}

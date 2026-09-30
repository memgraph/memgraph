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

// ApprovalTests requires its own main() for Google Test integration.
// APPROVAL_TESTS_SOURCE_DIR is set by CMake to CMAKE_CURRENT_SOURCE_DIR.
#define APPROVALS_GOOGLETEST
#include <ApprovalTests.hpp>

#include <sys/prctl.h>
#include <sys/resource.h>

// Death tests fork() and abort() the child; the kernel then pipes a core dump
// to apport, which dominates wall time when many death tests run.
// PR_SET_DUMPABLE=0 makes the kernel skip core-dump generation for this
// process and its forked children, regardless of /proc/sys/kernel/core_pattern.
// RLIMIT_CORE=0 is also set as a belt-and-braces fallback.
static auto const disable_core_dumps = [] {
  auto rl = rlimit{.rlim_cur = 0, .rlim_max = 0};
  setrlimit(RLIMIT_CORE, &rl);
  prctl(PR_SET_DUMPABLE, 0, 0, 0, 0);
  return 0;
}();

// Ninja produces relative __FILE__ paths, so ApprovalTests cannot locate test sources
// from the build directory. Disable the check and use a custom namer with absolute paths.
static auto buildConfigCheck = ApprovalTests::ApprovalTestNamer::setCheckBuildConfig(false);

// Place approved/received files next to the test sources in approval_tests/ subdirectory,
// regardless of the working directory when the test binary runs.
static auto namerDisposer = ApprovalTests::TemplatedCustomNamer::useAsDefaultNamer(
    APPROVAL_TESTS_SOURCE_DIR "/approval_tests/{TestFileName}.{TestCaseName}.{ApprovedOrReceived}.{FileExtension}");

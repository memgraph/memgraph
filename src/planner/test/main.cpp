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

// Ninja produces relative __FILE__ paths, so ApprovalTests cannot locate test sources
// from the build directory. Disable the check and use a custom namer with absolute paths.
static auto buildConfigCheck = ApprovalTests::ApprovalTestNamer::setCheckBuildConfig(false);

// Place approved/received files next to the test sources in approval_tests/ subdirectory,
// regardless of the working directory when the test binary runs.
static auto namerDisposer = ApprovalTests::TemplatedCustomNamer::useAsDefaultNamer(
    APPROVAL_TESTS_SOURCE_DIR "/approval_tests/{TestFileName}.{TestCaseName}.{ApprovedOrReceived}.{FileExtension}");

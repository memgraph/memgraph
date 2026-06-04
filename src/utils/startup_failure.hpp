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

#include <string_view>

#include "utils/exit_codes.hpp"

namespace memgraph::utils {

/// Terminate startup on a misconfiguration: routes `message` to the log
/// (best-effort - the default logger may not be set up, or may have no sinks)
/// and unconditionally to stderr, then exits with `code`'s status.
///
/// For operator errors, not bugs: exits cleanly with a stable, distinct
/// status and no core dump, so a restart policy can tell misconfiguration
/// from a crash.  Invariant violations should keep using MG_ASSERT, where the
/// core dump is the artifact.
[[noreturn]] void FailStartup(ExitCode code, std::string_view message);

}  // namespace memgraph::utils

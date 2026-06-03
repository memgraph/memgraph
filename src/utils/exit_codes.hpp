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

#include <cstdint>
#include <utility>

namespace memgraph::utils {

/// Distinct non-signal exit statuses for startup misconfigurations, so a
/// restart policy (systemd, k8s) can tell misconfiguration apart from a
/// crash.  Keep values stable: they are part of the operational interface.
/// std::uint8_t base: a process exit status is 0-255 by POSIX.
enum class ExitCode : std::uint8_t {
  StorageDirectoryOwnerMismatch = 12,
  LogFileNotWritable = 13,
};

/// `std::exit`-ready status of an ExitCode.
[[nodiscard]] constexpr auto AsExitStatus(ExitCode code) -> int { return std::to_underlying(code); }

}  // namespace memgraph::utils

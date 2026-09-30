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

#include <sys/types.h>

#include <filesystem>
#include <optional>
#include <string>

namespace memgraph::utils {

/// Username for `uid`, falling back to the numeric form when there is no
/// passwd entry.
std::string UsernameFor(uid_t uid);

/// Owner uid of `path`, or nullopt when it cannot be stat'ed.
std::optional<uid_t> OwnerOf(std::filesystem::path const &path);

/// The shared fact-sentence for owner-mismatch diagnostics: "The process is
/// running as user X, but '<path>' is owned by user Y."  nullopt when the
/// owners match (nothing to diagnose).  Callers append their own advice
/// (e.g. "run as Y" where ownership is required, "chown/chmod or run as Y"
/// where write access suffices).
std::optional<std::string> OwnerMismatchFact(std::filesystem::path const &path, uid_t process_euid, uid_t path_owner);

}  // namespace memgraph::utils

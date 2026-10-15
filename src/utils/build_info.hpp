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

#include <string>

namespace memgraph::utils {

struct BuildInfo {
  std::string build_name;  // CMake build type (Release / RelWithDebInfo / Debug / ...)
  std::string version;     // Memgraph version string
  std::string build_id;    // hex-encoded GNU build-id, or empty if unavailable
};

BuildInfo GetBuildInfo();

// Hex-encoded GNU build-id of the running executable, or empty if absent. Used to
// match a stripped binary to its debug-symbol sidecar when symbolizing.
std::string GetBuildId();

}  // namespace memgraph::utils

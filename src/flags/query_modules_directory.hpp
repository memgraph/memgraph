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

// Kept apart from the other flag declarations because it is the only one whose
// type needs <filesystem>, and that header reaches <format> through <ostream>,
// which is expensive enough to be worth keeping out of a header this widely
// included.

#include <filesystem>
#include <vector>

namespace memgraph::flags {
// The --query-modules-directory flag split into its individual paths.
auto ParseQueryModulesDirectory() -> std::vector<std::filesystem::path>;
}  // namespace memgraph::flags

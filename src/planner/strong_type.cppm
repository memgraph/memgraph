// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

module;

#include "strong_type/strong_type.hpp"

export module rollbear.strong_type;

// Re-export the strong namespace members that are currently used across the planner module
export namespace strong {
using strong::formattable;  // NOLINT (misc-unused-using-decls)
using strong::hashable;     // NOLINT (misc-unused-using-decls)
using strong::ordered;      // NOLINT (misc-unused-using-decls)
using strong::ostreamable;  // NOLINT (misc-unused-using-decls)
using strong::regular;      // NOLINT (misc-unused-using-decls)
using strong::type;         // NOLINT (misc-unused-using-decls)
using strong::type_is;      // NOLINT (misc-unused-using-decls)
}  // namespace strong

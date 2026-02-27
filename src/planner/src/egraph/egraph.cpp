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

// NOTE: These imports are intentionally kept here as a clangd modules workaround.
// clangd may infer header compile flags from this TU; if the inferred modmap
// misses planner core modules, egraph headers can report false "module not found"
// diagnostics even though CMake/Ninja builds correctly.
// TODO: remove in future versions of clangd
import memgraph.planner.core.concepts;
import memgraph.planner.core.eids;
import memgraph.planner.core.constants;
import memgraph.planner.core.union_find;

#include "planner/egraph/egraph.hpp"

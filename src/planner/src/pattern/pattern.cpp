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

// This file exists to provide clangd with correct module dependencies for pattern.hpp.
// The header is template-only, so this compilation unit is intentionally minimal.

import memgraph.planner.core.concepts;
import memgraph.planner.core.eids;
import memgraph.planner.core.constants;
import memgraph.planner.core.union_find;

#include "planner/pattern/pattern.hpp"

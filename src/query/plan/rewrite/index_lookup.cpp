// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/plan/rewrite/index_lookup.hpp"

#include "utils/flag_validation.hpp"

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int64(query_vertex_count_to_expand_existing, 10,
                       "Maximum count of indexed vertices which provoke "
                       "indexed lookup and then expand to existing, instead of "
                       "a regular expand. Default is 10, to turn off use -1.",
                       FLAG_IN_RANGE(-1, std::numeric_limits<std::int64_t>::max()));

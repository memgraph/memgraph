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

// Everything the memgraph.storage.property_value global module fragment includes. Include this before
// `import memgraph.storage.property_value;` so GCC never meets these headers textually for the first time after
// the import.

#include <compare>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <memory_resource>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <boost/container/flat_map.hpp>
#include "storage/v2/enum.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/point.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"
#include "utils/fnv.gmf.hpp"
#include "utils/small_vector.hpp"

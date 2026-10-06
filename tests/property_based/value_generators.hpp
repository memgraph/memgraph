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

#include <rapidcheck.h>

#include "storage/v2/property_value.hpp"

namespace memgraph::test::generators {

/// Draws a value at random, in the same terms the enumerated shapes are written
/// in: one generator per type, so a type gains a generator and a group of shapes
/// in the same place.
///
/// Each type's generator mixes two sources. The values a comparison, an encoding
/// or an ordering reaches by a distinct route come from that type's shapes, so a
/// run keeps meeting the ones somebody thought to write down. The rest are
/// composed freshly, so it also meets the ones nobody did.

/// How deeply a drawn container may nest. Three puts a value inside a container
/// inside a container, which is the depth at which a walk that forgets to
/// recurse still looks right at the level above.
inline constexpr int kDefaultDepth = 3;

/// Draws a value of one type. A container's elements are drawn at `depth - 1`,
/// and at zero no element is itself a list or a map.
auto ValueOfType(storage::PropertyValueType type, int depth) -> rc::Gen<storage::PropertyValue>;

/// Draws a value whose type is chosen uniformly, so that a pair of independent
/// draws reaches every cell of the type matrix rather than the cells whose types
/// happen to be common.
auto AnyValue(int depth = kDefaultDepth) -> rc::Gen<storage::PropertyValue>;

/// Draws a value that is not a list or a map, for the elements of a container
/// that has run out of depth.
auto AnyScalar() -> rc::Gen<storage::PropertyValue>;

}  // namespace memgraph::test::generators

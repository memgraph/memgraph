// Copyright 2022 Memgraph Ltd.
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

#include "query/v2/bindings/bindings.hpp"

#include "query/v2/bindings/typed_value.hpp"
#include "expr/interpret/frame.hpp"

namespace memgraph::query::v2 {
using Frame = memgraph::expr::Frame<TypedValue>;
} // namespace memgraph::query::v2

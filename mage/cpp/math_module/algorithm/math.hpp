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

#include <mgp.hpp>

namespace Math {

enum class RoundingMode : uint8_t { CEILING, FLOOR, UP, DOWN, HALF_EVEN, HALF_DOWN, HALF_UP, UNNECESSARY };

const std::string kProcedureRound = "round";
const std::string kArgumentValue = "value";
const std::string kArgumentPrecision = "precision";
const std::string kArgumentMode = "mode";
const std::string kArgumentResult = "result";

void Round(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

}  // namespace Math

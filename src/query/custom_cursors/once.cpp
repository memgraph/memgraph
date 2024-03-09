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

#include "query/custom_cursors/once.hpp"
#include "query/context.hpp"
#include "query/interpret/frame.hpp"
#include "spdlog/spdlog.h"
#include "utils/logging.hpp"

namespace memgraph::query::custom_cursors {

bool OnceCursor::Pull(Frame & /*unused*/, ExecutionContext & /*unused*/) {
  SPDLOG_WARN("Once");
  return false;
}

void OnceCursor::Shutdown() {}

void OnceCursor::Reset() {}

}  // namespace memgraph::query::custom_cursors

// Copyright 2023 Memgraph Ltd.
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

#include "query/v2/physical/execution/_execution.hpp"

namespace memgraph::query::v2::physical::execution {

inline Status Execute(Once &state) {
  MG_ASSERT(state.op->children.empty(), "{} should have 0 input/child", state.op->name);
  SPDLOG_TRACE("{} Execute()", state.op->name);
  auto is_emitted = state.op->Emit(DataOperator::TDataPool::TFrame{});
  MG_ASSERT(is_emitted, "{} should always be able to emit", state.op->name);
  state.op->CloseEmit();
  state.op->MarkWriterDone();
  state.op->stats.processed_frames = 1;
  return Status{.has_more = false};
}

}  // namespace memgraph::query::v2::physical::execution

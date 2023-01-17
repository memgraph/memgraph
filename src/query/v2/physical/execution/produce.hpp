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

template <typename TFun>
inline bool ProcessNext(DataOperator *input, TFun fun) {
  auto read_token = input->NextRead();
  if (read_token) {
    fun(*(read_token->multiframe));
    input->PassBackRead(*read_token);
    return true;
  }
  if (!read_token && input->IsWriterDone()) {
    // Even if read token was null before we have to exhaust the pool
    // again because in the meantime the writer could write more and hint
    // that it's done.
    read_token = input->NextRead();
    if (read_token) {
      fun(*(read_token->multiframe));
      input->PassBackRead(*read_token);
      return true;
    }
    return false;
  }
  return true;
}

inline Status Execute(Produce &state) {
  MG_ASSERT(state.op->children.size() == 1, "{} should have exactly 1 input/child", state.op->name);
  SPDLOG_TRACE("{} Execute()", state.op->name);
  auto *input = state.op->children[0].get();

  auto produce_fun = [&state](DataOperator::TDataPool::TMultiframe &multiframe) {
    auto size = multiframe.Data().size();
    state.op->stats.processed_frames += size;
  };

  return Status{.has_more = ProcessNext<decltype(produce_fun)>(input, std::move(produce_fun))};
}

}  // namespace memgraph::query::v2::physical::execution

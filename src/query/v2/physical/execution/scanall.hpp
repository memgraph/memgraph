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

inline Status Execute(ScanAll &state) {
  MG_ASSERT(state.op->children.size() == 1, "{} should have exactly 1 input/child", state.op->name);
  SPDLOG_TRACE("{} Execute()", state.op->name);
  auto *input = state.op->children[0].get();
  auto *output = state.op;

  // TODO(gitbuda): Add ExecutionContext and inject the data_fun probably via state.
  auto data_fun = [](ScanAll &state, DataOperator::TDataPool::TMultiframe &multiframe) {
    std::vector<DataOperator::TDataPool::TFrame> frames{};
    for (int i = 0; i < state.scan_all_elems; ++i) {
      for (int j = 0; j < multiframe.Data().size(); ++j) {
        frames.push_back(DataOperator::TDataPool::TFrame{});
      }
    }
    return frames;
  };

  // Returns true if data is inialized.
  auto init_data = [&data_fun](ScanAll &state, DataOperator *input) -> bool {
    auto read_token = input->NextRead();
    if (read_token) {
      state.data = data_fun(state, *(read_token->multiframe));
      input->PassBackRead(*read_token);
      if (state.data.empty()) {
        state.data_it = state.data.end();
        return false;
      }
      state.data_it = state.data.begin();
      return true;
    }
    return false;
  };

  // Returns true if all data has been written OR if there was no data at all.
  // Returns false if not all data has been written.
  auto write_fun = [](ScanAll &state, DataOperator *output) -> bool {
    if (state.data_it != state.data.end()) {
      int64_t cnt = 0;
      while (state.data_it != state.data.end()) {
        auto written = output->Emit(*state.data_it);
        if (!written) {
          // There is no more space -> return.
          output->stats.processed_frames += cnt;
          return false;
        }
        state.data_it += 1;
        cnt++;
      }
      output->stats.processed_frames += cnt;
      output->CloseEmit();
    }
    return true;
  };

  // First write if there is any data from the previous run.
  if (!write_fun(state, output)) {
    // If not all data has been written return control because our buffer is
    // full -> someone has to make it empty.
    return Status{.has_more = true};
  }

  MG_ASSERT(state.data_it == state.data.end(), "data_it has to be end()");
  bool more_data = init_data(state, input);
  if (!more_data && input->IsWriterDone()) {
    more_data = init_data(state, input);
    if (more_data) {
      write_fun(state, output);
      return Status{.has_more = true};
    }
    output->MarkWriterDone();
    return Status{.has_more = false};
  }
  write_fun(state, output);
  return Status{.has_more = true};
}

}  // namespace memgraph::query::v2::physical::execution

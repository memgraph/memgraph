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

#include <gflags/gflags.h>
#include <variant>
#include <vector>

#include "query/v2/physical/physical_async.hpp"
#include "utils/logging.hpp"

using namespace memgraph::query::v2::physical;

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  spdlog::set_level(spdlog::level::info);

  std::vector<OperatorStates> ops;
  ops.emplace_back(Once{});
  ops.emplace_back(ScanAll{});
  ops.emplace_back(Produce{});

  for (auto &op : ops) {
    std::visit([](auto &op) { Execute(op); }, op);
  }

  return 0;
}

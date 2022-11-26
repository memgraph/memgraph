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

#include <vector>

#include "utils/logging.hpp"

namespace memgraph::query::v2::physical::mock {

struct Frame {
  int64_t a;
  int64_t b;
};

enum class OpType { Once, ScanAll, Produce };
std::ostream &operator<<(std::ostream &os, const OpType &op_type) {
  switch (op_type) {
    case OpType::Once:
      os << "Once";
      break;
    case OpType::ScanAll:
      os << "ScanAll";
      break;
    case OpType::Produce:
      os << "Produce";
      break;
  }
  return os;
}
constexpr static int SCANALL_ELEMS_POS = 0;
struct Op {
  OpType type;
  std::vector<int> props;
};
void log_ops(const std::vector<Op> &ops) {
  for (const auto &op : ops) {
    if (op.type == OpType::ScanAll) {
      SPDLOG_INFO("{} elems: {}", op.type, op.props[SCANALL_ELEMS_POS]);
    } else {
      SPDLOG_INFO("{}", op.type);
    }
  }
}

}  // namespace memgraph::query::v2::physical::mock

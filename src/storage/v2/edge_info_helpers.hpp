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
#pragma once

#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

#include <shared_mutex>

namespace memgraph::storage {

inline bool IsEdgeVisible(Edge *edge, const Transaction *transaction, View view) {
  bool exists = true;
  bool deleted = true;
  Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{edge->lock};
    deleted = edge->deleted();
    delta = edge->delta();
  }
  ApplyDeltasForRead(transaction, delta, view, [&](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::SET_PROPERTY:
      case Delta::Action::ADD_IN_EDGE:
      case Delta::Action::ADD_OUT_EDGE:
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
        break;
      case Delta::Action::RECREATE_OBJECT: {
        deleted = false;
        break;
      }
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT: {
        exists = false;
        break;
      }
    }
  });
  return exists && !deleted;
}

}  // namespace memgraph::storage

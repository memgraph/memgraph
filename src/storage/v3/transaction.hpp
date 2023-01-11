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

#include <limits>
#include <list>
#include <memory>

#include "coordinator/hybrid_logical_clock.hpp"
#include "storage/v3/delta.hpp"
#include "storage/v3/edge.hpp"
#include "storage/v3/isolation_level.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/vertex.hpp"
#include "storage/v3/view.hpp"

namespace memgraph::storage::v3 {

struct CommitInfo {
  bool is_locally_committed{false};
  coordinator::Hlc start_or_commit_timestamp;
};

struct Transaction {
  Transaction(coordinator::Hlc start_timestamp, CommitInfo new_commit_info, uint64_t command_id, bool must_abort,
              bool is_aborted, IsolationLevel isolation_level)
      : start_timestamp{start_timestamp},
        commit_info{std::make_unique<CommitInfo>(new_commit_info)},
        command_id(command_id),
        deltas(CopyDeltas(commit_info.get())),
        must_abort(must_abort),
        is_aborted(is_aborted),
        isolation_level(isolation_level){};

  Transaction(coordinator::Hlc start_timestamp, IsolationLevel isolation_level)
      : start_timestamp(start_timestamp),
        commit_info(std::make_unique<CommitInfo>(CommitInfo{false, {start_timestamp}})),
        command_id(0),
        must_abort(false),
        is_aborted(false),
        isolation_level(isolation_level) {}

  Transaction(Transaction &&other) noexcept
      : start_timestamp(other.start_timestamp),
        commit_info(std::move(other.commit_info)),
        command_id(other.command_id),
        deltas(std::move(other.deltas)),
        must_abort(other.must_abort),
        is_aborted(other.is_aborted),
        isolation_level(other.isolation_level) {}

  Transaction(const Transaction &) = delete;
  Transaction &operator=(const Transaction &) = delete;
  Transaction &operator=(Transaction &&other) = delete;

  ~Transaction() {}

  std::list<Delta> CopyDeltas(CommitInfo *commit_info) const {
    // TODO This does not solve the next and prev deltas that also need to be set
    std::list<Delta> copied_deltas;
    for (const auto &delta : deltas) {
      switch (delta.action) {
        case Delta::Action::DELETE_OBJECT:
          copied_deltas.emplace_back(Delta::DeleteObjectTag{}, commit_info, command_id);
          break;
        case Delta::Action::RECREATE_OBJECT:
          copied_deltas.emplace_back(Delta::RecreateObjectTag{}, commit_info, command_id);
          break;
        case Delta::Action::ADD_LABEL:
          copied_deltas.emplace_back(Delta::AddLabelTag{}, delta.label, commit_info, command_id);
          break;
        case Delta::Action::REMOVE_LABEL:
          copied_deltas.emplace_back(Delta::RemoveLabelTag{}, delta.label, commit_info, command_id);
          break;
        case Delta::Action::ADD_IN_EDGE:
          copied_deltas.emplace_back(Delta::AddInEdgeTag{}, delta.vertex_edge.edge_type, delta.vertex_edge.vertex_id,
                                     delta.vertex_edge.edge, commit_info, command_id);
          break;
        case Delta::Action::ADD_OUT_EDGE:
          copied_deltas.emplace_back(Delta::AddOutEdgeTag{}, delta.vertex_edge.edge_type, delta.vertex_edge.vertex_id,
                                     delta.vertex_edge.edge, commit_info, command_id);
          break;
        case Delta::Action::REMOVE_IN_EDGE:
          copied_deltas.emplace_back(Delta::RemoveInEdgeTag{}, delta.vertex_edge.edge_type, delta.vertex_edge.vertex_id,
                                     delta.vertex_edge.edge, commit_info, command_id);
          break;
        case Delta::Action::REMOVE_OUT_EDGE:
          copied_deltas.emplace_back(Delta::RemoveOutEdgeTag{}, delta.vertex_edge.edge_type,
                                     delta.vertex_edge.vertex_id, delta.vertex_edge.edge, commit_info, command_id);
          break;
        case Delta::Action::SET_PROPERTY:
          copied_deltas.emplace_back(Delta::SetPropertyTag{}, delta.property.key, delta.property.value, commit_info,
                                     command_id);
          break;
      }
    }
    return copied_deltas;
  }

  // This does not solve the whole problem of copying deltas
  Transaction Clone() const {
    return {start_timestamp, *commit_info, command_id, must_abort, is_aborted, isolation_level};
  }

  coordinator::Hlc start_timestamp;
  std::unique_ptr<CommitInfo> commit_info;
  uint64_t command_id;
  std::list<Delta> deltas;
  bool must_abort;
  bool is_aborted;
  IsolationLevel isolation_level;
};

// Relies on start timestamps are unique
inline bool operator==(const Transaction &first, const Transaction &second) {
  return first.start_timestamp == second.start_timestamp;
}
inline bool operator<(const Transaction &first, const Transaction &second) {
  return first.start_timestamp < second.start_timestamp;
}
inline bool operator==(const Transaction &first, const uint64_t second) { return first.start_timestamp == second; }
inline bool operator<(const Transaction &first, const uint64_t second) { return first.start_timestamp < second; }

}  // namespace memgraph::storage::v3

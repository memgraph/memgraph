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

#include "storage/v2/storage_error.hpp"

#include <fmt/format.h>

namespace memgraph::storage {

auto ReplicaFailureReasonToString(ReplicaFailureReason reason) -> std::string {
  switch (reason) {
    case ReplicaFailureReason::NOT_IN_SYNC:
      return "replica is not reachable or not in sync with the main";
    case ReplicaFailureReason::FAILED_TO_GET_LOCK:
      return "failed to obtain RPC lock (another transaction is in progress)";
    case ReplicaFailureReason::RPC_ERROR:
      return "RPC communication error";
    case ReplicaFailureReason::DIVERGED:
      return "replica has diverged from main";
    case ReplicaFailureReason::TIMEOUT:
      return "RPC timeout while replicating";
  }
  std::unreachable();
}

auto FormatReplicationError(ReplicationError const &error) -> std::string {
  struct GroupKey {
    std::string mode;
    std::string reason;

    auto operator==(GroupKey const &o) const -> bool { return mode == o.mode && reason == o.reason; }
  };

  std::vector<std::pair<GroupKey, std::vector<std::string>>> groups;
  bool has_timeout = false;
  std::vector<std::string> timed_out_names;

  for (auto const &failure : error.failures) {
    GroupKey key{.mode = failure.mode, .reason = ReplicaFailureReasonToString(failure.reason)};
    auto it = std::ranges::find(groups, key, &std::pair<GroupKey, std::vector<std::string>>::first);
    if (it != groups.end()) {
      it->second.push_back(failure.name);
    } else {
      groups.emplace_back(std::move(key), std::vector<std::string>{failure.name});
    }
    if (failure.reason == ReplicaFailureReason::TIMEOUT) {
      has_timeout = true;
      timed_out_names.push_back(failure.name);
    }
  }

  std::string msg;
  for (auto const &[key, names] : groups) {
    if (!msg.empty()) msg += " ";
    if (names.size() == 1) {
      msg += fmt::format("Failed to replicate to {} replica '{}': {}.", key.mode, names[0], key.reason);
    } else {
      std::string joined;
      for (size_t i = 0; i < names.size(); ++i) {
        if (i > 0) joined += (i == names.size() - 1) ? " and " : ", ";
        joined += fmt::format("'{}'", names[i]);
      }
      auto plural_reason = key.reason;
      if (plural_reason.starts_with("replica is")) {
        plural_reason.replace(0, 10, "replicas are");
      } else if (plural_reason.starts_with("replica has")) {
        plural_reason.replace(0, 11, "replicas have");
      }
      msg += fmt::format("Failed to replicate to {} replicas {}: {}.", key.mode, joined, plural_reason);
    }
  }

  auto total_replicas = error.failures.size();
  msg +=
      total_replicas == 1 ? " Replica will be recovered automatically." : " Replicas will be recovered automatically.";
  msg += error.transaction_committed ? " Transaction is still committed on the main instance and other alive replicas."
                                     : " Transaction was aborted on all instances.";

  if (has_timeout) {
    std::string timed_out_joined;
    for (size_t i = 0; i < timed_out_names.size(); ++i) {
      if (i > 0) timed_out_joined += ", ";
      timed_out_joined += timed_out_names[i];
    }
    msg += fmt::format(
        " Main reached an RPC timeout while replicating to [{}]."
        " One possible reason for this error is that the replica is down and in that case make sure to recover it."
        " If all of your replicas are up and running normally, then please try setting a smaller parameter value for"
        " 'deltas_batch_progress_size' using 'SET COORDINATOR SETTING' query on the coordinator.",
        timed_out_joined);
  }

  return msg;
}

}  // namespace memgraph::storage

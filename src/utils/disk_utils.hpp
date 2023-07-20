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

#include "storage/v2/delta.hpp"

namespace memgraph::utils {

inline std::optional<std::string> GetOldDiskKeyOrNull(storage::Delta *head) {
  while (head->next != nullptr) {
    head = head->next;
  }
  if (head->action == storage::Delta::Action::DELETE_DESERIALIZED_OBJECT) {
    return head->old_disk_key;
  }
  return std::nullopt;
}

}  // namespace memgraph::utils

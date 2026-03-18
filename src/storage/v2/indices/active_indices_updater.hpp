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

#pragma once

#include <memory>

#include "storage/v2/indices/edge_property_index.hpp"
#include "storage/v2/indices/edge_type_index.hpp"
#include "storage/v2/indices/edge_type_property_index.hpp"
#include "storage/v2/indices/label_index.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

struct ActiveIndices;
using ActiveIndicesPtr = std::shared_ptr<ActiveIndices const>;
using ActiveIndicesStore = utils::Synchronized<ActiveIndicesPtr, utils::WritePrioritizedRWLock>;

struct ActiveIndicesUpdater {
  explicit ActiveIndicesUpdater(ActiveIndicesStore &active_indices) : active_indices_(active_indices) {}

  void operator()(std::shared_ptr<LabelIndexActiveIndices> const &x) const;
  void operator()(std::shared_ptr<LabelPropertyIndex::ActiveIndices> const &x) const;
  void operator()(std::shared_ptr<EdgeTypeIndex::ActiveIndices> const &x) const;
  void operator()(std::shared_ptr<EdgeTypePropertyIndex::ActiveIndices> const &x) const;
  void operator()(std::shared_ptr<EdgePropertyIndex::ActiveIndices> const &x) const;

 private:
  ActiveIndicesStore &active_indices_;
};
}  // namespace memgraph::storage

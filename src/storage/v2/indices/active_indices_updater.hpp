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

#include "storage/v2/indices/active_indices.hpp"

namespace memgraph::storage {

struct ActiveIndicesUpdater {
  explicit ActiveIndicesUpdater(ActiveIndicesStore &active_indices) : active_indices_(active_indices) {}

  void operator()(std::shared_ptr<LabelIndexActiveIndices> const &x) const;
  void operator()(std::shared_ptr<LabelPropertyIndexActiveIndices> const &x) const;
  void operator()(std::shared_ptr<EdgeTypeIndexActiveIndices> const &x) const;
  void operator()(std::shared_ptr<EdgeTypePropertyIndexActiveIndices> const &x) const;
  void operator()(std::shared_ptr<EdgePropertyIndexActiveIndices> const &x) const;
  void operator()(std::shared_ptr<TextIndexActiveIndices> const &x) const;
  void operator()(std::shared_ptr<TextEdgeIndexActiveIndices> const &x) const;
  void operator()(std::shared_ptr<PointIndexActiveIndices> const &x) const;

 private:
  ActiveIndicesStore &active_indices_;
};

}  // namespace memgraph::storage

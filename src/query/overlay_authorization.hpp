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

#include <optional>
#include <utility>
#include <vector>

#include "query/auth_checker.hpp"
#include "query/virtual_node.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::query {

// The single source of truth for whether a property of an overlay or synthetic node is READ-visible
// under fine-grained access control. Resolve once with For() (which may read the origin's labels),
// then decide per property with IsReadable().
//
// Resolving once is deliberate: the bulk callers (properties()/keys() and Bolt serialization) iterate
// every property, so a per-property predicate that re-read the origin's labels would turn one label
// read into N. A label-read failure is carried in the Result For() returns, so each caller surfaces it
// in the way its context requires - the read-path evaluator fails closed, the property functions throw,
// Bolt propagates the storage error - while the visibility decision itself lives in exactly one place.
class OverlayReadAuthorization {
 public:
  // Resolves the authorization context. A null checker or a synthetic node (no origin) mints no
  // real-graph data, so every property is readable. Otherwise the origin's labels are read (the only
  // fallible step); a failure is returned so the caller can surface it.
  static storage::Result<OverlayReadAuthorization> For(const VirtualNode &node, storage::View view,
                                                       const FineGrainedAuthChecker *checker) {
    if (checker == nullptr || !node.HasOrigin()) return OverlayReadAuthorization{nullptr, std::nullopt};
    auto maybe_labels = node.Origin()->Labels(view);
    if (!maybe_labels) return std::unexpected{maybe_labels.error()};
    return OverlayReadAuthorization{checker, std::vector<storage::LabelId>{maybe_labels->begin(), maybe_labels->end()}};
  }

  // An overlay-bound override is the author's own computed value (any origin data it derived from was
  // itself FGA-checked at construction), so it is always readable. Otherwise an origin-read-through
  // property gets the origin's per-property READ permission over the origin's labels.
  [[nodiscard]] bool IsReadable(const VirtualNode &node, storage::PropertyId prop) const {
    if (checker_ == nullptr || !origin_labels_) return true;
    if (node.IsOverlayBound(prop)) return true;
    return checker_->HasPropertyPermission(*origin_labels_, prop, AuthQuery::PropertyPermissionType::READ);
  }

 private:
  OverlayReadAuthorization(const FineGrainedAuthChecker *checker,
                           std::optional<std::vector<storage::LabelId>> origin_labels)
      : checker_(checker), origin_labels_(std::move(origin_labels)) {}

  const FineGrainedAuthChecker *checker_;
  std::optional<std::vector<storage::LabelId>> origin_labels_;
};

}  // namespace memgraph::query

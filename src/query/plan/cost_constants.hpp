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

namespace memgraph::query::plan {

/// Cost multipliers for different operator types.
/// Used by CostEstimator to calculate plan execution cost.
namespace CostParam {
inline constexpr double kMinimumCost{0.001};  // everything has some runtime cost
inline constexpr double kScanAll{1.0};
inline constexpr double kScanAllByLabel{1.1};
inline constexpr double kScanAllByLabelProperties{1.1};
inline constexpr double kScanAllByPointDistance{1.1};
inline constexpr double kScanAllByPointWithinbbox{1.1};
inline constexpr double kScanAllByEdgeType{1.1};
inline constexpr double kScanAllByEdgeTypePropertyValue{1.1};
inline constexpr double kScanAllByEdgeTypePropertyRange{1.1};
inline constexpr double kScanAllByEdgeTypeProperty{1.1};
inline constexpr double kScanAllByEdgePropertyValue{1.1};
inline constexpr double kScanAllByEdgePropertyRange{1.1};
inline constexpr double kScanAllByEdgeProperty{1.1};
inline constexpr double kExpand{2.0};
inline constexpr double kExpandVariable{3.0};
inline constexpr double kFilter{1.5};
inline constexpr double kEdgeUniquenessFilter{1.5};
inline constexpr double kUnwind{1.3};
inline constexpr double kForeach{1.0};
inline constexpr double kUnion{1.0};
inline constexpr double kSubquery{1.0};
}  // namespace CostParam

/// Cardinality multipliers for estimating output row counts.
namespace CardParam {
inline constexpr double kExpand{3.0};
inline constexpr double kExpandVariable{9.0};
inline constexpr double kFilter{0.25};
inline constexpr double kEdgeUniquenessFilter{0.95};
}  // namespace CardParam

/// Miscellaneous estimation parameters.
namespace MiscParam {
inline constexpr double kUnwindNoLiteral{10.0};
inline constexpr double kForeachNoLiteral{10.0};
}  // namespace MiscParam

}  // namespace memgraph::query::plan

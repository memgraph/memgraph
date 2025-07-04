// Copyright 2025 Memgraph Ltd.
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

#include <cstdint>
#include <string>
#include <type_traits>

namespace memgraph::storage::durability {

// The current version of snapshot and WAL encoding / decoding.
// IMPORTANT: Please bump this version for every snapshot and/or WAL format
// change!!!
const uint64_t kVersion{27};

const uint64_t kOldestSupportedVersion{14};
const uint64_t kUniqueConstraintVersion{13};
const uint64_t kMetaDataDeltasHaveExplicitTransactionEnd{16};
// Edge-type index version is 17. Edge-type property index version is 18.
// But they are written in the same section.
const uint64_t kEdgeIndicesVersion{17};
const uint64_t kEnumsVersion{18};
const uint64_t kPointDataType{19};
const uint64_t kPointIndexAndTypeConstraints{20};
const uint64_t kSridCartesian3DCorrected{21};
const uint64_t kEdgeSetDeltaWithVertexInfo{21};
const uint64_t kVectorIndex{22};
const uint64_t kDurableTS{23};
const uint64_t kCompositeIndicesForLabelProperties{24};
const uint64_t kEdgePropIndex{24};
const uint64_t kNestedIndices{25};
const uint64_t kVectorIndexWithScalarKind{26};
const uint64_t kVectorIndexWithEdgeTypeProp{27};

// Magic values written to the start of a snapshot/WAL file to identify it.
const std::string kSnapshotMagic{"MGsn"};
const std::string kWalMagic{"MGwl"};

static_assert(std::is_same_v<uint8_t, unsigned char>);

// Checks whether the loaded snapshot/WAL version is supported.
inline bool IsVersionSupported(uint64_t version) { return version >= kOldestSupportedVersion && version <= kVersion; }

}  // namespace memgraph::storage::durability

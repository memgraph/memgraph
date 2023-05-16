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

#include "storage/v2/storage.hpp"
#include "storage/v2/view.hpp"

size_t CountVertices(memgraph::storage::Storage::Accessor &storage_accessor, memgraph::storage::View view);

inline constexpr std::array storage_modes{memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL,
                                          memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL};

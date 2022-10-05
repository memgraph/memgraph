// Copyright 2022 Memgraph Ltd.
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

namespace memgraph::io {
/// Protocol:
///   crc32:        4 bytes
///   len:          8 bytes
///   buffer:       <len bytes>

std::optional<std::pair<size_t, size_t>> make_frame(char *ptr, size_t len) { return std::nullopt; }
}  // namespace memgraph::io

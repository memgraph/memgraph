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

#include <chrono>
#include <string>

namespace integrations {
constexpr int64_t kDefaultCheckBatchLimit{1};
constexpr std::chrono::milliseconds kDefaultCheckTimeout{30000};
constexpr std::chrono::milliseconds kMinimumInterval{1};
constexpr int64_t kMinimumSize{1};
const std::string kReducted{"<REDUCTED>"};
}  // namespace integrations

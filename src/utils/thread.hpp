// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file
#pragma once

#include <string>

namespace utils {

constexpr size_t GetMaxThreadNameSize() { return 16; }

/// This function sets the thread name of the calling thread.
/// Beware, the name length limit is 16 characters!
void ThreadSetName(const std::string &name);

};  // namespace utils

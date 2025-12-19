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

#include <iosfwd>

namespace memgraph::utils {

/**
 * Dump stacktrace to the stream and abort the program. For more details
 * about the abort please take a look at
 * http://en.cppreference.com/w/cpp/utility/program/abort.
 */
void TerminateHandler(std::ostream &stream) noexcept;

void TerminateHandler() noexcept;

}  // namespace memgraph::utils

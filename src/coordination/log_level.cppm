// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

module;

#include <cstdint>

export module memgraph.coordination.log_level;

#ifdef MG_ENTERPRISE

export namespace memgraph::coordination {

/**
 NuRaft log level:
 *    Trace:    6
 *    Debug:    5
 *    Info:     4
 *    Warning:  3
 *    Error:    2
 *    Fatal:    1
 */

enum class nuraft_log_level : uint8_t { FATAL = 1, ERROR = 2, WARNING = 3, INFO = 4, DEBUG = 5, TRACE = 6 };

}  // namespace memgraph::coordination

#endif

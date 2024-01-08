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

#include "replication.hpp"

#ifdef MG_ENTERPRISE
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(coordinator, false, "Controls whether the instance is a replication coordinator.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_uint32(coordinator_server_port, 0, "Port on which coordinator servers will be started.");
#endif

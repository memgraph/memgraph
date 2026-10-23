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

#include "replication.hpp"

#include "gflags/gflags.h"

#include "utils/flag_validation.hpp"

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_uint64(replication_replica_check_frequency_sec, 1,
                        "The time duration between two replica checks/pings. Must be at least 1 since the check is "
                        "what schedules catch-up of ASYNC replicas. NOTE: The MAIN instance allocates a new thread "
                        "for each REPLICA.",
                        FLAG_IN_RANGE(1, 24UL * 3600));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_uint64(replication_async_recovery_interval_sec, 10,
                        "Minimum number of seconds between two recovery rounds of an ASYNC replica. Each round ships "
                        "the durability files the replica is missing, so this bounds how often MAIN re-sends its "
                        "current WAL under sustained writes. 0 recovers as soon as the replica is seen behind.",
                        FLAG_IN_RANGE(0, 3600));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(replication_restore_state_on_startup, true, "Restore replication state on startup, e.g. recover replica");

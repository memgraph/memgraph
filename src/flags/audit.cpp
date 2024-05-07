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
#include "flags/audit.hpp"

#include "audit/log.hpp"

#include "utils/flag_validation.hpp"

// Audit logging flags.
#ifdef MG_ENTERPRISE
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(audit_enabled, false, "Set to true to enable audit logging.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(audit_buffer_size, memgraph::audit::kBufferSizeDefault,
                       "Maximum number of items in the audit log buffer.", FLAG_IN_RANGE(1, INT32_MAX));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(audit_buffer_flush_interval_ms, memgraph::audit::kBufferFlushIntervalMillisDefault,
                       "Interval (in milliseconds) used for flushing the audit log buffer.",
                       FLAG_IN_RANGE(10, INT32_MAX));
#endif

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
#include "flags/audit.hpp"

#include <gflags/gflags.h>
#include <cstdint>
#include <iostream>

#include "utils/flag_validation.hpp"
#include "utils/string.hpp"

const uint64_t kBufferSizeDefault = 100'000;
const uint64_t kBufferFlushIntervalMillisDefault = 200;
constexpr auto kAuditTimestampEpoch = "epoch";
constexpr auto kAuditTimestampIso8601 = "iso8601";

// Audit logging flags.
#ifdef MG_ENTERPRISE
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(audit_enabled, false, "Set to true to enable audit logging.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(audit_buffer_size, kBufferSizeDefault, "Maximum number of items in the audit log buffer.",
                       FLAG_IN_RANGE(1, INT32_MAX));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(audit_buffer_flush_interval_ms, kBufferFlushIntervalMillisDefault,
                       "Interval (in milliseconds) used for flushing the audit log buffer.",
                       FLAG_IN_RANGE(10, INT32_MAX));
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(audit_log_file, "",
              "Path to where the audit log should be stored. "
              "If empty, defaults to '<data-directory>/audit/audit.log'.");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_string(audit_log_timestamp_format, kAuditTimestampEpoch,
                        "Timestamp format for audit log entries. Options: epoch, iso8601.", {
                          auto const lower = memgraph::utils::ToLowerCase(value);
                          if (lower != kAuditTimestampEpoch && lower != kAuditTimestampIso8601) {
                            std::cout << "Expected --" << flagname << " to be 'epoch' or 'iso8601'\n";
                            return false;
                          }
                          return true;
                        });
#endif

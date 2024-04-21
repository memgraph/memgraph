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

#include <chrono>
#include <cstdint>

#include "storage/v2/temporal.hpp"

#include "utils/temporal.hpp"

namespace memgraph::storage {
TemporalData::TemporalData(TemporalType type, int64_t microseconds) : type{type}, microseconds{microseconds} {}

ZonedTemporalData::ZonedTemporalData(ZonedTemporalType type,
                                     std::chrono::sys_time<std::chrono::microseconds> microseconds,
                                     utils::Timezone timezone)
    : type{type}, microseconds{microseconds}, timezone{timezone} {}

int64_t ZonedTemporalData::IntMicroseconds() const { return microseconds.time_since_epoch().count(); }

std::string ZonedTemporalData::TimezoneToString() const { return timezone.ToString(); }

}  // namespace memgraph::storage

// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/event_trigger.hpp"

#include "utils/timestamp.hpp"

namespace memgraph::metrics {

// Initialize array for the global array with all values set to 0
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<double> global_one_shot_array[(int)OneShotEvents::kNum]{};
// Initialize one shot events
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
EventOneShot global_one_shot_events(global_one_shot_array);

void FirstSuccessfulQuery() {
  constexpr double kBusyMark = -1.0;
  // Mark as busy (if it fails, someone already set it)
  if (global_one_shot_events.Trigger(OneShotEvents::kFirstSuccessfulQueryTs, kBusyMark)) {
    global_one_shot_events.Trigger(OneShotEvents::kFirstSuccessfulQueryTs,
                                   utils::Timestamp::Now().SecWithNsecSinceTheEpoch(), kBusyMark);
  }
}
void FirstFailedQuery() {
  constexpr double kBusyMark = -1.0;
  // Mark as busy (if it fails, someone already set it)
  if (global_one_shot_events.Trigger(OneShotEvents::kFirstFailedQueryTs, kBusyMark)) {
    global_one_shot_events.Trigger(OneShotEvents::kFirstFailedQueryTs,
                                   utils::Timestamp::Now().SecWithNsecSinceTheEpoch(), kBusyMark);
  }
}
}  // namespace memgraph::metrics

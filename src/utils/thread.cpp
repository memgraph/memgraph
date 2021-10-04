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

#include "utils/thread.hpp"

#include <sys/prctl.h>

#include "utils/logging.hpp"

namespace utils {

void ThreadSetName(const std::string &name) {
  constexpr auto max_name_length = GetMaxThreadNameSize();
  MG_ASSERT(name.size() <= max_name_length, "Thread name '{}' is too long", max_name_length);

  if (prctl(PR_SET_NAME, name.c_str()) != 0) {
    spdlog::warn("Couldn't set thread name: {}!", name);
  }
}

}  // namespace utils

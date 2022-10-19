// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>
#include <spdlog/cfg/env.h>
#include <utils/logging.hpp>

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  memgraph::logging::RedirectToStderr();
  spdlog::set_level(spdlog::level::trace);
  spdlog::cfg::load_env_levels();
  return RUN_ALL_TESTS();
}

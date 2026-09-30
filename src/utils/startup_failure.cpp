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

#include "utils/startup_failure.hpp"

#include <cstdlib>
#include <iostream>

#include <spdlog/spdlog.h>

namespace memgraph::utils {

void FailStartup(ExitCode code, std::string_view message) {
  // Best-effort to the log: spdlog::critical() would dereference a null
  // default logger, so go through the null-safe shared_ptr getter.  A
  // sinkless logger is a safe no-op.  stderr below is the guaranteed channel.
  if (auto const logger = spdlog::default_logger()) {
    logger->critical("{}", message);
    logger->flush();
  }
  // Deliberate flush: the message must reach stderr before std::exit.
  std::cerr << message << std::endl;  // NOLINT(performance-avoid-endl)
  std::exit(AsExitStatus(code));
}

}  // namespace memgraph::utils

#pragma once

#include <cstdint>
#include <cstdlib>
#include <memory>

#include "logging/default.hpp"

namespace bolt {

class Session;

class State {
 public:
  using uptr = std::unique_ptr<State>;

  State() = default;
  State(Logger logger) : logger(logger) {}

  virtual ~State() = default;

  virtual State* run(Session& session) = 0;

 protected:
  Logger logger;
};
}

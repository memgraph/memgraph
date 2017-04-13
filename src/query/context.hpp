#pragma once

#include "antlr4-runtime.h"
#include "database/graph_db_accessor.hpp"

namespace query {

/**
 * Future-proofing for the time when we'll actually have
 * something to configure in query execution.
 */
struct Config {
};

class Context {
public:
  Context(Config config, GraphDbAccessor &db_accessor)
      : config_(config), db_accessor_(db_accessor) {}
  Config config_;
  GraphDbAccessor &db_accessor_;
};
}

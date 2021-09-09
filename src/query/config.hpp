#pragma once

#include <string>

namespace query {
struct InterpreterConfig {
  struct Query {
    bool allow_load_csv{true};
  } query;

  // The default execution timeout is 10 minutes.
  double execution_timeout_sec{600.0};
};
}  // namespace query

#pragma once

namespace query {
struct InterpreterConfig {
  struct Query {
    bool allow_load_csv{true};
  } query;

  // The default execution timeout is 3 minutes.
  double execution_timeout_sec{180.0};
};
}  // namespace query

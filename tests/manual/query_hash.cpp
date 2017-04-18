#include <iostream>
#include <vector>

#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "query/preprocessor.hpp"
#include "utils/command_line/arguments.hpp"
#include "utils/type_discovery.hpp"

/**
 * Useful when somebody wants to get a hash for some query.
 *
 * Usage:
 *     ./query_hash -q "CREATE (n {name: \"test\n"}) RETURN n"
 */
int main(int argc, char **argv) {
  logging::init_sync();
  logging::log->pipe(std::make_unique<Stdout>());

  // init args
  REGISTER_ARGS(argc, argv);

  // take query from input args
  auto query = GET_ARG("-q", "CREATE (n) RETURN n").get_string();

  // run preprocessing
  QueryPreprocessor preprocessor;
  auto preprocessed = preprocessor.preprocess(query);

  // print query, stripped query, hash and variable values (propertie values)
  std::cout << fmt::format("Query: {}\n", query);
  std::cout << fmt::format("Stripped query: {}\n", preprocessed.query);
  std::cout << fmt::format("Query hash: {}\n", preprocessed.hash);
  std::cout << fmt::format("Property values:\n");
  for (int i = 0; i < static_cast<int>(preprocessed.arguments.Size()); ++i) {
    fmt::format("    {}", preprocessed.arguments.At(i));
  }
  std::cout << std::endl;

  return 0;
}

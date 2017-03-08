#include <iostream>
#include <vector>

#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "query/preprocessor.hpp"
#include "utils/command_line/arguments.hpp"
#include "utils/string/file.hpp"
#include "utils/type_discovery.hpp"
#include "utils/variadic/variadic.hpp"

using utils::println;

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
  println("Query: ", query);
  println("Stripped query: ", preprocessed.query);
  println("Query hash: ", preprocessed.hash);
  println("Property values:");
  for (int i = 0; i < preprocessed.arguments.Size(); ++i)
    println("    ", preprocessed.arguments.At(i));
  println("");

  return 0;
}

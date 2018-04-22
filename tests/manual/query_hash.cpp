#include <iostream>
#include <vector>

#include <glog/logging.h>

#include "query/frontend/stripped.hpp"

DEFINE_string(q, "CREATE (n) RETURN n", "Query");

/**
 * Useful when somebody wants to get a hash for some query.
 *
 * Usage:
 *     ./query_hash -q "CREATE (n {name: \"test\n"}) RETURN n"
 */
int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // take query from input args
  auto query = FLAGS_q;

  // run preprocessing
  query::StrippedQuery preprocessed(query);

  // print query, stripped query, hash and variable values (propertie values)
  std::cout << fmt::format("Query: {}\n", query);
  std::cout << fmt::format("Stripped query: {}\n", preprocessed.query());
  std::cout << fmt::format("Query hash: {}\n", preprocessed.hash());
  std::cout << fmt::format("Property values:\n");
  for (int i = 0; i < preprocessed.literals().size(); ++i) {
    fmt::format("    {}", preprocessed.literals().At(i).second);
  }
  std::cout << std::endl;

  return 0;
}

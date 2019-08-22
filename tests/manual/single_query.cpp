#include "communication/result_stream_faker.hpp"
#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"
#include "query/interpreter.hpp"

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // parse the first cmd line argument as the query
  if (argc < 2) {
    std::cout << "Usage: ./single_query 'RETURN \"query here\"'" << std::endl;
    exit(1);
  }
  database::GraphDb db;
  auto dba = db.Access();
  ResultStreamFaker<query::TypedValue> stream;
  auto results =
      query::Interpreter()(argv[1], dba, {}, false, utils::NewDeleteResource());
  stream.Header(results.header());
  results.PullAll(stream);
  stream.Summary(results.summary());
  std::cout << stream;
  return 0;
}

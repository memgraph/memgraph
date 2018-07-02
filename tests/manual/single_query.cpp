#include "communication/result_stream_faker.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "query/interpreter.hpp"

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // parse the first cmd line argument as the query
  if (argc < 2) {
    std::cout << "Usage: ./single_query 'RETURN \"query here\"'" << std::endl;
    exit(1);
  }
  database::SingleNode db;
  database::GraphDbAccessor dba(db);
  ResultStreamFaker<query::TypedValue> results;
  query::Interpreter{db}(argv[1], dba, {}, false).PullAll(results);
  std::cout << results;
  return 0;
}

#include "communication/result_stream_faker.hpp"
#include "query/interpreter.hpp"
#include "storage/v2/storage.hpp"

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // parse the first cmd line argument as the query
  if (argc < 2) {
    std::cout << "Usage: ./single_query 'RETURN \"query here\"'" << std::endl;
    exit(1);
  }

  storage::Storage db;
  query::InterpreterContext interpreter_context{&db};
  query::Interpreter interpreter{&interpreter_context};

  ResultStreamFaker stream(&db);
  auto [header, _, qid] = interpreter.Prepare(argv[1], {});
  stream.Header(header);
  auto summary = interpreter.PullAll(&stream);
  stream.Summary(summary);
  std::cout << stream;

  return 0;
}

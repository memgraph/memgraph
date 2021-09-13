#include "communication/result_stream_faker.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage.hpp"
#include "utils/on_scope_exit.hpp"

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // parse the first cmd line argument as the query
  if (argc < 2) {
    std::cout << "Usage: ./single_query 'RETURN \"query here\"'" << std::endl;
    exit(1);
  }

  storage::Storage db;
  auto data_directory = std::filesystem::temp_directory_path() / "single_query_test";
  utils::OnScopeExit([&data_directory] { std::filesystem::remove_all(data_directory); });
  utils::Settings settings{data_directory / "settings"};
  query::InterpreterContext interpreter_context{&db, query::InterpreterConfig{}, data_directory,
                                                "non existing bootstrap servers", &settings};
  query::Interpreter interpreter{&interpreter_context};

  ResultStreamFaker stream(&db);
  auto [header, _, qid] = interpreter.Prepare(argv[1], {}, nullptr);
  stream.Header(header);
  auto summary = interpreter.PullAll(&stream);
  stream.Summary(summary);
  std::cout << stream;

  return 0;
}

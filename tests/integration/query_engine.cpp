#define HARDCODED_OUTPUT_STREAM

#include <gflags/gflags.h>

#include "dbms/dbms.hpp"
#include "query_engine_common.hpp"

DECLARE_bool(interpret);
DECLARE_string(compile_directory);
DEFINE_string(q, "../data/queries/core/mg_basic_002.txt",
              "Path to warm up queries");
DEFINE_string(i, "../integration/hardcoded_query",
              "Path to folder with query implementations");

using namespace std::chrono_literals;
using namespace tests::integration;

/**
 * IMPORTANT: tests only compilation and executability of implemented
 * hard code queries (not correctnes of implementation)
 *
 * NOTE: The correctnes can be tested by custom Stream object.
 * NOTE: This test will be usefull to test generated query plans.
 */
int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  /**
   * init engine
   */
  init_logging("IntegrationQueryEngine");
  // Manually set config compile_path to avoid loading whole config file with
  // the test.
  FLAGS_compile_directory = "../compiled/";
  // Set the interpret to false to avoid calling the interpreter which doesn't
  // support all the queries yet.
  FLAGS_interpret = false;
  Dbms dbms;
  StreamT stream(std::cout);
  QueryEngineT query_engine;
  // IMPORTANT: PrintRecordStream can be replaces with a smarter
  // object that can test the results

  WarmUpEngine(query_engine, dbms, stream);

  return 0;
}

#define HARDCODED_OUTPUT_STREAM

#include "config/config.hpp"
#include "dbms/dbms.hpp"
#include "query_engine_common.hpp"

using namespace std::chrono_literals;
using namespace tests::integration;

Logger logger;

/**
 * IMPORTANT: tests only compilation and executability of implemented
 * hard code queries (not correctnes of implementation)
 *
 * NOTE: The correctnes can be tested by custom Stream object.
 * NOTE: This test will be usefull to test generated query plans.
 */
int main(int argc, char *argv[]) {
  /**
   * init arguments
   */
  REGISTER_ARGS(argc, argv);

  /**
   * init engine
   */
  auto log = init_logging("IntegrationQueryEngine");
  // Manually set config compile_path to avoid loading whole config file with
  // the test.
  CONFIG(config::COMPILE_PATH) = "../compiled/";
  Dbms dbms;
  StreamT stream(std::cout);
  QueryEngineT query_engine;
  // IMPORTANT: PrintRecordStream can be replaces with a smarter
  // object that can test the results

  auto db_accessor = dbms.active();
  WarmUpEngine(log, query_engine, db_accessor, stream);

  return 0;
}

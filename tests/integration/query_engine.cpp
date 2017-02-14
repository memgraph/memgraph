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
int main(int argc, char *argv[])
{
    /**
     * init arguments
     */
    REGISTER_ARGS(argc, argv);

    /**
     * init engine
     */
    auto log = init_logging("IntegrationQueryEngine");
    Db db;
    StreamT stream(std::cout);
    QueryEngineT query_engine;
    // IMPORTANT: PrintRecordStream can be replaces with a smarter
    // object that can test the results
    
    WarmUpEngine(log, query_engine, db, stream);

    return 0;
}

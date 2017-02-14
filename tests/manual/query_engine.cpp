#include "../integration/query_engine_common.hpp"

#include "utils/fswatcher.hpp"

using namespace std::chrono_literals;
using namespace tests::integration;

int main(int argc, char *argv[])
{
    // init arguments
    REGISTER_ARGS(argc, argv);

    // forlder with query implementations
    auto implementations_folder = fs::path(
        GET_ARG("-i", "tests/integration/hardcoded_query").get_string());

    // init engine
    auto log = init_logging("ManualQueryEngine");
    Db db;
    StreamT stream(std::cout);
    QueryEngineT query_engine;
    // IMPORTANT: PrintRecordStream can be replaces with a smarter
    // object that can test the results

    WarmUpEngine(log, query_engine, db, stream);

    // init watcher
    FSWatcher watcher;
    QueryPreprocessor preprocessor;

    int i = 0;
    watcher.watch(
        WatchDescriptor(implementations_folder, FSEventType::CloseNowrite),
        [&](FSEvent event) {
            i++; // bacause only close_no_write could be detected and this
            // call will cause close no write again
            if (i % 2 == 1)
            {
                // take only cpp files
                if (event.path.extension() != ".cpp")
                    return;

                auto query_mark = std::string("// Query: ");
                auto lines = read_lines(event.path);
                for (auto &line : lines)
                {
                    auto pos = line.find(query_mark);
                    if (pos == std::string::npos) continue;
                    auto query = line.substr(pos + query_mark.size());
                    log.info("Reload: {}", query);
                    query_engine.Unload(query);
                    try {
                        query_engine.ReloadCustom(query, event.path);
                        query_engine.Run(query, db, stream);
                    } catch (PlanCompilationException& e) {
                        log.info("Query compilation failed: {}", e.what());
                    } catch (std::exception& e) {
                        log.info("Query execution failed: unknown reason");
                    }
                    log.info("Number of available query plans: {}", query_engine.Size());
                }
            }
        });

    // TODO: watcher for injected query

    std::this_thread::sleep_for(1000s);

    watcher.stop();

    return 0;
}

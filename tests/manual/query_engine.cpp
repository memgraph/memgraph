#define HARDCODED_OUTPUT_STREAM
#include "../integration/query_engine_common.hpp"
#include "dbms/dbms.hpp"

#include "utils/fswatcher.hpp"

using namespace std::chrono_literals;
using namespace tests::integration;

int main(int argc, char *argv[]) {
  // init arguments
  REGISTER_ARGS(argc, argv);

  // forlder with query implementations
  auto implementations_folder =
      fs::path(GET_ARG("-i", "tests/integration/hardcoded_query").get_string());

  // init engine
  init_logging("ManualQueryEngine");
  Dbms dbms;
  StreamT stream(std::cout);  // inject path to data queries
  QueryEngineT query_engine;
  // IMPORTANT: PrintRecordStream can be replaces with a smarter
  // object that can test the results

  WarmUpEngine(query_engine, dbms, stream);

  // init watcher
  FSWatcher watcher;

  int i = 0;
  watcher.watch(
      WatchDescriptor(implementations_folder, FSEventType::All),
      [&](FSEvent event) {
        i++;  // bacause only close_no_write could be detected and this
        // call will cause close no write again
        if (i % 2 == 1) {
          // take only cpp files
          if (event.path.extension() != ".cpp") return;

          auto comment = std::string("// ");
          auto query_mark = comment + std::string("Query: ");
          auto lines = utils::ReadLines(event.path);
          for (int i = 0; i < (int)lines.size(); ++i) {
            // find query in the line
            auto &line = lines[i];
            auto pos = line.find(query_mark);
            // if query doesn't exist pass
            if (pos == std::string::npos) continue;
            auto query = utils::Trim(line.substr(pos + query_mark.size()));
            while (i + 1 < (int)lines.size() &&
                   lines[i + 1].find(comment) != std::string::npos) {
              query += lines[i + 1].substr(lines[i + 1].find(comment) +
                                           comment.length());
              ++i;
            }

            DLOG(INFO) << fmt::format("Reload: {}", query);
            query_engine.Unload(query);
            try {
              query_engine.ReloadCustom(query, event.path);
              auto db_accessor = dbms.active();
              query_engine.Run(query, *db_accessor, stream);
            } catch (query::PlanCompilationException &e) {
              DLOG(ERROR) << fmt::format("Query compilation failed: {}",
                                         e.what());
            } catch (std::exception &e) {
              DLOG(WARNING) << "Query execution failed: unknown reason";
            }
            DLOG(INFO) << fmt::format("Number of available query plans: {}",
                                      query_engine.Size());
          }
        }
      });

  // TODO: watcher for injected query

  std::this_thread::sleep_for(1000s);

  watcher.stop();

  return 0;
}

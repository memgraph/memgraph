#pragma once

#include <experimental/filesystem>
#include <set>
namespace fs = std::experimental::filesystem;
#include "database/graph_db_accessor.hpp"
#include "dbms/dbms.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.cpp"
#include "query/engine.hpp"
#include "query/preprocessor.hpp"
#include "stream/print_record_stream.hpp"
#include "utils/command_line/arguments.hpp"
#include "utils/file.hpp"
#include "utils/string/file.hpp"
#include "utils/string/trim.hpp"

namespace tests {
namespace integration {

using namespace utils;
using QueryHashesT = std::set<HashType>;
using QueryEngineT = QueryEngine<PrintRecordStream>;
using StreamT = PrintRecordStream;

/**
 * Init logging for tested query_engine (test specific logger). It has to be
 * sync (easier debugging).
 *
 * @param logger_name the name of a logger
 *
 * @return logger instance
 */
auto init_logging(const std::string &logger_name) {
  logging::init_sync();
  logging::log->pipe(std::make_unique<Stdout>());
  return logging::log->logger(logger_name);
}

/**
 * Get query hashes from the file defied with a path.
 *
 * @param log external logger because this function should be called
 *               from test binaries
 * @param path path to a file with queries
 *
 * @return a set with all query hashes from the file
 */
auto LoadQueryHashes(Logger &log, const fs::path &path) {
  log.info("*** Get query hashes from the file defied with path ***");
  // the intention of following block is to get all hashes
  // for which query implementations have to be compiled
  // calculate all hashes from queries file
  QueryPreprocessor preprocessor;
  // hashes calculated from all queries in queries file
  QueryHashesT query_hashes;
  // fill the above set
  auto queries = utils::read_lines(path);
  for (auto &query : queries) {
    if (query.empty()) continue;
    query_hashes.insert(preprocessor.preprocess(query).hash);
  }
  permanent_assert(query_hashes.size() > 0,
                   "At least one hash has to be present");
  log.info("{} different query hashes exist", query_hashes.size());
  return query_hashes;
}

/**
 * Loads query plans into the engine passed by reference.
 *
 * @param log external logger reference
 * @param engine query engine
 * @param query_hashes hashes for which plans have to be loaded
 * @param path to a folder with query plan implementations
 *
 * @return void
 */
auto LoadQueryPlans(Logger &log, QueryEngineT &engine,
                    const QueryHashesT &query_hashes, const fs::path &path) {
  log.info("*** Load/compile needed query implementations ***");
  QueryPreprocessor preprocessor;
  auto plan_paths = LoadFilePaths(path, "cpp");
  // query mark will be used to extract queries from files (because we want
  // to be independent to a query hash)
  auto comment = std::string("// ");
  auto query_mark = comment + std::string("Query: ");
  for (auto &plan_path : plan_paths) {
    auto lines = read_lines(plan_path);
    // find the line with a query in order
    // be able to place it in the dynamic libs container (base on query
    // hash)
    for (int i = 0; i < (int)lines.size(); ++i) {
      // find query in the line
      auto &line = lines[i];
      auto pos = line.find(query_mark);
      // if query doesn't exist pass
      if (pos == std::string::npos) continue;
      auto query = trim(line.substr(pos + query_mark.size()));
      while (i + 1 < (int)lines.size() &&
             lines[i + 1].find(comment) != std::string::npos) {
        query +=
            lines[i + 1].substr(lines[i + 1].find(comment) + comment.length());
        ++i;
      }
      // load/compile implementations only for the queries which are
      // contained in queries_file
      // it doesn't make sense to compile something which won't be runned
      if (query_hashes.find(preprocessor.preprocess(query).hash) ==
          query_hashes.end())
        continue;
      log.info("Path {} will be loaded.", plan_path.c_str());
      engine.ReloadCustom(query, plan_path);
      break;
    }
  }
}

/**
 * Executa all query plans in file on the path.
 *
 * @param log external logger reference
 * @param engine query engine
 * @param dbms a database to execute queries on
 * @param path path a queries file
 * @param stream used by query plans to output the results
 *
 * @return void
 */
auto ExecuteQueryPlans(Logger &log, QueryEngineT &engine, Dbms &dbms,
                       const fs::path &path, StreamT &stream) {
  log.info("*** Execute the queries from the queries_file ***");
  // execute all queries from queries_file
  auto queries = utils::read_lines(path);
  for (auto &query : queries) {
    if (query.empty()) continue;
    permanent_assert(engine.Loaded(trim(query)),
                     "Implementation wasn't loaded");
    // Create new db_accessor since one query is associated with one
    // transaction.
    auto db_accessor = dbms.active();
    engine.Run(query, db_accessor, stream);
  }
}

/**
 * Warms Up the engine. Loads and executes query plans specified by the program
 * arguments:
 *     -q -> a file with queries
 *     -i -> a folder with query plans
 *
 * @param log external logger reference
 * @param engine query engine
 * @param dbms a database to execute queries on
 * @param stream used by query plans to output the results
 *
 * @return void
 */
auto WarmUpEngine(Logger &log, QueryEngineT &engine, Dbms &dbms,
                  StreamT &stream) {
  // path to a file with queries
  auto queries_file = fs::path(
      GET_ARG("-q", "../data/queries/core/mg_basic_002.txt").get_string());
  // folder with query implementations
  auto implementations_folder =
      fs::path(GET_ARG("-i", "../integration/hardcoded_query").get_string());

  // load all query hashes from queries file
  auto query_hashes = LoadQueryHashes(log, queries_file);

  // load compile all needed query plans
  LoadQueryPlans(log, engine, query_hashes, implementations_folder);

  // execute all loaded query plasn
  ExecuteQueryPlans(log, engine, dbms, queries_file, stream);
}
}
}

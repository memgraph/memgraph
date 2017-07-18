#pragma once

#include <experimental/filesystem>
#include <set>
namespace fs = std::experimental::filesystem;

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "database/dbms.hpp"
#include "database/graph_db_accessor.hpp"
#include "print_record_stream.hpp"
#include "query/engine.hpp"
#include "query/frontend/stripped.hpp"
#include "utils/file.hpp"
#include "utils/string.hpp"

DECLARE_string(q);
DECLARE_string(i);

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
 */
void init_logging(const std::string &logger_name) {
  google::InitGoogleLogging(logger_name.c_str());
}

/**
 * Get query hashes from the file defied with a path.
 *
 * @param path path to a file with queries
 *
 * @return a set with all query hashes from the file
 */
auto LoadQueryHashes(const fs::path &path) {
  DLOG(INFO) << "*** Get query hashes from the file defied with path ***";
  // the intention of following block is to get all hashes
  // for which query implementations have to be compiled
  // calculate all hashes from queries file
  // hashes calculated from all queries in queries file
  QueryHashesT query_hashes;
  // fill the above set
  auto queries = utils::ReadLines(path);
  for (auto &query : queries) {
    if (query.empty()) continue;
    query_hashes.insert(query::StrippedQuery(query).hash());
  }
  permanent_assert(query_hashes.size() > 0,
                   "At least one hash has to be present");
  DLOG(INFO) << fmt::format("{} different query hashes exist",
                            query_hashes.size());
  return query_hashes;
}

/**
 * Loads query plans into the engine passed by reference.
 *
 * @param engine query engine
 * @param query_hashes hashes for which plans have to be loaded
 * @param path to a folder with query plan implementations
 *
 * @return void
 */
auto LoadQueryPlans(QueryEngineT &engine, const QueryHashesT &query_hashes,
                    const fs::path &path) {
  DLOG(INFO) << "*** Load/compile needed query implementations ***";
  auto plan_paths = LoadFilePaths(path, "cpp");
  // query mark will be used to extract queries from files (because we want
  // to be independent to a query hash)
  auto comment = std::string("// ");
  auto query_mark = comment + std::string("Query: ");
  for (auto &plan_path : plan_paths) {
    auto lines = utils::ReadLines(plan_path);
    // find the line with a query in order
    // be able to place it in the dynamic libs container (base on query
    // hash)
    for (int i = 0; i < (int)lines.size(); ++i) {
      // find query in the line
      auto &line = lines[i];
      auto pos = line.find(query_mark);
      // if query doesn't exist pass
      if (pos == std::string::npos) continue;
      auto query = utils::Trim(line.substr(pos + query_mark.size()));
      while (i + 1 < (int)lines.size() &&
             lines[i + 1].find(comment) != std::string::npos) {
        query +=
            lines[i + 1].substr(lines[i + 1].find(comment) + comment.length());
        ++i;
      }
      // load/compile implementations only for the queries which are
      // contained in queries_file
      // it doesn't make sense to compile something which won't be runned
      if (query_hashes.find(query::StrippedQuery(query).hash()) ==
          query_hashes.end())
        continue;
      DLOG(INFO) << fmt::format("Path {} will be loaded.", plan_path.c_str());
      engine.ReloadCustom(query, plan_path);
      break;
    }
  }
}

/**
 * Execute all query plans in file on the path.
 *
 * @param engine query engine
 * @param dbms a database to execute queries on
 * @param path path a queries file
 * @param stream used by query plans to output the results
 *
 * @return void
 */
auto ExecuteQueryPlans(QueryEngineT &engine, Dbms &dbms, const fs::path &path,
                       StreamT &stream) {
  DLOG(INFO) << "*** Execute the queries from the queries_file ***";
  // execute all queries from queries_file
  auto queries = utils::ReadLines(path);
  for (auto &query : queries) {
    if (query.empty()) continue;
    permanent_assert(engine.Loaded(utils::Trim(query)),
                     "Implementation wasn't loaded");
    // Create new db_accessor since one query is associated with one
    // transaction.
    auto db_accessor = dbms.active();
    engine.Run(query, *db_accessor, stream);
  }
}

/**
 * Warms Up the engine. Loads and executes query plans specified by the program
 * arguments:
 *     -q -> a file with queries
 *     -i -> a folder with query plans
 *
 * @param engine query engine
 * @param dbms a database to execute queries on
 * @param stream used by query plans to output the results
 *
 * @return void
 */
auto WarmUpEngine(QueryEngineT &engine, Dbms &dbms, StreamT &stream) {
  // path to a file with queries
  auto queries_file = fs::path(FLAGS_q);
  // folder with query implementations
  auto implementations_folder = fs::path(FLAGS_i);

  // load all query hashes from queries file
  auto query_hashes = LoadQueryHashes(queries_file);

  // load compile all needed query plans
  LoadQueryPlans(engine, query_hashes, implementations_folder);

  // execute all loaded query plasn
  ExecuteQueryPlans(engine, dbms, queries_file, stream);
}
}
}

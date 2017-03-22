#pragma once

#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;

#include "config/config.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db.hpp"
#include "logging/loggable.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/plan_compiler.hpp"
#include "query/plan_interface.hpp"
#include "query/preprocessor.hpp"
#include "query/interpreter.hpp"
#include "utils/dynamic_lib.hpp"

/**
 * Responsible for query execution.
 *
 * Current Query Engine arhitecture:
 * query -> query_stripper -> [plan_generator] -> [plan_compiler] -> execution
 *
 * @tparam Stream the query engine has to be aware of the Stream because Stream
 *         is passed to the dynamic shared library because that is the way how
 *         the results should be returned (more optimal then just return
 *         the whole result set)
 */
template <typename Stream> class QueryEngine : public Loggable {
private:
  using QueryPlanLib = DynamicLib<QueryPlanTrait<Stream>>;

public:
  QueryEngine() : Loggable("QueryEngine") {}

  /**
   * Reloads query plan (plan_path contains compiled query plan).
   * This method just calculates stripped query and offloads everything else
   * to the LoadCpp method.
   *
   * @param query a query for which the plan will be loaded
   * @param plan_path a custom made cpp query plan
   *
   * @return void
   */
  auto ReloadCustom(const std::string &query, const fs::path &plan_path) {
    auto preprocessed = preprocessor.preprocess(query);
    Unload(query);
    LoadCpp(plan_path, preprocessed.hash);
  }

  /**
   * Executes query on the database with the stream. If a query plan is cached
   * (based on query hash) it will be used.
   *
   * @param query a query that is going to be executed
   * @param db database againt the query is going to be executed
   * @param stream the resuts will be send to the stream
   *
   * @return query execution status:
   *             false if query wasn't executed successfully
   *             true if query execution was successfull
   */
  auto Run(const std::string &query, GraphDbAccessor &db_accessor,
           Stream &stream) {

    if (CONFIG_BOOL(config::INTERPRET)) {
      query::Interpret(query, db_accessor, stream);
      return true;
    }

    auto preprocessed = preprocessor.preprocess(query);
    auto plan = LoadCypher(preprocessed);
    auto result = plan->run(db_accessor, preprocessed.arguments, stream);
    if (UNLIKELY(!result)) {
      // info because it might be something like deadlock in which
      // case one thread is stopped and user has try again
      logger.info("Unable to execute query (execution returned false)");
    }
    return result;
  }

  /**
   * Unloads query plan and release the resources (should be automatically).
   *
   * @param query a query for which the query plan will be unloaded.
   *
   * return bool is the plan unloaded
   */
  auto Unload(const std::string &query) {
    return query_plans.access().remove(preprocessor.preprocess(query).hash);
  }

  /**
   * Checks is a plan for the query loaded.
   *
   * @param query for which a plan existance will be checked
   *
   * return bool
   */
  auto Loaded(const std::string &query) {
    auto plans_accessor = query_plans.access();
    return plans_accessor.find(preprocessor.preprocess(query).hash) !=
           plans_accessor.end();
  }

  /**
   * The number of loaded query plans.
   *
   * @return size_t the number of loaded query plans
   */
  auto Size() { // TODO: const once whan ConcurrentMap::Accessor becomes const
    return query_plans.access().size();
  }
  // return query_plans.access().size(); }

private:
  /**
   * Loads query plan eather from hardcoded folder or from the file that is
   * generated in this method.
   *
   * @param stripped a stripped query
   *
   * @return runnable query plan
   */
  auto LoadCypher(const StrippedQuery &stripped) {
    auto plans_accessor = query_plans.access();

    // code is already compiled and loaded, just return runnable
    // instance
    auto query_plan_it = plans_accessor.find(stripped.hash);
    if (query_plan_it != plans_accessor.end())
      return query_plan_it->second->instance();

    // find hardcoded query plan if exists
    auto hardcoded_path = fs::path(CONFIG(config::COMPILE_PATH) + "hardcode/" +
                                   std::to_string(stripped.hash) + ".cpp");
    if (fs::exists(hardcoded_path))
      return LoadCpp(hardcoded_path, stripped.hash);

    // generate query plan
    auto generated_path = fs::path(CONFIG(config::COMPILE_PATH) +
                                   std::to_string(stripped.hash) + ".cpp");

    frontend::opencypher::Parser parser(stripped.query);
    // backend::cpp::Generator(parser.tree(), stripped.query, stripped.hash,
    //                         generated_path);
    return LoadCpp(generated_path, stripped.hash);
  }

  /**
   * Load cpp query plan from a file. Or if plan is already cached from the
   * cache.
   *
   * @param path_cpp a path to query plan
   * @param hash query hash
   *
   * @return runnable query plan
   */
  auto LoadCpp(const fs::path &path_cpp, const HashType hash) {
    auto plans_accessor = query_plans.access();

    // code is already compiled and loaded, just return runnable
    // instance
    auto query_plan_it = plans_accessor.find(hash);
    if (query_plan_it != plans_accessor.end())
      return query_plan_it->second->instance();

    // generate dynamic lib path
    // The timestamp has been added here because dlopen
    // uses path and returns the same handler for the same path
    // and that is a problem because at this point we want brand new
    // dynamic lib. That is the tmp solution. The right solution would be
    // to deal with this problem in DynamicLib
    auto path_so = CONFIG(config::COMPILE_PATH) + std::to_string(hash) + "_" +
                   (std::string)Timestamp::now() + ".so";

    plan_compiler.compile(path_cpp, path_so);

    auto query_plan = std::make_unique<QueryPlanLib>(path_so);
    // TODO: underlying object has to be live during query execution
    //       fix that when Antler will be introduced into the database

    auto query_plan_instance = query_plan->instance(); // because of move
    plans_accessor.insert(hash, std::move(query_plan));

    // return an instance of runnable code (PlanInterface)
    return query_plan_instance;
  }

  QueryPreprocessor preprocessor;
  PlanCompiler plan_compiler;
  ConcurrentMap<HashType, std::unique_ptr<QueryPlanLib>> query_plans;
};

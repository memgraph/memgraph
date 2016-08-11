#pragma once

#include "database/db.hpp"
#include "logging/default.hpp"
#include "program_executor.hpp"
#include "program_loader.hpp"
#include "query_result.hpp"

/*
 * Current arhitecture:
 * query -> code_loader -> query_stripper -> [code_generator]
 * -> [code_compiler] -> code_executor
 */

class QueryEngine
{
public:
    QueryEngine() : logger(logging::log->logger("QueryEngine")) {}

    auto execute(const std::string &query, Db &db,
                 communication::OutputStream &stream)
    {
        try {
            auto program = program_loader.load(query);
            auto result = program_executor.execute(program, db, stream);
            if (UNLIKELY(!result)) {
                // info because it might be something like deadlock in which
                // case one thread is stopped and user has try again
                logger.info(
                    "Unable to execute query (executor returned false)");
            }
            return result;
        } catch (QueryEngineException &e) {
            // in this case something fatal went wrong
            logger.error("QueryEngineException: {}", std::string(e.what()));
            return false;
        }
    }

protected:
    Logger logger;

private:
    ProgramExecutor program_executor;
    ProgramLoader program_loader;
};

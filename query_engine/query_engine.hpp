#pragma once

#include "code_loader.hpp"
#include "code_executor.hpp"
#include "query_result.hpp"

//
// Current arhitecture:
// query -> code_loader -> query_stripper -> [code_generator]
// -> [code_compiler] -> code_executor
//

class QueryEngine
{
public:
    QueryEngine()
    {
    }

    QueryResult* execute(const std::string& query)
    {
        executor.execute(loader.load_code_cpu(query));

        throw std::runtime_error("implement me");
    }

private:
    CodeLoader loader;
    CodeExecutor executor;
};

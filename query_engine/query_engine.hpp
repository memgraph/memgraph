#pragma once

#include <iostream>

#include "query_traverser.hpp"
#include "code_generator.hpp"
#include "code_compiler.hpp"
#include "query_executor.hpp"
#include "query_result.hpp"

using std::cout;
using std::endl;

//
// Current arhitecture:
// query -> traverser -> [generator] -> [compiler] -> executor
//

class QueryEngine
{
public:
    QueryResult execute(const std::string& query)
    {
        cout << "execute: " << query << endl;
        return QueryResult();
    }

private:
    QueryTraverser traverser;
    CodeGenerator generator;
    CodeCompiler compiler;
    QueryExecutor executor;
};

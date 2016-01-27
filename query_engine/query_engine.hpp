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
        traverser.build_tree(query);
        traverser.traverse();
        return QueryResult();
    }

private:
    // TODO: use IoC or something similar
    QueryTraverser traverser;
    CodeGenerator generator;
    CodeCompiler compiler;
    QueryExecutor executor;
};

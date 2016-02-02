#pragma once

#include <iostream>

#include "query_stripper.hpp"
#include "query_traverser.hpp"
#include "code_generator.hpp"
#include "code_compiler.hpp"
#include "query_executor.hpp"
#include "query_result.hpp"
#include "utils/hashing/fnv.hpp"

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
        auto stripped = stripper.strip(query);
        cout << "STRIPPED: " << stripped << endl;
        auto stripped_hash = fnv(stripped);
        cout << "STRIPPED HASH: " << stripped_hash << endl;

        // traverser.build_tree(query);
        // traverser.traverse();
        return QueryResult();
    }

private:
    // TODO: use IoC or something similar
    QueryStripper stripper;
    QueryTraverser traverser;
    CodeGenerator generator;
    CodeCompiler compiler;
    QueryExecutor executor;
};

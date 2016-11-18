#include <iostream>

#include "query/language/cypher/common.hpp"
#include "query/preprocesor.hpp"
#include "utils/command_line/arguments.hpp"
#include "utils/type_discovery.hpp"
#include "utils/variadic/variadic.hpp"

using utils::println;

int main(int argc, char **argv)
{
    // arguments parsing
    auto arguments = all_arguments(argc, argv);

    // query extraction
    auto queries = extract_queries(arguments);

    QueryPreprocessor preprocessor;

    for (auto &query : queries)
    {
        auto preprocessed = preprocessor.preprocess(query);
        println("QUERY: ", query);
        println("STRIPPED QUERY: ", preprocessed.query);
        println("QUERY HASH: ", preprocessed.hash);
        println("PROPERTIES:");
        for (auto property : preprocessed.arguments) {
            println("    ", property);
        }
        println("-----------------------------");
    }

    return 0;
}

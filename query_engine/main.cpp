#include <iostream>

#include "utils/command_line/arguments.hpp"
#include "cypher/common.hpp"
#include "query_engine.hpp"

using std::cout;
using std::endl;

int main(int argc, char** argv)
{   
    // arguments parsing
    auto arguments = all_arguments(argc, argv);

    // query extraction
    auto cypher_query = extract_query(arguments);
    cout << "QUERY: " << cypher_query << endl;

    QueryEngine engine;
    engine.execute(cypher_query);

    return 0;
}

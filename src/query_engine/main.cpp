#include <iostream>

#define DEBUG 1

#include "utils/command_line/arguments.hpp"
#include "cypher/common.hpp"
#include "query_engine.hpp"
#include "utils/time/timer.hpp"

using std::cout;
using std::endl;
using std::cin;

int main(int argc, char** argv)
{   
    // arguments parsing
    auto arguments = all_arguments(argc, argv);

    // query extraction
    auto cypher_query = extract_query(arguments);
    cout << "QUERY: " << cypher_query << endl;
    QueryEngine engine;
    engine.execute(cypher_query);

    // using std::placeholders::_1;
    // auto f = std::bind(&QueryEngine::execute, &engine, _1);

    // cout << std::fixed << timer(f, cypher_query) << endl;

    // double counter = 0;
    // for (int i = 0; i < 1000000; ++i) {
    //     counter += timer(f, cypher_query);
    // }
    // cout << 1000000 / (counter / 1000000000) << "create_transactions per sec" << endl;
    
    // shell
    std::string command;
    cout << "-- Memgraph query engine --" << endl;
    do {
        cout << "> ";
        std::getline(cin, command);
        engine.execute(command);
    } while (command != "quit");
    
    // engine.execute("CREATE (n{id:2}) RETURN n");
    // engine.execute("MATCH (n{id:0}) RETURN n");

    return 0;
}

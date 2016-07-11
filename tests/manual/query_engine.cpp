#include <iostream>

#define DEBUG 1

#include "utils/command_line/arguments.hpp"
#include "cypher/common.hpp"
#include "query_engine/query_engine.hpp"
#include "utils/time/timer.hpp"

using std::cout;
using std::endl;
using std::cin;

int main(void)
{   
    QueryEngine engine;

    std::string command;
    cout << "-- Memgraph query engine --" << endl;
    do {
        cout << "> ";
        std::getline(cin, command);
        engine.execute(command);
    } while (command != "quit");
    
    return 0;
}

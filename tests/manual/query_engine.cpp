#include <iostream>

#define DEBUG 1

#include "utils/command_line/arguments.hpp"
#include "cypher/common.hpp"
#include "query_engine/query_engine.hpp"
#include "utils/time/timer.hpp"
#include "utils/terminate_handler.hpp"

using std::cout;
using std::endl;
using std::cin;

int main(void)
{   
    std::set_terminate(&terminate_handler);

    QueryEngine engine;

    cout << "-- Memgraph query engine --" << endl;

    while (true) {
        // read command
        cout << "> ";
        std::string command;
        std::getline(cin, command);
        if (command == "quit")
            break;
        
        // execute command
        try {
            engine.execute(command);
        } catch (const std::exception& e) {
            cout << e.what() << endl;
        } catch (const QueryEngineException& e) {
            cout << e.what() << endl;
        }
    }
    
    return 0;
}

#include <iostream>

#define DEBUG 1

#include "communication/communication.hpp"
#include "query/language/cypher/common.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "query/engine.hpp"
#include "utils/command_line/arguments.hpp"
#include "utils/terminate_handler.hpp"
#include "utils/time/timer.hpp"

using std::cout;
using std::endl;
using std::cin;

int main(void)
{
    std::set_terminate(&terminate_handler);

    logging::init_sync();
    logging::log->pipe(std::make_unique<Stdout>());

    Db db;
    // TODO: write dummy socket that is going to execute test
    using stream_t = bolt::RecordStream<CoutSocket>;
    CoutSocket socket;
    stream_t stream(socket);
    QueryEngine<stream_t> engine;

    cout << "-- Memgraph query engine --" << endl;

    while (true) {
        // read command
        cout << "> ";
        std::string command;
        std::getline(cin, command);
        if (command == "quit") break;

        // execute command
        try {
            engine.execute(command, db, stream);
        } catch (const std::exception &e) {
            cout << e.what() << endl;
        } catch (const QueryEngineException &e) {
            cout << e.what() << endl;
        }
    }

    return 0;
}

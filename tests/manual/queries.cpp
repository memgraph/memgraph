#include <iostream>

#include "query_engine/hardcode/queries.hpp"

#ifdef BARRIER
#include "barrier/barrier.cpp"
#endif

#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "communication/bolt/v1/serialization/bolt_serializer.hpp"
#include "database/db.hpp"
#include "query_engine/query_stripper.hpp"
#include "storage/edges.cpp"
#include "storage/edges.hpp"
#include "storage/vertices.cpp"
#include "storage/vertices.hpp"

using namespace std;

int main(int argc, char **argv)
{
    logging::init_async();
    logging::log->pipe(std::make_unique<Stdout>());

    Db db;
#ifdef BARRIER
    auto queries = load_queries(barrier::trans(db));
#else
    auto queries = load_queries(db);
#endif

    auto stripper = make_query_stripper(TK_LONG, TK_FLOAT, TK_STR, TK_BOOL);

    vector<string> history;
    string command;
    cout << "-- Memgraph query engine --" << endl;
    do {

        cout << "> ";
        getline(cin, command);
        history.push_back(command);
        auto stripped = stripper.strip(command);

        if (queries.find(stripped.hash) == queries.end()) {
            cout << "unsupported query" << endl;
            continue;
        }

        auto result = queries[stripped.hash](std::move(stripped.arguments));
        cout << "RETURN: " << result << endl;

    } while (command != "quit");

    return 0;
}

#include <iostream>

#include "query_engine/hardcode/queries.hpp"

#include "barrier/barrier.cpp"

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
    Db db;
    auto queries = load_queries(barrier::trans(db));

    // auto arguments = all_arguments(argc, argv);
    // auto input_query = extract_query(arguments);
    auto stripper = make_query_stripper(TK_LONG, TK_FLOAT, TK_STR, TK_BOOL);
    // auto stripped = stripper.strip(input_query);
    //
    // auto time = timer<ms>([&stripped, &queries]() {
    //     for (int i = 0; i < 1000000; ++i) {
    //         queries[stripped.hash](stripped.arguments);
    //     }
    // });
    // std::cout << time << std::endl;

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

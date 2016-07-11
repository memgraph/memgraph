#include <iostream>

#include "query_engine/hardcode/queries.hpp"

using namespace std;

// --
// DESCRIPTION: create account
// FULL:        CREATE (n:ACCOUNT {id: 50, name: "Nikola", country: "Croatia",
// created_at: 14634563}) RETURN n
// STRIPPED:    CREATE(n:ACCOUNT{id:0,name:1,country:2,created_at:3})RETURNn
// HASH:        10597108978382323595
// STATUS:      DONE
// --
// DESCRIPTION: create personnel
// FULL:        CREATE (n:PERSONNEL {id: 23, role: "CTO", created_at: 1235345})
// RETURN n
// STRIPPED:    CREATE(n:PERSONNEL{id:0,role:1,created_at:2})RETURNn
// HASH:        4037885257628527960
// STATUS:      TODO
// --
// DESCRIPTION: create edge between ACCOUNT node and PERSONNEL node (IS type)
// FULL:        MATCH (a:ACCOUNT {id:50}), (p:PERSONNEL {id: 23}) CREATE
// (a)-[:IS]->(p)
// STRIPPED:    MATCH(a:ACCOUNT{id:0}),(p:PERSONNEL{id:1})CREATE(a)-[:IS]->(p)
// HASH:        16888190822925675190
// STATUS:      TODO
// --
// DESCRIPTION: find ACCOUNT node, PERSONNEL node and edge between them
// FULL:        MATCH (a:ACCOUNT {id:50})-[r:IS]->(p:PERSONNEL {id: 23}) RETURN
// a,r,p
// STRIPPED:    MATCH(a:ACCOUNT{id:0})-[r:IS]->(p:PERSONNEL{id:1})RETURNa,r,p
// HASH:        9672752533852902744
// STATUS:      TODO
// --
// DESCRIPTION: create OPPORTUNITY
// FULL:
// STRIPPED:
// HASH:
// STATUS:      TODO
// --
// DESCRIPTION: create PERSONNEL-[:CREATED]->OPPORTUNITY
// FULL:
// STRIPPED:
// HASH:
// STATUS:      TODO
// --
// DESCRIPTION: create COMPANY
// FULL:
// STRIPPED:
// HASH:
// STATUS:      TODO
// --
// DESCRIPTION: create OPPORTUNITY-[:MATCH]->COMPANY
// FULL:
// STRIPPED:
// HASH:
// STATUS:      TODO
// --
// DESCRIPTION: create an edge between two nodes that are found by the ID
// FULL:        MATCH (a {id:0}), (p {id: 1}) CREATE (a)-[r:IS]->(p) RETURN r
// STRIPPED:    MATCH(a{id:0}),(p{id:1})CREATE(a)-[r:IS]->(p)RETURNr
// HASH:        7939106225150551899
// STATUS:      DONE
// --
// DESCRIPTION: fine node by the ID
// FULL:        MATCH (n {id: 0}) RETURN n
// STRIPPED:    MATCH(n{id:0})RETURNn
// HASH:        11198568396549106428
// STATUS:      DONE
// --
// DESCRIPTION: find edge by the ID
// FULL:        MATCH ()-[r]-() WHERE ID(r)=0 RETURN r
// STRIPPED:    MATCH()-[r]-()WHEREID(r)=0RETURNr
// HASH:        8320600413058284114
// STATUS:      DONE
// --
// DESCRIPTION: update node that is found by the ID
// FULL:        MATCH (n: {id: 0}) SET n.name = "TEST100" RETURN n
// STRIPPED:    MATCH(n:{id:0})SETn.name=1RETURNn
// HASH:        6813335159006269041
// STATUS:      DONE
// --
// DESCRIPTION: find shortest path between two nodes specified by ID
// FULL:
// STRIPPED:
// HASH:
// STATUS:      TODO
// --
// DESCRIPTION: find all nodes by label name
// FULL:        MATCH (n:LABEL) RETURN n
// STRIPPPED:   MATCH(n:LABEL)RETURNn
// HASH:        4857652843629217005
// STATUS:      DONE
// --
// TODO: Labels and Types have to be extracted from a query during the
// query compile time

// void cout_properties(const Properties &properties)
// {
//     ConsoleWriter writer;
//     properties.accept(writer);
// }

int main(int argc, char **argv)
{
    Db db;
    auto queries = load_queries(db);

    // auto arguments = all_arguments(argc, argv);
    // auto input_query = extract_query(arguments);
    auto stripper = make_query_stripper(TK_INT, TK_FLOAT, TK_STR, TK_BOOL);
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

        auto result = queries[stripped.hash](stripped.arguments);
        cout << "RETURN: " << result << endl;

    } while (command != "quit");

    return 0;
}

#include <iostream>

#include "database/db.hpp"
#include "query_stripper.hpp"
#include "storage/model/properties/property.hpp"
#include "utils/command_line/arguments.hpp"
#include "cypher/common.hpp"
#include "utils/time/timer.hpp"

// --
// DESCRIPTION: create account
// FULL:        CREATE (n:ACCOUNT {id: 50, name: "Nikola", country: "Croatia", created_at: 14634563}) RETURN n
// STRIPPED:    CREATE(n:ACCOUNT{id:0,name:1,country:2,created_at:3})RETURNn
// HASH:        10597108978382323595
// --
// DESCRIPTION: create personnel
// FULL:        CREATE (n:PERSONNEL {id: 23, role: "CTO", created_at: 1235345}) RETURN n
// STRIPPED:    CREATE(n:PERSONNEL{id:0,role:1,created_at:2})RETURNn
// HASH:        4037885257628527960 
// --
// DESCRIPTION: create edge between ACCOUNT node and PERSONNEL node (IS type)
// FULL:        MATCH (a:ACCOUNT {id:50}), (p:PERSONNEL {id: 23}) CREATE (a)-[:IS]->(p) 
// STRIPPED:    MATCH(a:ACCOUNT{id:0}),(p:PERSONNEL{id:1})CREATE(a)-[:IS]->(p)
// HASH:        16888190822925675190 
// --
// DESCRIPTION: find ACCOUNT node, PERSONNEL node and edge between them
// FULL:        MATCH (a:ACCOUNT {id:50})-[r:IS]->(p:PERSONNEL {id: 23}) RETURN a,r,p 
// STRIPPED:    MATCH(a:ACCOUNT{id:0})-[r:IS]->(p:PERSONNEL{id:1})RETURNa,r,p
// HASH:        9672752533852902744 
// --
// DESCRIPTION: create OPPORTUNITY
// FULL:         
// STRIPPED:   
// HASH:       
// --
// DESCRIPTION: create PERSONNEL-[:CREATED]->OPPORTUNITY
// FULL:         
// STRIPPED:   
// HASH:       
// --
// DESCRIPTION: create COMPANY
// FULL:         
// STRIPPED:   
// HASH:       
// --
// DESCRIPTION: create OPPORTUNITY-[:MATCH]->COMPANY
// FULL:         
// STRIPPED:   
// HASH:       
// --
// DESCRIPTION: create edge between two nodes found by the ID
// DESCRIPTION: fine node by the ID
// DESCRIPTION: find edge by the ID
// DESCRIPTION: find shortest path between two nodes specified by ID

int main(int argc, char** argv)
{
    Db db;
    std::map<uint64_t, std::function<void(const properties_t&)>> queries;

    auto create_account = [&db](const properties_t& args)
    {
        auto& t = db.tx_engine.begin();
        auto vertex_accessor = db.graph.vertices.insert(t);
        vertex_accessor.property(
            "id", args[0]
        );
        vertex_accessor.property(
            "name", args[1]
        );
        vertex_accessor.property(
            "country", args[2]
        );
        vertex_accessor.property(
            "created_at", args[3]
        );
        auto label = db.graph.label_store.find_or_create("ACCOUNT");
        vertex_accessor.add_label(label);
        t.commit();
        // TODO: return the result once the bolt will be implemented
    };

    auto find_node_by_id = [&db](const properties_t& args)
    {
        auto& t = db.tx_engine.begin();
        auto id = static_cast<Int64&>(*args[0]);
        auto vertex_accessor = db.graph.vertices.find(t, Id(id.value));
        t.commit();
        // TODO: return the result once the bolt will be implemented
    };

    queries[10597108978382323595u] = create_account;

    auto arguments = all_arguments(argc, argv);
    auto input_query = extract_query(arguments);

    auto stripper = make_query_stripper(TK_INT, TK_FLOAT, TK_STR, TK_BOOL);
    auto stripped = stripper.strip(input_query);
    
    auto time = timer<ms>([&stripped, &queries]() {
        for (int i = 0; i < 1000000; ++i) {
            queries[stripped.hash](stripped.arguments);
        }
    });
    std::cout << time << std::endl;

    return 0;
}

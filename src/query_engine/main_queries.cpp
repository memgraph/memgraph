#include <iostream>

#include "cypher/common.hpp"
#include "database/db.hpp"
#include "query_stripper.hpp"
#include "storage/model/properties/property.hpp"
#include "storage/model/properties/traversers/consolewriter.hpp"
#include "utils/command_line/arguments.hpp"
#include "utils/time/timer.hpp"

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
// STATUS:      TODO
// --
// TODO: Labels and Types have to be extracted from a query during the
// query compile time

void cout_properties(const Properties &properties)
{
    ConsoleWriter writer;
    properties.accept(writer);
}

int main(int argc, char **argv)
{
    Db db;
    std::map<uint64_t, std::function<bool(const properties_t &)>> queries;

    auto create_labeled_and_named_node = [&db](const properties_t &args) {
        auto &t = db.tx_engine.begin();
        auto vertex_accessor = db.graph.vertices.insert(t);
        vertex_accessor.property("name", args[0]);
        auto &label = db.graph.label_store.find_or_create("LABEL");
        vertex_accessor.add_label(label);
        cout_properties(vertex_accessor.properties());
        t.commit();

        return true;
    };

    auto create_account = [&db](const properties_t &args) {
        auto &t = db.tx_engine.begin();
        auto vertex_accessor = db.graph.vertices.insert(t);
        // this can be a loop
        vertex_accessor.property("id", args[0]);
        vertex_accessor.property("name", args[1]);
        vertex_accessor.property("country", args[2]);
        vertex_accessor.property("created_at", args[3]);
        // here is the problem because LABEL can't be stripped out
        auto &label = db.graph.label_store.find_or_create("ACCOUNT");
        vertex_accessor.add_label(label);
        cout_properties(vertex_accessor.properties());
        t.commit();

        return true;
    };

    auto find_node_by_internal_id = [&db](const properties_t &args) {
        auto &t = db.tx_engine.begin();
        auto id = static_cast<Int32 &>(*args[0]);
        auto vertex_accessor = db.graph.vertices.find(t, Id(id.value));

        if (!vertex_accessor) {
            cout << "vertex doesn't exist" << endl;
            t.commit();
            return false;
        }

        cout_properties(vertex_accessor.properties());

        cout << "LABELS:" << endl;
        for (auto label_ref : vertex_accessor.labels()) {
            cout << label_ref.get() << endl;
        }

        t.commit();

        return true;
    };

    auto create_edge = [&db](const properties_t &args) {
        auto &t = db.tx_engine.begin();

        auto v1 = db.graph.vertices.find(t, args[0]->as<Int32>().value);
        if (!v1) return t.commit(), false;

        auto v2 = db.graph.vertices.find(t, args[1]->as<Int32>().value);
        if (!v2) return t.commit(), false;

        auto e = db.graph.edges.insert(t);

        v1.vlist->update(t)->data.out.add(e.vlist);
        v2.vlist->update(t)->data.in.add(e.vlist);

        e.from(v1.vlist);
        e.to(v2.vlist);

        e.edge_type(EdgeType("IN"));

        t.commit();

        cout << e.edge_type() << endl;
        cout_properties(e.properties());

        return true;
    };

    auto find_edge_by_internal_id = [&db](const properties_t &args) {
        auto &t = db.tx_engine.begin();
        auto e = db.graph.edges.find(t, args[0]->as<Int32>().value);
        if (!e) return t.commit(), false;

        // print edge type and properties
        cout << "EDGE_TYPE: " << e.edge_type() << endl;

        auto from = e.from();
        cout << "FROM:" << endl;
        cout_properties(from->find(t)->data.props);

        auto to = e.to();
        cout << "TO:" << endl;
        cout_properties(to->find(t)->data.props);

        t.commit();

        return true;
    };

    auto update_node = [&db](const properties_t &args) {
        auto &t = db.tx_engine.begin();

        auto v = db.graph.vertices.find(t, args[0]->as<Int32>().value);
        if (!v) return t.commit(), false;

        v.property("name", args[1]);
        cout_properties(v.properties());

        t.commit();

        return true;
    };

    auto find_by_label = [&db](const properties_t &args)
    {
        auto &t = db.tx_engine.begin();
        auto &label = db.graph.label_store.find_or_create("LABEL");
        auto &index_record_collection =
            db.graph.vertices.find_label_index(label);
        auto accessor = index_record_collection.access();
        cout << "VERTICES" << endl;
        for (auto& v : accessor) {
            cout << v.record->data.props.at("name").as<String>().value << endl;
        }
        return true;
    };

    queries[10597108978382323595u] = create_account;
    queries[5397556489557792025u] = create_labeled_and_named_node;
    queries[7939106225150551899u] = create_edge;
    queries[11198568396549106428u] = find_node_by_internal_id;
    queries[8320600413058284114u] = find_edge_by_internal_id;
    queries[6813335159006269041u] = update_node;
    queries[4857652843629217005u] = find_by_label;

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

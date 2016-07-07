#pragma once

#include "database/db.hpp"
#include "query_engine/query_stripper.hpp"
#include "storage/model/properties/property.hpp"
#include "storage/model/properties/traversers/consolewriter.hpp"
#include "utils/command_line/arguments.hpp"

void cout_properties(const Properties &properties)
{
    ConsoleWriter writer;
    properties.accept(writer);
}

auto load_queries(Db& db)
{
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
        vertex_accessor.property("id", args[0]);
        vertex_accessor.property("name", args[1]);
        vertex_accessor.property("country", args[2]);
        vertex_accessor.property("created_at", args[3]);
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
        
        auto &edge_type = db.graph.edge_type_store.find_or_create("IS");
        e.edge_type(edge_type);

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

    return queries;
}

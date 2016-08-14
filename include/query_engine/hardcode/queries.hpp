#pragma once

#include "database/db.hpp"
#include "database/db_accessor.cpp"
#include "database/db_accessor.hpp"
#include "query_engine/query_stripper.hpp"
#include "query_engine/util.hpp"
#include "storage/model/properties/property.hpp"
#include "utils/command_line/arguments.hpp"

auto load_queries(Db &db)
{
    std::map<uint64_t, std::function<bool(const properties_t &)>> queries;

    // CREATE (n {prop: 0}) RETURN n)
    auto create_node = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto vertex_accessor = t.vertex_insert();
        vertex_accessor.property("prop", args[0]);
        t.commit();
        return true;
    };
    queries[11597417457737499503u] = create_node;

    auto create_labeled_and_named_node = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto vertex_accessor = t.vertex_insert();
        vertex_accessor.property("name", args[0]);
        auto &label = t.label_find_or_create("LABEL");
        vertex_accessor.add_label(label);
        cout_properties(vertex_accessor.properties());
        t.commit();
        return true;
    };

    auto create_account = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto vertex_accessor = t.vertex_insert();
        vertex_accessor.property("id", args[0]);
        vertex_accessor.property("name", args[1]);
        vertex_accessor.property("country", args[2]);
        vertex_accessor.property("created_at", args[3]);
        auto &label = t.label_find_or_create("ACCOUNT");
        vertex_accessor.add_label(label);
        cout_properties(vertex_accessor.properties());
        t.commit();
        return true;
    };

    auto find_node_by_internal_id = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto id = static_cast<Int32 &>(*args[0]);
        auto maybe_va = t.vertex_find(Id(id.value));
        if (!option_fill(maybe_va)) {
            cout << "vertex doesn't exist" << endl;
            t.commit();
            return false;
        }
        auto vertex_accessor = maybe_va.get();
        cout_properties(vertex_accessor.properties());
        cout << "LABELS:" << endl;
        for (auto label_ref : vertex_accessor.labels()) {
            cout << label_ref.get() << endl;
        }
        t.commit();
        return true;
    };

    auto create_edge = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto v1 = t.vertex_find(args[0]->as<Int32>().value);
        if (!option_fill(v1)) return t.commit(), false;

        auto v2 = t.vertex_find(args[1]->as<Int32>().value);
        if (!option_fill(v2)) return t.commit(), false;

        auto edge_accessor = t.edge_insert(v1.get(), v2.get());

        auto &edge_type = t.type_find_or_create("IS");
        edge_accessor.edge_type(edge_type);

        t.commit();

        cout << edge_accessor.edge_type() << endl;

        cout_properties(edge_accessor.properties());

        return true;
    };

    auto find_edge_by_internal_id = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto maybe_ea = t.edge_find(args[0]->as<Int32>().value);
        if (!option_fill(maybe_ea)) return t.commit(), false;
        auto edge_accessor = maybe_ea.get();

        // print edge type and properties
        cout << "EDGE_TYPE: " << edge_accessor.edge_type() << endl;

        auto from = edge_accessor.from();
        if (!from.fill()) return t.commit(), false;

        cout << "FROM:" << endl;
        cout_properties(from->data.props);

        auto to = edge_accessor.to();
        if (!to.fill()) return t.commit(), false;

        cout << "TO:" << endl;
        cout_properties(to->data.props);

        t.commit();

        return true;
    };

    auto update_node = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto maybe_v = t.vertex_find(args[0]->as<Int32>().value);
        if (!option_fill(maybe_v)) return t.commit(), false;
        auto v = maybe_v.get();

        v.property("name", args[1]);
        cout_properties(v.properties());

        t.commit();

        return true;
    };

    // MATCH (n1), (n2) WHERE ID(n1)=0 AND ID(n2)=1 CREATE (n1)<-[r:IS {age: 25,
    // weight: 70}]-(n2) RETURN r
    auto create_edge_v2 = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto n1 = t.vertex_find(args[0]->as<Int64>().value);
        if (!option_fill(n1)) return t.commit(), false;
        auto n2 = t.vertex_find(args[1]->as<Int64>().value);
        if (!option_fill(n2)) return t.commit(), false;
        auto r = t.edge_insert(n2.get(), n1.get());
        r.property("age", args[2]);
        r.property("weight", args[3]);
        auto &IS = t.type_find_or_create("IS");
        r.edge_type(IS);

        t.commit();
        return true;
    };
    queries[15648836733456301916u] = create_edge_v2;

    // MATCH (n) RETURN n
    auto match_all_nodes = [&db](const properties_t &args) {
        DbAccessor t(db);

        iter::for_all(t.vertex_access(), [&](auto vertex) {
            if (vertex.fill()) {
                cout_properties(vertex->data.props);
            }
        });

        // TODO
        // db.graph.vertices.filter().all(t, handler);

        t.commit();

        return true;
    };
    queries[15284086425088081497u] = match_all_nodes;

    // MATCH (n:LABEL) RETURN n
    auto find_by_label = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto &label = t.label_find_or_create("LABEL");

        auto &index_record_collection = t.label_find_index(label);
        auto accessor = index_record_collection.access();
        cout << "VERTICES" << endl;
        for (auto &v : accessor) {
            cout << v.record->data.props.at("name").as<String>().value << endl;
        }

        // TODO
        // db.graph.vertices.fileter("LABEL").all(t, handler);

        return true;
    };
    queries[4857652843629217005u] = find_by_label;

    queries[10597108978382323595u] = create_account;
    queries[5397556489557792025u] = create_labeled_and_named_node;
    queries[7939106225150551899u] = create_edge;
    queries[6579425155585886196u] = create_edge;
    queries[11198568396549106428u] = find_node_by_internal_id;
    queries[8320600413058284114u] = find_edge_by_internal_id;
    queries[6813335159006269041u] = update_node;

    return queries;
}

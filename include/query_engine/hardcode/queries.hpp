#pragma once

#include <iostream>
#include <map>

#include "barrier/barrier.hpp"

using namespace std;

namespace barrier
{
auto load_queries(Db &db)
{
    std::map<uint64_t, std::function<bool(const properties_t &)>> queries;

    // CREATE (n {prop: 0}) RETURN n)
    auto create_node = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto prop_key = t.vertex_property_key("prop", args[0]->flags);

        auto vertex_accessor = t.vertex_insert();
        vertex_accessor.set(prop_key, args[0]);
        return t.commit();
    };
    queries[11597417457737499503u] = create_node;

    auto create_labeled_and_named_node = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto prop_key = t.vertex_property_key("name", args[0]->flags);
        auto &label = t.label_find_or_create("LABEL");

        auto vertex_accessor = t.vertex_insert();
        vertex_accessor.set(prop_key, args[0]);
        vertex_accessor.add_label(label);
        // cout_properties(vertex_accessor.properties());
        return t.commit();
    };

    auto create_account = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto prop_id = t.vertex_property_key("id", args[0]->flags);
        auto prop_name = t.vertex_property_key("name", args[1]->flags);
        auto prop_country = t.vertex_property_key("country", args[2]->flags);
        auto prop_created = t.vertex_property_key("created_at", args[3]->flags);
        auto &label = t.label_find_or_create("ACCOUNT");

        auto vertex_accessor = t.vertex_insert();
        vertex_accessor.set(prop_id, args[0]);
        vertex_accessor.set(prop_name, args[1]);
        vertex_accessor.set(prop_country, args[2]);
        vertex_accessor.set(prop_created, args[3]);
        vertex_accessor.add_label(label);
        // cout_properties(vertex_accessor.properties());
        return t.commit();
    };

    auto find_node_by_internal_id = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto maybe_va = t.vertex_find(Id(args[0]->as<Int32>().value));
        if (!option_fill(maybe_va)) {
            cout << "vertex doesn't exist" << endl;
            t.commit();
            return false;
        }
        auto vertex_accessor = maybe_va.get();
        // cout_properties(vertex_accessor.properties());
        cout << "LABELS:" << endl;
        for (auto label_ref : vertex_accessor.labels()) {
            // cout << label_ref.get() << endl;
        }
        return t.commit();
    };

    auto create_edge = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto &edge_type = t.type_find_or_create("IS");

        auto v1 = t.vertex_find(args[0]->as<Int32>().value);
        if (!option_fill(v1)) return t.commit(), false;

        auto v2 = t.vertex_find(args[1]->as<Int32>().value);
        if (!option_fill(v2)) return t.commit(), false;

        auto edge_accessor = t.edge_insert(v1.get(), v2.get());

        edge_accessor.edge_type(edge_type);

        bool ret = t.commit();

        // cout << edge_accessor.edge_type() << endl;

        // cout_properties(edge_accessor.properties());

        return ret;
    };

    auto find_edge_by_internal_id = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto maybe_ea = t.edge_find(args[0]->as<Int32>().value);
        if (!option_fill(maybe_ea)) return t.commit(), false;
        auto edge_accessor = maybe_ea.get();

        // print edge type and properties
        // cout << "EDGE_TYPE: " << edge_accessor.edge_type() << endl;

        auto from = edge_accessor.from();
        if (!from.fill()) return t.commit(), false;

        cout << "FROM:" << endl;
        // cout_properties(from->data.props);

        auto to = edge_accessor.to();
        if (!to.fill()) return t.commit(), false;

        cout << "TO:" << endl;
        // cout_properties(to->data.props);

        return t.commit();
    };

    auto update_node = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto prop_name = t.vertex_property_key("name", args[1]->flags);

        auto maybe_v = t.vertex_find(args[0]->as<Int32>().value);
        if (!option_fill(maybe_v)) return t.commit(), false;
        auto v = maybe_v.get();

        v.set(prop_name, args[1]);
        // cout_properties(v.properties());

        return t.commit();
    };

    // MATCH (n1), (n2) WHERE ID(n1)=0 AND ID(n2)=1 CREATE (n1)<-[r:IS {age: 25,
    // weight: 70}]-(n2) RETURN r
    auto create_edge_v2 = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto prop_age = t.edge_property_key("age", args[2]->flags);
        auto prop_weight = t.edge_property_key("weight", args[3]->flags);

        auto n1 = t.vertex_find(args[0]->as<Int64>().value);
        if (!option_fill(n1)) return t.commit(), false;
        auto n2 = t.vertex_find(args[1]->as<Int64>().value);
        if (!option_fill(n2)) return t.commit(), false;
        auto r = t.edge_insert(n2.get(), n1.get());
        r.set(prop_age, args[2]);
        r.set(prop_weight, args[3]);
        auto &IS = t.type_find_or_create("IS");
        r.edge_type(IS);

        return t.commit();
    };

    // MATCH (n) RETURN n
    auto match_all_nodes = [&db](const properties_t &args) {
        DbAccessor t(db);

        iter::for_all(t.vertex_access(), [&](auto vertex) {
            if (vertex.fill()) {
                cout << vertex.id() << endl;
            }
        });

        return t.commit();
    };

    // MATCH (n:LABEL) RETURN n
    auto match_by_label = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto &label = t.label_find_or_create("LABEL");
        auto prop_key = t.vertex_property_key("name", Flags::String);

        cout << "VERTICES" << endl;
        iter::for_all(label.index().for_range(t),
                      [&](auto a) { cout << a.at(prop_key) << endl; });

        return t.commit();
    };

    // MATCH (n) DELETE n
    auto match_all_delete = [&db](const properties_t &args) {
        DbAccessor t(db);

        iter::for_all(t.vertex_access(), [&](auto a) {
            if (a.fill()) {
                a.remove();
            }
        });

        return t.commit();
    };

    // MATCH (n:LABEL) DELETE n
    auto match_label_delete = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto &label = t.label_find_or_create("LABEL");

        iter::for_all(label.index().for_range(t), [&](auto a) {
            if (a.fill()) {
                a.remove();
            }
        });

        return t.commit();
    };

    // MATCH (n) WHERE ID(n) = id DELETE n
    auto match_id_delete = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto ov = t.vertex_find(args[0]->as<Int64>().value);
        if (!option_fill(ov)) return t.commit(), false;

        auto v = ov.take();
        v.remove();

        return t.commit();
    };

    // MATCH ()-[r]-() WHERE ID(r) = 0 DELETE r
    auto match_edge_id_delete = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto ov = t.edge_find(args[0]->as<Int64>().value);
        if (!option_fill(ov)) return t.commit(), false;

        auto v = ov.take();
        v.remove();

        return t.commit();
    };

    // MATCH ()-[r]-() DELETE r
    auto match_edge_all_delete = [&db](const properties_t &args) {
        DbAccessor t(db);

        iter::for_all(t.edge_access(), [&](auto a) {
            if (a.fill()) {
                a.remove();
            }
        });

        return t.commit();
    };

    // MATCH ()-[r:TYPE]-() DELETE r
    auto match_edge_type_delete = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto &type = t.type_find_or_create("TYPE");

        iter::for_all(type.index().for_range(t), [&](auto a) {
            if (a.fill()) {
                a.remove();
            }
        });

        return t.commit();
    };

    // MATCH ()-[r]-() WHERE ID(r) = 0 DELETE r
    auto = [&db](const properties_t &args) {
        DbAccessor t(db);

        return t.commit();
    };

    // Blueprint:
    // auto  = [&db](const properties_t &args) {
    //     DbAccessor t(db);
    //
    //
    //     return t.commit();
    // };

    queries[15284086425088081497u] = match_all_nodes;
    queries[4857652843629217005u] = match_by_label;
    queries[15648836733456301916u] = create_edge_v2;
    queries[10597108978382323595u] = create_account;
    queries[5397556489557792025u] = create_labeled_and_named_node;
    queries[7939106225150551899u] = create_edge;
    queries[6579425155585886196u] = create_edge;
    queries[11198568396549106428u] = find_node_by_internal_id;
    queries[8320600413058284114u] = find_edge_by_internal_id;
    queries[6813335159006269041u] = update_node;
    queries[10506105811763742758u] = match_all_delete;
    queries[13742779491897528506u] = match_label_delete;
    queries[11349462498691305864u] = match_id_delete;
    queries[6963549500479100885u] = match_edge_id_delete;
    queries[14897166600223619735u] = match_edge_all_delete;
    queries[16888549834923624215u] = match_edge_type_delete;

    return queries;
}
}
